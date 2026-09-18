package searchengine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Index 是可持久化、支持并发与崩溃恢复的全文检索索引。
//
// 一致性模型：
//   - 每次成功提交（Put/Delete 成功返回）对应一个单调递增的序号，
//     并先 append+fsync WAL，再原子发布新的不可变状态指针；
//   - Search 只抓取一次状态指针，因此整个查询对应同一个一致的索引状态，
//     绝不会读到“写了一半”的内容；
//   - 写入在单独的 commit 互斥下串行，查询全程无锁。
type Index struct {
	dir  string
	opts Options

	commitMu sync.Mutex
	state    atomic.Pointer[indexState]

	wakeCh chan struct{}
	wg     sync.WaitGroup

	closeOnce sync.Once
	closed    atomic.Bool
	closeErr  error

	// 以下字段仅在 commitMu 或 flush worker 中访问（通过 state/锁约束）。
	mu         sync.Mutex // 保护下方刷新协调字段
	wal        *walWriter
	walName    string
	walOld     []string // 已轮转、等待 frozen 全部刷盘后回收的旧 WAL
	segments   []*segment
	frozen     []*frozenMem
	nextSeq    uint64
	nextViewID uint64 // 所有只读视图（段/frozen/活动表）共享的单调 id 源（mu 保护）
	memDocs    int

	compactions atomic.Int64
	compacting  atomic.Bool

	lockFile *os.File
}

// indexState 是一次原子发布的不可变快照。
type indexState struct {
	segments []*segment   // 旧 -> 新
	frozen   []*frozenMem // 已冻结、等待/正在刷盘（对查询可见）
	mem      *memData     // 活动内存表（不可变）
	seq      uint64
	cache    *stateCache // 该状态共享的派生缓存（懒填充，sync.Once 安全）
}

// frozenMem 是已冻结、正在异步刷盘的内存段。
type frozenMem struct {
	segID      uint64
	data       *memData
	hiddenDocs int
	hiddenLen  int64
}

// snapshot 组装当前状态的只读视图，并按视图序号（提交时序）从旧到新排序。
// 排序是正确性要求：flush/compact 后段的物理排列不保证等于提交时序。
func (st *indexState) snapshot() *snapshot {
	views := make([]readView, 0, len(st.segments)+len(st.frozen)+1)
	for _, seg := range st.segments {
		views = append(views, seg)
	}
	for _, f := range st.frozen {
		views = append(views, f.data)
	}
	views = append(views, st.mem)
	sort.SliceStable(views, func(i, j int) bool { return views[i].ID() < views[j].ID() })
	return &snapshot{views: views, cache: st.cache}
}

// Open 打开（必要时创建）dir 下的索引并执行崩溃恢复。
func Open(dir string, opts *Options) (*Index, error) {
	o := opts.withDefaults()
	if err := os.MkdirAll(dir, FileMode); err != nil {
		return nil, errf(CodeCorruptData, "open", dir, "%v", err)
	}
	lock, err := acquireLock(dir)
	if err != nil {
		return nil, err
	}

	x := &Index{
		dir:      dir,
		opts:     *o,
		wakeCh:   make(chan struct{}, 1),
		lockFile: lock,
	}

	if err := x.recoverAndLoad(o); err != nil {
		releaseLock(lock)
		return nil, err
	}

	x.wg.Add(1)
	go x.flushLoop()
	return x, nil
}

func (x *Index) recoverAndLoad(o *Options) error {
	notice := o.CorruptionNotice
	if notice == nil {
		notice = func(error) {}
	}

	mf, err := readManifest(x.dir)
	isNew := false
	if err != nil {
		var ie *IndexError
		if errors.As(err, &ie) && ie.Code == CodeMissingFile {
			isNew = true
			mf = &manifest{Version: formatVersion}
		} else {
			return err
		}
	}

	var segs []*segment
	for _, sm := range mf.Segments {
		seg, err := openSegment(x.dir, sm, o.Repair, notice)
		if err != nil {
			var ie *IndexError
			if o.Repair && errors.As(err, &ie) &&
				(ie.Code == CodeCorruptData || ie.Code == CodeMissingFile ||
					ie.Code == CodeIncompatibleVersion) {
				notice(err)
				continue // 隔离坏段，用其余完好数据继续
			}
			return err
		}
		segs = append(segs, seg)
	}

	// 初始活动内存表；WAL 回放进入它。
	mt := newMemtable()
	var maxSeq uint64
	for _, s := range segs {
		for _, id := range s.AllDocIDs() {
			if st, ok := s.Lookup(id); ok && st.Seq > maxSeq {
				maxSeq = st.Seq
			}
		}
	}

	// 统一视图 id 源：所有段/frozen/活动表共享同一单调计数。
	var maxViewID uint64
	for _, seg := range segs {
		if seg.meta.ID > maxViewID {
			maxViewID = seg.meta.ID
		}
	}
	if mf.NextSegID > maxViewID {
		maxViewID = mf.NextSegID
	}

	// 按记录顺序回放所有 WAL（旧 -> 新：WALs + ActiveWAL）。去重由 seq 保证。
	// 每次有效回放都从统一计数器取一个严格递增的视图 id。
	walNames := append([]string(nil), mf.WALs...)
	if mf.ActiveWAL != "" {
		walNames = append(walNames, mf.ActiveWAL)
	}
	if len(walNames) > 0 {
		if err := walReplay(x.dir, walNames, o.Repair, notice, func(e walEntry) error {
			if e.Seq <= maxSeq {
				return nil // 已被某段包含
			}
			maxSeq = e.Seq
			maxViewID++
			switch e.Type {
			case walFrameUpsert:
				mt.put(e.Seq, maxViewID, e.Doc)
			case walFrameDelete:
				mt.delete(e.Seq, maxViewID, e.DocID)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	replayMem := mt.current

	x.mu.Lock()
	x.segments = segs
	x.nextSeq = maxSeq
	x.nextViewID = maxViewID
	x.memDocs = replayMem.count()
	x.mu.Unlock()

	// 打开（或创建）活动 WAL。若回放后内存中有数据，轮转新 WAL，
	// 旧 WAL 在下次成功 flush 后才会被删除。
	walName := fmt.Sprintf("%s%016x.wal", walPrefix, time.Now().UnixNano())
	w, err := openWALWriter(x.dir, walName, o.SyncWrites)
	if err != nil {
		return err
	}
	x.mu.Lock()
	x.wal = w
	x.walName = walName
	x.walOld = walNames // 恢复后这些旧 WAL 暂保留，待内存数据冻结刷盘后随清单回收
	x.mu.Unlock()

	st := &indexState{segments: segs, mem: replayMem, seq: maxSeq, cache: newStateCache()}
	x.state.Store(st)

	// 写一份清单：新 ActiveWAL，旧 WAL 挂在 WALs 中（崩溃后仍可幂等回放）。
	x.mu.Lock()
	newManifest := x.buildManifestLocked(nil)
	x.mu.Unlock()
	if err := writeManifest(x.dir, newManifest); err != nil {
		w.close()
		return err
	}
	// 孤儿 WAL（上次崩溃轮转后残留、未被任何清单引用）可安全删除：
	// 被引用的全部在 walNames 或 walName 中。
	referenced := make(map[string]struct{}, len(walNames)+1)
	for _, n := range walNames {
		referenced[n] = struct{}{}
	}
	referenced[walName] = struct{}{}
	cleanupOrphanWALs(x.dir, referenced)
	if isNew {
		_ = fsyncDir(x.dir)
	}
	return nil
}

// buildManifestLocked 在 mu 保护下根据当前元数据构造清单。
func (x *Index) buildManifestLocked(adjust func(*manifest)) *manifest {
	metas := make([]segMeta, 0, len(x.segments))
	for _, s := range x.segments {
		metas = append(metas, s.meta)
	}
	m := &manifest{
		Version:   formatVersion,
		Segments:  metas,
		ActiveWAL: x.walName,
		WALs:      append([]string(nil), x.walOld...),
		NextSegID: x.nextViewID,
	}
	if adjust != nil {
		adjust(m)
	}
	return m
}

func (x *Index) currentState() *indexState { return x.state.Load() }

// Put 增量写入或更新文档。逐字段内容一致的重复提交是幂等的。
// 成功返回后，该写入已 fsync，进程被强杀也不丢失。
func (x *Index) Put(ctx context.Context, doc Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if doc.ID == "" {
		return errf(CodeInvalidArgument, "put", "", "document id is required")
	}
	for _, f := range doc.Fields {
		if f.Name == "" {
			return errf(CodeInvalidArgument, "put", doc.ID, "field name is required")
		}
	}
	if x.closed.Load() {
		return ErrClosed
	}

	x.commitMu.Lock()
	defer x.commitMu.Unlock()
	if x.closed.Load() {
		return ErrClosed
	}

	// 整个“读当前状态 -> COW 生成新表 -> WAL -> 发布”必须在 mu 下原子完成：
	// 否则 flush 持 mu 完成冻结并发布后，本提交若仍基于冻结前的旧状态发布，
	// 就会丢失冻结边界上的写入（这是必须杜绝的竞态）。
	x.mu.Lock()
	seq := x.nextSeq + 1
	x.nextViewID++
	newID := x.nextViewID
	st := x.currentState()
	mt := &memtable{current: st.mem}
	newMem, _, _ := mt.put(seq, newID, doc)
	if newMem == st.mem {
		// 幂等：内容无变化。不写 WAL、不占序号，保证重复提交不放大。
		x.mu.Unlock()
		return nil
	}

	if err := x.wal.append(walEntry{Seq: seq, Type: walFrameUpsert, Doc: doc}); err != nil {
		x.mu.Unlock()
		return err
	}

	x.nextSeq = seq
	x.memDocs = newMem.count()
	frozen := append([]*frozenMem(nil), x.frozen...)
	segs := append([]*segment(nil), x.segments...)
	x.state.Store(&indexState{segments: segs, frozen: frozen, mem: newMem, seq: seq, cache: newStateCache()})

	needFreeze := newMem.count() >= x.opts.FlushAtDocs
	x.mu.Unlock()
	if needFreeze {
		x.freezeActiveLocked()
		x.signal()
	}
	return nil
}

// Delete 删除文档；重复删除不存在或已删文档均幂等。
func (x *Index) Delete(ctx context.Context, docID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if docID == "" {
		return errf(CodeInvalidArgument, "delete", "", "document id is required")
	}
	if x.closed.Load() {
		return ErrClosed
	}

	x.commitMu.Lock()
	defer x.commitMu.Unlock()
	if x.closed.Load() {
		return ErrClosed
	}

	x.mu.Lock()
	seq := x.nextSeq + 1
	x.nextViewID++
	newID := x.nextViewID
	st := x.currentState()
	mt := &memtable{current: st.mem}
	newMem, _, _, _ := mt.delete(seq, newID, docID)
	if newMem == st.mem {
		// 已删除/不存在：幂等，无写入。
		x.mu.Unlock()
		return nil
	}
	if err := x.wal.append(walEntry{Seq: seq, Type: walFrameDelete, DocID: docID}); err != nil {
		x.mu.Unlock()
		return err
	}
	x.nextSeq = seq
	x.memDocs = newMem.count()
	frozen := append([]*frozenMem(nil), x.frozen...)
	segs := append([]*segment(nil), x.segments...)
	x.state.Store(&indexState{segments: segs, frozen: frozen, mem: newMem, seq: seq, cache: newStateCache()})
	needFreeze := newMem.count() >= x.opts.FlushAtDocs
	x.mu.Unlock()
	if needFreeze {
		x.freezeActiveLocked()
		x.signal()
	}
	return nil
}

// freezeActiveLocked 冻结活动内存表：为其分配新段 ID，换成空活动表，
// 旧表作为 frozen 对查询继续可见，并轮转 WAL。
func (x *Index) freezeActiveLocked() *frozenMem {
	x.mu.Lock()
	st := x.currentState()
	if st.mem.count() == 0 {
		x.mu.Unlock()
		return nil
	}
	oldFrozen := append([]*frozenMem(nil), x.frozen...)
	// 段直接继承 frozen memData 的视图 id：刷盘前后视图顺序不变。
	frozen := &frozenMem{segID: st.mem.id, data: st.mem}
	x.frozen = append(x.frozen, frozen)
	// 新活动表取统一计数器的下一个 id，严格大于 frozen 与所有段。
	if st.mem.id > x.nextViewID {
		x.nextViewID = st.mem.id
	}
	x.nextViewID++
	empty := newMemData(x.nextViewID)

	oldWAL := x.wal
	oldName := x.walName
	newName := fmt.Sprintf("%s%016x.wal", walPrefix, time.Now().UnixNano())
	nw, err := openWALWriter(x.dir, newName, x.opts.SyncWrites)
	if err != nil {
		// WAL 轮转失败：回滚冻结，保留活动表与旧 WAL；下次阈值检查/Flush 重试。
		x.frozen = oldFrozen
		x.mu.Unlock()
		return nil
	}
	x.wal = nw
	x.walName = newName
	x.walOld = appendUnique(x.walOld, oldName)
	x.memDocs = 0

	// 计算 frozen 的隐藏计数：统计所有更早视图（磁盘段 + 先前 frozen）中
	// 被本 frozen 更新覆盖或删除的存活文档。
	earlier := make([]*memData, 0, len(oldFrozen))
	for _, f := range oldFrozen {
		earlier = append(earlier, f.data)
	}
	hiddenDocs, hiddenLen := computeHiddenFor(st.segments, earlier, frozen.data)
	frozen.hiddenDocs = hiddenDocs
	frozen.hiddenLen = hiddenLen

	// 立即发布：查询可看到新的空活动表 + frozen 段（对旧数据的覆盖即时生效）。
	newState := &indexState{
		segments: append([]*segment(nil), st.segments...),
		frozen:   append([]*frozenMem(nil), x.frozen...),
		mem:      empty,
		seq:      st.seq,
	}
	newState.cache = newStateCache()
	x.state.Store(newState)

	// 清单记录：新 ActiveWAL + WALs 中的旧 WAL（含本 frozen 的全部提交）。
	manifest := x.buildManifestLocked(nil)
	x.mu.Unlock()

	// 关闭旧 WAL（数据已 fsync，但暂不删除），再持久化清单。
	// 若在清单持久化前崩溃：旧 WAL 与新 WAL 都会在重启时被扫描/回放，
	// seq 去重保证幂等。
	_ = oldWAL.close()
	if err := writeManifest(x.dir, manifest); err != nil {
		// 清单失败不影响查询；frozen 与 WAL 均完整，下次 flush 会再次写清单。
		return frozen
	}
	return frozen
}

// computeHiddenFor 计算 newData 覆盖了多少更早视图中的存活文档。
func computeHiddenFor(segs []*segment, earlierFrozen []*memData, newData *memData) (int, int64) {
	docs := 0
	var length int64
	// 只关心在新表中出现（更新或删除）的 docID。
	for id := range newData.docs {
		// 找到更早视图里该文档的最新存活版本。
		// earlierFrozen 由旧到新，segs 也是旧到新；优先在 frozen 中找。
		var liveLen int64
		found := false
		for i := len(earlierFrozen) - 1; i >= 0 && !found; i-- {
			if d, ok := earlierFrozen[i].docs[id]; ok && !d.deleted {
				liveLen = d.length
				found = true
			}
		}
		if !found {
			for i := len(segs) - 1; i >= 0 && !found; i-- {
				if sd, ok := segs[i].Stored(id); ok {
					for _, l := range sd.FieldLens {
						liveLen += int64(l)
					}
					found = true
				}
			}
		}
		if found {
			docs++
			length += liveLen
		}
	}
	return docs, length
}

func (x *Index) signal() {
	select {
	case x.wakeCh <- struct{}{}:
	default:
	}
}

// ---- 刷盘 worker：串行 flush + compaction，绝不阻塞查询 ----

func (x *Index) flushLoop() {
	defer x.wg.Done()
	for range x.wakeCh {
		if x.closed.Load() {
			return
		}
		x.flushReady()
		x.maybeCompact()
	}
}

func (x *Index) flushReady() {
	// worker 串行排空：一轮中可能又有新的 frozen 产生，循环到稳定为空，
	// 这样随后的压缩面对的是静默点，更容易一次成功。
	for {
		x.commitMu.Lock()
		x.mu.Lock()
		pending := append([]*frozenMem(nil), x.frozen...)
		x.mu.Unlock()
		x.commitMu.Unlock()
		if len(pending) == 0 {
			return
		}
		for _, f := range pending {
			x.flushOne(f)
		}
	}
}

func (x *Index) flushOne(f *frozenMem) {
	x.mu.Lock()
	present := false
	for _, e := range x.frozen {
		if e == f {
			present = true
			break
		}
	}
	x.mu.Unlock()
	if !present {
		return // 已被压缩或已刷盘
	}

	seg, _, err := writeSegment(x.dir, f.data, f.segID, f.hiddenDocs, f.hiddenLen)
	if err != nil {
		// 刷盘失败：WAL 仍完整，数据不丢；保留 frozen，等待下次唤醒重试。
		return
	}

	x.mu.Lock()
	newFrozen := make([]*frozenMem, 0, len(x.frozen))
	for _, e := range x.frozen {
		if e != f {
			newFrozen = append(newFrozen, e)
		}
	}
	x.frozen = newFrozen
	x.segments = append(x.segments, seg)
	segs := append([]*segment(nil), x.segments...)
	frozens := append([]*frozenMem(nil), x.frozen...)
	// frozen 全部刷完后，清单中 WALs 记录的旧 WAL 已被段覆盖，可回收。
	var removedWALs []string
	if len(x.frozen) == 0 {
		removedWALs = append(removedWALs, x.pruneWALsLocked()...)
	}
	// 状态发布必须在 mu 内完成，与元数据修改原子一致，
	// 否则发布窗口内的并发提交会被我们用过期的 frozen/segs 组装覆盖。
	cur := x.currentState()
	x.state.Store(&indexState{
		segments: segs,
		frozen:   frozens,
		mem:      cur.mem,
		seq:      cur.seq,
		cache:    newStateCache(),
	})
	manifest := x.buildManifestLocked(nil)
	x.mu.Unlock()

	if err := writeManifest(x.dir, manifest); err != nil {
		// 段文件已在磁盘并已发布进内存；清单未更新时崩溃会重放 WAL，幂等安全。
		return
	}
	// 清单已不再引用这些 WAL，删除安全（崩溃残留由启动时孤儿清理兜底）。
	for _, name := range removedWALs {
		_ = os.Remove(filepath.Join(x.dir, name))
	}
}

// pruneWALsLocked 在 mu 下清空已被段覆盖的旧 WAL 列表并返回待删除文件名。
func (x *Index) pruneWALsLocked() []string {
	old := append([]string(nil), x.walOld...)
	x.walOld = nil
	return old
}

// Flush 强制冻结活动内存表并等待其落盘与清单更新。
func (x *Index) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if x.closed.Load() {
		return ErrClosed
	}
	x.commitMu.Lock()
	frozen := x.freezeActiveLocked()
	x.commitMu.Unlock()
	if frozen != nil {
		x.signal()
	}
	// 等待 frozen 清空（刷盘完成）。
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		x.mu.Lock()
		n := len(x.frozen)
		x.mu.Unlock()
		if n == 0 {
			// 刷盘稳定后主动收敛压缩（例如测试/关闭前收敛到单段）。
			x.compactToStable()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// Search 在单个一致性快照上执行查询并分页。
func (x *Index) Search(ctx context.Context, query string, offset, limit int) (*SearchResult, error) {
	if x.closed.Load() {
		return nil, ErrClosed
	}
	if offset < 0 {
		return nil, errf(CodeInvalidArgument, "search", "", "offset must be >= 0")
	}
	if limit < 0 {
		return nil, errf(CodeInvalidArgument, "search", "", "limit must be >= 0")
	}
	q, err := ParseQuery(query)
	if err != nil {
		return nil, err
	}
	st := x.currentState()
	snap := st.snapshot()
	start := time.Now()
	res, err := snap.execute(ctx, q, offset, limit, true)
	if err != nil {
		return nil, err
	}
	res.TookMicros = time.Since(start).Microseconds()
	return res, nil
}

// Stats 返回当前统计快照（用于可观测/测试）。
func (x *Index) Stats() Stats {
	st := x.currentState()
	snap := st.snapshot()
	ids := snap.liveDocIDs()
	var walSize, segSize int64
	x.mu.Lock()
	for _, s := range x.segments {
		if info, err := os.Stat(s.path); err == nil {
			segSize += info.Size()
		}
	}
	if x.wal != nil {
		if info, err := os.Stat(x.wal.path); err == nil {
			walSize += info.Size()
		}
	}
	x.mu.Unlock()
	entries, _ := os.ReadDir(x.dir)
	for _, e := range entries {
		name := e.Name()
		if filepath.Ext(name) == ".wal" {
			if info, err := e.Info(); err == nil {
				walSize += info.Size()
			}
		}
	}
	return Stats{
		Documents:      len(ids),
		Segments:       len(st.segments),
		MemDocs:        st.mem.count() + sumFrozen(st.frozen),
		CompactionRuns: x.compactions.Load(),
		IndexBytes:     segSize,
		WALBytes:       walSize,
	}
}

func sumFrozen(fs []*frozenMem) int {
	n := 0
	for _, f := range fs {
		n += f.data.count()
	}
	return n
}

// Close 刷盘待写数据、停止 worker 并释放文件锁。
func (x *Index) Close() error {
	x.closeOnce.Do(func() {
		// 先冻结并刷尽。
		_ = x.Flush(context.Background())
		x.closed.Store(true)
		close(x.wakeCh)
		x.wg.Wait()

		x.mu.Lock()
		if x.wal != nil {
			x.closeErr = x.wal.close()
		}
		lock := x.lockFile
		x.mu.Unlock()
		if lock != nil {
			if err := releaseLock(lock); err != nil && x.closeErr == nil {
				x.closeErr = err
			}
		}
	})
	return x.closeErr
}

func appendUnique(list []string, v string) []string {
	for _, e := range list {
		if e == v {
			return list
		}
	}
	return append(list, v)
}
