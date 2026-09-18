package searchengine

import (
	"errors"
	"os"
	"sort"
	"time"
)

// maybeCompact 执行一次后台压缩。
//
// 正确性与并发：
//   - 查询全程不持任何互斥锁：压缩期间旧段继续可读，在途快照持有旧 *segment。
//   - 写入由 commitMu 串行。压缩只在两个极短临界区持有 commitMu：
//     1) 选定段集合并记录提交序号；2) 合并完成后原子替换。
//   - 合并的 CPU/IO 在锁外完成，不阻塞写入与查询。
//   - 若合并期间有新提交，直接在锁内用最新段集合重新合并并重试若干次
//     （再次合并数据已在内存，代价小）；仍冲突则保留旧段延后再做。
//   - 任何情况下都不会丢失并发写入，也不会让旧版本/墓碑复活。
func (x *Index) maybeCompact() {
	x.compact(false)
}

// compactToStable 反复触发压缩直到段数低于阈值或到达尝试上限。
// 用于显式 Flush/关闭前的收敛；静默后台场景仍用单次 maybeCompact。
func (x *Index) compactToStable() {
	for i := 0; i < 32; i++ {
		// 等待在途后台压缩结束，避免 CAS 竞争导致误判“无法收敛”。
		if x.compacting.Load() {
			time.Sleep(time.Millisecond)
			continue
		}
		x.mu.Lock()
		n := len(x.segments)
		frozen := len(x.frozen)
		threshold := x.opts.CompactionSegments
		x.mu.Unlock()
		if n < threshold || frozen > 0 {
			return
		}
		before := n
		x.compact(true)
		x.mu.Lock()
		after := len(x.segments)
		x.mu.Unlock()
		if after >= before {
			time.Sleep(time.Millisecond)
		}
	}
}

func (x *Index) compact(force bool) {
	if x.closed.Load() {
		return
	}
	if !x.compacting.CompareAndSwap(false, true) {
		// 已有压缩在进行；强制模式下等待其结束后由调用方重试。
		return
	}
	defer x.compacting.Store(false)

	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if x.closed.Load() {
			return
		}
		// 1) 选定集合（要求无在途 frozen，避免与 flush 交错）。
		x.commitMu.Lock()
		x.mu.Lock()
		if len(x.frozen) > 0 || (!force && len(x.segments) < x.opts.CompactionSegments) {
			x.mu.Unlock()
			x.commitMu.Unlock()
			return
		}
		chosen := append([]*segment(nil), x.segments...)
		startSeq := x.nextSeq
		x.mu.Unlock()
		x.commitMu.Unlock()

		// 2) 锁外合并。
		merged, _, err := mergeSegments(x.dir, chosen, 0)
		if err != nil {
			return
		}

		// 3) 锁内尝试提交。
		committed, retry := x.commitMerged(chosen, merged, startSeq)
		if committed {
			return
		}
		// 清理本次失败的临时合并文件（commitMerged 在 retry 时已处理）。
		if !retry {
			_ = os.Remove(merged.path)
			return
		}
		// retry：有并发提交，下一轮用最新集合重做。
	}
}

// commitMerged 在 commitMu+mu 下尝试用 merged 原子替换 chosen。
// 返回 (committed, shouldRetry)。
func (x *Index) commitMerged(chosen []*segment, merged *segment, startSeq uint64) (bool, bool) {
	x.commitMu.Lock()
	defer x.commitMu.Unlock()
	x.mu.Lock()

	// 合并期间有新提交或有在途 frozen：本次不能直接提交。
	if x.nextSeq != startSeq || len(x.frozen) != 0 {
		x.mu.Unlock()
		// 丢弃这份基于旧集合的合并结果。
		_ = os.Remove(merged.path)
		return false, true
	}
	if !sameSegmentSet(x.segments, chosen) {
		x.mu.Unlock()
		_ = os.Remove(merged.path)
		return false, false
	}

	oldPaths := make([]string, 0, len(chosen))
	for _, s := range chosen {
		oldPaths = append(oldPaths, s.path)
	}
	x.segments = []*segment{merged}
	st := x.currentState()
	manifest := x.buildManifestLocked(nil)
	x.state.Store(&indexState{
		segments: []*segment{merged},
		frozen:   append([]*frozenMem(nil), st.frozen...),
		mem:      st.mem,
		seq:      st.seq,
		cache:    newStateCache(),
	})
	x.mu.Unlock()

	if err := writeManifest(x.dir, manifest); err != nil {
		// 内存已用合并段；磁盘清单仍指旧段，崩溃恢复为旧集合，数据正确。
		return true, false
	}
	for _, p := range oldPaths {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			continue
		}
	}
	x.compactions.Add(1)
	return true, false
}

func sameSegmentSet(a, b []*segment) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[uint64]int, len(a))
	for _, s := range a {
		set[s.meta.ID]++
	}
	for _, s := range b {
		set[s.meta.ID]--
		if set[s.meta.ID] < 0 {
			return false
		}
	}
	return true
}

// mergeSegments 把若干旧段合并为单个新段：
// 同一文档仅保留 viewSeq 最新的存活版本，墓碑被丢弃（其作用已固化），
// 从而回收重复更新/删除造成的空间增长。
// newID 传 0 时使用参与段中最大视图序号 + 1，保证顺序确定。
func mergeSegments(dir string, segs []*segment, newID uint64) (*segment, segMeta, error) {
	var maxView uint64
	for _, s := range segs {
		if s.viewSeq > maxView {
			maxView = s.viewSeq
		}
	}
	if newID == 0 {
		newID = maxView + 1
	}

	latest := make(map[string]storedDocView)
	latestView := make(map[string]uint64)
	for _, s := range segs {
		for _, did := range s.AllDocIDs() {
			sd, ok := s.Stored(did) // 墓碑返回 false
			if !ok {
				continue
			}
			if vid, exists := latestView[did]; !exists || s.viewSeq > vid {
				latest[did] = sd
				latestView[did] = s.viewSeq
			}
		}
	}

	owner := make(map[string]*segment)
	for did, sd := range latest {
		owner[did] = findOwner(segs, latestView[did], did, sd.Seq)
	}

	md := newMemData(newID)
	docIDs := make([]string, 0, len(latest))
	for did := range latest {
		docIDs = append(docIDs, did)
	}
	sort.Strings(docIDs)
	for _, did := range docIDs {
		md.docs[did] = memDocFromView(latest[did])
	}
	for _, did := range docIDs {
		if s := owner[did]; s != nil {
			addPostingsToMerged(md, s, did)
		}
	}
	// 合并段的段 meta ID 必须在全局唯一：使用其视图序号即可（视图序号全局唯一）。
	return writeSegment(dir, md, newID, 0, 0)
}

func findOwner(segs []*segment, viewSeq uint64, docID string, seq uint64) *segment {
	for _, s := range segs {
		if s.viewSeq != viewSeq {
			continue
		}
		if st, ok := s.Lookup(docID); ok && st.Seq == seq && !st.Deleted {
			return s
		}
	}
	for _, s := range segs {
		if s.viewSeq == viewSeq {
			return s
		}
	}
	return nil
}

func memDocFromView(sd storedDocView) *memDoc {
	length := int64(0)
	for _, l := range sd.FieldLens {
		length += int64(l)
	}
	return &memDoc{
		seq:       sd.Seq,
		stored:    append([]StoredField(nil), sd.Stored...),
		fieldLens: cloneFieldLens(sd.FieldLens),
		length:    length,
	}
}

func cloneFieldLens(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// addPostingsToMerged 把段 s 中文档 docID 的倒排加入合并表（同包可访问内部映射）。
func addPostingsToMerged(md *memData, s *segment, docID string) {
	ord, ok := s.docIndex[docID]
	if !ok {
		return
	}
	for term, list := range s.postings {
		for _, sp := range list {
			if sp.DocOrd != ord {
				continue
			}
			field := s.fields[sp.Field]
			byDoc := md.terms[term]
			if byDoc == nil {
				byDoc = make(map[string][]termPosting)
				md.terms[term] = byDoc
			}
			byDoc[docID] = append(byDoc[docID], termPosting{
				field: field,
				pos:   cloneInts(sp.Pos),
			})
		}
	}
}

// cleanupOrphanWALs 删除目录中未被清单引用的 wal 文件（崩溃残留）。
func cleanupOrphanWALs(dir string, referenced map[string]struct{}) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if len(name) < 4 || name[len(name)-4:] != ".wal" {
			continue
		}
		if _, keep := referenced[name]; keep {
			continue
		}
		_ = os.Remove(dirJoin(dir, name))
	}
}
