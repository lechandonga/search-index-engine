package searchengine

import "sort"

// docState 是某视图下文档的最新状态。
type docState struct {
	Seq     uint64
	Deleted bool
}

// posting 是跨视图统一的倒排记录。
type posting struct {
	DocID string
	Field string
	TF    int
	Pos   []int
}

// storedDocView 是查询侧使用的已存储文档视图。
type storedDocView struct {
	ID        string
	Seq       uint64
	Stored    []StoredField
	FieldLens map[string]int
}

// readView 是内存态与磁盘段共同实现的只读视图。
// 所有实现都是不可变的，查询可以无锁长期持有。
type readView interface {
	// ID 单调递增，越小越旧。
	ID() uint64
	// Lookup 返回该视图内文档状态。
	Lookup(docID string) (docState, bool)
	// AllDocIDs 以确定顺序（升序）返回该视图内所有文档（含墓碑）。
	AllDocIDs() []string
	// Postings 返回该视图内包含 term 的所有文档倒排（顺序不限，求值层排序）。
	Postings(term string) []posting
	// Stored 返回已存储文档（墓碑文档返回 false）。
	Stored(docID string) (storedDocView, bool)
	// HiddenCounts 统计在该视图中被更新/删除所隐藏的旧文档：
	// 返回 (文档数, 被隐藏旧版本的字段长度合计)。
	HiddenCounts() (int, int64)
}

// termPosting 内存倒排记录。
type termPosting struct {
	field string
	pos   []int
}

// memDoc 内存表中的文档。
type memDoc struct {
	seq       uint64
	deleted   bool
	stored    []StoredField
	fieldLens map[string]int
	length    int64
}

// memData 是不可变的内存表快照（提交时写时复制）。
type memData struct {
	id        uint64 // 仅用于内部标识/调试，不参与视图新旧判定
	commitSeq uint64 // 该快照所包含的最大提交序号；决定视图新旧
	docs      map[string]*memDoc
	terms     map[string]map[string][]termPosting // term -> docID -> 每字段一条

	// 存活语料聚合（随 COW 增量维护），用于 O(视图数) 计算语料统计。
	liveN    int
	liveLen  int64
	fieldLen map[string]int64
	fieldN   map[string]int
}

func newMemData(id uint64) *memData {
	return &memData{
		id:       id,
		docs:     make(map[string]*memDoc),
		terms:    make(map[string]map[string][]termPosting),
		fieldLen: make(map[string]int64),
		fieldN:   make(map[string]int),
	}
}

func (m *memData) count() int { return len(m.docs) }

func (m *memData) ID() uint64 { return m.commitSeq }

func (m *memData) Lookup(docID string) (docState, bool) {
	d, ok := m.docs[docID]
	if !ok {
		return docState{}, false
	}
	return docState{Seq: d.seq, Deleted: d.deleted}, true
}

func (m *memData) AllDocIDs() []string {
	ids := make([]string, 0, len(m.docs))
	for id := range m.docs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (m *memData) Postings(term string) []posting {
	byDoc := m.terms[term]
	if len(byDoc) == 0 {
		return nil
	}
	out := make([]posting, 0, len(byDoc))
	for docID, tps := range byDoc {
		for _, tp := range tps {
			out = append(out, posting{
				DocID: docID,
				Field: tp.field,
				TF:    len(tp.pos),
				Pos:   tp.pos,
			})
		}
	}
	return out
}

func (m *memData) Stored(docID string) (storedDocView, bool) {
	d, ok := m.docs[docID]
	if !ok || d.deleted {
		return storedDocView{}, false
	}
	return storedDocView{
		ID:        docID,
		Seq:       d.seq,
		Stored:    d.stored,
		FieldLens: d.fieldLens,
	}, true
}

func (m *memData) HiddenCounts() (int, int64) { return 0, 0 }

// forEachTermPostingRaw 零分配遍历 memData 中某词项的原始倒排。
func (m *memData) forEachTermPostingRaw(term string, f func(docID, field string, tf int, pos []int)) {
	for docID, tps := range m.terms[term] {
		for _, tp := range tps {
			f(docID, tp.field, len(tp.pos), tp.pos)
		}
	}
}

// ---- 可变包装：仅在持有 commitMu 时使用 ----

// memtable 是可变内存表：每次提交基于上一份不可变 memData 做浅拷贝，
// 产生新的不可变快照后原子发布，旧快照继续服务查询。
type memtable struct {
	current    *memData
	hiddenDocs int
	hiddenLen  int64
}

func newMemtable() *memtable {
	return &memtable{current: newMemData(0)}
}

// cloneForWrite 以当前快照为基础做浅拷贝（仅拷贝被修改的 map）。
// newID 必须由引擎的全局单调源分配，避免并发 freeze 与提交产生相同视图 id。
func (t *memtable) cloneForWrite(newID uint64) *memData {
	old := t.current
	nm := &memData{
		id:       newID,
		docs:     make(map[string]*memDoc, len(old.docs)+1),
		terms:    make(map[string]map[string][]termPosting, len(old.terms)+4),
		liveN:    old.liveN,
		liveLen:  old.liveLen,
		fieldLen: cloneI64Map(old.fieldLen),
		fieldN:   cloneIntMap(old.fieldN),
	}
	for k, v := range old.docs {
		nm.docs[k] = v
	}
	for k, v := range old.terms {
		nm.terms[k] = v // 内层 map 保持共享；仅被修改的词项在 put 中单独 COW
	}
	return nm
}

// put 写入/更新文档。
// 返回新快照；hiddenDelta 为本次操作隐藏掉的旧存活版本长度（幂等时为 0）。
// 若文档与现存版本逐字段一致，则为幂等提交：不产生新数据，返回当前快照。
func (t *memtable) put(seq, newID uint64, d Document) (*memData, int, int64) {
	nm := t.cloneForWrite(newID)

	fieldTokens, fieldLens := analyzeDocument(d)
	totalLen := int64(0)
	for _, l := range fieldLens {
		totalLen += int64(l)
	}
	var stored []StoredField
	for _, f := range d.Fields {
		if f.Store {
			stored = append(stored, StoredField{Name: f.Name, Value: f.Value})
		}
	}

	hiddenDelta := int64(0)
	hiddenDocs := 0
	if old, ok := nm.docs[d.ID]; ok && !old.deleted && docsEqual(old, stored, fieldLens, totalLen) {
		// 幂等：内容完全一致，直接复用旧快照，避免重复写入放大。
		return t.current, 0, 0
	} else if ok && !old.deleted {
		// 更新：旧存活版本被隐藏，从语料聚合中减去。
		hiddenDelta = old.length
		hiddenDocs = 1
		subtractCorpus(nm, old)
	}
	// 加入新版本的语料聚合。
	addCorpus(nm, fieldLens)

	// 重建该文档的倒排：先删除旧 term 中该文档的条目。
	t.removeDocTerms(nm, d.ID)

	// 字段按名排序处理，保证同一份文档恒产生相同的内部结构（可复现）。
	fieldNames := make([]string, 0, len(fieldTokens))
	for field := range fieldTokens {
		fieldNames = append(fieldNames, field)
	}
	sort.Strings(fieldNames)
	for _, field := range fieldNames {
		toks := fieldTokens[field]
		// 同字段内按 term 聚合位置。
		positions := make(map[string][]int)
		for _, tk := range toks {
			positions[tk.Term] = append(positions[tk.Term], tk.Position)
		}
		for _, term := range sortedKeys(positions) {
			pos := positions[term]
			oldByDoc := nm.terms[term]
			// COW：内层 map 也必须复制，旧快照可能正被查询无锁读取。
			byDoc := make(map[string][]termPosting, len(oldByDoc)+1)
			for k, v := range oldByDoc {
				byDoc[k] = v
			}
			nm.terms[term] = byDoc
			byDoc[d.ID] = []termPosting{{field: field, pos: append([]int(nil), pos...)}}
		}
	}

	nm.docs[d.ID] = &memDoc{
		seq:       seq,
		deleted:   false,
		stored:    stored,
		fieldLens: fieldLens,
		length:    totalLen,
	}
	nm.commitSeq = seq
	t.current = nm
	t.hiddenDocs += hiddenDocs
	t.hiddenLen += hiddenDelta
	return nm, hiddenDocs, hiddenDelta
}

// delete 标记墓碑。alreadyKnown=true 表示视图中已存在该文档（墓碑或存活）。
// 返回新快照、以及隐藏增量。
func (t *memtable) delete(seq, newID uint64, docID string) (*memData, bool, int, int64) {
	nm := t.cloneForWrite(newID)
	old, existed := nm.docs[docID]

	hiddenDocs := 0
	var hiddenDelta int64
	if existed && old.deleted {
		// 重复删除：幂等，无变化。
		return t.current, true, 0, 0
	}
	if existed {
		hiddenDocs = 1
		hiddenDelta = old.length
		subtractCorpus(nm, old)
	}

	t.removeDocTerms(nm, docID)
	nm.docs[docID] = &memDoc{seq: seq, deleted: true}
	nm.commitSeq = seq
	t.current = nm
	t.hiddenDocs += hiddenDocs
	t.hiddenLen += hiddenDelta
	return nm, existed, hiddenDocs, hiddenDelta
}

// removeDocTerms 从新拷贝的 terms 结构中移除属于 docID 的倒排项。
// 它只修改新 memData（map 为浅拷贝），需要对命中的内层 map 也做拷贝，
// 以免污染旧快照。
func (t *memtable) removeDocTerms(nm *memData, docID string) {
	if _, ok := nm.docs[docID]; !ok {
		return
	}
	for term, byDoc := range nm.terms {
		if _, has := byDoc[docID]; !has {
			continue
		}
		newByDoc := make(map[string][]termPosting, len(byDoc))
		for k, v := range byDoc {
			if k != docID {
				newByDoc[k] = v
			}
		}
		if len(newByDoc) == 0 {
			delete(nm.terms, term)
		} else {
			nm.terms[term] = newByDoc
		}
	}
}

// hiddenSnapshot 返回当前累计隐藏统计（供冻结时携带）。
func (t *memtable) hiddenSnapshot() (int, int64) { return t.hiddenDocs, t.hiddenLen }

func docsEqual(old *memDoc, stored []StoredField, fieldLens map[string]int, length int64) bool {
	if old.length != length {
		return false
	}
	if !sameFieldLens(old.fieldLens, fieldLens) {
		return false
	}
	return sameStored(old.stored, stored)
}

func sameFieldLens(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func sameStored(a, b []StoredField) bool {
	if len(a) != len(b) {
		return false
	}
	// 调用方按字段顺序构造；直接顺序比较。
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// corpusAgg 返回该视图的存活语料聚合，供语料统计 O(视图数) 求和。
func (m *memData) corpusAgg() (n int, totalLen int64, fieldLen map[string]int64, fieldN map[string]int) {
	return m.liveN, m.liveLen, m.fieldLen, m.fieldN
}

func addCorpus(m *memData, fieldLens map[string]int) {
	m.liveN++
	for f, l := range fieldLens {
		m.liveLen += int64(l)
		m.fieldLen[f] += int64(l)
		if l > 0 {
			m.fieldN[f]++
		}
	}
}

func subtractCorpus(m *memData, d *memDoc) {
	m.liveN--
	for f, l := range d.fieldLens {
		m.liveLen -= int64(l)
		m.fieldLen[f] -= int64(l)
		if l > 0 {
			m.fieldN[f]--
			if m.fieldN[f] <= 0 {
				delete(m.fieldN, f)
			}
		}
	}
}
