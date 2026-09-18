package searchengine

import (
	"context"
	"math"
	"sort"
	"sync"
)

// BM25 参数（固定常量，保证评分跨进程/重排可复现）。
const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

// corpusStats 是查询时刻从快照实时汇总的语料统计。
// 不持久化任何统计量，因此段合并/重排不会改变评分，满足可复现性。
type corpusStats struct {
	n        int
	avgdl    float64
	fieldAvg map[string]float64 // 字段平均长度
	// 针对本次查询词项的 df
	dfAny   map[string]int            // term -> 含该词的存活文档数（任意字段）
	dfField map[string]map[string]int // term -> field -> 文档数
}

type winnerInfo struct {
	viewID  uint64
	seq     uint64 // 获胜文档自身的提交序号
	deleted bool
}

// snapshot 是某一时刻的一致性只读快照；views 由旧到新排列。
type snapshot struct {
	views []readView
	cache *stateCache // 跨查询共享的不可变缓存（可能为 nil）
}

// stateCache 缓存在同一索引状态下不变的派生量：获胜视图、存活文档与语料长度。
// indexState 不可变，因此其缓存一旦填充即可被该状态上的所有查询安全并发复用。
type stateCache struct {
	once     sync.Once
	winners  map[string]winnerInfo
	live     map[string]storedDocView
	totalLen int64
	fieldLen map[string]int64
	fieldN   map[string]int
	n        int

	dfMu    sync.Mutex
	dfAny   map[string]int
	dfField map[string]map[string]int
}

func newStateCache() *stateCache {
	return &stateCache{}
}

func (c *stateCache) populate(s *snapshot) {
	c.once.Do(func() {
		w, l := s.winners()
		c.winners, c.live = w, l
		c.n = len(l)
		c.dfAny = make(map[string]int)
		c.dfField = make(map[string]map[string]int)
		// 语料长度聚合：对每个视图，只计入“该视图即获胜视图”的存活文档。
		// 无法直接用视图整体语料求和（视图可能含已被更新覆盖的旧版本），
		// 因此按获胜文档所属视图分组统计字段长度——仍只需遍历一次 live。
		c.fieldLen = make(map[string]int64)
		c.fieldN = make(map[string]int)
		for _, d := range l {
			for f, ln := range d.FieldLens {
				c.totalLen += int64(ln)
				c.fieldLen[f] += int64(ln)
				if ln > 0 {
					c.fieldN[f]++
				}
			}
		}
	})
}

func (s *snapshot) baseStats() (map[string]winnerInfo, map[string]storedDocView, *stateCache) {
	c := s.cache
	if c == nil {
		c = newStateCache()
		s.cache = c
	}
	c.populate(s)
	return c.winners, c.live, c
}

// postingIsWinner 判断一条倒排是否属于该文档当前获胜的物理版本。
// 必须按“文档自身的提交序号”比较，而非视图的聚合序号：
// 一个段的 viewSeq 是其全部文档的最大 seq，段内个别文档的 seq 可能更小，
// 仅比较视图序号会让已被更新覆盖的旧倒排错误计入。
func postingIsWinner(v readView, p posting, win winnerInfo) bool {
	st, ok := v.Lookup(p.DocID)
	if !ok || st.Deleted {
		return false
	}
	return st.Seq == win.seq
}

func postingIsWinnerRaw(v readView, docID string, win winnerInfo) bool {
	st, ok := v.Lookup(docID)
	if !ok || st.Deleted {
		return false
	}
	return st.Seq == win.seq
}

// winners 计算每个文档当前获胜的视图（最新包含它的视图）与状态。
func (s *snapshot) winners() (map[string]winnerInfo, map[string]storedDocView) {
	w := make(map[string]winnerInfo)
	live := make(map[string]storedDocView)
	for i := len(s.views) - 1; i >= 0; i-- {
		v := s.views[i]
		for _, id := range v.AllDocIDs() {
			if _, done := w[id]; done {
				continue
			}
			st, _ := v.Lookup(id)
			w[id] = winnerInfo{viewID: v.ID(), seq: st.Seq, deleted: st.Deleted}
			if !st.Deleted {
				if sd, ok := v.Stored(id); ok {
					live[id] = sd
				}
			}
		}
	}
	return w, live
}

func (s *snapshot) liveDocIDs() []string {
	w, _ := s.winners()
	ids := make([]string, 0, len(w))
	for id, info := range w {
		if !info.deleted {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func (s *snapshot) statsFor(queryTerms []string) (*corpusStats, map[string]winnerInfo, map[string]storedDocView) {
	winners, live, c := s.baseStats()
	cs := &corpusStats{
		n:        c.n,
		avgdl:    0,
		fieldAvg: make(map[string]float64),
		dfAny:    make(map[string]int),
		dfField:  make(map[string]map[string]int),
	}
	if c.n > 0 {
		cs.avgdl = float64(c.totalLen) / float64(c.n)
	}
	for f := range c.fieldLen {
		if c.fieldN[f] > 0 {
			cs.fieldAvg[f] = float64(c.fieldLen[f]) / float64(c.fieldN[f])
		}
	}

	// df 只针对查询词项；同一索引状态下结果可缓存复用。
	c.dfMu.Lock()
	missing := false
	for _, term := range queryTerms {
		if _, ok := c.dfAny[term]; !ok {
			missing = true
			break
		}
	}
	if missing {
		for _, term := range queryTerms {
			if _, ok := c.dfAny[term]; ok {
				continue
			}
			seenAny := make(map[string]struct{})
			seenField := make(map[string]map[string]struct{})
			s.forEachTermPosting(term, func(v readView, docID, field string, _ int, _ []int) {
				win, ok := winners[docID]
				if !ok || win.deleted || !postingIsWinnerRaw(v, docID, win) {
					return
				}
				seenAny[docID] = struct{}{}
				m := seenField[field]
				if m == nil {
					m = make(map[string]struct{})
					seenField[field] = m
				}
				m[docID] = struct{}{}
			})
			c.dfAny[term] = len(seenAny)
			fm := make(map[string]int, len(seenField))
			for f, set := range seenField {
				fm[f] = len(set)
			}
			c.dfField[term] = fm
		}
	}
	for _, term := range queryTerms {
		cs.dfAny[term] = c.dfAny[term]
		cs.dfField[term] = c.dfField[term]
	}
	c.dfMu.Unlock()
	return cs, winners, live
}

// collectQueryTerms 收集 AST 中全部词项（去重排序），用于预算 df。
func collectQueryTerms(q QueryNode) []string {
	set := make(map[string]struct{})
	var walk func(QueryNode)
	walk = func(n QueryNode) {
		switch x := n.(type) {
		case *TermQuery:
			set[x.Term] = struct{}{}
		case *PhraseQuery:
			for _, t := range x.Terms {
				set[t] = struct{}{}
			}
		case *AndQuery:
			for _, c := range x.Children {
				walk(c)
			}
		case *OrQuery:
			for _, c := range x.Children {
				walk(c)
			}
		case *NotQuery:
			walk(x.Child)
		}
	}
	walk(q)
	terms := make([]string, 0, len(set))
	for t := range set {
		terms = append(terms, t)
	}
	sort.Strings(terms)
	return terms
}

func bm25(tf, df, dl int, avgdl float64, n int) float64 {
	if tf <= 0 || df <= 0 || dl <= 0 || avgdl <= 0 || n <= 0 {
		return 0
	}
	// Lucene 经典 IDF：log(1 + (N - df + 0.5)/(df + 0.5))。
	// 相比 log((N-df+0.5)/(df+0.5))，它在小语料（如 N=1,df=1）时仍为正，
	// 避免“唯一命中文档得 0 分被过滤”的问题，且保持确定可复现。
	idf := math.Log(1 + (float64(n)-float64(df)+0.5)/(float64(df)+0.5))
	denom := bm25K1*(1-bm25B+bm25B*float64(dl)/avgdl) + float64(tf)
	return idf * (bm25K1 * float64(tf)) / denom
}

// matchTerm 返回 docID -> BM25 分数。
func (s *snapshot) matchTerm(q *TermQuery, cs *corpusStats,
	winners map[string]winnerInfo, live map[string]storedDocView) map[string]float64 {
	// 聚合每个命中文档、每个字段的 tf。
	type agg struct {
		tfByField map[string]int
		tfTotal   int
	}
	aggs := make(map[string]*agg)
	s.forEachTermPosting(q.Term, func(v readView, docID, field string, tf int, _ []int) {
		win, ok := winners[docID]
		if !ok || win.deleted || !postingIsWinnerRaw(v, docID, win) {
			return
		}
		if q.Field != "" && field != q.Field {
			return
		}
		a := aggs[docID]
		if a == nil {
			a = &agg{tfByField: make(map[string]int)}
			aggs[docID] = a
		}
		a.tfByField[field] += tf
		a.tfTotal += tf
	})
	out := make(map[string]float64, len(aggs))
	for docID, a := range aggs {
		d := live[docID]
		var score float64
		if q.Field != "" {
			dl := d.FieldLens[q.Field]
			score = bm25(a.tfByField[q.Field], cs.dfField[q.Term][q.Field], dl, cs.fieldAvg[q.Field], cs.n)
		} else {
			dl := 0
			for _, l := range d.FieldLens {
				dl += l
			}
			score = bm25(a.tfTotal, cs.dfAny[q.Term], dl, cs.avgdl, cs.n)
		}
		if score > 0 {
			out[docID] = score
		}
	}
	return out
}

// matchPhrase 要求短语词项在同一字段内按位置连续；tf 为匹配次数。
func (s *snapshot) matchPhrase(q *PhraseQuery, cs *corpusStats,
	winners map[string]winnerInfo, live map[string]storedDocView) map[string]float64 {
	type fieldPos map[string][]int // field -> positions（首个词项位置）
	firstPos := make(map[string]fieldPos)
	// 仅来自获胜视图的 posting 才有效。
	validPostings := func(term string, fn func(docID, field string, pos []int)) {
		s.forEachTermPosting(term, func(v readView, docID, field string, _ int, pos []int) {
			win, ok := winners[docID]
			if !ok || win.deleted || !postingIsWinnerRaw(v, docID, win) {
				return
			}
			if q.Field != "" && field != q.Field {
				return
			}
			fn(docID, field, pos)
		})
	}
	validPostings(q.Terms[0], func(docID, field string, pos []int) {
		fp := firstPos[docID]
		if fp == nil {
			fp = fieldPos{}
			firstPos[docID] = fp
		}
		fp[field] = append(fp[field], pos...)
	})
	if len(firstPos) == 0 {
		return map[string]float64{}
	}

	// 对后续每个词项，收窄候选位置。
	for ti := 1; ti < len(q.Terms); ti++ {
		nextPos := make(map[string]map[string][]int)
		validPostings(q.Terms[ti], func(docID, field string, pos []int) {
			cand, ok := firstPos[docID]
			if !ok {
				return
			}
			starts, ok := cand[field]
			if !ok {
				return
			}
			poset := make(map[int]struct{}, len(pos))
			for _, p := range pos {
				poset[p] = struct{}{}
			}
			var kept []int
			for _, st := range starts {
				if _, ok := poset[st+ti]; ok {
					kept = append(kept, st)
				}
			}
			if len(kept) > 0 {
				m := nextPos[docID]
				if m == nil {
					m = make(map[string][]int)
					nextPos[docID] = m
				}
				m[field] = kept
			}
		})
		// firstPos 的类型与 nextPos 一致，重建。
		rebuilt := make(map[string]fieldPos, len(nextPos))
		for docID, fm := range nextPos {
			rebuilt[docID] = fieldPos(fm)
		}
		firstPos = rebuilt
		if len(firstPos) == 0 {
			break
		}
	}

	out := make(map[string]float64, len(firstPos))
	for docID, fm := range firstPos {
		tf := 0
		for _, starts := range fm {
			tf += len(starts)
		}
		d := live[docID]
		var score float64
		if q.Field != "" {
			// 短语 df：用 dfField 近似（含该字段首词的文档数偏松），
			// 但评分只用于排序；为严格起见用实际命中文档的 df 在函数后无法得知，
			// 这里使用首词 df（标准做法，可复现）。
			dl := d.FieldLens[q.Field]
			score = bm25(tf, cs.dfField[q.Terms[0]][q.Field], dl, cs.fieldAvg[q.Field], cs.n)
		} else {
			dl := 0
			for _, l := range d.FieldLens {
				dl += l
			}
			score = bm25(tf, cs.dfAny[q.Terms[0]], dl, cs.avgdl, cs.n)
		}
		if score > 0 {
			out[docID] = score
		}
	}
	return out
}

func intersect(a, b map[string]float64) map[string]float64 {
	if len(a) > len(b) {
		a, b = b, a
	}
	out := make(map[string]float64, len(a))
	for k, va := range a {
		if vb, ok := b[k]; ok {
			out[k] = va + vb
		}
	}
	return out
}

func union(a, b map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] += v
	}
	return out
}

func (s *snapshot) matchQuery(q QueryNode, cs *corpusStats,
	winners map[string]winnerInfo, live map[string]storedDocView) map[string]float64 {
	switch x := q.(type) {
	case *TermQuery:
		return s.matchTerm(x, cs, winners, live)
	case *PhraseQuery:
		return s.matchPhrase(x, cs, winners, live)
	case *AndQuery:
		if len(x.Children) == 0 {
			return map[string]float64{}
		}
		cur := s.matchQuery(x.Children[0], cs, winners, live)
		for _, c := range x.Children[1:] {
			cur = intersect(cur, s.matchQuery(c, cs, winners, live))
		}
		return cur
	case *OrQuery:
		cur := map[string]float64{}
		for _, c := range x.Children {
			cur = union(cur, s.matchQuery(c, cs, winners, live))
		}
		return cur
	case *NotQuery:
		matched := s.matchQuery(x.Child, cs, winners, live)
		out := make(map[string]float64)
		for id := range live {
			if _, ok := matched[id]; !ok {
				out[id] = 0
			}
		}
		return out
	default:
		return map[string]float64{}
	}
}

// execute 在快照上执行查询，排序（分数降序、docID 升序作为确定性平局规则）并分页。
func (s *snapshot) execute(ctx context.Context, q QueryNode, offset, limit int, stored bool) (*SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	terms := collectQueryTerms(q)
	cs, winners, live := s.statsFor(terms)
	scores := s.matchQuery(q, cs, winners, live)

	// 出现在 scores 中即为命中；纯 NOT 查询命中文档分数为 0，也应返回。
	ids := make([]string, 0, len(scores))
	for id := range scores {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if scores[ids[i]] != scores[ids[j]] {
			return scores[ids[i]] > scores[ids[j]]
		}
		return ids[i] < ids[j]
	})
	total := len(ids)

	if offset > total {
		offset = total
	}
	end := total
	if limit >= 0 && offset+limit < end {
		end = offset + limit
	}
	page := ids[offset:end]

	hits := make([]SearchHit, len(page))
	for i, id := range page {
		hits[i] = SearchHit{ID: id, Score: scores[id]}
		if stored {
			d := live[id]
			hits[i].Stored = append([]StoredField(nil), d.Stored...)
		}
	}
	return &SearchResult{Hits: hits, Total: total, Offset: offset, Limit: limit}, nil
}

// forEachTermPosting 跨视图零分配遍历词项倒排（类型断言到具体实现，
// 避免在热路径上构造 posting 切片）。
func (s *snapshot) forEachTermPosting(term string, f func(v readView, docID, field string, tf int, pos []int)) {
	for _, v := range s.views {
		switch t := v.(type) {
		case *memData:
			t.forEachTermPostingRaw(term, func(docID, field string, tf int, pos []int) {
				f(v, docID, field, tf, pos)
			})
		case *segment:
			t.forEachTermPostingRaw(term, func(docID, field string, tf int, pos []int) {
				f(v, docID, field, tf, pos)
			})
		default:
			for _, p := range v.Postings(term) {
				f(v, p.DocID, p.Field, p.TF, p.Pos)
			}
		}
	}
}
