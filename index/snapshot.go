package index

// posting 记录某词项在一篇文档中的一次索引信息。
type posting struct {
	docID     uint64
	freq      int    // 词频
	positions []int  // 出现位置（用于短语匹配）
	field     string // 所属字段
}

// docMeta 是文档的元数据。
type docMeta struct {
	externalID string
	length     int // 文档总词数（用于 BM25）
	deleted    bool
}

// snapshot 是某一时刻索引的一致性只读视图。
// 写操作通过拷贝-修改生成新快照并原子替换，保证查询看到一致状态。
type snapshot struct {
	postings map[string][]posting // term -> 倒排列表（按 docID 升序）
	docs     map[uint64]*docMeta
	idByExt  map[string]uint64 // 外部 ID -> 内部 ID
	avgLen   float64
}

func newSnapshot() *snapshot {
	return &snapshot{
		postings: make(map[string][]posting),
		docs:     make(map[uint64]*docMeta),
		idByExt:  make(map[string]uint64),
	}
}
