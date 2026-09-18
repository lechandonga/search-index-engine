package searchengine

// Field 表示文档的一个命名字段。
// Store=true 时原文随结果返回；Index=true 时参与检索与短语匹配。
type Field struct {
	Name  string
	Value string
	Store bool
	Index bool
}

// Document 是写入索引的最小单元。ID 由调用方提供且全局唯一。
type Document struct {
	ID     string
	Fields []Field
}

// StoredField 是查询结果中返回的已存储字段。
type StoredField struct {
	Name  string
	Value string
}

// SearchHit 单条命中结果。
type SearchHit struct {
	ID     string
	Score  float64
	Stored []StoredField
}

// SearchResult 分页查询结果。
type SearchResult struct {
	Hits       []SearchHit
	Total      int
	Offset     int
	Limit      int
	TookMicros int64
}

// Stats 索引统计快照，供可观测与测试使用。
type Stats struct {
	Documents      int
	Segments       int
	MemDocs        int
	CompactionRuns int64
	IndexBytes     int64
	WALBytes       int64
}
