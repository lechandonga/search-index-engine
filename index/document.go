package index

// Document 是可被索引的文档。
type Document struct {
	ID     string            // 文档唯一标识，重复提交同一 ID 视为更新
	Fields map[string]string // 字段名 -> 文本内容
}

// Hit 是一条查询结果。
type Hit struct {
	ID    string
	Score float64
}

// SearchResult 是分页后的查询结果。
type SearchResult struct {
	Total int   // 命中总数（不受分页影响）
	Hits  []Hit // 当前页命中，按相关度降序
}
