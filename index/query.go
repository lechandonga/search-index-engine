package index

// Query 是解析后的查询语法树节点。
type Query interface {
	// eval 在给定快照上求值，返回 docID -> 得分。
	eval(s *snapshot) map[uint64]float64
}

// ParseQuery 解析查询串。空实现，后续填充。
func ParseQuery(input string) (Query, error) {
	return nil, ErrQueryParse
}
