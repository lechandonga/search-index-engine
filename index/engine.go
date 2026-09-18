package index

// Engine 是全文检索索引引擎，支持并发读写与持久化。
type Engine struct {
	dir string
}

// Open 打开（或创建）位于 dir 的索引引擎。空实现，后续填充。
func Open(dir string) (*Engine, error) {
	return &Engine{dir: dir}, nil
}

// Add 写入或更新文档；同一 ID 重复提交幂等。
func (e *Engine) Add(doc Document) error { return ErrClosed }

// Delete 删除文档；删除不存在的文档为空操作。
func (e *Engine) Delete(id string) error { return ErrClosed }

// Search 执行查询并返回分页结果。
func (e *Engine) Search(queryStr string, offset, limit int) (*SearchResult, error) {
	return nil, ErrClosed
}

// Compact 后台压缩索引，回收被更新/删除占用的空间。
func (e *Engine) Compact() error { return ErrClosed }

// Close 关闭引擎并落盘未持久化的数据。
func (e *Engine) Close() error { return nil }
