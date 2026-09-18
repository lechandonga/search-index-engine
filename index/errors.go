package index

import "errors"

// 可区分的错误类型，调用方可用 errors.Is 判断。
var (
	// ErrQueryParse 查询串无法解析。
	ErrQueryParse = errors.New("index: query parse error")
	// ErrVersionMismatch 索引格式版本不兼容。
	ErrVersionMismatch = errors.New("index: format version mismatch")
	// ErrCorrupted 索引数据损坏（校验和失败、记录不完整等）。
	ErrCorrupted = errors.New("index: corrupted data")
	// ErrMissingFile 必需的索引文件缺失。
	ErrMissingFile = errors.New("index: missing index file")
	// ErrClosed 引擎已关闭。
	ErrClosed = errors.New("index: engine closed")
	// ErrDocumentNotFound 文档不存在。
	ErrDocumentNotFound = errors.New("index: document not found")
)
