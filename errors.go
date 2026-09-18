package searchengine

import (
	"errors"
	"fmt"
)

// 可区分的哨兵错误。调用方可使用 errors.Is 判断。
var (
	// ErrQuerySyntax 查询串无法解析（分词器非法输入等）。
	ErrQuerySyntax = errors.New("searchengine: query syntax error")
	// ErrClosed 索引已关闭后继续使用。
	ErrClosed = errors.New("searchengine: index closed")
	// ErrInvalidArgument 参数非法。
	ErrInvalidArgument = errors.New("searchengine: invalid argument")
	// ErrIncompatibleVersion 索引格式版本不被当前代码支持。
	ErrIncompatibleVersion = errors.New("searchengine: incompatible index format version")
	// ErrMissingFile 索引所需文件缺失。
	ErrMissingFile = errors.New("searchengine: index file missing")
	// ErrCorruptData 索引数据截断或校验失败。
	ErrCorruptData = errors.New("searchengine: corrupt index data")
)

// ErrorCode 对错误进行分类，便于程序化处理与文档化。
type ErrorCode int

const (
	CodeUnknown ErrorCode = iota
	CodeQuerySyntax
	CodeIncompatibleVersion
	CodeMissingFile
	CodeCorruptData
	CodeInvalidArgument
	CodeClosed
)

func (c ErrorCode) String() string {
	switch c {
	case CodeQuerySyntax:
		return "query syntax error"
	case CodeIncompatibleVersion:
		return "incompatible version"
	case CodeMissingFile:
		return "missing file"
	case CodeCorruptData:
		return "corrupt data"
	case CodeInvalidArgument:
		return "invalid argument"
	case CodeClosed:
		return "index closed"
	default:
		return "unknown error"
	}
}

// IndexError 携带错误类别与上下文，可通过 errors.As 获取。
type IndexError struct {
	Code ErrorCode
	Op   string // 出错的操作（如 "open segment"）
	Path string // 相关文件路径（如有）
	Err  error  // 被包装的底层错误
}

func (e *IndexError) Error() string {
	s := "searchengine: " + e.Code.String()
	if e.Op != "" {
		s += " [" + e.Op + "]"
	}
	if e.Path != "" {
		s += " " + e.Path
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

func (e *IndexError) Unwrap() error { return e.Err }

func errf(code ErrorCode, op, path string, format string, args ...any) *IndexError {
	base := errSentinel(code)
	return &IndexError{
		Code: code,
		Op:   op,
		Path: path,
		Err:  fmt.Errorf("%w: %s", base, fmt.Sprintf(format, args...)),
	}
}

func errSentinel(code ErrorCode) error {
	switch code {
	case CodeQuerySyntax:
		return ErrQuerySyntax
	case CodeIncompatibleVersion:
		return ErrIncompatibleVersion
	case CodeMissingFile:
		return ErrMissingFile
	case CodeCorruptData:
		return ErrCorruptData
	case CodeInvalidArgument:
		return ErrInvalidArgument
	case CodeClosed:
		return ErrClosed
	default:
		return nil
	}
}
