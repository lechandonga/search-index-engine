package searchengine

import "os"

const (
	defaultFlushAtDocs    = 1024
	defaultCompactionSegs = 4
	// formatVersion 是当前代码写入的索引格式版本。
	// 读取时若发现更高主版本则返回 ErrIncompatibleVersion。
	formatVersion uint32 = 1
)

// Options 控制引擎行为。零值（经 Open 填充默认值）即可使用。
type Options struct {
	// FlushAtDocs 活动内存表文档数达到该值时冻结并刷盘。<=0 使用默认值。
	FlushAtDocs int
	// CompactionSegments 磁盘段数达到该值时触发后台压缩。<=0 使用默认值。
	CompactionSegments int
	// Repair 为 true 时，启动遇到损坏/截断的段文件会隔离该段，
	// 用其余完好数据打开并通过 CorruptionNotice 上报；
	// 为 false（默认）时返回明确的分类错误，避免静默丢数据。
	Repair bool
	// CorruptionNotice 在发现并处理损坏/截断时被调用（可能为 nil）。
	// 即使 Repair=false，WAL 尾部因上次强杀产生的半帧也会被截断并在此上报。
	CorruptionNotice func(error)
	// SyncWrites 为 true 时每个成功提交都 fsync WAL（默认 true）。
	SyncWrites bool
}

func (o *Options) withDefaults() *Options {
	cp := &Options{}
	if o != nil {
		*cp = *o
	}
	if cp.FlushAtDocs <= 0 {
		cp.FlushAtDocs = defaultFlushAtDocs
	}
	if cp.CompactionSegments <= 1 {
		cp.CompactionSegments = defaultCompactionSegs
	}
	// SyncWrites 默认为 true：显式置 false 才关闭 fsync。
	cp.SyncWrites = o == nil || o.SyncWrites
	return cp
}

// FormatVersion 返回当前写入的索引格式版本。
func FormatVersion() uint32 { return formatVersion }

// FileMode 是索引目录默认权限。
const FileMode os.FileMode = 0o755
