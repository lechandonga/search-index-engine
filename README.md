# search-index-engine

一个可持久化的全文检索索引引擎，以 Go 库形式供项目内检索能力直接使用。
支持增量写入/更新/删除、并发读写、进程崩溃恢复、后台压缩与相关度排序。

> 包导入路径：`github.com/lechandonga/search-index-engine`（根包 `searchengine`）。
> 仅依赖 Go 标准库；Go >= 1.25。

---

## 1. 快速开始

```go
import (
    "context"
    "github.com/lechandonga/search-index-engine"
)

idx, err := searchengine.Open("./data/index", nil) // nil 使用默认 Options
if err != nil { /* 分类错误，见第 5 节 */ }
defer idx.Close()

ctx := context.Background()

// 增量写入 / 更新（相同 ID + 相同字段内容是幂等的，不会重复占空间）
err = idx.Put(ctx, searchengine.Document{
    ID: "doc-1",
    Fields: []searchengine.Field{
        {Name: "title", Value: "The Quick Brown Fox", Store: true, Index: true},
        {Name: "body",  Value: "jumps over the lazy dog", Store: false, Index: true},
        {Name: "tag",   Value: "v1", Store: true, Index: false},
    },
})

// 删除（重复删除、删除不存在的文档均幂等）
_ = idx.Delete(ctx, "doc-1")

// 查询
res, err := idx.Search(ctx, `title:brown AND (fox OR dog) NOT cat`, 0, 20)
for _, hit := range res.Hits {
    // hit.ID, hit.Score, hit.Stored（仅 Store=true 的字段）
}
```

`Put` / `Delete` **成功返回即持久化**：记录已追加并 `fsync` 到 WAL，
随后即使进程被 `kill -9` 或断电，重启后数据仍在。

---

## 2. 查询语言

由递归下降解析器解析，**无法解析的查询返回明确错误**（包装 `ErrQuerySyntax`），
而不是返回空结果或 panic：

| 写法 | 含义 |
| --- | --- |
| `term` | 单个词元（大小写不敏感） |
| `"a phrase"` | 短语：词元需在同一字段内按位置相邻 |
| `field:value` / `field:"a b"` | 限定字段 |
| `a AND b` | 逻辑与（`AND` 可省略，相邻词元默认相与） |
| `a OR b` | 逻辑或 |
| `NOT a` / `-a` | 逻辑非（从全集排除） |
| `( ... )` | 分组 |

* 分词：Unicode 字母/数字连续成词并转小写；**CJK 表意文字/假名/谚文逐字切分**，
  不依赖外部词典，因此结果完全确定、跨平台一致。
* 排序：BM25（固定参数 `k1=1.2, b=0.75`，Lucene 经典 IDF，恒为正）；
  分数相同按文档 ID 升序，保证顺序确定、可复现。
* 分页：`Search(ctx, q, offset, limit)`；`limit=0` 表示不返回命中但仍计算 `Total`。

---

## 3. 索引格式与版本

数据目录结构：

```
LOCK                 进程级排他锁（flock）
MANIFEST             清单：格式版本 + 段集合 + 活动/待回收 WAL（原子写入）
wal_<ts>.wal         预写日志：头魔数 + 帧(魔数/seq/类型/长度/payload/CRC32)
seg_<id>.seg         不可变段（见下）
*.tmp                崩溃残留的临时文件（启动时忽略）
```

**段文件布局**（全部大端，变长整数用 uvarint，记录级 CRC32-C=IEEE）：

```
header  : "SISEG001" + version(u32)
record  : metadata(id, 存活文档数, 墓碑数, viewSeq, 隐藏计数...)
record  : field table（字段名，有序）
record  : tombstones(docID, seq) 列表
record* : documents（docID, seq, 字段长度表, 已存储字段）
record  : term count
record* : term postings（term, 按 docID 排序的字段/词频/位置）
footer  : "SIEND001" + version(u32)
```

**格式版本**：当前 `FormatVersion() == 1`，写入清单与段头/段尾。
* 打开时若文件版本 **高于** 当前代码支持版本 → `ErrIncompatibleVersion`
  （`*IndexError{Code: CodeIncompatibleVersion}`），拒绝猜测读取。
* 未来若引入不兼容变更会提升版本号并在兼容窗口内提供迁移；同版本内向前兼容。

清单项与段写入均采用 **临时文件 → fsync → rename → fsync(目录)**，
因此任意时刻崩溃要么是旧文件、要么是新文件，不会出现半份清单/段。

---

## 4. 持久化、一致性与崩溃恢复

* **WAL + 不可变段 + MVCC**
  * 每次提交先 append+fsync WAL，再原子发布一个新的不可变状态指针。
  * 查询只抓取一次状态指针，整个查询对应**同一个一致快照**：
    写入进行中查询始终可用，绝不会读到写一半的内容，也不会前后矛盾。
  * 内存表写时复制（COW，内层 map 也复制），旧快照无锁安全可读。
* **视图按提交序号（commit seq）排序**，文档以“自身的 seq”判定获胜版本，
  因此跨 flush/压缩后更新覆盖与删除仍严格正确。
* **恢复流程**：读清单 → 校验/加载段 → 按序回放 WAL（`WALs` + `ActiveWAL`）。
  * WAL 帧带全局递增 seq：已被段包含的旧帧按 seq **幂等跳过**，
    因此“段已落盘但旧 WAL 尚未删除”这种崩溃窗口也能安全恢复。
  * WAL 尾部因强杀产生的**半个/损坏帧**会被截断并通过 `CorruptionNotice` 上报，
    其之前的完整帧照常可见。
  * 已确认成功返回的写入必定可见；未完成的写入不会污染索引。

---

## 5. 错误类型（可区分、可程序化处理）

所有结构化错误都是 `*searchengine.IndexError`，并可用 `errors.Is` 匹配哨兵、
`errors.As` 取分类码 `Code`：

| 哨兵 | Code | 触发场景 |
| --- | --- | --- |
| `ErrQuerySyntax` | `CodeQuerySyntax` | 查询无法解析（带字节位置） |
| `ErrInvalidArgument` | `CodeInvalidArgument` | 空文档 ID、空字段名、负 offset/limit |
| `ErrClosed` | `CodeClosed` | Close 后继续使用 |
| `ErrIncompatibleVersion` | `CodeIncompatibleVersion` | 清单/段版本高于当前代码 |
| `ErrMissingFile` | `CodeMissingFile` | 清单引用的段/WAL 文件缺失 |
| `ErrCorruptData` | `CodeCorruptData` | 魔数错、文件截断、记录 CRC 不符 |

默认是**严格模式**：发现损坏段/缺失文件直接返回错误，绝不静默返回错误数据。

`Options{Repair: true, CorruptionNotice: fn}` 时：
* 损坏/缺失的段会被**隔离**，用其余完好数据打开，并对每个问题调用一次 `notice`；
* WAL 尾部半帧始终自动截断并上报（无论是否 Repair）。

---

## 6. 后台压缩与资源控制

* 内存表达到 `FlushAtDocs`（默认 1024）即冻结、轮转 WAL，并在后台异步刷成段；
  刷盘不阻塞查询（冻结段对查询继续可见）。
* 磁盘段数达到 `CompactionSegments`（默认 4）触发后台重写：
  * 合并旧段、丢弃被覆盖的旧版本与墓碑，**重复更新/删除不会让体积无限增长**；
  * 合并的 CPU/IO 在互斥锁外进行，**压缩期间查询不阻塞**；
  * 采用乐观提交（记录起始提交序号，提交时校验）+ 锁内重试，
    有并发写入时宁可放弃本轮稍后重试，**绝不丢失并发写入**。
* 被段完全覆盖的旧 WAL 在清单原子更新后回收；崩溃残留由启动时孤儿清理兜底。
* 每个索引目录有 `LOCK` 排他锁，防止两个进程同时写同一索引。

`Options.SyncWrites` 默认 `true`（逐提交 fsync）。仅在可接受极端情况下丢失
最近写入时才显式置 `false` 以换取吞吐。

---

## 7. 可复现性与性能验证

* 固定文档集合与操作序列下：
  * 段文件**字节确定**（字段/文档/词项全部有序写入）；
  * 评分不持久化任何语料统计，查询时按当前快照实时汇总，
    因此段合并/重排**不会改变评分与排序**。
* 性能对比工具（固定种子 mulberry32，相同参数生成相同语料/操作/查询）：

```bash
# 基础规模
go run ./cmd/perfbench -seed 1 -docs 5000 -scale 1
# 4 倍规模（观察扩展性；输出 build/updates/queries 吞吐、段数、字节数）
go run ./cmd/perfbench -seed 1 -docs 5000 -scale 4
# 保留索引目录以便检查
go run ./cmd/perfbench -seed 1 -docs 20000 -scale 1 -keep -dir ./bench-idx
```

相同参数跨进程、跨目录重复运行，查询命中顺序一致、吞吐可横向比较。
也可使用 Go benchmark / race：

```bash
go test -bench=. -benchmem ./...
CGO_ENABLED=1 go test -race ./...
```

---

## 8. 自动化测试覆盖

| 测试 | 覆盖内容 |
| --- | --- |
| `TestSmokePutSearchDelete` | 词项/短语/布尔/NOT、更新覆盖、删除、幂等、分页 |
| `TestPagination` | 分页无重复无遗漏、平局顺序确定 |
| `TestConcurrentReadWrite` | 多 writer + 多 reader 并发（`-race`），快照一致性、无旧新版本混合 |
| `TestCompactionReclaimsSpace` | 20 轮重复更新/删除后收敛到单段、体积有界 |
| `TestCompactionDuringWrites` | 压缩与写入交替不丢数据、更新版本正确 |
| `TestDeterministicResults` | 两次独立构建结果与评分完全一致 |
| `TestReopenPersistsAfterFlush` / `TestReopenReplaysAfterClose` | 重开持久化与 WAL 回放 |
| `TestSubprocessHardKillRecovery` | 子进程写入确认后 `kill -9`，重启数据仍可见且完整 |
| `TestCorruptSegment` / `TestMissingSegment` | 截断/魔数/CRC、文件缺失的分类错误与 Repair 隔离 |
| `TestIncompatibleVersion` | 版本不兼容明确失败 |
| `TestWALTornTail` | WAL 半帧截断上报且完整帧保留 |
| `TestQuerySyntaxErrors` / `TestInvalidArguments` | 非法查询与参数的可区分错误 |
| `FuzzParseQuery` | 任意查询输入不 panic，非法输入返回 `ErrQuerySyntax` |

运行：`go test ./...`；并发与恢复建议：`CGO_ENABLED=1 go test -race ./...`。

---

## 9. 公开接口（稳定性约定）

```go
type Document struct { ID string; Fields []Field }
type Field    struct { Name, Value string; Store, Index bool }
type Options  struct {
    FlushAtDocs, CompactionSegments int
    Repair bool
    CorruptionNotice func(error)
    SyncWrites bool
}
type SearchResult struct { Hits []SearchHit; Total, Offset, Limit int; TookMicros int64 }
type SearchHit    struct { ID string; Score float64; Stored []StoredField }
type Stats        struct { Documents, Segments, MemDocs int; CompactionRuns int64; IndexBytes, WALBytes int64 }

func Open(dir string, opts *Options) (*Index, error)
func (*Index) Put(ctx, Document) error
func (*Index) Delete(ctx, docID string) error
func (*Index) Search(ctx, query string, offset, limit int) (*SearchResult, error)
func (*Index) Flush(ctx) error
func (*Index) Stats() Stats
func (*Index) Close() error
func ParseQuery(string) (QueryNode, error)
func FormatVersion() uint32
```
