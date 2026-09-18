// Command perfbench 用固定种子与固定操作序列构建索引，输出可复现的
// 增量更新与查询耗时，便于对比不同规模/版本下的性能表现。
//
// 用法：
//
//	go run ./cmd/perfbench -seed 1 -docs 20000 -scale 1
//	go run ./cmd/perfbench -seed 1 -docs 20000 -scale 4
//
// 两次运行（含跨进程、跨目录）在相同参数下应得到相同的查询命中文档顺序，
// 以及可横向比较的耗时数字。工具默认把索引建在临时目录并在结束时报告大小，
// 用 -dir 指定目录可在运行后保留索引用于检查。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	searchengine "github.com/lechandonga/search-index-engine"
)

// mulberry32 是确定性的 32 位 PRNG；相同种子产生相同序列，跨平台一致。
type rng struct{ s uint32 }

func newRNG(seed uint32) *rng { return &rng{s: seed} }

func (r *rng) next() uint32 {
	r.s += 0x6D2B79F5
	t := r.s
	t = uint32(int32(t)^int32(t>>15)) * uint32(int32(t)|1)
	t ^= t + uint32(int32(t)^int32(t>>7))*uint32(int32(t)|61)
	return (t ^ (t >> 14)) >> 0
}

func (r *rng) intn(n int) int { return int(r.next() % uint32(n)) }

var words = []string{
	"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf",
	"hotel", "india", "juliet", "kilo", "lima", "mike", "november",
	"oscar", "papa", "quebec", "romeo", "sierra", "tango", "uniform",
}

var fields = []string{"title", "body", "tags"}

func genDoc(r *rng, id int) searchengine.Document {
	n := 20 + r.intn(40)
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(words[r.intn(len(words))])
	}
	title := words[r.intn(len(words))] + " " + words[r.intn(len(words))]
	tags := words[r.intn(len(words))]
	return searchengine.Document{
		ID: fmt.Sprintf("doc-%08d", id),
		Fields: []searchengine.Field{
			{Name: "title", Value: title, Store: true, Index: true},
			{Name: "body", Value: b.String(), Store: false, Index: true},
			{Name: "tags", Value: tags, Store: true, Index: true},
		},
	}
}

func main() {
	seed := flag.Int64("seed", 1, "固定随机种子（相同参数 => 相同语料与操作序列）")
	docs := flag.Int("docs", 10000, "基础文档数量")
	scale := flag.Int("scale", 1, "规模倍率：文档数 = docs*scale（用于扩展性对比）")
	flushAt := flag.Int("flush-at", 1024, "内存表刷盘阈值")
	keep := flag.Bool("keep", false, "保留索引目录（默认用临时目录并清理）")
	dirFlag := flag.String("dir", "", "指定索引目录（为空则使用临时目录）")
	flag.Parse()

	n := *docs * *scale
	dir := *dirFlag
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "perfbench-")
		if err != nil {
			fail(err)
		}
		if !*keep {
			defer os.RemoveAll(dir)
		}
	}
	fmt.Printf("perfbench seed=%d docs=%d scale=%d flushAt=%d dir=%s formatVersion=%d\n",
		*seed, n, *scale, *flushAt, dir, searchengine.FormatVersion())

	opts := &searchengine.Options{FlushAtDocs: *flushAt, CompactionSegments: 4}
	idx, err := searchengine.Open(dir, opts)
	if err != nil {
		fail(err)
	}
	ctx := context.Background()
	r := newRNG(uint32(*seed))

	// 1) 构建（首次写入）。
	buildStart := time.Now()
	for i := 0; i < n; i++ {
		if err := idx.Put(ctx, genDoc(r, i)); err != nil {
			fail(err)
		}
	}
	buildDur := time.Since(buildStart)

	// 2) 增量更新：对 10% 的文档用相同种子序列再次写入新版本。
	updStart := time.Now()
	nUpdate := n / 10
	for i := 0; i < nUpdate; i++ {
		id := r.intn(n)
		if err := idx.Put(ctx, genDoc(r, id)); err != nil {
			fail(err)
		}
	}
	updDur := time.Since(updStart)

	if err := idx.Flush(ctx); err != nil {
		fail(err)
	}

	// 3) 固定查询集合（来自相同种子，跨运行一致）。
	qr := newRNG(uint32(*seed) ^ 0x9e3779b9)
	queries := make([]string, 200)
	for i := range queries {
		w := words[qr.intn(len(words))]
		switch qr.intn(4) {
		case 0:
			queries[i] = fmt.Sprintf(`%s:%s`, fields[qr.intn(len(fields))], w)
		case 1:
			queries[i] = fmt.Sprintf(`%s AND %s`, w, words[qr.intn(len(words))])
		case 2:
			queries[i] = fmt.Sprintf(`%s OR %s`, w, words[qr.intn(len(words))])
		default:
			queries[i] = w
		}
	}
	// 3a) 冷查询：紧随写入之后，状态缓存尚未填充（含一次性统计成本）。
	res0, err := idx.Search(ctx, queries[0], 0, 20)
	if err != nil {
		fail(err)
	}
	_ = res0
	qStart := time.Now()
	var lastTotal int
	var firstOrder []string
	for i := 0; i < 5; i++ { // 重复 5 轮摊薄噪声
		for _, q := range queries {
			res, err := idx.Search(ctx, q, 0, 20)
			if err != nil {
				fail(err)
			}
			lastTotal = res.Total
			if len(firstOrder) == 0 && res.Total > 0 {
				for _, h := range res.Hits {
					firstOrder = append(firstOrder, h.ID)
				}
			}
		}
	}
	qDur := time.Since(qStart)

	// 3b) 稳态查询：停止写入后，同一索引状态被反复查询，
	// 获胜视图与语料统计来自共享缓存，反映读多写少场景的稳态吞吐。
	steadyStart := time.Now()
	for i := 0; i < 5; i++ {
		for _, q := range queries {
			if _, err := idx.Search(ctx, q, 0, 20); err != nil {
				fail(err)
			}
		}
	}
	steadyDur := time.Since(steadyStart)

	stats := idx.Stats()
	fmt.Printf("build:        %12v  (%9.1f docs/s)\n",
		buildDur, float64(n)/buildDur.Seconds())
	fmt.Printf("updates:      %12v  (%9.1f updates/s)\n",
		updDur, float64(nUpdate)/updDur.Seconds())
	fmt.Printf("queries cold: %12v  (%9.1f queries/s, %d queries)\n",
		qDur, float64(len(queries)*5)/qDur.Seconds(), len(queries)*5)
	fmt.Printf("queries warm: %12v  (%9.1f queries/s, %d queries, no intervening writes)\n",
		steadyDur, float64(len(queries)*5)/steadyDur.Seconds(), len(queries)*5)
	fmt.Printf("index:        liveDocs=%d segments=%d indexBytes=%d walBytes=%d compactions=%d\n",
		stats.Documents, stats.Segments, stats.IndexBytes, stats.WALBytes, stats.CompactionRuns)
	fmt.Printf("sample query: %q -> total=%d top=%s\n",
		queries[0], lastTotal, strings.Join(shortIDs(firstOrder), ","))

	if err := idx.Close(); err != nil {
		fail(err)
	}
}

func shortIDs(ids []string) []string {
	if len(ids) > 5 {
		ids = ids[:5]
	}
	return append([]string(nil), ids...)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "perfbench error:", err)
	os.Exit(1)
}
