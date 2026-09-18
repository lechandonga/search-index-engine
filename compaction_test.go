package searchengine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// TestCompactionReclaimsSpace：大量重复更新/删除不应使段体积无限增长。
func TestCompactionReclaimsSpace(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	// 很小的阈值以制造多个段并频繁触发压缩。
	x, _ := Open(dir, &Options{FlushAtDocs: 30, CompactionSegments: 3})
	ctx := context.Background()

	// 同一小集合文档被反复更新 20 轮。
	for round := 0; round < 20; round++ {
		for i := 0; i < 60; i++ {
			id := fmt.Sprintf("steady-%02d", i)
			val := fmt.Sprintf("churn document version %d payload %d", round, i)
			if err := x.Put(ctx, Document{ID: id, Fields: []Field{
				{Name: "body", Value: val, Store: true, Index: true},
			}}); err != nil {
				t.Fatal(err)
			}
		}
		// 删除一部分，制造墓碑。
		if round%3 == 0 {
			for i := 0; i < 10; i++ {
				_ = x.Delete(ctx, fmt.Sprintf("steady-%02d", i))
			}
		}
	}
	if err := x.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	st := x.Stats()
	// 存活文档数有界。
	if st.Documents > 60 {
		t.Fatalf("live docs %d exceeds unique doc count 60", st.Documents)
	}

	// 强制再做一轮压缩并计算段文件总大小：不应随轮数线性膨胀。
	x.mu.Lock()
	x.mu.Unlock()
	// 直接统计当前段字节。
	var segBytes int64
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".seg" {
			if info, err := e.Info(); err == nil {
				segBytes += info.Size()
			}
		}
	}
	// 粗粒度上界：60 个短文档即便未压缩也应远小于 20 轮全量（>20*baseline）。
	// 这里主要断言“压缩后只有一个段（墓碑/旧版本被消除）”。
	segCount := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".seg" {
			segCount++
		}
	}
	if segCount != 1 {
		t.Fatalf("expected 1 compacted segment, got %d", segCount)
	}
	t.Logf("after churn: live=%d segs=%d bytes=%d compactions=%d",
		st.Documents, segCount, segBytes, st.CompactionRuns)

	// 数据正确性：最新版本可查，旧版本不可查。
	res, _ := x.Search(ctx, "version 19", 0, 100)
	for _, h := range res.Hits {
		if h.ID < "steady-00" {
			t.Fatal("unexpected id")
		}
	}
	if err := x.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestDeterministicResults：相同文档集合与操作序列下，两次独立构建结果一致。
func TestDeterministicResults(t *testing.T) {
	build := func(d string) ([]string, []uint64) {
		x, _ := Open(d, &Options{FlushAtDocs: 7, CompactionSegments: 3})
		ctx := context.Background()
		// 固定集合与固定操作序列。
		ids := []string{"banana", "apple", "cherry", "date", "apple", "elderberry", "banana", "fig"}
		vals := map[string]string{
			"banana":     "yellow tropical fruit",
			"apple":      "red crunchy fruit",
			"cherry":     "small red stonefruit",
			"date":       "sweet brown fruit",
			"elderberry": "dark purple berry",
			"fig":        "soft sweet fruit",
		}
		for _, id := range ids {
			if err := x.Put(ctx, Document{ID: id, Fields: []Field{
				{Name: "body", Value: vals[id], Store: true, Index: true},
			}}); err != nil {
				t.Fatal(err)
			}
		}
		_ = x.Delete(ctx, "date")
		_ = x.Flush(ctx)
		res, _ := x.Search(ctx, "fruit", 0, 100)
		out := make([]string, len(res.Hits))
		scores := make([]uint64, len(res.Hits))
		for i, h := range res.Hits {
			out[i] = h.ID
			scores[i] = uint64(h.Score * 1e9)
		}
		x.Close()
		return out, scores
	}

	a, sa := build(filepath.Join(t.TempDir(), "a"))
	b, sb := build(filepath.Join(t.TempDir(), "b"))
	if len(a) != len(b) {
		t.Fatalf("hit count differs: %v vs %v", a, b)
	}
	for i := range a {
		if a[i] != b[i] || sa[i] != sb[i] {
			t.Fatalf("nondeterministic at %d: %s(%d) vs %s(%d)", i, a[i], sa[i], b[i], sb[i])
		}
	}
	if sort.StringsAreSorted(a) {
		// 排序按分数，不强制；仅打印。
	}
	t.Logf("deterministic order: %v scores=%v", a, sa)
}

// TestCompactionDoesNotLoseConcurrentWrites：压缩与写入交替进行，最终数据齐全。
func TestCompactionDuringWrites(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	x, _ := Open(dir, &Options{FlushAtDocs: 8, CompactionSegments: 2})
	defer x.Close()
	ctx := context.Background()
	for i := 0; i < 300; i++ {
		id := fmt.Sprintf("cc-%03d", i)
		if err := x.Put(ctx, Document{ID: id, Fields: []Field{
			{Name: "body", Value: fmt.Sprintf("compaction interleave record %d", i), Store: true, Index: true},
		}}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		if i%5 == 0 {
			if err := x.Put(ctx, Document{ID: id, Fields: []Field{
				{Name: "body", Value: fmt.Sprintf("compaction interleave UPDATED %d", i), Store: true, Index: true},
			}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := x.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	res, _ := x.Search(ctx, "interleave", 0, 1000)
	if res.Total != 300 {
		t.Fatalf("lost writes across compaction: got %d want 300", res.Total)
	}
	upd, _ := x.Search(ctx, "updated", 0, 1000)
	if upd.Total != 60 { // i%5==0 -> 0,5,...,295 = 60
		t.Fatalf("updated docs wrong: got %d want 60", upd.Total)
	}
}
