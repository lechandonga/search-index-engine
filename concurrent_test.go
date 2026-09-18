package searchengine

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestConcurrentReadWrite：写入进行中持续查询，每次查询必须对应某个一致状态，
// 不能观察到部分写入、同一文档新旧版本并存，或 panic。
func TestConcurrentReadWrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	opts := &Options{FlushAtDocs: 50, CompactionSegments: 3}
	x, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	ctx := context.Background()

	var writers sync.WaitGroup
	stop := make(chan struct{})
	var readerErr error
	var errMu sync.Mutex
	setErr := func(e error) {
		errMu.Lock()
		if readerErr == nil {
			readerErr = e
		}
		errMu.Unlock()
	}

	updated := make(map[string]bool)
	var updMu sync.Mutex

	for w := 0; w < 4; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for i := 0; i < 200; i++ {
				id := fmt.Sprintf("w%d-d%d", w, i)
				if err := x.Put(ctx, Document{ID: id, Fields: []Field{
					{Name: "body", Value: fmt.Sprintf("concurrent marker %d word%d", w, i%10), Store: true, Index: true},
				}}); err != nil {
					setErr(err)
					return
				}
				if i%7 == 0 {
					if err := x.Put(ctx, Document{ID: id, Fields: []Field{
						{Name: "body", Value: fmt.Sprintf("updated marker %d word%d", w, i%10), Store: true, Index: true},
					}}); err != nil {
						setErr(err)
						return
					}
					updMu.Lock()
					updated[id] = true
					updMu.Unlock()
				}
				if i%11 == 0 {
					if err := x.Delete(ctx, id); err != nil {
						setErr(err)
						return
					}
				}
			}
		}(w)
	}

	var readers sync.WaitGroup
	for r := 0; r < 3; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := x.Search(ctx, "marker", 0, 1000)
				if err != nil {
					setErr(err)
					return
				}
				seen := make(map[string]bool)
				for _, h := range res.Hits {
					if seen[h.ID] {
						setErr(fmt.Errorf("duplicate hit %s", h.ID))
						return
					}
					seen[h.ID] = true
					isOld, isNew := false, false
					for _, sf := range h.Stored {
						if strings.Contains(sf.Value, "concurrent marker") {
							isOld = true
						}
						if strings.Contains(sf.Value, "updated marker") {
							isNew = true
						}
					}
					if isOld && isNew {
						setErr(fmt.Errorf("doc %s mixes old and new version in one snapshot", h.ID))
						return
					}
				}
				if res.Total < len(res.Hits) {
					setErr(fmt.Errorf("total %d < hits %d", res.Total, len(res.Hits)))
					return
				}
			}
		}()
	}

	writers.Wait()
	close(stop)
	readers.Wait()
	if readerErr != nil {
		t.Fatal(readerErr)
	}

	// 最终一致性：删除的文档不得再出现；旧版本内容不得出现。
	res, err := x.Search(ctx, "marker", 0, 5000)
	if err != nil {
		t.Fatal(err)
	}
	// 每 writer 删除 i%11==0 的文档（唯一 doc 集合，与是否先更新无关）。
	deletedPerWriter := 0
	for i := 0; i < 200; i++ {
		if i%11 == 0 {
			deletedPerWriter++
		}
	}
	want := 800 - 4*deletedPerWriter
	if res.Total != want {
		t.Fatalf("final total: want %d, got %d", want, res.Total)
	}
	for _, h := range res.Hits {
		if !updated[h.ID] {
			continue // 未更新文档保留原始 "concurrent marker" 内容是正确的
		}
		for _, sf := range h.Stored {
			if strings.Contains(sf.Value, "concurrent marker") {
				t.Fatalf("superseded content still visible in %s", h.ID)
			}
		}
	}
}

// TestPagination 验证分页稳定、无重复无遗漏、平局顺序确定。
func TestPagination(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	x, _ := Open(dir, &Options{FlushAtDocs: 20})
	defer x.Close()
	ctx := context.Background()
	for i := 0; i < 57; i++ {
		if err := x.Put(ctx, Document{ID: fmt.Sprintf("p%03d", i), Fields: []Field{
			{Name: "body", Value: "paginated common content", Store: true, Index: true},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	var all []string
	for off := 0; off < 80; off += 10 {
		res, err := x.Search(ctx, "common", off, 10)
		if err != nil {
			t.Fatal(err)
		}
		if off < 57 && len(res.Hits) == 0 {
			t.Fatalf("empty page at offset %d", off)
		}
		for _, h := range res.Hits {
			if seen[h.ID] {
				t.Fatalf("duplicate across pages: %s", h.ID)
			}
			seen[h.ID] = true
			all = append(all, h.ID)
		}
	}
	if len(all) != 57 {
		t.Fatalf("pagination collected %d, want 57", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i-1] >= all[i] {
			t.Fatalf("pagination order not deterministic: %s then %s", all[i-1], all[i])
		}
	}
}
