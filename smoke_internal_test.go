package searchengine

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSmokePutSearchDelete(t *testing.T) {
	dir := t.TempDir()
	x, err := Open(filepath.Join(dir, "idx"), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	docs := []Document{
		{ID: "d1", Fields: []Field{
			{Name: "title", Value: "the quick brown fox", Store: true, Index: true},
			{Name: "body", Value: "jumps over the lazy dog", Store: true, Index: true},
		}},
		{ID: "d2", Fields: []Field{
			{Name: "title", Value: "brown bears are strong", Store: true, Index: true},
		}},
		{ID: "d3", Fields: []Field{
			{Name: "title", Value: "fox news today", Store: true, Index: true},
		}},
	}
	for _, d := range docs {
		if err := x.Put(ctx, d); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	// term
	res, err := x.Search(ctx, "brown", 0, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if res.Total != 2 {
		t.Fatalf("brown: want 2 hits, got %d", res.Total)
	}
	// phrase
	res, _ = x.Search(ctx, `"quick brown"`, 0, 10)
	if res.Total != 1 || res.Hits[0].ID != "d1" {
		t.Fatalf("phrase: got %+v", res)
	}
	// boolean AND + field
	res, _ = x.Search(ctx, "title:brown AND fox", 0, 10)
	if res.Total != 1 || res.Hits[0].ID != "d1" {
		t.Fatalf("bool: got %+v", res)
	}
	// OR
	res, _ = x.Search(ctx, "fox OR bears", 0, 10)
	if res.Total != 3 {
		t.Fatalf("or: want 3, got %d", res.Total)
	}
	// NOT
	res, _ = x.Search(ctx, "NOT fox", 0, 10)
	if res.Total != 1 || res.Hits[0].ID != "d2" {
		t.Fatalf("not: got %+v", res)
	}
	// delete
	if err := x.Delete(ctx, "d1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	res, _ = x.Search(ctx, "fox", 0, 10)
	if res.Total != 1 || res.Hits[0].ID != "d3" {
		t.Fatalf("after delete: got %+v", res)
	}
	// idempotent re-delete and re-put same content
	if err := x.Delete(ctx, "d1"); err != nil {
		t.Fatalf("redelete: %v", err)
	}
	if err := x.Put(ctx, docs[1]); err != nil {
		t.Fatalf("idempotent put: %v", err)
	}
	if err := x.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := x.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
