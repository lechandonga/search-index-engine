package searchengine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkPut(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "idx")
	idx, err := Open(dir, &Options{FlushAtDocs: 4096})
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()
	ctx := context.Background()
	docs := make([]Document, 4096)
	for i := range docs {
		docs[i] = Document{
			ID: fmt.Sprintf("bench-%06d", i),
			Fields: []Field{{Name: "body",
				Value: fmt.Sprintf("alpha bravo charlie delta %d echo foxtrot", i%64),
				Index: true, Store: true}},
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := idx.Put(ctx, docs[i%len(docs)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSearch(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "idx")
	idx, err := Open(dir, &Options{FlushAtDocs: 4096})
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()
	ctx := context.Background()
	for i := 0; i < 4096; i++ {
		_ = idx.Put(ctx, Document{ID: fmt.Sprintf("b-%06d", i), Fields: []Field{{
			Name:  "body",
			Value: fmt.Sprintf("alpha bravo charlie delta echo foxtrot %d", i%128),
			Index: true,
		}}})
	}
	queries := []string{"alpha", "bravo AND charlie", "delta OR echo", `title:"x y"`, "foxtrot"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := idx.Search(ctx, queries[i%len(queries)], 0, 20); err != nil {
			b.Fatal(err)
		}
	}
}
