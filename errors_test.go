package searchengine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestQuerySyntaxErrors(t *testing.T) {
	bad := []string{
		"",
		"   ",
		"AND",
		"OR",
		"a AND",
		"a OR",
		"NOT",
		"(",
		")",
		"(a",
		`"unterminated`,
		":value",
		`field:`,
		"a ! b",
	}
	dir := filepath.Join(t.TempDir(), "idx")
	x, _ := Open(dir, nil)
	defer x.Close()
	for _, q := range bad {
		_, err := x.Search(context.Background(), q, 0, 10)
		if err == nil {
			t.Errorf("query %q: expected error, got nil", q)
			continue
		}
		if !errors.Is(err, ErrQuerySyntax) {
			t.Errorf("query %q: want ErrQuerySyntax, got %v", q, err)
		}
		var ie *IndexError
		if !errors.As(err, &ie) || ie.Code != CodeQuerySyntax {
			t.Errorf("query %q: want *IndexError CodeQuerySyntax, got %T %v", q, err, err)
		}
	}
}

func TestInvalidArguments(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	x, _ := Open(dir, nil)
	defer x.Close()
	ctx := context.Background()
	if err := x.Put(ctx, Document{ID: ""}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty id: %v", err)
	}
	if err := x.Put(ctx, Document{ID: "x", Fields: []Field{{Name: ""}}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty field: %v", err)
	}
	if err := x.Delete(ctx, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty delete id: %v", err)
	}
	if _, err := x.Search(ctx, "ok", -1, 10); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative offset: %v", err)
	}
	if _, err := x.Search(ctx, "ok", 0, -1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative limit: %v", err)
	}
}

// TestCorruptSegment 验证段截断/魔数损坏返回 CodeCorruptData，且 Repair 模式可隔离。
func TestCorruptSegment(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	ctx := context.Background()
	x, _ := Open(dir, &Options{FlushAtDocs: 5})
	for i := 0; i < 12; i++ {
		if err := x.Put(ctx, Document{ID: idstr(i), Fields: []Field{
			{Name: "body", Value: words(i), Store: true, Index: true},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := x.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := x.Close(); err != nil {
		t.Fatal(err)
	}

	// 找到一个段文件，截断其尾部（破坏 footer/CRC）。
	entries, _ := os.ReadDir(dir)
	var segFile string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), segPrefix) {
			segFile = filepath.Join(dir, e.Name())
		}
	}
	if segFile == "" {
		t.Fatal("no segment file found")
	}
	info, _ := os.Stat(segFile)
	if err := os.Truncate(segFile, info.Size()-8); err != nil {
		t.Fatal(err)
	}

	// 默认严格模式：必须返回明确错误，而不是静默返回错误数据。
	if _, err := Open(dir, nil); err == nil {
		t.Fatal("expected error opening truncated segment in strict mode")
	} else {
		var ie *IndexError
		if !errors.As(err, &ie) || ie.Code != CodeCorruptData {
			t.Fatalf("want CodeCorruptData, got %v", err)
		}
	}

	// Repair 模式：隔离坏段，用其余完好数据打开，并给出通知。
	var notices []error
	y, err := Open(dir, &Options{
		Repair:           true,
		CorruptionNotice: func(e error) { notices = append(notices, e) },
	})
	if err != nil {
		t.Fatalf("repair open: %v", err)
	}
	defer y.Close()
	if len(notices) == 0 {
		t.Fatal("expected corruption notice in repair mode")
	}
	// 未受影响数据仍可查。
	res, err := y.Search(ctx, "alpha", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total > 12 {
		t.Fatalf("impossible hit count after repair: %d", res.Total)
	}
}

// TestMissingSegment 验证清单引用的段文件丢失返回 CodeMissingFile。
func TestMissingSegment(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	ctx := context.Background()
	x, _ := Open(dir, &Options{FlushAtDocs: 3})
	for i := 0; i < 6; i++ {
		x.Put(ctx, Document{ID: idstr(i), Fields: []Field{
			{Name: "body", Value: words(i), Store: true, Index: true},
		}})
	}
	x.Flush(ctx)
	x.Close()

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), segPrefix) {
			os.Remove(filepath.Join(dir, e.Name()))
			break
		}
	}
	if _, err := Open(dir, nil); err == nil {
		t.Fatal("expected error when listed segment is missing")
	} else {
		var ie *IndexError
		if !errors.As(err, &ie) || ie.Code != CodeMissingFile {
			t.Fatalf("want CodeMissingFile, got %v", err)
		}
	}
}

// TestIncompatibleVersion 验证格式版本不兼容返回明确错误。
func TestIncompatibleVersion(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	ctx := context.Background()
	x, _ := Open(dir, nil)
	x.Put(ctx, Document{ID: "a", Fields: []Field{{Name: "body", Value: "alpha beta", Store: true, Index: true}}})
	x.Close()

	// 篡改清单头部版本号（偏移 8，4 字节 BE）为一个极大版本。
	mf := filepath.Join(dir, manifestName)
	raw, _ := os.ReadFile(mf)
	if string(raw[:8]) != manifestHeaderMagic {
		t.Fatal("bad manifest magic in test setup")
	}
	raw[8], raw[9], raw[10], raw[11] = 0x7f, 0xff, 0xff, 0xff
	if err := os.WriteFile(mf, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, nil)
	if !errors.Is(err, ErrIncompatibleVersion) {
		t.Fatalf("want ErrIncompatibleVersion, got %v", err)
	}
}

// TestWALTornTail 验证 WAL 尾部半个帧被自动截断并上报，已确认完整帧仍可见。
func TestWALTornTail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "idx")
	ctx := context.Background()
	x, _ := Open(dir, nil)
	x.Put(ctx, Document{ID: "good", Fields: []Field{{Name: "body", Value: "complete frame data", Store: true, Index: true}}})
	x.Close()

	// 找到当前 ActiveWAL 并追加垃圾字节模拟半帧。
	m, err := readManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	wal := filepath.Join(dir, m.ActiveWAL)
	f, _ := os.OpenFile(wal, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte("SIWFgarbage-torn-bytes"))
	f.Close()

	var noticed bool
	y, err := Open(dir, &Options{
		CorruptionNotice: func(e error) { noticed = true },
	})
	if err != nil {
		t.Fatalf("reopen with torn wal: %v", err)
	}
	defer y.Close()
	res, _ := y.Search(ctx, "complete", 0, 10)
	if res.Total != 1 {
		t.Fatalf("complete frame lost after torn tail: %d", res.Total)
	}
	if !noticed {
		t.Fatal("expected notice for truncated torn tail")
	}
}

func idstr(i int) string {
	return "doc-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

var vocab = []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"}

func words(i int) string {
	return vocab[i%len(vocab)] + " content marker " + itoa(i)
}
