package searchengine

import (
	"errors"
	"testing"
)

// FuzzParseQuery：任意非法输入都必须返回 ErrQuerySyntax 或成功解析，绝不 panic。
func FuzzParseQuery(f *testing.F) {
	seeds := []string{
		"hello AND", "a OR b", `NOT (x OR "y z")`, "-foo",
		`"unterminated`, "field:value", `field:"a b"`, "(()",
		"a:b:c", "&& ||", "café 中文 テスト", "",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, q string) {
		node, err := ParseQuery(q)
		if err != nil {
			if !errors.Is(err, ErrQuerySyntax) {
				t.Fatalf("non-syntax error returned: %v", err)
			}
			return
		}
		if node == nil {
			t.Fatal("nil node with nil error")
		}
		// 合法 AST 必须能在空快照上安全求值。
		dir := t.TempDir()
		x, err := Open(dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer x.Close()
		if _, err := x.Search(t.Context(), q, 0, 10); err != nil {
			// 引擎可能返回 syntax error；其他错误都不可接受。
			if !errors.Is(err, ErrQuerySyntax) {
				t.Fatalf("execute returned non-syntax error: %v", err)
			}
		}
	})
}
