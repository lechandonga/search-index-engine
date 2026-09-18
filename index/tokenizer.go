package index

import (
	"strings"
	"unicode"
)

// Token 是分词结果，Position 为词项在原文中的序号（用于短语匹配）。
type Token struct {
	Term     string
	Position int
}

// Tokenize 将文本切分为词项序列。
// 规则：连续的 ASCII 字母/数字归并为小写词；CJK 等表意文字按单字切分；
// 其余字符视为分隔符。同一输入永远产生同一输出（可复现）。
func Tokenize(text string) []Token {
	var tokens []Token
	var word strings.Builder
	pos := 0

	flush := func() {
		if word.Len() > 0 {
			tokens = append(tokens, Token{Term: word.String(), Position: pos})
			pos++
			word.Reset()
		}
	}

	for _, r := range text {
		switch {
		case r <= unicode.MaxASCII && (unicode.IsLetter(r) || unicode.IsDigit(r)):
			word.WriteRune(unicode.ToLower(r))
		case isCJK(r):
			flush()
			tokens = append(tokens, Token{Term: string(r), Position: pos})
			pos++
		default:
			flush()
		}
	}
	flush()
	return tokens
}

func isCJK(r rune) bool {
	return unicode.Is(unicode.Han, r) ||
		unicode.Is(unicode.Hiragana, r) ||
		unicode.Is(unicode.Katakana, r) ||
		unicode.Is(unicode.Hangul, r)
}
