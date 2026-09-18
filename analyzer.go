package searchengine

import (
	"strings"
	"unicode"
)

// Token 是分析器产生的词元，Position 为其在字段内的词元序号（从 0 起）。
// 短语匹配依赖这些位置。
type Token struct {
	Term     string
	Position int
}

// analyzeText 把文本转换为确定性的小写词元序列：
//   - 连续的 Unicode 字母/数字组成一个词元，统一转小写；
//   - CJK 表意文字（Han/Hiragana/Katakana/Hangul）逐 rune 切分为单字词元，
//     这样不依赖任何外部词典即可工作，且结果完全可复现；
//   - 其他字符（空白、标点）作为词元边界。
//
// 该函数无任何全局状态，相同输入恒得相同输出。
func analyzeText(text string) []Token {
	tokens := make([]Token, 0, 8)
	pos := 0

	var latin strings.Builder
	flushLatin := func() {
		if latin.Len() > 0 {
			tokens = append(tokens, Token{Term: latin.String(), Position: pos})
			pos++
			latin.Reset()
		}
	}

	for _, r := range text {
		switch {
		case isCJK(r):
			flushLatin()
			tokens = append(tokens, Token{Term: strings.ToLower(string(r)), Position: pos})
			pos++
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			latin.WriteRune(unicode.ToLower(r))
		default:
			flushLatin()
		}
	}
	flushLatin()
	return tokens
}

// isCJK 判断 rune 是否应按单字切分。
func isCJK(r rune) bool {
	switch {
	case r >= 0x4E00 && r <= 0x9FFF: // CJK Unified Ideographs
		return true
	case r >= 0x3400 && r <= 0x4DBF: // CJK Ext A
		return true
	case r >= 0x20000 && r <= 0x2A6DF: // CJK Ext B
		return true
	case r >= 0x3040 && r <= 0x30FF: // Hiragana + Katakana
		return true
	case r >= 0xAC00 && r <= 0xD7AF: // Hangul Syllables
		return true
	case r >= 0x3130 && r <= 0x318F: // Hangul Compatibility Jamo
		return true
	case r >= 0xFF00 && r <= 0xFFEF: // 全角符号区中可能混入的全角字符不按 CJK，交给默认边界
		return false
	default:
		return false
	}
}

// analyzeDocument 对文档的可索引字段分词，返回 field -> tokens，以及字段长度表。
// 字段处理顺序按字段在文档中的出现顺序，保证可复现。
func analyzeDocument(d Document) (map[string][]Token, map[string]int) {
	fieldTokens := make(map[string][]Token, len(d.Fields))
	fieldLens := make(map[string]int, len(d.Fields))
	for _, f := range d.Fields {
		if !f.Index {
			continue
		}
		toks := analyzeText(f.Value)
		fieldTokens[f.Name] = toks
		fieldLens[f.Name] += len(toks)
	}
	return fieldTokens, fieldLens
}
