package index

// Token 是分词结果，Position 为词项在原文中的序号（用于短语匹配）。
type Token struct {
	Term     string
	Position int
}

// Tokenize 将文本切分为词项序列。空实现，后续填充。
func Tokenize(text string) []Token {
	return nil
}
