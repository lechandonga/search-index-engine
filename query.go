package searchengine

import (
	"fmt"
	"strings"
)

// QueryNode 是解析后的查询 AST。
type QueryNode interface {
	isQueryNode()
}

// TermQuery 单词匹配。Field 为空表示匹配任意索引字段。
type TermQuery struct {
	Field string
	Term  string
}

// PhraseQuery 短语匹配：词项需在同一字段内按序相邻出现。
type PhraseQuery struct {
	Field string
	Terms []string
}

// AndQuery 逻辑与（交集，分数求和）。
type AndQuery struct{ Children []QueryNode }

// OrQuery 逻辑或（并集，取较高分）。
type OrQuery struct{ Children []QueryNode }

// NotQuery 逻辑非：从全集排除 Child 的命中文档。
type NotQuery struct{ Child QueryNode }

func (*TermQuery) isQueryNode()   {}
func (*PhraseQuery) isQueryNode() {}
func (*AndQuery) isQueryNode()    {}
func (*OrQuery) isQueryNode()     {}
func (*NotQuery) isQueryNode()    {}

// ---- 词法 ----

type qTokenKind int

const (
	tkWord qTokenKind = iota
	tkPhrase
	tkLParen
	tkRParen
	tkAnd
	tkOr
	tkNot
)

type qToken struct {
	kind qTokenKind
	text string
	pos  int // 基于字节的起始位置
}

type qLexer struct {
	input  string
	tokens []qToken
}

func lexQuery(input string) ([]qToken, error) {
	l := &qLexer{input: input}
	i := 0
	for i < len(input) {
		r := input[i]
		switch {
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			i++
		case r == '(':
			l.tokens = append(l.tokens, qToken{kind: tkLParen, pos: i})
			i++
		case r == ')':
			l.tokens = append(l.tokens, qToken{kind: tkRParen, pos: i})
			i++
		case r == '"':
			start := i
			i++
			var sb strings.Builder
			closed := false
			for i < len(input) {
				c := input[i]
				if c == '"' {
					closed = true
					i++
					break
				}
				sb.WriteByte(c)
				i++
			}
			if !closed {
				return nil, qSyntaxErr(start, "unterminated quoted phrase")
			}
			l.tokens = append(l.tokens, qToken{kind: tkPhrase, text: sb.String(), pos: start})
		default:
			// '-' 作为 NOT 前缀（"-foo" 与 "- foo" 均支持）。
			if r == '-' {
				l.tokens = append(l.tokens, qToken{kind: tkNot, pos: i})
				i++
				continue
			}
			start := i
			for i < len(input) {
				c := input[i]
				if c == ' ' || c == '\t' || c == '\n' || c == '\r' ||
					c == '(' || c == ')' || c == '"' {
					break
				}
				i++
			}
			word := input[start:i]
			l.tokens = append(l.tokens, qToken{kind: wordKind(word), text: word, pos: start})
		}
	}
	return l.tokens, nil
}

func wordKind(w string) qTokenKind {
	switch strings.ToUpper(w) {
	case "AND", "&&":
		return tkAnd
	case "OR", "||":
		return tkOr
	case "NOT":
		return tkNot
	default:
		return tkWord
	}
}

// ---- 语法 ----

type qParser struct {
	tokens []qToken
	pos    int
}

func qSyntaxErr(bytePos int, format string, args ...any) error {
	return errf(CodeQuerySyntax, "parse query", "",
		"at position %d: %s", bytePos, fmt.Sprintf(format, args...))
}

func (p *qParser) peek() (qToken, bool) {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos], true
	}
	return qToken{}, false
}

func (p *qParser) next() qToken {
	t := p.tokens[p.pos]
	p.pos++
	return t
}

func (p *qParser) parseOr() (QueryNode, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for {
		t, ok := p.peek()
		if !ok || t.kind != tkOr {
			return left, nil
		}
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &OrQuery{Children: []QueryNode{left, right}}
	}
}

func (p *qParser) parseAnd() (QueryNode, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	children := []QueryNode{left}
	for {
		t, ok := p.peek()
		if !ok {
			break
		}
		// 显式 AND 或紧邻（默认 AND）；OR / 右括号结束本层。
		switch t.kind {
		case tkRParen, tkOr:
			goto done
		case tkAnd:
			p.next()
		}
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		children = append(children, right)
	}
done:
	if len(children) == 1 {
		return children[0], nil
	}
	return &AndQuery{Children: children}, nil
}

func (p *qParser) parseUnary() (QueryNode, error) {
	t, ok := p.peek()
	if !ok {
		return nil, qSyntaxErr(len(""), "unexpected end of query")
	}
	if t.kind == tkNot {
		p.next()
		child, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &NotQuery{Child: child}, nil
	}
	if t.kind == tkLParen {
		p.next()
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		cl, ok := p.peek()
		if !ok {
			return nil, qSyntaxErr(t.pos, "missing closing parenthesis")
		}
		if cl.kind != tkRParen {
			return nil, qSyntaxErr(cl.pos, "expected ')'")
		}
		p.next()
		return inner, nil
	}
	return p.parseAtom()
}

func (p *qParser) parseAtom() (QueryNode, error) {
	t, ok := p.peek()
	if !ok {
		return nil, qSyntaxErr(0, "unexpected end of query")
	}
	if t.kind == tkRParen {
		return nil, qSyntaxErr(t.pos, "unexpected ')'")
	}
	if t.kind == tkAnd || t.kind == tkOr {
		return nil, qSyntaxErr(t.pos, "operator %q requires operands on both sides", t.text)
	}

	field := ""
	if t.kind == tkWord {
		word := t.text
		if idx := strings.IndexByte(word, ':'); idx >= 0 {
			field = word[:idx]
			rest := word[idx+1:]
			if field == "" {
				return nil, qSyntaxErr(t.pos, "missing field name before ':'")
			}
			if rest != "" {
				p.next()
				return p.makeTerm(field, rest, t.pos)
			}
			// word 仅为 "field:"，值是下一个 token（允许 field:"phrase"）。
			p.next()
			return p.parseFieldValue(field, t.pos)
		}
	}
	p.next()
	return p.tokenToNode("", t)
}

func (p *qParser) parseFieldValue(field string, colonPos int) (QueryNode, error) {
	t, ok := p.peek()
	if !ok {
		return nil, qSyntaxErr(colonPos, "missing value after 'field:'")
	}
	p.next()
	return p.tokenToNode(field, t)
}

func (p *qParser) makeTerm(field, raw string, pos int) (QueryNode, error) {
	toks := analyzeText(raw)
	if len(toks) == 0 {
		return nil, qSyntaxErr(pos, "term %q has no searchable characters", raw)
	}
	if len(toks) > 1 {
		// 未经引号包裹的多词输入视为 AND 短语，保持可预期；此处直接报语法错误，
		// 强制用户使用引号，避免歧义。
		return nil, qSyntaxErr(pos, "multi-word term must be quoted: %q", raw)
	}
	return &TermQuery{Field: field, Term: toks[0].Term}, nil
}

func (p *qParser) tokenToNode(field string, t qToken) (QueryNode, error) {
	switch t.kind {
	case tkWord:
		return p.makeTerm(field, t.text, t.pos)
	case tkPhrase:
		toks := analyzeText(t.text)
		if len(toks) == 0 {
			return nil, qSyntaxErr(t.pos, "empty phrase")
		}
		terms := make([]string, len(toks))
		for i, tk := range toks {
			terms[i] = tk.Term
		}
		return &PhraseQuery{Field: field, Terms: terms}, nil
	case tkNot:
		child, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &NotQuery{Child: child}, nil
	case tkLParen:
		// 不经过 parseAtom 的括号入口（理论上不会到达）。
		return nil, qSyntaxErr(t.pos, "unexpected '('")
	default:
		return nil, qSyntaxErr(t.pos, "expected term or phrase, got %q", t.text)
	}
}

// ParseQuery 解析查询语言：
//
//	term            单个词元（大小写不敏感）
//	"a phrase"      短语（词元相邻）
//	field:value     限定字段；也支持 field:"phrase words"
//	( q )           分组
//	q AND q         逻辑与（AND 可省略，默认即为与）
//	q OR q          逻辑或
//	NOT q / -q      逻辑非
//
// 空输入或无法解析时返回包装了 ErrQuerySyntax 的 *IndexError（CodeQuerySyntax），
// 绝不返回 nil 错误的空 AST。
func ParseQuery(input string) (QueryNode, error) {
	tokens, err := lexQuery(input)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, qSyntaxErr(0, "empty query")
	}
	p := &qParser{tokens: tokens}
	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.pos != len(tokens) {
		t := p.tokens[p.pos]
		return nil, qSyntaxErr(t.pos, "unexpected token %q", t.text)
	}
	return node, nil
}
