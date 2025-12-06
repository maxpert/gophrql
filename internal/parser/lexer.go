package parser

import (
	"fmt"
	"strings"
	"unicode"
)

type TokenType string

const (
	ILLEGAL TokenType = "ILLEGAL"
	EOF               = "EOF"
	IDENT             = "IDENT"
	NUMBER            = "NUMBER"
	STRING            = "STRING"
	NEWLINE           = "NEWLINE"

	LPAREN = "("
	RPAREN = ")"
	LBRACE = "{"
	RBRACE = "}"
	COMMA  = ","
	EQUAL  = "="
	DOT    = "."
	PIPE   = "|"
	STAR   = "*"
	PLUS   = "+"
	MINUS  = "-"
	SLASH  = "/"
	CARET  = "^"
	POW    = "**"
	RANGE  = ".."
	EQ     = "=="
	NEQ    = "!="
	LT     = "<"
	GT     = ">"
	LTE    = "<="
	GTE    = ">="
)

type Token struct {
	Typ TokenType
	Lit string
}

func Lex(input string) ([]Token, error) {
	var tokens []Token
	i := 0

	for i < len(input) {
		ch := input[i]

		// Newlines become tokens to simplify statement parsing.
		if ch == '\n' {
			tokens = append(tokens, Token{Typ: NEWLINE, Lit: "\n"})
			i++
			continue
		}

		// Skip whitespace.
		if unicode.IsSpace(rune(ch)) {
			i++
			continue
		}

		// Comments: lines starting with # until newline.
		if ch == '#' {
			for i < len(input) && input[i] != '\n' {
				i++
			}
			continue
		}

		// Strings.
		if ch == '\'' {
			start := i + 1
			i++
			for i < len(input) && input[i] != '\'' {
				i++
			}
			if i >= len(input) {
				return nil, fmt.Errorf("unterminated string literal")
			}
			lit := input[start:i]
			i++ // closing '
			tokens = append(tokens, Token{Typ: STRING, Lit: lit})
			continue
		}

		// Numbers (integers and floats).
		if unicode.IsDigit(rune(ch)) {
			start := i
			i++
			for i < len(input) {
				if unicode.IsDigit(rune(input[i])) {
					i++
					continue
				}
				if input[i] == '.' && (i+1 < len(input) && input[i+1] == '.') {
					break
				}
				if input[i] == '.' {
					i++
					continue
				}
				break
			}
			tokens = append(tokens, Token{Typ: NUMBER, Lit: input[start:i]})
			continue
		}

		// Double-quoted strings.
		if ch == '"' {
			start := i + 1
			i++
			for i < len(input) && input[i] != '"' {
				i++
			}
			if i >= len(input) {
				return nil, fmt.Errorf("unterminated string literal")
			}
			lit := input[start:i]
			i++
			tokens = append(tokens, Token{Typ: STRING, Lit: lit})
			continue
		}

		// Identifiers (including module path with dots).
		if isIdentStart(rune(ch)) {
			start := i
			i++
			for i < len(input) && isIdentPart(rune(input[i])) {
				i++
			}
			tokens = append(tokens, Token{Typ: IDENT, Lit: input[start:i]})
			continue
		}

		// Multi-char operators.
		if strings.HasPrefix(input[i:], "**") {
			tokens = append(tokens, Token{Typ: POW, Lit: "**"})
			i += 2
			continue
		}
		if strings.HasPrefix(input[i:], "==") {
			tokens = append(tokens, Token{Typ: EQ, Lit: "=="})
			i += 2
			continue
		}
		if strings.HasPrefix(input[i:], "!=") {
			tokens = append(tokens, Token{Typ: NEQ, Lit: "!="})
			i += 2
			continue
		}
		if strings.HasPrefix(input[i:], "<=") {
			tokens = append(tokens, Token{Typ: LTE, Lit: "<="})
			i += 2
			continue
		}
		if strings.HasPrefix(input[i:], ">=") {
			tokens = append(tokens, Token{Typ: GTE, Lit: ">="})
			i += 2
			continue
		}
		if strings.HasPrefix(input[i:], "..") {
			tokens = append(tokens, Token{Typ: RANGE, Lit: ".."})
			i += 2
			continue
		}

		// Single-char tokens.
		switch ch {
		case '(':
			tokens = append(tokens, Token{Typ: LPAREN, Lit: "("})
		case ')':
			tokens = append(tokens, Token{Typ: RPAREN, Lit: ")"})
		case '{':
			tokens = append(tokens, Token{Typ: LBRACE, Lit: "{"})
		case '}':
			tokens = append(tokens, Token{Typ: RBRACE, Lit: "}"})
		case ',':
			tokens = append(tokens, Token{Typ: COMMA, Lit: ","})
		case '=':
			tokens = append(tokens, Token{Typ: EQUAL, Lit: "="})
		case '.':
			tokens = append(tokens, Token{Typ: DOT, Lit: "."})
		case '|':
			tokens = append(tokens, Token{Typ: PIPE, Lit: "|"})
		case '*':
			tokens = append(tokens, Token{Typ: STAR, Lit: "*"})
		case '+':
			tokens = append(tokens, Token{Typ: PLUS, Lit: "+"})
		case '-':
			tokens = append(tokens, Token{Typ: MINUS, Lit: "-"})
		case '/':
			tokens = append(tokens, Token{Typ: SLASH, Lit: "/"})
		case '^':
			tokens = append(tokens, Token{Typ: CARET, Lit: "^"})
		case '<':
			tokens = append(tokens, Token{Typ: LT, Lit: "<"})
		case '>':
			tokens = append(tokens, Token{Typ: GT, Lit: ">"})
		default:
			return nil, fmt.Errorf("unexpected character %q", ch)
		}
		i++
	}

	tokens = append(tokens, Token{Typ: EOF, Lit: ""})
	return tokens, nil
}

func isIdentStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_'
}

func isIdentPart(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '.'
}
