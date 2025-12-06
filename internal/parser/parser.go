package parser

import (
	"fmt"
	"strings"

	"github.com/maxpert/gophrql/internal/ast"
)

// Parse converts PRQL source into an AST.Query.
func Parse(src string) (*ast.Query, error) {
	tokens, err := Lex(src)
	if err != nil {
		return nil, err
	}
	p := &Parser{tokens: tokens}
	return p.parseQuery()
}

type Parser struct {
	tokens []Token
	pos    int
}

func (p *Parser) parseQuery() (*ast.Query, error) {
	p.skipNewlines()
	// Skip leading let-declarations (not yet represented in AST).
	for p.peekIs(IDENT) && p.peek().Lit == "let" {
		p.skipToLineEnd()
		p.skipNewlines()
	}

	if !p.matchIdent("from") {
		return nil, fmt.Errorf("query must start with 'from'")
	}
	source, err := p.parseSource()
	if err != nil {
		return nil, err
	}

	var steps []ast.Step
	for !p.peekIs(EOF) {
		p.skipNewlines()
		if p.peekIs(EOF) {
			break
		}

		switch tok := p.peek(); tok.Typ {
		case IDENT:
			switch tok.Lit {
			case "filter":
				p.next()
				step, err := p.parseFilter()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			case "derive":
				p.next()
				step, err := p.parseDerive()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			case "select":
				p.next()
				step, err := p.parseSelect()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			case "aggregate":
				p.next()
				step, err := p.parseAggregate()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			case "take":
				p.next()
				step, err := p.parseTake()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			case "append":
				p.next()
				step, err := p.parseAppend()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			case "group":
				p.next()
				step, err := p.parseGroup()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			case "sort":
				p.next()
				step, err := p.parseSort()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			default:
				return nil, fmt.Errorf("unexpected token %q", tok.Lit)
			}
		case NEWLINE:
			p.next()
		default:
			return nil, fmt.Errorf("unexpected token %v at pos %d", tok, p.pos)
		}
	}

	return &ast.Query{
		From:  source,
		Steps: steps,
	}, nil
}

func (p *Parser) parseSource() (ast.Source, error) {
	p.skipNewlines()
	tok := p.next()
	if tok.Typ != IDENT {
		return ast.Source{}, fmt.Errorf("expected source after from, got %v", tok)
	}
	return ast.Source{Table: tok.Lit}, nil
}

func (p *Parser) parseFilter() (ast.Step, error) {
	p.skipNewlines()
	expr, err := p.parseExpr(0)
	if err != nil {
		return nil, err
	}
	p.skipToLineEnd()
	return &ast.FilterStep{Expr: expr}, nil
}

func (p *Parser) parseDerive() (ast.Step, error) {
	p.skipNewlines()
	var assigns []ast.Assignment
	if p.peekIs(LBRACE) {
		p.next() // consume {
		for {
			p.skipNewlines()
			if p.peekIs(RBRACE) {
				p.next()
				break
			}
			assign, err := p.parseAssignment()
			if err != nil {
				return nil, err
			}
			assigns = append(assigns, assign)
			if p.peekIs(COMMA) {
				p.next()
			}
			p.skipNewlines()
		}
	} else {
		assign, err := p.parseAssignment()
		if err != nil {
			return nil, err
		}
		assigns = append(assigns, assign)
	}
	p.skipToLineEnd()
	return &ast.DeriveStep{Assignments: assigns}, nil
}

func (p *Parser) parseAssignment() (ast.Assignment, error) {
	if !p.peekIs(IDENT) {
		return ast.Assignment{}, fmt.Errorf("expected identifier in assignment")
	}
	name := p.next().Lit
	if !p.peekIs(EQUAL) {
		return ast.Assignment{}, fmt.Errorf("expected = in assignment")
	}
	p.next()
	expr, err := p.parseExpr(0)
	if err != nil {
		return ast.Assignment{}, err
	}
	return ast.Assignment{Name: name, Expr: expr}, nil
}

func (p *Parser) parseSelect() (ast.Step, error) {
	p.skipNewlines()
	var items []ast.SelectItem
	if p.peekIs(LBRACE) {
		p.next()
		for {
			p.skipNewlines()
			if p.peekIs(RBRACE) {
				p.next()
				break
			}
			item, err := p.parseSelectItem()
			if err != nil {
				return nil, err
			}
			items = append(items, item)
			if p.peekIs(COMMA) {
				p.next()
			}
			p.skipNewlines()
		}
	} else {
		item, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	p.skipToLineEnd()
	return &ast.SelectStep{Items: items}, nil
}

func (p *Parser) parseSelectItem() (ast.SelectItem, error) {
	expr, err := p.parseExpr(0)
	if err != nil {
		return ast.SelectItem{}, err
	}
	alias := ""
	if p.peekIs(EQUAL) {
		p.next()
		rhs, err := p.parseExpr(0)
		if err != nil {
			return ast.SelectItem{}, err
		}
		id, ok := expr.(*ast.Ident)
		if !ok {
			return ast.SelectItem{}, fmt.Errorf("left side of assignment must be identifier")
		}
		alias = strings.Join(id.Parts, ".")
		expr = rhs
	}
	if p.peekIs(IDENT) && p.peek().Lit == "as" {
		p.next()
		if !p.peekIs(IDENT) {
			return ast.SelectItem{}, fmt.Errorf("expected alias after as")
		}
		alias = p.next().Lit
	}
	return ast.SelectItem{Expr: expr, As: alias}, nil
}

func (p *Parser) parseAggregate() (ast.Step, error) {
	p.skipNewlines()
	if !p.peekIs(LBRACE) {
		return nil, fmt.Errorf("aggregate expects {")
	}
	p.next()
	var items []ast.AggregateItem
	for {
		p.skipNewlines()
		if p.peekIs(RBRACE) {
			p.next()
			break
		}
		item, err := p.parseAggregateItem()
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		if p.peekIs(COMMA) {
			p.next()
		}
		p.skipNewlines()
	}
	p.skipToLineEnd()
	return &ast.AggregateStep{Items: items}, nil
}

func (p *Parser) parseAggregateItem() (ast.AggregateItem, error) {
	funcExpr, err := p.parseExpr(0)
	if err != nil {
		return ast.AggregateItem{}, err
	}
	call, ok := funcExpr.(*ast.Call)
	if !ok {
		return ast.AggregateItem{}, fmt.Errorf("aggregate item must be a function call")
	}
	if len(call.Args) != 1 {
		return ast.AggregateItem{}, fmt.Errorf("aggregate functions require exactly one argument")
	}
	fnName := exprToIdent(call.Func)
	alias := ""
	if p.peekIs(IDENT) && p.peek().Lit == "as" {
		p.next()
		if !p.peekIs(IDENT) {
			return ast.AggregateItem{}, fmt.Errorf("expected alias after as")
		}
		alias = p.next().Lit
	}
	return ast.AggregateItem{Func: fnName, Arg: call.Args[0], As: alias}, nil
}

func (p *Parser) parseTake() (ast.Step, error) {
	p.skipNewlines()
	// range form: number .. number
	if p.peekIs(NUMBER) && p.peekN(1).Typ == RANGE {
		start := p.next().Lit
		if strings.Contains(start, ".") {
			return nil, fmt.Errorf("`take` expected int or range, but found %s", start)
		}
		p.next() // ..
		if !p.peekIs(NUMBER) {
			return nil, fmt.Errorf("expected end of range")
		}
		end := p.next().Lit
		if strings.Contains(end, ".") {
			return nil, fmt.Errorf("`take` expected int or range, but found %s", end)
		}
		startInt := atoi(start)
		endInt := atoi(end)
		limit := endInt - startInt + 1
		offset := startInt - 1
		p.skipToLineEnd()
		return &ast.TakeStep{Limit: limit, Offset: offset}, nil
	}

	if !p.peekIs(NUMBER) {
		return nil, fmt.Errorf("take expects number or range")
	}
	lit := p.next().Lit
	if strings.Contains(lit, ".") {
		return nil, fmt.Errorf("`take` expected int or range, but found %s", lit)
	}
	limit := atoi(lit)
	p.skipToLineEnd()
	return &ast.TakeStep{Limit: limit}, nil
}

func (p *Parser) parseAppend() (ast.Step, error) {
	p.skipNewlines()
	if !p.peekIs(LPAREN) {
		return nil, fmt.Errorf("append expects '('")
	}
	p.next()
	subTokens := p.collectUntilMatching(RPAREN)
	subParser := &Parser{tokens: subTokens}
	subQuery, err := subParser.parseQuery()
	if err != nil {
		return nil, err
	}
	return &ast.AppendStep{Query: subQuery}, nil
}

func (p *Parser) parseGroup() (ast.Step, error) {
	p.skipNewlines()
	if !p.peekIs(IDENT) {
		return nil, fmt.Errorf("group expects identifier key")
	}
	keyTok := p.next()
	keyExpr := &ast.Ident{Parts: strings.Split(keyTok.Lit, ".")}
	p.skipNewlines()
	if !p.peekIs(LPAREN) {
		return nil, fmt.Errorf("group expects '(' block")
	}
	p.next()
	subTokens := p.collectUntilMatching(RPAREN)
	subParser := &Parser{tokens: subTokens}
	subQuery, err := subParser.parseGroupSteps()
	if err != nil {
		return nil, err
	}
	return &ast.GroupStep{
		Key:   keyExpr,
		Steps: subQuery,
	}, nil
}

func (p *Parser) parseGroupSteps() ([]ast.Step, error) {
	var steps []ast.Step
	for !p.peekIs(EOF) {
		p.skipNewlines()
		if p.peekIs(EOF) {
			break
		}
		switch tok := p.peek(); tok.Typ {
		case IDENT:
			switch tok.Lit {
			case "filter":
				p.next()
				step, err := p.parseFilter()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			case "derive":
				p.next()
				step, err := p.parseDerive()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			case "select":
				p.next()
				step, err := p.parseSelect()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			case "aggregate":
				p.next()
				step, err := p.parseAggregate()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			case "take":
				p.next()
				step, err := p.parseTake()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			case "sort":
				p.next()
				step, err := p.parseSort()
				if err != nil {
					return nil, err
				}
				steps = append(steps, step)
			default:
				return nil, fmt.Errorf("unexpected token %q in group", tok.Lit)
			}
		case NEWLINE:
			p.next()
		default:
			return nil, fmt.Errorf("unexpected token %v in group at pos %d", tok, p.pos)
		}
	}
	return steps, nil
}

func (p *Parser) parseSort() (ast.Step, error) {
	p.skipNewlines()
	var items []ast.SortItem
	if p.peekIs(LBRACE) {
		p.next()
		for {
			p.skipNewlines()
			if p.peekIs(RBRACE) {
				p.next()
				break
			}
			item, err := p.parseSortItem()
			if err != nil {
				return nil, err
			}
			items = append(items, item)
			if p.peekIs(COMMA) {
				p.next()
			}
			p.skipNewlines()
		}
	} else {
		item, err := p.parseSortItem()
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	p.skipToLineEnd()
	return &ast.SortStep{Items: items}, nil
}

func (p *Parser) parseSortItem() (ast.SortItem, error) {
	expr, err := p.parseExpr(0)
	if err != nil {
		return ast.SortItem{}, err
	}
	desc := false
	if p.peekIs(IDENT) && p.peek().Lit == "desc" {
		p.next()
		desc = true
	}
	return ast.SortItem{Expr: expr, Desc: desc}, nil
}

// Expression parsing (Pratt-style with limited operators).
var precedences = map[TokenType]int{
	EQ:    2,
	NEQ:   2,
	LT:    3,
	GT:    3,
	LTE:   3,
	GTE:   3,
	PLUS:  4,
	MINUS: 4,
	STAR:  5,
	SLASH: 5,
	POW:   6,
}

func (p *Parser) parseExpr(precedence int) (ast.Expr, error) {
	p.skipNewlines()
	left, err := p.parsePrefix()
	if err != nil {
		return nil, err
	}

	for {
		if p.peekIs(EOF) || p.peekIs(NEWLINE) || p.peekIs(COMMA) || p.peekIs(RBRACE) || p.peekIs(RPAREN) {
			break
		}

		// Pipe operator has low precedence; handle directly.
		if p.peekIs(PIPE) {
			p.next()
			fn, err := p.parsePrefix()
			if err != nil {
				return nil, err
			}
			var args []ast.Expr
			for p.canStartExpr(p.peek()) {
				arg, err := p.parsePrefix()
				if err != nil {
					return nil, err
				}
				args = append(args, arg)
			}
			left = &ast.Pipe{Input: left, Func: fn, Args: args}
			continue
		}

		// Function application by adjacency.
		if p.canStartExpr(p.peek()) {
			arg, err := p.parsePrefix()
			if err != nil {
				return nil, err
			}
			left = appendCallArg(left, arg)
			continue
		}

		op := p.peek()
		opPrec, ok := precedences[op.Typ]
		if !ok || opPrec < precedence {
			break
		}
		p.next()
		right, err := p.parseExpr(opPrec + 1)
		if err != nil {
			return nil, err
		}
		left = &ast.Binary{Op: op.Lit, Left: left, Right: right}
	}

	return left, nil
}

func (p *Parser) parsePrefix() (ast.Expr, error) {
	tok := p.next()
	switch tok.Typ {
	case IDENT:
		return &ast.Ident{Parts: strings.Split(tok.Lit, ".")}, nil
	case NUMBER:
		return &ast.Number{Value: tok.Lit}, nil
	case STRING:
		return &ast.StringLit{Value: tok.Lit}, nil
	case LPAREN:
		expr, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		if !p.peekIs(RPAREN) {
			return nil, fmt.Errorf("expected ) at pos %d", p.pos)
		}
		p.next()
		return expr, nil
	case MINUS:
		expr, err := p.parseExpr(precedences[MINUS])
		if err != nil {
			return nil, err
		}
		return &ast.Binary{Op: "*", Left: &ast.Number{Value: "-1"}, Right: expr}, nil
	default:
		return nil, fmt.Errorf("unexpected token %v at pos %d", tok, p.pos-1)
	}
}

// Helpers
func (p *Parser) peek() Token {
	return p.tokens[p.pos]
}

func (p *Parser) peekN(n int) Token {
	if p.pos+n >= len(p.tokens) {
		return Token{Typ: EOF}
	}
	return p.tokens[p.pos+n]
}

func (p *Parser) peekIs(tt TokenType) bool {
	return p.peek().Typ == tt
}

func (p *Parser) next() Token {
	t := p.tokens[p.pos]
	p.pos++
	return t
}

func (p *Parser) skipNewlines() {
	for p.peekIs(NEWLINE) {
		p.next()
	}
}

func (p *Parser) skipToLineEnd() {
	for !p.peekIs(EOF) && !p.peekIs(NEWLINE) {
		p.next()
	}
	p.skipNewlines()
}

func (p *Parser) matchIdent(lit string) bool {
	if p.peekIs(IDENT) && p.peek().Lit == lit {
		p.next()
		return true
	}
	return false
}

func (p *Parser) canStartExpr(tok Token) bool {
	switch tok.Typ {
	case IDENT, NUMBER, STRING, LPAREN, MINUS:
		return true
	default:
		return false
	}
}

func (p *Parser) collectUntilMatching(end TokenType) []Token {
	var collected []Token
	depth := 1
	for {
		tok := p.next()
		if tok.Typ == EOF {
			break
		}
		if tok.Typ == end {
			depth--
			if depth == 0 {
				break
			}
		}
		if tok.Typ == LPAREN && end == RPAREN {
			depth++
		}
		if tok.Typ == LBRACE && end == RBRACE {
			depth++
		}
		collected = append(collected, tok)
	}
	collected = append(collected, Token{Typ: EOF})
	return collected
}

func atoi(s string) int {
	var n int
	for _, r := range s {
		n = n*10 + int(r-'0')
	}
	return n
}

func exprToIdent(e ast.Expr) string {
	if id, ok := e.(*ast.Ident); ok {
		return strings.Join(id.Parts, ".")
	}
	return ""
}

func appendCallArg(fn ast.Expr, arg ast.Expr) ast.Expr {
	if call, ok := fn.(*ast.Call); ok {
		return &ast.Call{Func: call.Func, Args: append(call.Args, arg)}
	}
	return &ast.Call{Func: fn, Args: []ast.Expr{arg}}
}
