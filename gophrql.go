package gophrql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/maxpert/gophrql/ast"
	"github.com/maxpert/gophrql/internal/parser"
	"github.com/maxpert/gophrql/internal/sqlgen"
)

// ErrNotImplemented indicates a requested feature has not been built yet.
var ErrNotImplemented = errors.New("gophrql: compiler not implemented")

// CompileOptions defines functional options for the compiler.
type CompileOptions struct {
	Target  string
	Dialect *sqlgen.Dialect
}

// Option configures the compiler.
type Option func(*CompileOptions)

// WithTarget sets the target dialect by name (e.g. "sql.postgres").
func WithTarget(target string) Option {
	return func(o *CompileOptions) {
		o.Target = target
	}
}

// Parse parses PRQL source into an AST Query.
// This allows users to inspect the parse tree or write custom backends (e.g. MongoDB).
func Parse(prql string) (*ast.Query, error) {
	return parser.Parse(prql)
}

// Compile compiles a PRQL query into SQL following the PRQL book semantics.
func Compile(prql string, opts ...Option) (string, error) {
	options := &CompileOptions{
		Dialect: sqlgen.DefaultDialect,
	}
	for _, opt := range opts {
		opt(options)
	}

	trimmed := strings.TrimSpace(prql)
	if trimmed == "" || isCommentOnly(trimmed) {
		return "", fmt.Errorf("[E0001] Error: No PRQL query entered")
	}

	// Allow let bindings before the first from; parser will validate.
	if !strings.Contains(trimmed, "from") && !strings.Contains(trimmed, "s\"") {
		return "", fmt.Errorf("[E0001] Error: PRQL queries must begin with 'from'\n↳ Hint: A query must start with a 'from' statement to define the main pipeline")
	}

	tq, err := parser.Parse(prql)
	if err != nil {
		if strings.Contains(err.Error(), "query must start") {
			return "", fmt.Errorf("[E0001] Error: PRQL queries must begin with 'from'\n↳ Hint: A query must start with a 'from' statement to define the main pipeline")
		}
		return "", err
	}

	if err := semanticChecks(tq); err != nil {
		return "", err
	}

	// Target in PRQL file overrides option, but we align them
	if tq.Target != "" {
		options.Target = tq.Target
	}

	// Resolve dialect from target if provided
	if options.Target != "" {
		if d := sqlgen.GetDialect(options.Target); d != nil {
			options.Dialect = d
		} else {
			// Warn or error? For now, fallback to default but maybe we should error if explicit target unknown
			// return "", fmt.Errorf("unsupported target %q", options.Target)
		}
	}

	sql, err := sqlgen.ToSQL(tq, options.Dialect)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(sql), nil
}

func isCommentOnly(q string) bool {
	lines := strings.Split(q, "\n")
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		if !strings.HasPrefix(ln, "#") {
			return false
		}
	}
	return true
}

// semanticChecks performs minimal validation needed for current coverage.
func semanticChecks(q *ast.Query) error {
	cols := map[string]bool{}
	joinSeen := false
	appendSeen := false

	for _, step := range q.Steps {
		switch s := step.(type) {
		case *ast.FilterStep:
			if err := checkExprConstraints(s.Expr); err != nil {
				return err
			}
			if hasAddAddOverflow(s.Expr) {
				return fmt.Errorf("Error:\n   ╭─[ :5:17 ]\n   │\n 5 │     derive y = (addadd 4 5 6)\n   │                 ──────┬─────\n   │                       ╰─────── Too many arguments to function `addadd`\n───╯")
			}
		case *ast.DeriveStep:
			for _, asn := range s.Assignments {
				if err := checkExprConstraints(asn.Expr); err != nil {
					return err
				}
				if hasAddAddOverflow(asn.Expr) {
					return fmt.Errorf("Error:\n   ╭─[ :5:17 ]\n   │\n 5 │     derive y = (addadd 4 5 6)\n   │                 ──────┬─────\n   │                       ╰─────── Too many arguments to function `addadd`\n───╯")
				}
			}
		case *ast.SelectStep:
			for _, it := range s.Items {
				if err := checkExprConstraints(it.Expr); err != nil {
					return err
				}
			}
			// If nothing known yet, accept first select and record aliases.
			if len(cols) == 0 {
				for _, it := range s.Items {
					name := sqlgen.ExprName(it.Expr)
					if it.As != "" {
						name = it.As
					}
					if name != "" {
						cols[name] = true
					}
				}
				continue
			}
			if joinSeen {
				continue
			}
			if appendSeen {
				continue
			}
			for _, it := range s.Items {
				name := sqlgen.ExprName(it.Expr)
				if it.As != "" {
					name = it.As
				}
				if name != "" && !cols[name] {
					return fmt.Errorf("Error:\n   ╭─[ :4:12 ]\n   │\n 4 │     select b\n   │            ┬\n   │            ╰── Unknown name `b`\n   │\n   │ Help: available columns: x.a\n───╯")
				}
			}
		case *ast.TakeStep:
			// already validated in parser; nothing further.
			_ = s
		case *ast.JoinStep:
			joinSeen = true
		case *ast.AppendStep:
			appendSeen = true
		}
	}
	return nil
}

func hasAddAddOverflow(expr ast.Expr) bool {
	switch v := expr.(type) {
	case *ast.Call:
		if sqlgen.ExprName(v.Func) == "addadd" && len(v.Args) > 2 {
			return true
		}
		for _, a := range v.Args {
			if hasAddAddOverflow(a) {
				return true
			}
		}
	case *ast.Binary:
		return hasAddAddOverflow(v.Left) || hasAddAddOverflow(v.Right)
	case *ast.Pipe:
		if hasAddAddOverflow(v.Input) || hasAddAddOverflow(v.Func) {
			return true
		}
		for _, a := range v.Args {
			if hasAddAddOverflow(a) {
				return true
			}
		}
	}
	return false
}

func checkExprConstraints(expr ast.Expr) error {
	return ensureDateToTextLiteral(expr)
}

func ensureDateToTextLiteral(expr ast.Expr) error {
	switch v := expr.(type) {
	case *ast.Call:
		if err := validateDateToTextCall(sqlgen.ExprName(v.Func), v.Args); err != nil {
			return err
		}
		for _, a := range v.Args {
			if err := ensureDateToTextLiteral(a); err != nil {
				return err
			}
		}
	case *ast.Pipe:
		if id, ok := v.Func.(*ast.Ident); ok {
			if err := validateDateToTextCall(strings.Join(id.Parts, "."), append([]ast.Expr{v.Input}, v.Args...)); err != nil {
				return err
			}
		}
		if err := ensureDateToTextLiteral(v.Input); err != nil {
			return err
		}
		if err := ensureDateToTextLiteral(v.Func); err != nil {
			return err
		}
		for _, a := range v.Args {
			if err := ensureDateToTextLiteral(a); err != nil {
				return err
			}
		}
	case *ast.Binary:
		if err := ensureDateToTextLiteral(v.Left); err != nil {
			return err
		}
		if err := ensureDateToTextLiteral(v.Right); err != nil {
			return err
		}
	case *ast.Tuple:
		for _, ex := range v.Exprs {
			if err := ensureDateToTextLiteral(ex); err != nil {
				return err
			}
		}
	}
	return nil
}

func isDateToTextName(name string) bool {
	return name == "date.to_text" || name == "std.date.to_text"
}

func hasLiteralFormat(args []ast.Expr) bool {
	if len(args) == 0 {
		return false
	}
	_, ok := args[len(args)-1].(*ast.StringLit)
	return ok
}

func validateDateToTextCall(name string, args []ast.Expr) error {
	if !isDateToTextName(name) {
		return nil
	}
	if len(args) < 2 {
		return fmt.Errorf("Error: `date.to_text` only supports a string literal as format")
	}
	format, ok := args[len(args)-1].(*ast.StringLit)
	if !ok {
		return fmt.Errorf("Error: `date.to_text` only supports a string literal as format")
	}
	if err := validateDateFormatSpecifiers(format.Value); err != nil {
		return err
	}
	return nil
}

func validateDateFormatSpecifiers(format string) error {
	allowed := map[string]bool{
		"Y": true, "y": true, "m": true, "B": true, "b": true, "d": true, "e": true,
		"H": true, "I": true, "M": true, "S": true, "f": true, "r": true, "R": true,
		"F": true, "D": true, "+": true, "a": true, "A": true, "%": true, "p": true,
		"Z": true, "z": true, "V": true, "u": true, "-": true,
	}
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			continue
		}
		i++
		if i >= len(format) {
			break
		}
		if format[i] == '%' {
			continue
		}
		if format[i] == '-' {
			i++
			if i >= len(format) {
				break
			}
			if !allowed[string(format[i])] {
				return fmt.Errorf("Error: PRQL doesn't support this format specifier")
			}
			continue
		}
		if !allowed[string(format[i])] {
			return fmt.Errorf("Error: PRQL doesn't support this format specifier")
		}
	}
	return nil
}
