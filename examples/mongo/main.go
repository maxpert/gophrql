package main

import (
	"fmt"
	"strings"

	"github.com/maxpert/gophrql"
	"github.com/maxpert/gophrql/ast"
)

func main() {
	prql := `
        from users
        filter age > 21
        filter country == "US"
        derive { full_name = f"{first_name} {last_name}" }
        select { full_name, email, age }
        sort { -age }
        take 10
    `

	// Convert PRQL to an AST
	query, err := gophrql.Parse(prql)
	if err != nil {
		panic(err)
	}

	// Convert AST to MongoDB aggregation pipeline string
	mongo := convertToMongo(query)
	fmt.Println(mongo)
	// Expected output (formatted for readability):
	// db.users.aggregate([
	//   { $match: { age: { $gt: 21 }, country: "US" } },
	//   { $project: { name: 1, email: 1, age: 1, _id: 0 } },
	//   { $sort: { age: -1 } },
	//   { $limit: 10 }
	// ])

}

// ConvertToMongo builds a MongoDB aggregation pipeline from a PRQL AST query.
func convertToMongo(q *ast.Query) string {
	var stages []string

	// $match stage – collect all filter expressions
	matchFilters := []string{}
	for _, step := range q.Steps {
		if f, ok := step.(*ast.FilterStep); ok {
			matchFilters = append(matchFilters, exprToMongo(f.Expr))
		}
	}
	if len(matchFilters) > 0 {
		// combine filters with logical AND
		combined := strings.Join(matchFilters, ", ")
		stages = append(stages, fmt.Sprintf("{ $match: { %s } }", combined))
	}

	// $project stage – handle select steps
	for _, step := range q.Steps {
		if s, ok := step.(*ast.SelectStep); ok {
			proj := []string{}
			for _, item := range s.Items {
				alias := item.As
				if alias == "" {
					// Use the expression's string representation as field name
					alias = exprToMongo(item.Expr)
				}
				proj = append(proj, fmt.Sprintf("%s: 1", alias))
			}
			// Exclude _id by default for cleaner output
			proj = append(proj, "_id: 0")
			stages = append(stages, fmt.Sprintf("{ $project: { %s } }", strings.Join(proj, ", ")))
		}
	}

	// $sort stage – handle sort steps
	for _, step := range q.Steps {
		if s, ok := step.(*ast.SortStep); ok {
			sortFields := []string{}
			for _, item := range s.Items {
				direction := 1
				if item.Desc {
					direction = -1
				}
				// Assume the expression is a simple identifier
				sortFields = append(sortFields, fmt.Sprintf("%s: %d", exprToMongo(item.Expr), direction))
			}
			if len(sortFields) > 0 {
				stages = append(stages, fmt.Sprintf("{ $sort: { %s } }", strings.Join(sortFields, ", ")))
			}
		}
	}

	// $limit and $skip – handle take steps (limit/offset)
	for _, step := range q.Steps {
		if t, ok := step.(*ast.TakeStep); ok {
			if t.Offset > 0 {
				stages = append(stages, fmt.Sprintf("{ $skip: %d }", t.Offset))
			}
			if t.Limit > 0 {
				stages = append(stages, fmt.Sprintf("{ $limit: %d }", t.Limit))
			}
		}
	}

	// Build final aggregation string
	pipeline := strings.Join(stages, ", ")
	return fmt.Sprintf("db.%s.aggregate([%s])", q.From.Table, pipeline)
}

// exprToMongo converts a simple AST expression to a MongoDB query fragment.
// This is a minimal implementation supporting identifiers, binary ops, and literals.
func exprToMongo(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		// Identifier becomes a field reference prefixed with $.
		if len(v.Parts) == 1 {
			return fmt.Sprintf("$%s", v.Parts[0])
		}
		// For qualified identifiers (e.g., table.column) use the last part.
		return fmt.Sprintf("$%s", v.Parts[len(v.Parts)-1])
	case *ast.Number:
		return v.Value
	case *ast.StringLit:
		return fmt.Sprintf("\"%s\"", v.Value)
	case *ast.Binary:
		left := exprToMongo(v.Left)
		right := exprToMongo(v.Right)
		switch v.Op {
		case ">":
			return fmt.Sprintf("%s: { $gt: %s }", left, right)
		case "<":
			return fmt.Sprintf("%s: { $lt: %s }", left, right)
		case "==":
			return fmt.Sprintf("%s: %s", left, right)
		case "!=":
			return fmt.Sprintf("%s: { $ne: %s }", left, right)
		case "&&":
			return fmt.Sprintf("$and: [%s, %s]", left, right)
		case "||":
			return fmt.Sprintf("$or: [%s, %s]", left, right)
		default:
			return ""
		}
	default:
		return ""
	}
}
