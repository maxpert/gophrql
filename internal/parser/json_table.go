package parser

import (
	"encoding/json"
	"fmt"

	"github.com/maxpert/gophrql/ast"
)

func parseJSONTable(raw string) ([]ast.InlineRow, error) {
	var payload struct {
		Columns []string            `json:"columns"`
		Data    [][]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, fmt.Errorf("invalid json: %w", err)
	}
	var rows []ast.InlineRow
	for _, row := range payload.Data {
		var fields []ast.Field
		for i, col := range payload.Columns {
			if i >= len(row) {
				continue
			}
			var num json.Number
			if err := json.Unmarshal(row[i], &num); err == nil {
				fields = append(fields, ast.Field{Name: col, Expr: &ast.Number{Value: num.String()}})
				continue
			}
			var str string
			if err := json.Unmarshal(row[i], &str); err == nil {
				fields = append(fields, ast.Field{Name: col, Expr: &ast.StringLit{Value: str}})
				continue
			}
		}
		rows = append(rows, ast.InlineRow{Fields: fields})
	}
	return rows, nil
}
