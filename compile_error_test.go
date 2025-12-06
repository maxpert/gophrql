package gophrql

import (
	"strings"
	"testing"
)

func TestCompileErrors(t *testing.T) {
	t.Helper()

	cases := []struct {
		name         string
		prql         string
		wantContains string
	}{
		{
			name: "unsupported_target",
			prql: `
target sql.duckdb
from tracks
take 1
`,
			wantContains: "unsupported target",
		},
		{
			name:         "comment_only",
			prql:         `# just a comment`,
			wantContains: "No PRQL query entered",
		},
		{
			name:         "empty_query",
			prql:         ``,
			wantContains: "No PRQL query entered",
		},
		{
			name: "missing_from",
			prql: `
let x = 5
let y = 10
`,
			wantContains: "PRQL queries must begin with 'from'",
		},
		{
			name: "declaration_only",
			prql: `
let x = 5
let y = 10
let z = 15
`,
			wantContains: "PRQL queries must begin with 'from'",
		},
		{
			name: "too_many_args_to_function",
			prql: `
let addadd = a b -> a + b

from x
derive y = (addadd 4 5 6)
`,
			wantContains: "Too many arguments to function `addadd`",
		},
		{
			name: "unknown_name",
			prql: `
from x
select a
select b
`,
			wantContains: "Unknown name `b`",
		},
		{
			name: "bad_take_type",
			prql: `
from employees
take 1.8
`,
			wantContains: "`take` expected int or range",
		},
		{
			name: "comment_then_empty",
			prql: `
# header
 
`,
			wantContains: "No PRQL query entered",
		},
		{
			name: "date_to_text_literal_format",
			prql: `
from invoices
select { date.to_text invoice_date billing_city }
`,
			wantContains: "`date.to_text` only supports a string literal as format",
		},
		{
			name: "date_to_text_unsupported_specifier",
			prql: `
from invoices
select { (invoice_date | date.to_text "%_j") }
`,
			wantContains: "PRQL doesn't support this format specifier",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sql, err := Compile(tc.prql)
			if err == nil {
				t.Fatalf("expected error, got SQL: %s", sql)
			}
			if !strings.Contains(err.Error(), tc.wantContains) {
				t.Fatalf("error mismatch for %s:\nwant substring: %q\ngot: %v", tc.name, tc.wantContains, err)
			}
		})
	}
}
