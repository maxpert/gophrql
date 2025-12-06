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
