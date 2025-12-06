package gophrql

import (
	"testing"
)

func TestDialectCompilation(t *testing.T) {
	cases := []struct {
		name    string
		target  string
		prql    string
		wantSQL string
	}{
		// --- Quoting Style Tests ---
		{
			name:   "mysql_backticks",
			target: "sql.mysql",
			prql: `
from employees
select {` + "`first-name`, `last-name`" + `}
`,
			wantSQL: "SELECT `first-name` AS `first-name`, `last-name` AS `last-name` FROM employees",
		},
		{
			name:   "postgres_quotes",
			target: "sql.postgres",
			prql: `
from employees
select {` + "`first-name`, `last-name`" + `}
`,
			wantSQL: `SELECT "first-name" AS "first-name", "last-name" AS "last-name" FROM employees`,
		},
		{
			name:   "snowflake_quoting",
			target: "sql.snowflake",
			prql: `
from employees
select { a, b, ` + "`col space`" + ` }
`,
			wantSQL: `SELECT a, b, "col space" AS "col space" FROM employees`,
		},

		// --- MSSQL TOP vs LIMIT ---
		{
			name:   "mssql_top",
			target: "sql.mssql",
			prql: `
from employees
take 10
`,
			wantSQL: `SELECT TOP 10   * FROM employees`,
		},

		// --- Date to Text (Dialect Specific Functions) ---
		// Note: Requires implementing date.to_text in sqlgen
		{
			name:   "postgres_date_to_text",
			target: "sql.postgres",
			prql: `
from invoices
select { d = (invoice_date | date.to_text "DD/MM/YYYY") }
`,
			wantSQL: `SELECT TO_CHAR(invoice_date, 'DD/MM/YYYY') AS d FROM invoices`,
		},
		{
			name:   "mysql_date_to_text",
			target: "sql.mysql",
			prql: `
from invoices
select { d = (invoice_date | date.to_text "%d/%m/%Y") }
`,
			wantSQL: `SELECT DATE_FORMAT(invoice_date, '%d/%m/%Y') AS d FROM invoices`,
		},
		{
			name:   "duckdb_date_to_text",
			target: "sql.duckdb",
			prql: `
from invoices
select { d = (invoice_date | date.to_text "%d/%m/%Y") }
`,
			wantSQL: `SELECT strftime(invoice_date, '%d/%m/%Y') AS d FROM invoices`,
		},

		// --- MSSQL Math Functions (partial) ---
		{
			name:   "mssql_math",
			target: "sql.mssql",
			prql: `
from employees
select {
  c = math.ceil salary,
  l = math.ln salary,
  p = math.pow salary 2
}
`,
			wantSQL: `SELECT CEILING(salary) AS c, LOG(salary) AS l, POWER(salary, 2) AS p FROM employees`,
		},

		// --- Generic Fallback ---
		{
			name:   "generic_fallback",
			target: "sql.unknown_dialect",
			prql: `
from employees
select {` + "`first-name`" + `}
`,
			wantSQL: `SELECT "first-name" AS "first-name" FROM employees`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql, err := Compile(tc.prql, WithTarget(tc.target))
			if err != nil {
				t.Fatalf("Compile error: %v", err)
			}
			if normalize(sql) != normalize(tc.wantSQL) {
				t.Errorf("SQL mismatch.\nWant: %s\nGot:  %s", tc.wantSQL, sql)
			}
		})
	}
}
