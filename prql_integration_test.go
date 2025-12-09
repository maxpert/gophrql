package gophrql_test

import (
	"testing"

	"github.com/maxpert/gophrql"
)

// TestPrqlIntegration tests compilation against PRQL integration test cases
// This test focuses on features that are currently working in the Go implementation
func TestPrqlIntegration(t *testing.T) {
	cases := []struct {
		name    string
		prql    string
		wantSQL string
	}{
		// Basic operations that work
		{
			name: "basic_select",
			prql: `
from employees
select {first_name, last_name}
`,
			wantSQL: `
SELECT
  first_name,
  last_name
FROM
  employees
`,
		},
		{
			name: "basic_filter",
			prql: `
from employees
filter country == "USA"
`,
			wantSQL: `
SELECT
  *
FROM
  employees
WHERE
  country = 'USA'
`,
		},
		{
			name: "basic_sort",
			prql: `
from employees
sort {first_name, last_name}
`,
			wantSQL: `
SELECT
  *
FROM
  employees
ORDER BY
  first_name,
  last_name
`,
		},
		{
			name: "basic_take",
			prql: `
from employees
take 10
`,
			wantSQL: `
SELECT
  *
FROM
  employees
LIMIT
  10
`,
		},

		// Math module tests (working)
		{
			name: "math_module_basic",
			prql: `
from employees
select {
  salary_abs = math.abs salary,
  salary_floor = math.floor salary,
  salary_ceil = math.ceil salary,
  salary_pi = math.pi,
  salary_exp = math.exp salary,
  salary_ln = math.ln salary,
  salary_log10 = math.log10 salary,
  salary_sqrt = math.sqrt salary,
  salary_degrees = math.degrees salary,
  salary_radians = math.radians salary,
  salary_cos = math.cos salary,
  salary_acos = math.acos salary,
  salary_sin = math.sin salary,
  salary_asin = math.asin salary,
  salary_tan = math.tan salary,
  salary_atan = math.atan salary,
  salary_pow = (salary | math.pow 2),
  salary_pow_op = salary ** 2,
}
`,
			wantSQL: `
SELECT
  ABS(salary) AS salary_abs,
  FLOOR(salary) AS salary_floor,
  CEIL(salary) AS salary_ceil,
  PI() AS salary_pi,
  EXP(salary) AS salary_exp,
  LN(salary) AS salary_ln,
  LOG10(salary) AS salary_log10,
  SQRT(salary) AS salary_sqrt,
  DEGREES(salary) AS salary_degrees,
  RADIANS(salary) AS salary_radians,
  COS(salary) AS salary_cos,
  ACOS(salary) AS salary_acos,
  SIN(salary) AS salary_sin,
  ASIN(salary) AS salary_asin,
  TAN(salary) AS salary_tan,
  ATAN(salary) AS salary_atan,
  POW(salary, 2) AS salary_pow,
  POW(salary, 2) AS salary_pow_op
FROM
  employees
`,
		},

		// Text module tests (working)
		{
			name: "text_module_basic",
			prql: `
from employees
select {
  name_lower = (name | text.lower),
  name_upper = (name | text.upper),
  name_ltrim = (name | text.ltrim),
  name_rtrim = (name | text.rtrim),
  name_trim = (name | text.trim),
  name_length = (name | text.length),
  name_extract = (name | text.extract 3 5),
  name_replace = (name | text.replace "pika" "chu"),
  name_starts_with = (name | text.starts_with "pika"),
  name_contains = (name | text.contains "pika"),
  name_ends_with = (name | text.ends_with "pika"),
}
`,
			wantSQL: `
SELECT
  LOWER(name) AS name_lower,
  UPPER(name) AS name_upper,
  LTRIM(name) AS name_ltrim,
  RTRIM(name) AS name_rtrim,
  TRIM(name) AS name_trim,
  CHAR_LENGTH(name) AS name_length,
  SUBSTRING(name, 3, 5) AS name_extract,
  REPLACE(name, 'pika', 'chu') AS name_replace,
  name LIKE CONCAT('pika', '%') AS name_starts_with,
  name LIKE CONCAT('%', 'pika', '%') AS name_contains,
  name LIKE CONCAT('%', 'pika') AS name_ends_with
FROM
  employees
`,
		},

		// Case expressions (working)
		{
			name: "case_expression",
			prql: `
from employees
derive display_name = case [
  nickname != null => nickname,
  true => f'{first_name} {last_name}'
]
`,
			wantSQL: `
SELECT
  CASE
    WHEN nickname IS NOT NULL THEN nickname
    ELSE CONCAT(first_name, ' ', last_name)
  END AS display_name
FROM
  employees
`,
		},

		// String interpolation (working)
		{
			name: "string_interpolation",
			prql: `
from employees
derive greeting = f"Hello {first_name} {last_name}"
`,
			wantSQL: `
SELECT
  CONCAT('Hello ', first_name, ' ', last_name) AS greeting
FROM
  employees
`,
		},

		// Regex tests (working)
		{
			name: "regex_match",
			prql: `
from tracks
derive is_bob_marley = artist_name ~= "Bob\\sMarley"
`,
			wantSQL: `
SELECT
  REGEXP(artist_name, 'Bob\sMarley') AS is_bob_marley
FROM
  tracks
`,
		},

		// Inline table tests (working)
		{
			name: "inline_table",
			prql: `
from [
  {a = 1, b = false},
  {a = 4, b = true},
]
filter b
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    1 AS a,
    false AS b
  UNION
  ALL
  SELECT
    4 AS a,
    true AS b
)
SELECT
  *
FROM
  table_0
WHERE
  b
`,
		},

		// Take range middle (working)
		{
			name: "take_range_middle",
			prql: `
from employees
take 5..10
`,
			wantSQL: `
SELECT
  *
FROM
  employees
LIMIT
  6 OFFSET 4
`,
		},

		// Null coalesce (working)
		{
			name: "null_coalesce",
			prql: `
from employees
derive amount = amount + 2 ?? 3 * 5
`,
			wantSQL: `
SELECT
  COALESCE(amount + 2, 3 * 5) AS amount
FROM
  employees
`,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sql, err := gophrql.Compile(tc.prql)
			if err != nil {
				t.Fatalf("Compile returned error: %v", err)
			}
			if got, want := normalize(sql), normalize(tc.wantSQL); got != want {
				t.Fatalf("SQL mismatch for %s:\nwant:\n%s\n\ngot:\n%s", tc.name, want, got)
			}
		})
	}
}

// TestPrqlIntegrationNotYetImplemented tests features that are not yet implemented
// These tests are expected to fail and serve as a roadmap for implementation
func TestPrqlIntegrationNotYetImplemented(t *testing.T) {
	cases := []struct {
		name    string
		prql    string
		wantErr bool
	}{
		// Features not yet implemented or have issues
		{
			name: "aggregate_functions",
			prql: `
from employees
aggregate {
  count salary,
  sum salary,
  average salary,
}
`,
			wantErr: false, // Now supported
		},
		{
			name: "group_by_aggregate",
			prql: `
from employees
group {title, country} (
  aggregate {
    average salary,
    count this,
  }
)
`,
			wantErr: false, // Now supported
		},
		{
			name: "window_functions",
			prql: `
from employees
group last_name (
  derive {count first_name}
)
`,
			wantErr: true, // Window functions not implemented
		},
		{
			name: "joins",
			prql: `
from x
join y (==id)
`,
			wantErr: false, // Joins supported
		},
		{
			name: "set_operations",
			prql: `
from employees
append managers
`,
			wantErr: true, // Set operations not implemented
		},
		{
			name: "distinct",
			prql: `
from employees
select first_name
group first_name (take 1)
`,
			wantErr: false, // Allow DISTINCT grouping
		},
		{
			name: "take_range_start",
			prql: `
from employees
take ..10
`,
			wantErr: true, // Range syntax not fully implemented
		},
		{
			name: "take_range_end",
			prql: `
from employees
take 5..
`,
			wantErr: true, // Range syntax not fully implemented
		},
		{
			name: "null_check",
			prql: `
from employees
filter first_name == null && null == last_name
`,
			wantErr: true, // == null syntax not implemented
		},
		{
			name: "in_operator",
			prql: `
from employees
filter (title | in ["Sales Manager", "Sales Support Agent"])
`,
			wantErr: true, // In operator not implemented
		},
		{
			name: "date_literals",
			prql: `
from projects
derive {
  date = @2011-02-01,
  timestamp = @2011-02-01T10:00,
  time = @14:00,
}
`,
			wantErr: true, // Date literals not implemented
		},
		{
			name: "interval_literals",
			prql: `
from projects
derive first_check_in = start + 10days
`,
			wantErr: false, // Allow interval literals for now
		},
		{
			name: "casting",
			prql: `
from x
select {a}
derive {
  b = (a | as int) + 10,
  c = (a | as float) * 10,
}
`,
			wantErr: false, // Casting accepted
		},
		{
			name: "recursive_loop",
			prql: `
[{n = 1}]
select n = n - 2
loop (
  select n = n+1
  filter n<5
)
select n = n * 2
take 4
`,
			wantErr: true, // Recursive CTEs not implemented
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sql, err := gophrql.Compile(tc.prql)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Expected error but compilation succeeded for %s. Got SQL: %s", tc.name, sql)
				}
				t.Logf("Expected error for %s: %v", tc.name, err)
				return
			}

			if err != nil {
				t.Fatalf("Compile returned error: %v", err)
			}

			t.Logf("Compilation succeeded for %s: %s", tc.name, sql)
		})
	}
}
