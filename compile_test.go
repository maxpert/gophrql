package gophrql

import (
	"strings"
	"testing"
)

func TestCompileSnapshots(t *testing.T) {
	t.Helper()

	cases := []struct {
		name    string
		prql    string
		wantSQL string
	}{
		{
			name: "aggregation",
			prql: `
from tracks
filter genre_id == 100
derive empty_name = name == ''
aggregate {sum track_id, concat_array name, all empty_name, any empty_name}
`,
			wantSQL: `
SELECT
  COALESCE(SUM(track_id), 0),
  COALESCE(STRING_AGG(name, ''), ''),
  COALESCE(BOOL_AND(name = ''), TRUE),
  COALESCE(BOOL_OR(name = ''), FALSE)
FROM
  tracks
WHERE
  genre_id = 100
`,
		},
		{
			name: "append_select_union",
			prql: `
from invoices
select { customer_id, invoice_id, billing_country }
take 10..15
append (
  from invoices
  select { customer_id, invoice_id, billing_country }
  take 40..45
)
select { billing_country, invoice_id }
`,
			wantSQL: `
SELECT
  *
FROM
  (
    SELECT
      billing_country,
      invoice_id
    FROM
      invoices
    LIMIT
      6 OFFSET 9
  ) AS table_2
UNION
ALL
SELECT
  *
FROM
  (
    SELECT
      billing_country,
      invoice_id
    FROM
      invoices
    LIMIT
      6 OFFSET 39
  ) AS table_3
`,
		},
		{
			name: "window_functions",
			prql: `
from tracks
group genre_id (
  sort milliseconds
  derive {
    num = row_number this,
    total = count this,
    last_val = last track_id,
  }
  take 10
)
sort {genre_id, milliseconds}
select {track_id, genre_id, num, total, last_val}
filter genre_id >= 22
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    track_id,
    genre_id,
    ROW_NUMBER() OVER (
      PARTITION BY genre_id
      ORDER BY
        milliseconds
    ) AS num,
    COUNT(*) OVER (
      PARTITION BY genre_id
      ORDER BY
        milliseconds ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS total,
    LAST_VALUE(track_id) OVER (
      PARTITION BY genre_id
      ORDER BY
        milliseconds
    ) AS last_val,
    milliseconds,
    ROW_NUMBER() OVER (
      PARTITION BY genre_id
      ORDER BY
        milliseconds
    ) AS _expr_0
  FROM
    tracks
),
table_1 AS (
  SELECT
    track_id,
    genre_id,
    num,
    total,
    last_val,
    milliseconds
  FROM
    table_0
  WHERE
    _expr_0 <= 10
    AND genre_id >= 22
)
SELECT
  track_id,
  genre_id,
  num,
  total,
  last_val
FROM
  table_1
ORDER BY
  genre_id,
  milliseconds
`,
		},
		{
			name: "stdlib_math_module",
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
  salary_log = math.log 2 salary,
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
  LOG10(salary) / LOG10(2) AS salary_log,
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
		{
			name: "stdlib_text_module",
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
		{
			name: "pipelines_filters_sort_take",
			prql: `
from tracks

filter (name ~= "Love")
filter ((milliseconds / 1000 / 60) | in 3..4)
sort track_id
take 1..15
select {name, composer}
`,
			wantSQL: `
SELECT
  name,
  composer
FROM
  tracks
WHERE
  REGEXP(name, 'Love') AND milliseconds / 1000 / 60 BETWEEN 3 AND 4
ORDER BY
  track_id
LIMIT
  15
`,
		},
		{
			name: "distinct_group_take_one",
			prql: `
from tracks
select {album_id, genre_id}
group tracks.* (take 1)
sort tracks.*
`,
			wantSQL: `
SELECT
  DISTINCT album_id,
          genre_id
FROM
  tracks
ORDER BY
  album_id, genre_id
`,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sql, err := Compile(tc.prql)
			if err != nil {
				t.Fatalf("Compile returned error: %v", err)
			}
			if got, want := normalize(sql), normalize(tc.wantSQL); got != want {
				t.Fatalf("SQL mismatch for %s:\nwant:\n%s\n\ngot:\n%s", tc.name, want, got)
			}
		})
	}
}

func normalize(s string) string {
	return strings.TrimSpace(s)
}
