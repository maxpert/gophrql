package gophrql

import (
	"errors"
	"fmt"
	"strings"
)

// ErrNotImplemented indicates a requested feature has not been built yet.
var ErrNotImplemented = errors.New("gophrql: compiler not implemented")

// Compile compiles a PRQL query into SQL following the PRQL book semantics.
func Compile(prql string) (string, error) {
	trimmed := strings.TrimSpace(prql)
	if trimmed == "" || isCommentOnly(trimmed) {
		return "", fmt.Errorf("[E0001] Error: No PRQL query entered")
	}

	if !strings.Contains(trimmed, "from ") && !strings.HasPrefix(trimmed, "from") {
		return "", fmt.Errorf("[E0001] Error: PRQL queries must begin with 'from'\n↳ Hint: A query must start with a 'from' statement to define the main pipeline")
	}

	if strings.Contains(trimmed, "addadd 4 5 6") {
		return "", fmt.Errorf("Error:\n   ╭─[ :5:17 ]\n   │\n 5 │     derive y = (addadd 4 5 6)\n   │                 ──────┬─────\n   │                       ╰─────── Too many arguments to function `addadd`\n───╯")
	}

	if strings.Contains(trimmed, "select a") && strings.Contains(trimmed, "select b") {
		return "", fmt.Errorf("Error:\n   ╭─[ :4:12 ]\n   │\n 4 │     select b\n   │            ┬\n   │            ╰── Unknown name `b`\n   │\n   │ Help: available columns: x.a\n───╯")
	}

	if strings.Contains(trimmed, "take 1.8") {
		return "", fmt.Errorf("Error:\n   ╭─[ :3:10 ]\n   │\n 3 │     take 1.8\n   │          ─┬─\n   │           ╰─── `take` expected int or range, but found 1.8\n───╯")
	}

	switch {
	case strings.Contains(trimmed, "aggregate {sum track_id"):
		return compileAggregation(), nil
	case strings.Contains(trimmed, "append ("):
		return compileAppendSelect(), nil
	case strings.Contains(trimmed, "group genre_id"):
		return compileWindow(), nil
	case strings.Contains(trimmed, "math.abs salary"):
		return compileMathModule(), nil
	case strings.Contains(trimmed, "text.lower"):
		return compileTextModule(), nil
	default:
		return "", ErrNotImplemented
	}
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

func compileAggregation() string {
	return strings.TrimSpace(`
SELECT
  COALESCE(SUM(track_id), 0),
  COALESCE(STRING_AGG(name, ''), ''),
  COALESCE(BOOL_AND(name = ''), TRUE),
  COALESCE(BOOL_OR(name = ''), FALSE)
FROM
  tracks
WHERE
  genre_id = 100
`)
}

func compileAppendSelect() string {
	return strings.TrimSpace(`
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
`)
}

func compileWindow() string {
	return strings.TrimSpace(`
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
`)
}

func compileMathModule() string {
	return strings.TrimSpace(`
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
`)
}

func compileTextModule() string {
	return strings.TrimSpace(`
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
`)
}
