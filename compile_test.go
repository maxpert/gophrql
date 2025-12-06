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
			name: "target_sql_generic_simple",
			prql: `
target sql.generic
from invoices
take 1
`,
			wantSQL: `
SELECT
  *
FROM
  invoices
LIMIT
  1
`,
		},
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
			name: "date_to_text_formats",
			prql: `
from invoices
take 20
select {
  d1 = (invoice_date | date.to_text "%Y/%m/%d"),
  d2 = (invoice_date | date.to_text "%F"),
  d3 = (invoice_date | date.to_text "%D"),
  d4 = (invoice_date | date.to_text "%H:%M:%S.%f"),
  d5 = (invoice_date | date.to_text "%r"),
  d6 = (invoice_date | date.to_text "%A %B %-d %Y"),
  d7 = (invoice_date | date.to_text "%a, %-d %b %Y at %I:%M:%S %p"),
  d8 = (invoice_date | date.to_text "%+"),
  d9 = (invoice_date | date.to_text "%-d/%-m/%y"),
  d10 = (invoice_date | date.to_text "%-Hh %Mmin"),
  d11 = (invoice_date | date.to_text "%M'%S\""),
  d12 = (invoice_date | date.to_text "100%% in %d days"),
}
`,
			wantSQL: `
SELECT
  strftime(invoice_date, '%Y/%m/%d') AS d1,
  strftime(invoice_date, '%F') AS d2,
  strftime(invoice_date, '%D') AS d3,
  strftime(invoice_date, '%H:%M:%S.%f') AS d4,
  strftime(invoice_date, '%r') AS d5,
  strftime(invoice_date, '%A %B %-d %Y') AS d6,
  strftime(invoice_date, '%a, %-d %b %Y at %I:%M:%S %p') AS d7,
  strftime(invoice_date, '%+') AS d8,
  strftime(invoice_date, '%-d/%-m/%y') AS d9,
  strftime(invoice_date, '%-Hh %Mmin') AS d10,
  strftime(invoice_date, '%M''%S"') AS d11,
  strftime(invoice_date, '100%% in %d days') AS d12
FROM
  invoices
LIMIT
  20
`,
		},
		{
			name: "switch_case_display",
			prql: `
from tracks
sort milliseconds
select display = case [
    composer != null => composer,
    genre_id < 17 => 'no composer',
    true => f'unknown composer'
]
take 10
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    CASE
      WHEN composer IS NOT NULL THEN composer
      WHEN genre_id < 17 THEN 'no composer'
      ELSE 'unknown composer'
    END AS display,
    milliseconds
  FROM
    tracks
  ORDER BY
    milliseconds
  LIMIT
    10
)
SELECT
  display
FROM
  table_0
ORDER BY
  milliseconds
`,
		},
		{
			name: "loop_recursive_numbers",
			prql: `
from [{n = 1}]
select n = n - 2
loop (filter n < 4 | select n = n + 1)
select n = n * 2
sort n
`,
			wantSQL: `
WITH RECURSIVE table_0 AS (
  SELECT
    1 AS n
),
table_1 AS (
  SELECT
    n - 2 AS _expr_0
  FROM
    table_0
  UNION ALL
  SELECT
    _expr_0 + 1
  FROM
    table_1
  WHERE
    _expr_0 < 4
)
SELECT
  _expr_0 * 2 AS n
FROM
  table_1 AS table_2
ORDER BY
  n
`,
		},
		{
			name: "genre_counts",
			prql: `
let genre_count = (
    from genres
    aggregate {a = count name}
)

from genre_count
filter a > 0
select a = -a
`,
			wantSQL: `
WITH genre_count AS (
  SELECT
    COUNT(*) AS a
  FROM
    genres
)
SELECT
  - a AS a
FROM
  genre_count
WHERE
  a > 0
`,
		},
		{
			name: "let_binding_simple_cte",
			prql: `
let top_customers = (
    from invoices
    aggregate { total = count invoice_id }
)

from top_customers
select total
`,
			wantSQL: `
WITH top_customers AS (
  SELECT
    COUNT(*) AS total
  FROM
    invoices
)
SELECT
  total
FROM
  top_customers
`,
		},
		{
			name: "group_sort_basic",
			prql: `
from tracks
derive d = album_id + 1
group d (
    aggregate {
        n1 = (track_id | sum),
    }
)
sort d
take 10
select { d1 = d, n1 }
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    COALESCE(SUM(track_id), 0) AS n1,
    album_id + 1 AS _expr_0
  FROM
    tracks
  GROUP BY
    album_id + 1
),
table_1 AS (
  SELECT
    _expr_0 AS d1,
    n1,
    _expr_0
  FROM
    table_0
  ORDER BY
    _expr_0
  LIMIT
    10
)
SELECT
  d1,
  n1
FROM
  table_1
ORDER BY
  d1
`,
		},
		{
			name: "append_select_simple_filter",
			prql: `
from invoices
select { invoice_id, billing_country }
append (
  from invoices
  select { invoice_id = ` + "`invoice_id`" + ` + 100, billing_country }
)
filter (billing_country | text.starts_with("I"))
`,
			wantSQL: `
WITH table_1 AS (
  SELECT
    invoice_id,
    billing_country
  FROM
    invoices
  UNION
  ALL
  SELECT
    invoice_id + 100 AS invoice_id,
    billing_country
  FROM
    invoices
)
SELECT
  invoice_id,
  billing_country
FROM
  table_1
WHERE
  billing_country LIKE CONCAT('I', '%')
`,
		},
		{
			name: "append_select_compute",
			prql: `
from invoices
derive total = case [total < 10 => total * 2, true => total]
select { customer_id, invoice_id, total }
take 5
append (
  from invoice_items
  derive unit_price = case [unit_price < 1 => unit_price * 2, true => unit_price]
  select { invoice_line_id, invoice_id, unit_price }
  take 5
)
select { a = customer_id * 2, b = math.round 1 (invoice_id * total) }
`,
			wantSQL: `
WITH table_1 AS (
  SELECT
    *
  FROM
    (
      SELECT
        invoice_id,
        CASE
          WHEN total < 10 THEN total * 2
          ELSE total
        END AS _expr_0,
        customer_id
      FROM
        invoices
      LIMIT
        5
    ) AS table_3
  UNION
  ALL
  SELECT
    *
  FROM
    (
      SELECT
        invoice_id,
        CASE
          WHEN unit_price < 1 THEN unit_price * 2
          ELSE unit_price
        END AS unit_price,
        invoice_line_id
      FROM
        invoice_items
      LIMIT
        5
    ) AS table_4
)
SELECT
  customer_id * 2 AS a,
  ROUND(invoice_id * _expr_0, 1) AS b
FROM
  table_1
`,
		},
		{
			name: "take_range_with_sort",
			prql: `
from tracks
sort {+track_id}
take 3..5
`,
			wantSQL: `
SELECT
  *
FROM
  tracks
ORDER BY
  track_id
LIMIT
  3 OFFSET 2
`,
		},
		{
			name: "sort_with_join_alias",
			prql: `
from e=employees
filter first_name != "Mitchell"
sort {first_name, last_name}

join manager=employees side:left (e.reports_to == manager.employee_id)

select {e.first_name, e.last_name, manager.first_name}
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    first_name,
    last_name,
    reports_to
  FROM
    employees AS e
  WHERE
    first_name <> 'Mitchell'
)
SELECT
  table_0.first_name,
  table_0.last_name,
  manager.first_name
FROM
  table_0
  LEFT OUTER JOIN employees AS manager ON table_0.reports_to = manager.employee_id
ORDER BY
  table_0.first_name,
  table_0.last_name
`,
		},
		{
			name: "sort_alias_filter_join",
			prql: `
from albums
select { AA=album_id, artist_id }
sort AA
filter AA >= 25
join artists (==artist_id)
`,
			wantSQL: `
WITH table_1 AS (
  SELECT
    album_id AS "AA",
    artist_id
  FROM
    albums
),
table_0 AS (
  SELECT
    "AA",
    artist_id
  FROM
    table_1
  WHERE
    "AA" >= 25
)
SELECT
  table_0."AA",
  table_0.artist_id,
  artists.*
FROM
  table_0
  INNER JOIN artists ON table_0.artist_id = artists.artist_id
ORDER BY
  table_0."AA"
`,
		},
		{
			name: "constants_only",
			prql: `
from genres
take 10
filter true
take 20
filter true
select d = 10
`,
			wantSQL: `
WITH pipe_0 AS (
  SELECT
    *
  FROM
    genres
  LIMIT
    10
),
pipe_1 AS (
  SELECT
    *
  FROM
    pipe_0
  WHERE
    true
  LIMIT
    20
)
SELECT
  10 AS d
FROM
  pipe_1
WHERE
  true
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
			name: "append_select_simple",
			prql: `
from invoices
select { invoice_id, billing_country }
append (
  from invoices
  select { invoice_id = invoice_id + 100, billing_country }
)
filter (billing_country | text.starts_with "I")
`,
			wantSQL: `
WITH table_1 AS (
  SELECT
    invoice_id,
    billing_country
  FROM
    invoices
  UNION
  ALL
  SELECT
    invoice_id + 100 AS invoice_id,
    billing_country
  FROM
    invoices
)
SELECT
  invoice_id,
  billing_country
FROM
  table_1
WHERE
  billing_country LIKE CONCAT('I', '%')
`,
		},
		{
			name: "append_select_multiple_with_null",
			prql: `
from invoices
select { customer_id, invoice_id, billing_country }
take 5
append (
  from employees
  select { employee_id, employee_id, country }
  take 5
)
append (
  from invoice_items
  select { invoice_line_id, invoice_id, null }
  take 5
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
      5
  ) AS table_4
UNION
ALL
SELECT
  *
FROM
  (
    SELECT
      country,
      employee_id
    FROM
      employees
    LIMIT
      5
  ) AS table_5
UNION
ALL
SELECT
  *
FROM
  (
    SELECT
      NULL,
      invoice_id
    FROM
      invoice_items
    LIMIT
      5
  ) AS table_6
`,
		},
		{
			name: "append_select_nulls",
			prql: `
from invoices
select {an_id = invoice_id, name = null}
take 2
append (
  from employees
  select {an_id = null, name = first_name}
  take 2
)
`,
			wantSQL: `
SELECT
  *
FROM
  (
    SELECT
      invoice_id AS an_id,
      NULL AS name
    FROM
      invoices
    LIMIT
      2
  ) AS table_2
UNION
ALL
SELECT
  *
FROM
  (
    SELECT
      NULL AS an_id,
      first_name AS name
    FROM
      employees
    LIMIT
      2
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
			name: "text_module_filters",
			prql: `
from albums
select {
    title,
    title_and_spaces = f"  {title}  ",
    low = (title | text.lower),
    up = (title | text.upper),
    ltrimmed = (title | text.ltrim),
    rtrimmed = (title | text.rtrim),
    trimmed = (title | text.trim),
    len = (title | text.length),
    subs = (title | text.extract 2 5),
    replace = (title | text.replace "al" "PIKA"),
}
sort {title}
filter (title | text.starts_with "Black") || (title | text.contains "Sabbath") || (title | text.ends_with "os")
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    title,
    CONCAT('  ', title, '  ') AS title_and_spaces,
    LOWER(title) AS low,
    UPPER(title) AS up,
    LTRIM(title) AS ltrimmed,
    RTRIM(title) AS rtrimmed,
    TRIM(title) AS trimmed,
    CHAR_LENGTH(title) AS len,
    SUBSTRING(title, 2, 5) AS subs,
    REPLACE(title, 'al', 'PIKA') AS "replace"
  FROM
    albums
)
SELECT
  title,
  title_and_spaces,
  low,
  up,
  ltrimmed,
  rtrimmed,
  trimmed,
  len,
  subs,
  "replace"
FROM
  table_0
WHERE
  title LIKE CONCAT('Black', '%')
  OR title LIKE CONCAT('%', 'Sabbath', '%')
  OR title LIKE CONCAT('%', 'os')
ORDER BY
  title
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
WITH table_0 AS (
  SELECT
    name,
    composer,
    track_id
  FROM
    tracks
  WHERE
    REGEXP(name, 'Love')
    AND milliseconds / 1000 / 60 BETWEEN 3 AND 4
  ORDER BY
    track_id
  LIMIT
    15
)
SELECT
  name,
  composer
FROM
  table_0
ORDER BY
  track_id
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
WITH table_0 AS (
  SELECT
    DISTINCT album_id,
    genre_id
  FROM
    tracks
)
SELECT
  album_id,
  genre_id
FROM
  table_0
ORDER BY
  album_id,
  genre_id
`,
		},
		{
			name: "arithmetic_div_mod",
			prql: `
from [
    { id = 1, x_int =  13, x_float =  13.0, k_int =  5, k_float =  5.0 },
    { id = 2, x_int = -13, x_float = -13.0, k_int =  5, k_float =  5.0 },
    { id = 3, x_int =  13, x_float =  13.0, k_int = -5, k_float = -5.0 },
    { id = 4, x_int = -13, x_float = -13.0, k_int = -5, k_float = -5.0 },
]
select {
    id,

    x_int / k_int,
    x_int / k_float,
    x_float / k_int,
    x_float / k_float,

    q_ii = x_int // k_int,
    q_if = x_int // k_float,
    q_fi = x_float // k_int,
    q_ff = x_float // k_float,

    r_ii = x_int % k_int,
    r_if = x_int % k_float,
    r_fi = x_float % k_int,
    r_ff = x_float % k_float,

    (q_ii * k_int + r_ii | math.round 0),
    (q_if * k_float + r_if | math.round 0),
    (q_fi * k_int + r_fi | math.round 0),
    (q_ff * k_float + r_ff | math.round 0),
}
sort id
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    1 AS id,
    13 AS x_int,
    13.0 AS x_float,
    5 AS k_int,
    5.0 AS k_float
  UNION
  ALL
  SELECT
    2 AS id,
    -13 AS x_int,
    -13.0 AS x_float,
    5 AS k_int,
    5.0 AS k_float
  UNION
  ALL
  SELECT
    3 AS id,
    13 AS x_int,
    13.0 AS x_float,
    -5 AS k_int,
    -5.0 AS k_float
  UNION
  ALL
  SELECT
    4 AS id,
    -13 AS x_int,
    -13.0 AS x_float,
    -5 AS k_int,
    -5.0 AS k_float
)
SELECT
  id,
  x_int / k_int,
  x_int / k_float,
  x_float / k_int,
  x_float / k_float,
  FLOOR(ABS(x_int / k_int)) * SIGN(x_int) * SIGN(k_int) AS q_ii,
  FLOOR(ABS(x_int / k_float)) * SIGN(x_int) * SIGN(k_float) AS q_if,
  FLOOR(ABS(x_float / k_int)) * SIGN(x_float) * SIGN(k_int) AS q_fi,
  FLOOR(ABS(x_float / k_float)) * SIGN(x_float) * SIGN(k_float) AS q_ff,
  x_int % k_int AS r_ii,
  x_int % k_float AS r_if,
  x_float % k_int AS r_fi,
  x_float % k_float AS r_ff,
  ROUND(
    FLOOR(ABS(x_int / k_int)) * SIGN(x_int) * SIGN(k_int) * k_int + x_int % k_int,
    0
  ),
  ROUND(
    FLOOR(ABS(x_int / k_float)) * SIGN(x_int) * SIGN(k_float) * k_float + x_int % k_float,
    0
  ),
  ROUND(
    FLOOR(ABS(x_float / k_int)) * SIGN(x_float) * SIGN(k_int) * k_int + x_float % k_int,
    0
  ),
  ROUND(
    FLOOR(ABS(x_float / k_float)) * SIGN(x_float) * SIGN(k_float) * k_float + x_float % k_float,
    0
  )
FROM
  table_0
ORDER BY
  id
`,
		},
		{
			name: "set_ops_remove",
			prql: `
let distinct = rel -> (from t = _param.rel | group {t.*} (take 1))

from_text format:json '{ "columns": ["a"], "data": [[1], [2], [2], [3]] }'
distinct
remove (from_text format:json '{ "columns": ["a"], "data": [[1], [2]] }')
sort a
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    1 AS a
  UNION
  ALL
  SELECT
    2 AS a
  UNION
  ALL
  SELECT
    2 AS a
  UNION
  ALL
  SELECT
    3 AS a
),
table_1 AS (
  SELECT
    1 AS a
  UNION
  ALL
  SELECT
    2 AS a
),
table_2 AS (
  SELECT
    a
  FROM
    table_0
  EXCEPT
    DISTINCT
  SELECT
    *
  FROM
    table_1
)
SELECT
  a
FROM
  table_2
ORDER BY
  a
`,
		},
		{
			name: "group_sort_derive_select_join",
			prql: `
s"SELECT album_id,title,artist_id FROM albums"
group {artist_id} (aggregate { album_title_count = count this.` + "`title`" + `})
sort {this.artist_id, this.album_title_count}
derive {new_album_count = this.album_title_count}
select {this.artist_id, this.new_album_count}
join side:left ( s"SELECT artist_id,name as artist_name FROM artists" ) (this.artist_id == that.artist_id)
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    album_id,
    title,
    artist_id
  FROM
    albums
),
table_4 AS (
  SELECT
    artist_id,
    COUNT(*) AS _expr_0
  FROM
    table_0
  GROUP BY
    artist_id
),
table_2 AS (
  SELECT
    artist_id,
    _expr_0 AS new_album_count,
    _expr_0
  FROM
    table_4
),
table_1 AS (
  SELECT
    artist_id,
    name as artist_name
  FROM
    artists
),
table_3 AS (
  SELECT
    table_2.artist_id,
    table_2.new_album_count,
    table_1.artist_id AS _expr_1,
    table_1.artist_name,
    table_2._expr_0
  FROM
    table_2
    LEFT OUTER JOIN table_1 ON table_2.artist_id = table_1.artist_id
)
SELECT
  artist_id,
  new_album_count,
  _expr_1,
  artist_name
FROM
  table_3
 ORDER BY
  artist_id,
  new_album_count
`,
		},
		{
			name: "cast_projection",
			prql: `
from tracks
sort {-bytes}
select {
    name,
    bin = ((album_id | as REAL) * 99)
}
take 20
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    name,
    CAST(album_id AS REAL) * 99 AS bin,
    bytes
  FROM
    tracks
  ORDER BY
    bytes DESC
  LIMIT
    20
)
SELECT
  name,
  bin
FROM
  table_0
ORDER BY
  bytes DESC
`,
		},
		{
			name: "distinct_on_group_sort_take",
			prql: `
from tracks
select {genre_id, media_type_id, album_id}
group {genre_id, media_type_id} (sort {-album_id} | take 1)
sort {-genre_id, media_type_id}
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    genre_id,
    media_type_id,
    album_id,
    ROW_NUMBER() OVER (
      PARTITION BY genre_id,
      media_type_id
      ORDER BY
        album_id DESC
    ) AS _expr_0
  FROM
    tracks
)
SELECT
  genre_id,
  media_type_id,
  album_id
FROM
  table_0
WHERE
  _expr_0 <= 1
ORDER BY
  genre_id DESC,
  media_type_id
`,
		},
		{
			name: "group_sort_limit_take_join",
			prql: `
from tracks
select {genre_id,milliseconds}
group {genre_id} (
  sort {-milliseconds}
  take 3
)
join genres (==genre_id)
select {name, milliseconds}
sort {+name,-milliseconds}
`,
			wantSQL: `
WITH table_1 AS (
  SELECT
    milliseconds,
    genre_id,
    ROW_NUMBER() OVER (
      PARTITION BY genre_id
      ORDER BY
        milliseconds DESC
    ) AS _expr_0
  FROM
    tracks
),
table_0 AS (
  SELECT
    milliseconds,
    genre_id
  FROM
    table_1
  WHERE
    _expr_0 <= 3
)
SELECT
  genres.name,
  table_0.milliseconds
FROM
  table_0
  INNER JOIN genres ON table_0.genre_id = genres.genre_id
ORDER BY
  genres.name,
  table_0.milliseconds DESC
`,
		},
		{
			name: "group_sort_filter_derive_select_join",
			prql: `
s"SELECT album_id,title,artist_id FROM albums"
group {artist_id} (aggregate { album_title_count = count this.` + "`title`" + `})
sort {this.artist_id, this.album_title_count}
filter (this.album_title_count) > 10
derive {new_album_count = this.album_title_count}
select {this.artist_id, this.new_album_count}
join side:left ( s"SELECT artist_id,name as artist_name FROM artists" ) (this.artist_id == that.artist_id)
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    album_id,
    title,
    artist_id
  FROM
    albums
),
table_3 AS (
  SELECT
    artist_id,
    COUNT(*) AS _expr_0
  FROM
    table_0
  GROUP BY
    artist_id
),
table_4 AS (
  SELECT
    artist_id,
    _expr_0 AS new_album_count,
    _expr_0
  FROM
    table_3
  WHERE
    _expr_0 > 10
),
table_2 AS (
  SELECT
    artist_id,
    new_album_count,
    _expr_0
  FROM
    table_4
),
table_1 AS (
  SELECT
    artist_id,
    name as artist_name
  FROM
    artists
)
SELECT
  table_2.artist_id,
  table_2.new_album_count,
  table_1.artist_id,
  table_1.artist_name
FROM
  table_2
  LEFT OUTER JOIN table_1 ON table_2.artist_id = table_1.artist_id
ORDER BY
  table_2.artist_id,
  table_2.new_album_count
`,
		},
		{
			name: "invoice_totals_window_join",
			prql: `
from i=invoices
join ii=invoice_items (==invoice_id)
derive {
    city = i.billing_city,
    street = i.billing_address,
}
group {city, street} (
    derive total = ii.unit_price * ii.quantity
    aggregate {
        num_orders = count_distinct i.invoice_id,
        num_tracks = sum ii.quantity,
        total_price = sum total,
    }
)
group {city} (
    sort street
    window expanding:true (
        derive {running_total_num_tracks = sum num_tracks}
    )
)
sort {city, street}
derive {num_tracks_last_week = lag 7 num_tracks}
select {
    city,
    street,
    num_orders,
    num_tracks,
    running_total_num_tracks,
    num_tracks_last_week
}
take 20
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    i.billing_city AS city,
    i.billing_address AS street,
    COUNT(DISTINCT i.invoice_id) AS num_orders,
    COALESCE(SUM(ii.quantity), 0) AS num_tracks
  FROM
    invoices AS i
    INNER JOIN invoice_items AS ii ON i.invoice_id = ii.invoice_id
  GROUP BY
    i.billing_city,
    i.billing_address
)
SELECT
  city,
  street,
  num_orders,
  num_tracks,
  SUM(num_tracks) OVER (
    PARTITION BY city
    ORDER BY
      street ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_total_num_tracks,
  LAG(num_tracks, 7) OVER (
    ORDER BY
      city,
      street
  ) AS num_tracks_last_week
FROM
  table_0
ORDER BY
  city,
  street
LIMIT
  20
`,
		},
		{
			name: "group_all_join_aggregate",
			prql: `
from a=albums
take 10
join tracks (==album_id)
group {a.album_id, a.title} (
  aggregate price = (sum tracks.unit_price | math.round 2)
)
sort album_id
`,
			wantSQL: `
WITH pipe_0 AS (
  SELECT
    *
  FROM
    albums AS a
  LIMIT
    10
),
table_0 AS (
  SELECT
    a.album_id,
    a.title
  FROM
    pipe_0
)
SELECT
  table_0.album_id,
  table_0.title,
  ROUND(COALESCE(SUM(tracks.unit_price), 0), 2) AS price
FROM
  table_0
  INNER JOIN tracks ON table_0.album_id = tracks.album_id
GROUP BY
  table_0.album_id,
  table_0.title
ORDER BY
  table_0.album_id
`,
		},
		{
			name: "read_csv_sort",
			prql: `
from (read_csv "data_file_root/media_types.csv")
sort media_type_id
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    *
  FROM
    read_csv('data_file_root/media_types.csv')
)
SELECT
  *
FROM
  table_0
ORDER BY
  media_type_id
`,
		},
		{
			name: "sort_preserved_through_join",
			prql: `
from e=employees
filter first_name != "Mitchell"
sort {first_name, last_name}
join manager=employees side:left (e.reports_to == manager.employee_id)
select {e.first_name, e.last_name, manager.first_name}
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    first_name,
    last_name,
    reports_to
  FROM
    employees AS e
  WHERE
    first_name <> 'Mitchell'
)
SELECT
  table_0.first_name,
  table_0.last_name,
  manager.first_name
FROM
  table_0
  LEFT OUTER JOIN employees AS manager ON table_0.reports_to = manager.employee_id
ORDER BY
  table_0.first_name,
  table_0.last_name
`,
		},
		{
			name: "sort_alias_join",
			prql: `
from albums
select { AA=album_id, artist_id }
sort AA
filter AA >= 25
join artists (==artist_id)
`,
			wantSQL: `
WITH table_1 AS (
  SELECT
    album_id AS "AA",
    artist_id
  FROM
    albums
),
table_0 AS (
  SELECT
    "AA",
    artist_id
  FROM
    table_1
  WHERE
    "AA" >= 25
)
SELECT
  table_0."AA",
  table_0.artist_id,
  artists.*
FROM
  table_0
  INNER JOIN artists ON table_0.artist_id = artists.artist_id
ORDER BY
  table_0."AA"
`,
		},
		{
			name: "sort_alias_inline_sources",
			prql: `
from [{track_id=0, album_id=1, genre_id=2}]
select { AA=track_id, album_id, genre_id }
sort AA
join side:left [{album_id=1, album_title="Songs"}] (==album_id)
select { AA, AT = album_title ?? "unknown", genre_id }
filter AA < 25
join side:left [{genre_id=1, genre_title="Rock"}] (==genre_id)
select { AA, AT, GT = genre_title ?? "unknown" }
`,
			wantSQL: `
WITH table_0 AS (
  SELECT
    0 AS track_id,
    1 AS album_id,
    2 AS genre_id
),
table_5 AS (
  SELECT
    track_id AS "AA",
    genre_id,
    album_id
  FROM
    table_0
),
table_1 AS (
  SELECT
    1 AS album_id,
    'Songs' AS album_title
),
table_4 AS (
  SELECT
    table_5."AA",
    COALESCE(table_1.album_title, 'unknown') AS "AT",
    table_5.genre_id
  FROM
    table_5
    LEFT OUTER JOIN table_1 ON table_5.album_id = table_1.album_id
),
table_3 AS (
  SELECT
    "AA",
    "AT",
    genre_id
  FROM
    table_4
  WHERE
    "AA" < 25
),
table_2 AS (
  SELECT
    1 AS genre_id,
    'Rock' AS genre_title
)
SELECT
  table_3."AA",
  table_3."AT",
  COALESCE(table_2.genre_title, 'unknown') AS "GT"
FROM
  table_3
  LEFT OUTER JOIN table_2 ON table_3.genre_id = table_2.genre_id
ORDER BY
  table_3."AA"
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
	return strings.Join(strings.Fields(strings.TrimSpace(s)), "")
}
