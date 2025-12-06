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
