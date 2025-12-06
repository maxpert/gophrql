package sqlgen

import (
	"strings"
)

type DialectType string

const (
	DialectGeneric    DialectType = "sql.generic"
	DialectPostgres   DialectType = "sql.postgres"
	DialectSQLite     DialectType = "sql.sqlite"
	DialectDuckDB     DialectType = "sql.duckdb"
	DialectMySQL      DialectType = "sql.mysql"
	DialectMSSQL      DialectType = "sql.mssql"
	DialectClickHouse DialectType = "sql.clickhouse"
	DialectBigQuery   DialectType = "sql.bigquery"
	DialectSnowflake  DialectType = "sql.snowflake"
)

// Dialect defines the capabilities and syntax variations for a SQL target.
type Dialect struct {
	Type DialectType

	// Identifier quoting
	IdentQuoteChar byte // 0 for no quoting/default

	// Limit/Offset handling
	UseTopClause      bool // SELECT TOP N ...
	UseLimitOffset    bool // LIMIT N OFFSET M
	UseLimitComma     bool // LIMIT M, N (MySQL style)
	OffsetFetchSyntax bool // OFFSET M ROWS FETCH NEXT N ROWS ONLY

	// Function mapping overrides
	// key: PRQL function name (e.g. "math.round")
	// value: SQL pattern (e.g. "ROUND(%s, %s)")
	Functions map[string]string
}

// DefaultDialect is the generic dialect (Postgres-like).
var DefaultDialect = &Dialect{
	Type:           DialectGeneric,
	IdentQuoteChar: '"',
	UseLimitOffset: true,
	Functions:      map[string]string{},
}

func (d *Dialect) QuoteIdent(s string) string {
	if s == "*" {
		return "*"
	}
	// If the identifier is safe, don't quote it (unless forced?)
	// We assume generic dialect prefers cleaner SQL like standard PRQL compiler
	if isSafeIdent(s) {
		return s
	}

	q := d.IdentQuoteChar
	if q == 0 {
		return s
	}
	// Simple escaping: duplicate the quote character
	escaped := strings.ReplaceAll(s, string(q), string(q)+string(q))
	return string(q) + escaped + string(q)
}
