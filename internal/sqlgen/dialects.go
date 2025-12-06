package sqlgen

import "strings"

// DialectMap holds the registered dialects.
var DialectMap = map[string]*Dialect{
	"sql.generic":    DefaultDialect,
	"sql.postgres":   PostgresDialect,
	"sql.sqlite":     SQLiteDialect,
	"sql.duckdb":     DuckDBDialect,
	"sql.mysql":      MySQLDialect,
	"sql.mssql":      MSSQLDialect,
	"sql.clickhouse": ClickHouseDialect,
	"sql.bigquery":   BigQueryDialect,
	"sql.snowflake":  SnowflakeDialect,
}

// GetDialect returns the dialect for the given target, or nil if not found.
// It tries to match by exact string first, then by the dialect type.
func GetDialect(target string) *Dialect {
	if d, ok := DialectMap[target]; ok {
		return d
	}
	// Fallback/Aliases
	switch strings.ToLower(target) {
	case "postgres", "postgresql":
		return PostgresDialect
	case "sqlite":
		return SQLiteDialect
	case "duckdb":
		return DuckDBDialect
	case "mysql":
		return MySQLDialect
	case "mssql", "sqlserver":
		return MSSQLDialect
	case "clickhouse":
		return ClickHouseDialect
	case "bigquery":
		return BigQueryDialect
	case "snowflake":
		return SnowflakeDialect
	}
	return nil
}

// PostgresDialect defines the dialect for PostgreSQL.
var PostgresDialect = &Dialect{
	Type:           DialectPostgres,
	IdentQuoteChar: '"',
	UseLimitOffset: true,
	Functions: map[string]string{
		"date.to_text": "TO_CHAR(%[1]s, %[2]s)", // date, format
	},
}

// SQLiteDialect defines the dialect for SQLite.
var SQLiteDialect = &Dialect{
	Type:           DialectSQLite,
	IdentQuoteChar: '"',
	UseLimitOffset: true,
	Functions:      map[string]string{},
}

// DuckDBDialect defines the dialect for DuckDB.
var DuckDBDialect = &Dialect{
	Type:           DialectDuckDB,
	IdentQuoteChar: '"',
	UseLimitOffset: true,
	Functions: map[string]string{
		"std.read_csv": "read_csv_auto",
		"date.to_text": "strftime(%[1]s, %[2]s)", // DuckDB: strftime(date, format)
	},
}

// MySQLDialect defines the dialect for MySQL.
var MySQLDialect = &Dialect{
	Type:           DialectMySQL,
	IdentQuoteChar: '`',
	UseLimitComma:  true, // LIMIT offset, count
	Functions: map[string]string{
		"date.to_text": "DATE_FORMAT(%[1]s, %[2]s)",
	},
}

// MSSQLDialect defines the dialect for Microsoft SQL Server.
var MSSQLDialect = &Dialect{
	Type:              DialectMSSQL,
	IdentQuoteChar:    '"',
	UseTopClause:      true, // TOP N
	OffsetFetchSyntax: true, // OFFSET M ROWS FETCH NEXT N ROWS ONLY
	Functions: map[string]string{
		"math.ceil": "CEILING(%s)",
		"math.ln":   "LOG(%s)",
		"math.pow":  "POWER(%s, %s)",
	},
}

// ClickHouseDialect defines the dialect for ClickHouse.
var ClickHouseDialect = &Dialect{
	Type:           DialectClickHouse,
	IdentQuoteChar: '"', // OR backticks, " is standard SQL
	UseLimitOffset: true,
	Functions: map[string]string{
		"date.to_text": "formatDateTimeInJodaSyntax(%[1]s, %[2]s)",
	},
}

// BigQueryDialect defines the dialect for BigQuery.
var BigQueryDialect = &Dialect{
	Type:           DialectBigQuery,
	IdentQuoteChar: '`',
	UseLimitOffset: true,
	Functions:      map[string]string{},
}

// SnowflakeDialect defines the dialect for Snowflake.
var SnowflakeDialect = &Dialect{
	Type:           DialectSnowflake,
	IdentQuoteChar: '"',
	UseLimitOffset: true,
	Functions:      map[string]string{},
}
