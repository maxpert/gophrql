# gophrql

[![Go Reference](https://pkg.go.dev/badge/github.com/maxpert/gophrql.svg)](https://pkg.go.dev/github.com/maxpert/gophrql)
[![Go Report Card](https://goreportcard.com/badge/github.com/maxpert/gophrql)](https://goreportcard.com/report/github.com/maxpert/gophrql)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**gophrql** is a Go implementation of [PRQL](https://prql-lang.org) (Pipelined Relational Query Language) — a modern, composable query language that compiles to SQL.

**P**ipelined **R**elational **Q**uery **L**anguage, pronounced "Prequel".

PRQL is a modern language for transforming data — a simple, powerful, pipelined SQL replacement. Like SQL, it's readable, explicit and declarative. Unlike SQL, it forms a logical pipeline of transformations, and supports abstractions such as variables and functions.

## PRQL Language Overview

PRQL queries are pipelines of transformations, where each line transforms the result of the previous line:

```prql
from employees              # Start with a table
filter department == "Sales"  # Filter rows
derive {                    # Add computed columns
  monthly_salary = salary / 12,
  annual_bonus = salary * 0.1
}
select {                    # Choose columns
  first_name,
  last_name, 
  monthly_salary,
  annual_bonus
}
sort {-monthly_salary}      # Sort descending by monthly_salary
take 20                     # Limit results
```

### Key Features

- **Pipelines**: `|` chains transformations (optional, newlines also work)
- **Variables**: Define reusable expressions with `let`
- **Functions**: Create custom transformations
- **Dates**: First-class date support with `@2024-01-01` syntax
- **F-strings**: String interpolation with `f"{first_name} {last_name}"`
- **S-strings**: SQL escape hatch with `s"UPPER(name)"`
- **Comments**: `#` for single-line comments

For the complete language reference, visit [PRQL Book](https://prql-lang.org/book/).

## Features

- ✅ **Full PRQL Syntax Support** - Implements the PRQL language spec
- ✅ **Multi-Dialect SQL Generation** - Postgres, MySQL, SQLite, MSSQL, DuckDB, BigQuery, Snowflake, ClickHouse
- ✅ **Composable Pipelines** - Transform data with intuitive, chained operations
- ✅ **Type-Safe** - Catch errors at compile time, not runtime
- ✅ **Extensible** - Access the AST directly to build custom backends (MongoDB, ElasticSearch, etc.)

## Quick Start

### Installation

```bash
go get github.com/maxpert/gophrql
```

### Basic Usage

```go
package main

import (
    "fmt"
    "github.com/maxpert/gophrql"
)

func main() {
    prql := `
        from employees
        filter department == "Engineering"
        select {first_name, last_name, salary}
        sort {-salary}
        take 10
    `
    
    sql, err := gophrql.Compile(prql)
    if err != nil {
        panic(err)
    }
    
    fmt.Println(sql)
    // Output:
    // SELECT
    //   first_name,
    //   last_name,
    //   salary
    // FROM
    //   employees
    // WHERE
    //   department = 'Engineering'
    // ORDER BY
    //   salary DESC
    // LIMIT 10
}
```

### Dialect-Specific Compilation

```go
// PostgreSQL
sql, err := gophrql.Compile(prql, gophrql.WithTarget("sql.postgres"))

// MySQL
sql, err := gophrql.Compile(prql, gophrql.WithTarget("sql.mysql"))

// Microsoft SQL Server
sql, err := gophrql.Compile(prql, gophrql.WithTarget("sql.mssql"))

// DuckDB
sql, err := gophrql.Compile(prql, gophrql.WithTarget("sql.duckdb"))
```

## Examples

### Aggregations

```go
prql := `
    from orders
    group {customer_id} (
        aggregate {
            total_orders = count this,
            total_revenue = sum amount,
            avg_order_value = average amount
        }
    )
    filter total_revenue > 1000
    sort {-total_revenue}
`

sql, _ := gophrql.Compile(prql)
```

### Joins

```go
prql := `
    from employees
    join departments (==department_id)
    select {
        employees.first_name,
        employees.last_name,
        departments.name
    }
`

sql, _ := gophrql.Compile(prql)
```

### Advanced Transformations

```go
prql := `
    from sales
    derive {
        gross_revenue = quantity * price,
        discount_amount = gross_revenue * discount_rate,
        net_revenue = gross_revenue - discount_amount
    }
    filter net_revenue > 0
    group {product_id, year} (
        aggregate {
            total_quantity = sum quantity,
            total_revenue = sum net_revenue,
            avg_price = average price
        }
    )
`

sql, _ := gophrql.Compile(prql)
```

## Extensibility: Custom Backends

One of gophrql's unique features is exposing the parse tree, allowing you to build custom backends for non-SQL databases. Here's a basic example converting PRQL syntax to a MongoDB aggregation pipeline:

### DuckDB Analytics Demo

Here's a real-world time series analytics query transpiled to DuckDB, based on actual user workflows from the data community. This example analyzes cryptocurrency OHLCV data with moving averages and rolling statistics:

```go
package main

import (
    "fmt"
    "github.com/maxpert/gophrql"
)

func main() {
    prql := `
        # Time series analysis with rolling windows and aggregations
        from ohlcv_data
        filter s"date_part(['year', 'month'], time) = {year: 2021, month: 2}"
        
        # Calculate moving averages and rolling statistics
        window rolling:28 (
            derive {
                ma_28d = average close,
                volatility_28d = stddev close
            }
        )
        
        # Calculate expanding cumulative average
        window rows:..0 (
            derive {
                expanding_avg = average close,
                cumulative_volume = sum volume
            }
        )
        
        # Combine rolling aggregations for Bollinger Bands
        window rows:-15..14 (
            derive {
                rolling_mean = average close,
                rolling_std = stddev close,
                upper_band = average close + 2 * stddev close,
                lower_band = average close - 2 * stddev close
            }
        )
        
        # Final selection with technical indicators
        select {
            time,
            close,
            ma_28d,
            expanding_avg,
            volatility_28d,
            rolling_mean,
            upper_band,
            lower_band,
            volume,
            cumulative_volume
        }
        sort time
        take 10
    `
    
    sql, err := gophrql.Compile(prql, gophrql.WithTarget("sql.duckdb"))
    if err != nil {
        panic(err)
    }
    
    fmt.Println(sql)
    // Output: Optimized DuckDB query with window functions,
    // perfect for financial analysis and time series workloads
}
```

This demonstrates gophrql's ability to handle:
- **Time series filtering** with DuckDB's date functions
- **Window functions** for moving averages and rolling statistics
- **Multiple window frames** (rolling, expanding, centered)
- **Technical indicators** like Bollinger Bands and volatility
- **Complex analytics** common in financial data analysis

Based on real user workflows from [eitsupi/querying-with-prql](https://github.com/eitsupi/querying-with-prql), this example shows how PRQL simplifies complex time series analytics that would be verbose in raw SQL.

### MongoDB Example

```go
package main

import (
    "fmt"
    "strings"

    "github.com/maxpert/gophrql"
    "github.com/maxpert/gophrql/ast"
)

func main() {
    prql := `
        from users
        filter age > 21
        filter country == "US"
        select { name, email, age }
        sort { -age }
        take 10
    `

    // Parse PRQL to an AST
    query, err := gophrql.Parse(prql)
    if err != nil {
        panic(err)
    }

    // Convert AST to MongoDB aggregation pipeline string
    mongo := convertToMongo(query)
    fmt.Println(mongo)
    // db.users.aggregate([
    //   { $match: { age: { $gt: 21 }, country: "US" } },
    //   { $project: { name: 1, email: 1, age: 1, _id: 0 } },
    //   { $sort: { age: -1 } },
    //   { $limit: 10 }
    // ])
}

func convertToMongo(q *ast.Query) string {
    var stages []string

    // Combine all filters into a single $match
    filters := []string{}
    for _, step := range q.Steps {
        if f, ok := step.(*ast.FilterStep); ok {
            if cond := toMongoCondition(f.Expr); cond != "" {
                filters = append(filters, cond)
            }
        }
    }
    if len(filters) > 0 {
        stages = append(stages, fmt.Sprintf("{ $match: { %s } }", strings.Join(filters, ", ")))
    }

    for _, step := range q.Steps {
        switch s := step.(type) {
        case *ast.SelectStep:
            fields := []string{}
            for _, item := range s.Items {
                name := item.As
                if name == "" {
                    name = exprToField(item.Expr)
                }
                fields = append(fields, fmt.Sprintf("%s: 1", name))
            }
            // Exclude _id for clarity
            fields = append(fields, "_id: 0")
            stages = append(stages, fmt.Sprintf("{ $project: { %s } }", strings.Join(fields, ", ")))
        case *ast.SortStep:
            sorts := []string{}
            for _, item := range s.Items {
                dir := 1
                if item.Desc {
                    dir = -1
                }
                sorts = append(sorts, fmt.Sprintf("%s: %d", exprToField(item.Expr), dir))
            }
            if len(sorts) > 0 {
                stages = append(stages, fmt.Sprintf("{ $sort: { %s } }", strings.Join(sorts, ", ")))
            }
        case *ast.TakeStep:
            if s.Limit > 0 {
                stages = append(stages, fmt.Sprintf("{ $limit: %d }", s.Limit))
            }
        }
    }

    return fmt.Sprintf("db.%s.aggregate([%s])", q.From.Table, strings.Join(stages, ", "))
}

func toMongoCondition(e ast.Expr) string {
    b, ok := e.(*ast.Binary)
    if !ok {
        return ""
    }

    field := exprToField(b.Left)
    value := exprToValue(b.Right)

    switch b.Op {
    case "==":
        return fmt.Sprintf("%s: %s", field, value)
    case ">":
        return fmt.Sprintf("%s: { $gt: %s }", field, value)
    case "<":
        return fmt.Sprintf("%s: { $lt: %s }", field, value)
    default:
        return ""
    }
}

func exprToField(e ast.Expr) string {
    if id, ok := e.(*ast.Ident); ok && len(id.Parts) > 0 {
        return strings.Join(id.Parts, ".")
    }
    return e.String()
}

func exprToValue(e ast.Expr) string {
    switch v := e.(type) {
    case *ast.Number:
        return v.Value
    case *ast.StringLit:
        return fmt.Sprintf("\"%s\"", v.Value)
    default:
        return "null"
    }
}
```

See `examples/mongo/main.go` for the full example with more operators and safer parsing.

## PRQL Language Overview

PRQL queries are pipelines of transformations, where each line transforms the result of the previous line:

```prql
from employees              # Start with a table
filter department == "Sales"  # Filter rows
derive {                    # Add computed columns
  monthly_salary = salary / 12,
  annual_bonus = salary * 0.1
}
select {                    # Choose columns
  first_name,
  last_name, 
  monthly_salary,
  annual_bonus
}
sort {-monthly_salary}      # Sort descending by monthly_salary
take 20                     # Limit results
```

### Key Features

- **Pipelines**: `|` chains transformations (optional, newlines also work)
- **Variables**: Define reusable expressions with `let`
- **Functions**: Create custom transformations
- **Dates**: First-class date support with `@2024-01-01` syntax
- **F-strings**: String interpolation with `f"{first_name} {last_name}"`
- **S-strings**: SQL escape hatch with `s"UPPER(name)"`
- **Comments**: `#` for single-line comments

For the complete language reference, visit [PRQL Book](https://prql-lang.org/book/).

## Supported Dialects

| Dialect | Status | Notes |
|---------|--------|-------|
| Generic | ✅ | Postgres-compatible fallback |
| PostgreSQL | ✅ | Full support |
| MySQL | ✅ | Backtick identifiers, LIMIT syntax |
| SQLite | ✅ | Standard SQL subset |
| DuckDB | ✅ | Advanced analytics functions |
| MS SQL Server | ✅ | TOP clause, T-SQL functions |
| BigQuery | ✅ | Google BigQuery syntax |
| Snowflake | ✅ | Snowflake-specific features |
| ClickHouse | ✅ | ClickHouse syntax |

## Development

### Prerequisites

- Go 1.21+

### Building

```bash
go build ./...
```

### Testing

```bash
go test ./...
```

### Running Examples

```bash
go run examples/basic/main.go
go run examples/mongo/main.go
```

## Project Structure

```
gophrql/
├── ast/              # Public AST types
├── internal/
│   ├── parser/       # PRQL parser
│   └── sqlgen/       # SQL generation + dialects
├── examples/         # Usage examples
├── docs/             # Documentation
└── gophrql.go        # Public API
```

## Contributing

Contributions are welcome! Please see [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for community guidelines.

### Guidelines

1. **Keep changes focused** - One feature/fix per PR
2. **Add tests** - Ensure coverage for new features
3. **Follow conventions** - Use `gofmt` and follow existing patterns
4. **Update docs** - Keep README and examples current

## Acknowledgments

This project is inspired by and implements the [PRQL language specification](https://prql-lang.org/book/). Special thanks to the PRQL community and the upstream [prql](https://github.com/PRQL/prql) project.

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

## Resources

- [PRQL Website](https://prql-lang.org)
- [PRQL Book](https://prql-lang.org/book/)
- [PRQL Playground](https://prql-lang.org/playground/)
- [PRQL Discord](https://discord.gg/eQcfaCmsNc)
