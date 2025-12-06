gophrql
=======

Reference Go implementation of the concepts from the [PRQL book](https://prql-lang.org/book/). The goal is to provide a clear, well-documented library that compiles PRQL to SQL while drawing inspiration from the upstream [prql](https://github.com/PRQL/prql) project.

## Status

Early scaffolding. The core compiler is not implemented yet; expect rapid iteration and breaking changes until the API stabilizes.

## Getting started

```bash
go get github.com/maxpert/gophrql
```

```go
package main

import (
	"fmt"

	"github.com/maxpert/gophrql"
)

func main() {
	sql, err := gophrql.Compile("from employees | select first_name")
	if err != nil {
		panic(err)
	}
	fmt.Println(sql)
}
```

## Development

- Go 1.21+ recommended.
- Tests: `go test ./...`
- Source of truth: the [PRQL book](https://prql-lang.org/book/) and the upstream [prql](https://github.com/PRQL/prql) reference implementation.

## Contributing

Issues and PRs are welcome. Please keep changes small, add tests where possible, and document any deviations from the PRQL book or upstream behavior in `AGENTS.md`.
