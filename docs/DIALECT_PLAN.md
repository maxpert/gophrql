PRQL Dialect Support Plan
=========================

Goals
-----
- Provide a clear roadmap for matching the official PRQL compiler’s SQL dialect coverage.
- Enumerate required features, blockers, and validation artifacts (snapshots/tests) per dialect.
- Keep the effort incremental so snapshots stay green after each dialect landing.

Current State
-------------
- The Go compiler currently emits a single “generic” SQL flavor tuned to the integration snapshots (≈ PostgreSQL syntax).
- No `target sql.<dialect>` selection hook is exposed to callers.
- Dialect-specific constructs (identifier quoting, function names, LIMIT/OFFSET semantics, date functions, joins, window quirks) are hard-coded for the generic flavor.

Priority Dialects
-----------------

| Dialect         | Status   | Required Work                                                                                     | References                                             |
|-----------------|----------|----------------------------------------------------------------------------------------------------|--------------------------------------------------------|
| `sql.generic`   | ✅ (base) | Keep as compatibility fallback for snapshots.                                                     | Existing `compile_test.go` and upstream `compile__*.snap` |
| `sql.postgres`  | 🟡        | Add target flag, Postgres-specific casting (e.g., `::type`), JSON ops, `ILIKE`, quoted identifiers.| `prqlc` `Target::Postgres`, `snapshots/...postgres...` |
| `sql.sqlite`    | 🟡        | Handle lack of `WITH RECURSIVE` in some scenarios, `strftime` formats, limited window support.     | `snapshots/...sqlite...`, book’s SQLite notes          |
| `sql.duckdb`    | 🟡        | Support `read_csv_auto`, `MAP` types, `LIMIT` ordering semantics.                                 | DuckDB tutorial + `prqlc` target                       |
| `sql.mysql`     | 🟡        | Switch to backtick quoting, `LIMIT offset, count`, no `WITH RECURSIVE` (fallback to temp tables). | MySQL target snapshots                                 |
| `sql.mssql`     | 🟡        | `TOP`, `OFFSET FETCH`, string concatenation `+`, `DATEPART`.                                       | `# mssql:test` fixtures already in PRQL repo           |
| `sql.bigquery`  | ⬜        | Backtick quoting, `STRUCT`, `UNNEST`, positional parameters.                                      | Official PRQL backlog                                  |
| `sql.clickhouse`| ⬜        | `ARRAY JOIN`, limited `WITH`, distinct ordering semantics.                                        | Upstream `clickhouse` snapshots                        |

Legend: ✅ done, 🟡 planned (near-term), ⬜ later.

Implementation Phases
---------------------

1. **Target Selection Plumbed**
   - Parse optional `target` statement, expose `Compile(prql string, opts ...Option)`.
   - Default to `sql.generic` for backward compatibility.

2. **Configuration Plumbing**
   - Represent dialect capabilities in a struct (identifier quoting strategy, function remaps, limit syntax, boolean literal style, etc.).
   - Refactor `sqlgen` helpers to depend on that configuration instead of hard-coded literals.

3. **Dialect-by-Dialect Enablement**
   - Postgres: mostly alias for generic but add `ILIKE`, `::type`, JSON operators.
   - SQLite: ensure `strftime`, `LIMIT` semantics, disable unsupported window constructs by lowering them.
   - DuckDB/MySQL/MSSQL: each requires quoting/cast adjustments and new intrinsic mappings.
   - For each dialect, import upstream `integration__queries__compile__*` snapshots and add new Go tests (e.g., `TestCompileSnapshotsSQLite`).

4. **Validation & Tooling**
   - Expand `docs/SNAPSHOTS_PLAN.md` with per-dialect coverage checkboxes.
   - Provide a helper script (`cmd/snapdiff`) to re-run snapshots against upstream PRQL for regression checks.

5. **Future Dialects**
   - Once core SQL engines are covered, evaluate additional targets (BigQuery, Snowflake, ClickHouse) using the same pattern.

Risk & Mitigation
-----------------
- **Config Drift:** Keep dialect configs in a single package with unit tests ensuring defaults match the upstream Rust compiler.
- **Snapshot Explosion:** Gate new dialect tests behind build tags or sub-tests to keep runtime manageable.
- **Feature Gaps:** Document unsupported PRQL features per dialect in the README until parity is reached.

Next Actions
------------
1. Define a `sqlgen.Dialect` struct + registry.
2. Add `CompileOptions{Target string}` plumbing.
3. Port Postgres-specific snapshot tests to verify plumbing.
4. Iterate through the priority table above, updating docs/tests per dialect.
