gophrql Plan
============

Reference review (Rust `prql`)
------------------------------
- Core tests live in `prqlc/prqlc/tests/integration`: snapshot suites for lexing, formatting round-trips, PRQL → SQL compilation (generic + per-dialect diffs), lineage debug output, and db-backed execution (`results` gated by feature flags).
- Error coverage is in `error_messages.rs` and `bad_error_messages.rs`, asserting precise diagnostics for arity errors, unknown names, type mismatches, dialect constraints, and malformed input.
- Additional unit suites (`sql.rs`) exercise stdlib modules (math, text), dialect-specific SQL lowering, and feature toggles.

Go test strategy (write first)
------------------------------
- Port a representative slice of Rust snapshots as table-driven tests asserting PRQL → SQL (generic dialect) for: aggregation, unions/append, window functions, stdlib math/text helpers, and pipeline transforms.
- Add negative tests asserting error surfaces for empty queries, missing `from`, too many args, bad types (`take 1.8`), and unknown/ambiguous names. Match key substrings to allow formatting differences while keeping semantics strict.
- Future: dialect-specific fixtures (e.g., SQLite vs MSSQL concatenation) and formatting round-trips once formatting API exists.

Implementation roadmap (bottom-up)
----------------------------------
1) Front-end: lexer + parser aligned to PRQL grammar; build an AST that preserves spans for diagnostics and supports module references.
2) Semantic analysis: name resolution, type checking, pipeline validation, stdlib catalog, and user-defined functions; produce enriched IR.
3) SQL planner: relational lowering (projections, joins, windows, set ops), CTE management, column aliasing, and deterministic ordering.
4) Dialect layer: target abstraction for operators/functions, identifiers, limits/offsets, regex, date formatting, and string concat; start with Generic, then SQLite/Postgres/MySQL/MSSQL.
5) Formatting + tooling: PRQL formatter, lineage/introspection outputs, and richer error rendering (codes, spans, hints).
6) Execution harness: optional db-backed golden tests mirroring Rust `results` suite; add CLI/sample programs under `examples/`.

Notes
-----
- Keep fixtures in `testdata/` with PRQL and expected SQL/error text to mirror upstream organization.
- Maintain parity notes in `AGENTS.md` for any intentional deviations from the Rust behavior or the PRQL book.
