Agents Guide
============

Context
-------
- Project: `github.com/maxpert/gophrql`
- Purpose: Reference Go implementation of the PRQL book; inspired by upstream behavior from https://github.com/PRQL/prql where practical.
- Status: Pre-implementation scaffold; public API is not stable.

Expectations
------------
- Fidelity first: match the PRQL book semantics before adding convenience APIs.
- Parity notes: document any intentional differences from upstream `prql` in this file.
- Tests: prefer table-driven tests that quote book examples; add regression tests for every fixed bug.
- Errors: return informative, composable errors; avoid panics.
- Dependencies: keep minimal; avoid cgo and heavy transitive trees.

Initial roadmap
---------------
- Parser: parse PRQL into an AST aligned with the book chapters and upstream definitions.
- Compiler: translate AST to SQL with deterministic output; support dialect abstractions early.
- Diagnostics: helpful error messages that point at spans in the source.
- Examples: executable snippets reflecting book examples (`examples/` folder).

Workflow tips
-------------
- Run `go test ./...` before pushing.
- Keep public surface documented with Go doc comments.
- Prefer small, reviewable commits with context in the messages.
