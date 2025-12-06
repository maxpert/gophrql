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


## Engineering Principles to Enforce

| Principle | Description |
|-----------|-------------|
| **KISS** | Keep It Simple, Stupid. Reject unnecessary complexity. |
| **DRY** | Don't Repeat Yourself. Flag duplicated logic. |
| **YAGNI** | You Aren't Gonna Need It. Remove speculative code. |
| **SOLID** | Single responsibility, Open/closed, Liskov substitution, Interface segregation, Dependency inversion. |
| **SoC** | Separation of Concerns. Each package/function has one job. |
| **LoD** | Law of Demeter. Minimize knowledge between components. |
| **Fail Fast** | Validate early, return early. |
| **Explicit > Implicit** | No magic. Clear intent. |

---

## Severity Classification

### CRITICAL (must fix before merge)
- Security vulnerabilities (injection, auth bypass, secrets in code)
- Data corruption or loss risks
- Race conditions causing crashes
- Unrecovered panics in goroutines
- Unbounded resource consumption (memory leaks, goroutine leaks)
- Broken error handling that silences failures

### HIGH (must fix, blocking)
- Unchecked errors on I/O, network, or database operations
- Missing context cancellation propagation
- Improper mutex usage (deadlock potential, unlock not deferred)
- Nil pointer dereference risks
- SQL injection or command injection vectors
- Missing input validation on public APIs

### MEDIUM (should fix)
- Non-idiomatic Go patterns
- Stuttering names (`user.UserName` → `user.Name`)
- Large functions (>50 lines) that should be split
- Missing godoc on exported symbols
- Inconsistent error wrapping
- Magic numbers without constants
- Test coverage gaps on critical paths
- Suboptimal allocations in hot paths

### LOW (nice to have)
- Minor naming improvements
- Comment typos or formatting
- Import ordering
- Redundant else blocks
- Slightly verbose code that could be condensed
- Missing test cases for edge cases
