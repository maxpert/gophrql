package gophrql_test

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maxpert/gophrql"
)

func TestIntegrationSnapshots(t *testing.T) {
	// Locate the snapshots directory
	// expected path: tmp/prql/prqlc/prqlc/tests/integration/snapshots
	root := "tmp/prql/prqlc/prqlc/tests/integration/snapshots"
	if _, err := os.Stat(root); os.IsNotExist(err) {
		t.Skipf("Snapshots directory not found at %s; skipping integration tests", root)
	}

	files, err := filepath.Glob(filepath.Join(root, "integration__queries__compile__*.snap"))
	if err != nil {
		t.Fatalf("Failed to glob snapshots: %v", err)
	}

	for _, snapPath := range files {
		testName := filepath.Base(snapPath)
		t.Run(testName, func(t *testing.T) {
			prql, expectedSQL, err := parseSnapshotAndInput(snapPath)
			if err != nil {
				t.Fatalf("Failed to parse snapshot %s: %v", snapPath, err)
			}

			// We only target generic dialect for now (default); skip cases that explicitly opt out.
			if strings.Contains(prql, "# generic:skip") {
				t.Skip("Upstream query skips the generic dialect")
			}

			gotSQL, err := gophrql.Compile(prql)
			if err != nil {
				t.Fatalf("Compile failed: %v", err)
			}

			if normalize(gotSQL) != normalize(expectedSQL) {
				t.Errorf("SQL Mismatch.\nPRQL:\n%s\n\nExpected:\n%s\n\nGot:\n%s", prql, expectedSQL, gotSQL)
			}
		})
	}
}

// TestIntegrationCompileSnapshots ensures each query read by `sql.rs` has an upstream SQL snapshot.
// The PRQL file list under tmp/prql/.../queries comes from `prqlc/tests/integration/sql.rs`, so if
// every PRQL query there has an `integration__queries__compile__*.snap` file, we know the Go integration
// tests exercise the same set of cases.
func TestIntegrationCompileSnapshots(t *testing.T) {
	queryDir := "tmp/prql/prqlc/prqlc/tests/integration/queries"
	snapshotsDir := "tmp/prql/prqlc/prqlc/tests/integration/snapshots"

	if _, err := os.Stat(queryDir); os.IsNotExist(err) {
		t.Skipf("Queries directory not found at %s; skipping integration coverage check", queryDir)
	}

	queryFiles, err := filepath.Glob(filepath.Join(queryDir, "*.prql"))
	if err != nil {
		t.Fatalf("Failed to list queries: %v", err)
	}

	var missing []string
	for _, queryPath := range queryFiles {
		name := strings.TrimSuffix(filepath.Base(queryPath), ".prql")
		snapshotPath := filepath.Join(snapshotsDir, "integration__queries__compile__"+name+".snap")

		queryContent, err := os.ReadFile(queryPath)
		if err != nil {
			t.Fatalf("Failed to read query %s: %v", queryPath, err)
		}
		if strings.Contains(string(queryContent), "# generic:skip") {
			continue
		}

		if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
			missing = append(missing, name)
		} else if err != nil {
			t.Fatalf("Failed to stat snapshot %s: %v", snapshotPath, err)
		}
	}

	if len(missing) > 0 {
		t.Fatalf("Missing compile snapshots for queries: %s", strings.Join(missing, ", "))
	}
}

func parseSnapshotAndInput(snapPath string) (string, string, error) {
	f, err := os.Open(snapPath)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	// Format:
	// ---
	// source: ...
	// expression: ...
	// input_file: path/to/file.prql
	// ---
	// SQL CONTENT...

	var inputFileRel string

	// minimal YAML-like parsing for the header
	inHeader := false
	dashCount := 0
	var sqlBody strings.Builder

	for scanner.Scan() {
		line := scanner.Text()
		if line == "---" {
			dashCount++
			if dashCount == 1 {
				inHeader = true
				continue
			}
			if dashCount == 2 {
				inHeader = false
				continue
			}
		}

		if inHeader {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "input_file:") {
				// input_file: prqlc/prqlc/tests/integration/queries/foo.prql
				parts := strings.SplitN(trimmed, ":", 2)
				if len(parts) == 2 {
					inputFileRel = strings.TrimSpace(parts[1])
				}
			}
		} else {
			// Body
			if dashCount >= 2 {
				sqlBody.WriteString(line)
				sqlBody.WriteString("\n")
			}
		}
	}

	if inputFileRel == "" {
		return "", "", fmt.Errorf("input_file not found in snapshot header")
	}

	// Resolve input file path
	// Snapshot path: tmp/prql/prqlc/prqlc/tests/integration/snapshots/foo.snap
	// inputFileRel: prqlc/prqlc/tests/integration/queries/foo.prql
	// effectively, we need to map the relative path to our workspace.
	// The repo root in tmp matches `prqlc` in input_file?
	// tmp/prql structure:
	// tmp/prql/prqlc/prqlc/tests...
	// input_file starts with `prqlc/prqlc...`?
	// Let's verify via the file content observed earlier:
	// "input_file: prqlc/prqlc/tests/integration/queries/constants_only.prql"
	// Our root is `tmp/prql`.
	// So `tmp/prql/` + `prqlc/prqlc/tests...`?
	// Let's check if `tmp/prql` contains `prqlc` directory? Yes.

	// So we construct path: "tmp/prql" + "/" + inputFileRel ?
	// inputFileRel 'prqlc/prqlc/...' matches exactly the struct under tmp/prql?
	// Let's check `tmp/prql` listing again.
	// List dir `tmp/prql`:
	// .git, Cargo.toml, ..., prqlc (dir)
	// So `tmp/prql/prqlc` exists.
	// inputFileRel starts with `prqlc/prqlc`.
	// Does `tmp/prql/prqlc` contain `prqlc`?
	// List `tmp/prql/prqlc`:
	// README.md, bindings, packages, prqlc (dir), ...
	// Yes! `tmp/prql/prqlc` contains `prqlc` subdir.
	// So yes, `tmp/prql` + "/" + inputFileRel should be the path.

	inputPath := filepath.Join("tmp/prql", inputFileRel)
	prqlBytes, err := os.ReadFile(inputPath)
	if err != nil {
		return "", "", fmt.Errorf("failed to read input PRQL %s: %v", inputPath, err)
	}

	return string(prqlBytes), strings.TrimSpace(sqlBody.String()), nil
}

func normalize(s string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(s)), "")
}
