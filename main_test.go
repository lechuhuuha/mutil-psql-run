package main

import "testing"

func TestStatementReturnsRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		stmt string
		want bool
	}{
		{
			name: "plain select",
			stmt: "SELECT 1;",
			want: true,
		},
		{
			name: "cte select",
			stmt: `
WITH params AS (
	SELECT 1 AS id
)
SELECT * FROM params;
`,
			want: true,
		},
		{
			name: "cte update without returning",
			stmt: `
WITH params AS (
	SELECT 1 AS id
)
UPDATE foo
SET bar = 1;
`,
			want: false,
		},
		{
			name: "cte update with returning",
			stmt: `
WITH params AS (
	SELECT 1 AS id
)
UPDATE foo
SET bar = 1
RETURNING id;
`,
			want: true,
		},
		{
			name: "with recursive select",
			stmt: `
WITH RECURSIVE nums AS (
	SELECT 1 AS n
	UNION ALL
	SELECT n + 1 FROM nums WHERE n < 3
)
SELECT * FROM nums;
`,
			want: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := statementReturnsRows(tt.stmt); got != tt.want {
				t.Fatalf("statementReturnsRows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSplitSQLStatements(t *testing.T) {
	t.Parallel()

	script := `
WITH params AS (
	SELECT ';' AS literal
)
SELECT * FROM params;

DO $proc$
BEGIN
	RAISE NOTICE 'keep;inside';
END
$proc$;

SELECT 2;
`

	stmts := splitSQLStatements(script)
	if len(stmts) != 3 {
		t.Fatalf("splitSQLStatements() returned %d statements, want 3", len(stmts))
	}

	if !statementReturnsRows(stmts[0]) {
		t.Fatalf("first statement should be treated as row-returning: %q", stmts[0])
	}

	if statementReturnsRows(stmts[1]) {
		t.Fatalf("DO block should not be treated as row-returning: %q", stmts[1])
	}

	if !statementReturnsRows(stmts[2]) {
		t.Fatalf("third statement should be treated as row-returning: %q", stmts[2])
	}
}
