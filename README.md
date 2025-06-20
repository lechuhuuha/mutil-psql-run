# Market SQL Runner CLI

A Go-based CLI tool to execute arbitrary SQL scripts across multiple PostgreSQL databases (markets) in a single run. It supports transactions with dry-run (rollback) and commit modes, captures per-statement results (including `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and stored procedures), and renders a summary table in the terminal or writes it to a file.

---

## Features

* **Market-specific SQL**: Write SQL for a specific market using comment headers like `-- MY`, `-- SG`, or use `-- ALL` for fallback.
* **Multi-market execution**: Loops through all markets defined in a JSON config and runs only their designated SQL blocks.
* **Transactional support**: Dry-run by default (rollback all changes) or commit mode.
* **Per-statement feedback**:
  * **SELECT**: returns full rowsets as JSON arrays.
  * **Exec statements**: returns `lastInsertId`, `rowsAffected`, or a generic `executed` label.
* **Customizable output**: Render results in a wrapped, aligned terminal table or write to an output file.

---

## Prerequisites

* Go 1.18+
* PostgreSQL driver: `github.com/lib/pq`
* Table writer libraries:

  * `github.com/olekukonko/tablewriter`
  * `github.com/olekukonko/tablewriter/tw`

---

## Installation

1. Clone this repo:

   ```bash
   git clone https://your.repo.url/market-sql-runner.git
   cd market-sql-runner
   ```
2. Fetch dependencies:

   ```bash
   go mod tidy
   ```
3. Build the CLI:

   ```bash
   go build -o market-runner main.go
   ```

---

## Configuration

### `creds.json.example`

Copy or rename the example to `creds.json` (this file is in `.gitignore`):

```json
{
  "markets": [
    {
      "name": "MY",
      "host": "db-my.example.com",
      "port": 5432,
      "user": "username",
      "password": "password",
      "dbname": "ezrx_my",
      "sslmode": "disable"
    },
    /* ...other markets... */
  ]
}
```

> **Note**: 
Do **not** commit your real `creds.json`; use `creds.json.example` as a template. The same goes with `out` file and `query.sql` file


### `query.sql`

Place any SQL statements (DDL, DML, SELECTs, stored procedures) separated by semicolons. The tool will respect `$$` dollar-quoted blocks when splitting statements.

```sql
-- Example query.sql
CREATE TABLE IF NOT EXISTS public.keyword_boost (
  id SERIAL PRIMARY KEY,
  sales_org VARCHAR(50) NOT NULL
);

INSERT INTO public.keyword_boost (sales_org) VALUES ('2001') RETURNING id;
SELECT * FROM public.keyword_boost;
DROP TABLE public.keyword_boost;
```

---

## Usage

```bash
# Dry-run (default rollback mode):
./market-runner \
  --creds creds.json \
  --sql query.sql

# Commit mode (apply changes):
./market-runner \
  --creds creds.json \
  --sql query.sql \
  --commit

# Write output to a file (use ‘out.txt’ as example):
./market-runner \
  --creds creds.json \
  --sql query.sql \
  --out out.txt
```

### Flags

| Flag       | Default      | Description                                                      |
| ---------- | ------------ | ---------------------------------------------------------------- |
| `--creds`  | `creds.json` | Path to JSON file with market credentials (ignored by Git).      |
| `--sql`    | `query.sql`  | Path to the SQL script containing statements.                    |
| `--out`    | (none)       | If set, write results table to this file instead of stdout.      |
| `--commit` | `false`      | When `true`, commits transactions; otherwise rolls back changes. |

---

## Example Output

```
MARKET | QUERY                                         | RESULT
-------|-----------------------------------------------|----------------------------------------------------------
MY     | CREATE TABLE IF NOT EXISTS ... ;              | {"rowsAffected":0}
MY     | INSERT INTO ... RETURNING id;                 | {"lastInsertId":123}
MY     | SELECT * FROM public.keyword_boost;           | [{"id":123, "sales_org":"2001"}]
MY     | DROP TABLE public.keyword_boost;              | {"rowsAffected":1}
```

If `--out out.txt` is provided, the same table is written to `out.txt`.

---
## Notes
- SQL under `-- MY`, `-- SG`, etc. is only executed for matching markets.
- If a market has no specific SQL, it will fall back to `-- ALL` if defined.
- Unlabeled SQL (before any `-- MARKET`) is treated as part of `-- ALL`.

---

## Troubleshooting

* **Cannot create output file**: Ensure the `--out` flag value is non-empty and points to a writable path.
* **Invalid JSON**: Check `creds.json` structure against `creds.json.example`.
* **SQL errors**: Errors in any statement will be reported per-market, and the transaction will rollback (unless `--commit`).

---

## License