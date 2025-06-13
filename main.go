package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	_ "github.com/lib/pq"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
)

// DBConfig holds the connection info for a single market
type DBConfig struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DBName   string `json:"dbname"`
	SSLMode  string `json:"sslmode"` // e.g. "disable"
}

// Config holds all market configs
type Config struct {
	Markets []DBConfig `json:"markets"`
}

// QueryResult pairs a SQL statement with its returned data
type QueryResult struct {
	Stmt string `json:"stmt"`
	Data any    `json:"data"`
}

func main() {
	// CLI flags
	credsFile := flag.String("creds", "creds.json", "JSON file with database credentials")
	sqlFile := flag.String("sql", "query.sql", "SQL file to execute on each database")
	outputFile := flag.String("out", "out", "Optional output file for the results (defaults to stdout)")
	commitFlag := flag.Bool("commit", false, "commit transactions if true; otherwise rollback")
	flag.Parse()

	// Load credentials
	cfgData, err := os.ReadFile(*credsFile)
	if err != nil {
		log.Fatalf("Failed to read creds file: %v", err)
	}
	var cfg Config
	if err := json.Unmarshal(cfgData, &cfg); err != nil {
		log.Fatalf("Failed to parse creds JSON: %v", err)
	}

	// Load SQL script
	scriptData, err := os.ReadFile(*sqlFile)
	if err != nil {
		log.Fatalf("Failed to read SQL file: %v", err)
	}
	script := string(scriptData)

	// Prepare results
	tableData := [][]string{}

	// Regex to identify SELECT statements
	selectRe := regexp.MustCompile(`(?i)^\s*SELECT`) // case-insensitive

	// Loop markets
	for _, m := range cfg.Markets {
		connStr := fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
			m.Host, m.Port, m.User, m.Password, m.DBName, m.SSLMode,
		)
		db, err := sql.Open("postgres", connStr)
		if err != nil {
			tableData = append(tableData, []string{m.Name, "", fmt.Sprintf("connect error: %v", err)})
			continue
		}
		defer db.Close()

		tx, err := db.Begin()
		if err != nil {
			tableData = append(tableData, []string{m.Name, "", fmt.Sprintf("begin error: %v", err)})
			continue
		}

		// execute script
		results, execErr := executeScript(tx, script, selectRe)
		if execErr != nil {
			rbErr := tx.Rollback()
			msg := fmt.Sprintf("exec error: %v", execErr)
			if rbErr != nil {
				msg += fmt.Sprintf("; rollback error: %v", rbErr)
			}
			tableData = append(tableData, []string{m.Name, "", msg})
			continue
		}

		// commit or rollback
		if *commitFlag {
			if cmErr := tx.Commit(); cmErr != nil {
				tableData = append(tableData, []string{m.Name, "", fmt.Sprintf("commit error: %v", cmErr)})
				continue
			}
		} else {
			if rbErr := tx.Rollback(); rbErr != nil {
				tableData = append(tableData, []string{m.Name, "", fmt.Sprintf("rollback error: %v", rbErr)})
				continue
			}
		}

		// append each query result
		for _, qr := range results {
			j, err := json.Marshal(qr.Data)
			if err != nil {
				j = []byte(fmt.Sprintf(`"json error: %v"`, err))
			}
			tableData = append(tableData, []string{m.Name, qr.Stmt, string(j)})
		}
	}

	f, err := os.Create(*outputFile)
	if err != nil {
		log.Fatalf("create output file: %v", err)
	}
	defer f.Close()

	// Print results table
	table := tablewriter.NewTable(f,
		tablewriter.WithConfig(tablewriter.Config{
			Row: tw.CellConfig{
				Formatting: tw.CellFormatting{AutoWrap: tw.WrapNormal}, // Wrap long content
				Alignment:  tw.CellAlignment{Global: tw.AlignLeft},     // Left-align rows
				// ColMaxWidths: tw.CellWidth{Global: 25},
			},
			Footer: tw.CellConfig{
				Alignment: tw.CellAlignment{Global: tw.AlignRight},
			},
		}),
	)
	table.Header([]string{"Market", "Query", "Result"})
	err = table.Bulk(tableData)
	if err != nil {
		fmt.Println("error when append data to table " + err.Error())
		return
	}
	err = table.Render()
	if err != nil {
		fmt.Println("error when render data to table " + err.Error())
		return
	}
}

// executeScript splits on semicolons and runs each statement, returning per-stmt responses
func executeScript(tx *sql.Tx, script string, selectRe *regexp.Regexp) ([]QueryResult, error) {
	// split statements respecting $$
	var stmts []string
	var sb strings.Builder
	inDollar := false
	for i := 0; i < len(script); i++ {
		if strings.HasPrefix(script[i:], "$$") {
			inDollar = !inDollar
			sb.WriteString("$$")
			i += 1
			continue
		}
		c := script[i]
		sb.WriteByte(c)
		if c == ';' && !inDollar {
			stmts = append(stmts, sb.String())
			sb.Reset()
		}
	}
	if s := strings.TrimSpace(sb.String()); s != "" {
		stmts = append(stmts, s)
	}

	results := make([]QueryResult, 0, len(stmts))
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		var data any
		var err error
		if selectRe.MatchString(stmt) {
			// fetch rows into slice of maps
			rows, qErr := tx.Query(stmt)
			if qErr != nil {
				err = qErr
			} else {
				cols, _ := rows.Columns()
				for rows.Next() {
					vals := make([]any, len(cols))
					ptrs := make([]any, len(cols))
					for i := range vals {
						ptrs[i] = &vals[i]
					}
					if err := rows.Scan(ptrs...); err != nil {
						fmt.Println("rows scan encoutner error " + err.Error())
					}
					row := map[string]any{}
					for i, col := range cols {
						row[col] = vals[i]
					}
					if data == nil {
						data = []map[string]any{row}
					} else {
						data = append(data.([]map[string]any), row)
					}
				}
				rows.Close()
			}
		} else {
			// exec statement
			res, xErr := tx.Exec(stmt)
			if xErr != nil {
				err = xErr
			} else {
				if id, e := res.LastInsertId(); e == nil {
					data = map[string]int64{"lastInsertId": id}
				} else if cnt, e := res.RowsAffected(); e == nil {
					data = map[string]int64{"rowsAffected": cnt}
				} else {
					data = "executed"
				}
			}
		}
		if err != nil {
			return results, err
		}
		results = append(results, QueryResult{Stmt: stmt, Data: data})
	}
	return results, nil
}
