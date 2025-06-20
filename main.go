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

type DBConfig struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DBName   string `json:"dbname"`
	SSLMode  string `json:"sslmode"`
}

type Config struct {
	Markets []DBConfig `json:"markets"`
}

type QueryResult struct {
	Stmt string `json:"stmt"`
	Data any    `json:"data"`
}

func main() {
	credsFile := flag.String("creds", "creds.json", "JSON file with database credentials")
	sqlFile := flag.String("sql", "query.sql", "SQL file to execute on each database")
	outputFile := flag.String("out", "out", "Optional output file for the results (defaults to stdout)")
	commitFlag := flag.Bool("commit", false, "commit transactions if true; otherwise rollback")
	flag.Parse()

	cfgData, err := os.ReadFile(*credsFile)
	if err != nil {
		log.Fatalf("Failed to read creds file: %v", err)
	}
	var cfg Config
	if err := json.Unmarshal(cfgData, &cfg); err != nil {
		log.Fatalf("Failed to parse creds JSON: %v", err)
	}

	marketSQLs, err := parseMarketSQL(*sqlFile)
	if err != nil {
		log.Fatalf("Failed to parse market SQL: %v", err)
	}

	tableData := [][]string{}
	selectRe := regexp.MustCompile(`(?i)^\s*SELECT`)

	for _, m := range cfg.Markets {
		sqlText, ok := marketSQLs[m.Name]
		if !ok {
			sqlText, ok = marketSQLs["ALL"]
			if !ok {
				tableData = append(tableData, []string{m.Name, "", "no SQL defined for this market"})
				continue
			}
		}

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

		results, execErr := executeScript(tx, sqlText, selectRe)
		if execErr != nil {
			rbErr := tx.Rollback()
			msg := fmt.Sprintf("exec error: %v", execErr)
			if rbErr != nil {
				msg += fmt.Sprintf("; rollback error: %v", rbErr)
			}
			tableData = append(tableData, []string{m.Name, "", msg})
			continue
		}

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

	table := tablewriter.NewTable(f,
		tablewriter.WithConfig(tablewriter.Config{
			Row: tw.CellConfig{
				Formatting: tw.CellFormatting{AutoWrap: tw.WrapNormal},
				Alignment:  tw.CellAlignment{Global: tw.AlignLeft},
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

func parseMarketSQL(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	sqlByMarket := make(map[string][]string)
	currentMarket := "ALL"

	for _, line := range lines {
		lineTrim := strings.TrimSpace(line)
		if strings.HasPrefix(lineTrim, "--") {
			marketTag := strings.TrimSpace(strings.TrimPrefix(lineTrim, "--"))
			if marketTag != "" {
				currentMarket = marketTag
				continue
			}
		}
		sqlByMarket[currentMarket] = append(sqlByMarket[currentMarket], line)
	}

	final := make(map[string]string)
	for market, parts := range sqlByMarket {
		final[market] = strings.Join(parts, "\n")
	}
	return final, nil
}

func executeScript(tx *sql.Tx, script string, selectRe *regexp.Regexp) ([]QueryResult, error) {
	var stmts []string
	var sb strings.Builder
	inDollar := false
	for i := 0; i < len(script); i++ {
		if strings.HasPrefix(script[i:], "$$") {
			inDollar = !inDollar
			sb.WriteString("$$")
			i++
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
						fmt.Println("rows scan error: " + err.Error())
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
