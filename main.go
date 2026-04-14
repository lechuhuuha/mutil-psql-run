package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"unicode"

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

		results, execErr := executeScript(tx, sqlText)
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

func executeScript(tx *sql.Tx, script string) ([]QueryResult, error) {
	stmts := splitSQLStatements(script)
	results := make([]QueryResult, 0, len(stmts))
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		var data any
		var err error
		if statementReturnsRows(stmt) {
			rows, qErr := tx.Query(stmt)
			if qErr != nil {
				err = qErr
			} else {
				data, err = collectRows(rows)
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

func collectRows(rows *sql.Rows) ([]map[string]any, error) {
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	data := make([]map[string]any, 0)
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = normalizeSQLValue(vals[i])
		}
		data = append(data, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return data, nil
}

func normalizeSQLValue(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

func splitSQLStatements(script string) []string {
	var stmts []string
	var sb strings.Builder
	var dollarTag string
	inSingleQuote := false
	inDoubleQuote := false
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(script); i++ {
		switch {
		case inLineComment:
			sb.WriteByte(script[i])
			if script[i] == '\n' {
				inLineComment = false
			}
			continue
		case inBlockComment:
			sb.WriteByte(script[i])
			if script[i] == '*' && i+1 < len(script) && script[i+1] == '/' {
				sb.WriteByte(script[i+1])
				i++
				inBlockComment = false
			}
			continue
		case dollarTag != "":
			if strings.HasPrefix(script[i:], dollarTag) {
				sb.WriteString(dollarTag)
				i += len(dollarTag) - 1
				dollarTag = ""
				continue
			}
			sb.WriteByte(script[i])
			continue
		case inSingleQuote:
			sb.WriteByte(script[i])
			if script[i] == '\'' {
				if i+1 < len(script) && script[i+1] == '\'' {
					sb.WriteByte(script[i+1])
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		case inDoubleQuote:
			sb.WriteByte(script[i])
			if script[i] == '"' {
				if i+1 < len(script) && script[i+1] == '"' {
					sb.WriteByte(script[i+1])
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		}

		if script[i] == '-' && i+1 < len(script) && script[i+1] == '-' {
			sb.WriteString("--")
			i++
			inLineComment = true
			continue
		}
		if script[i] == '/' && i+1 < len(script) && script[i+1] == '*' {
			sb.WriteString("/*")
			i++
			inBlockComment = true
			continue
		}
		if tag, ok := readDollarQuoteTag(script[i:]); ok {
			sb.WriteString(tag)
			i += len(tag) - 1
			dollarTag = tag
			continue
		}

		switch script[i] {
		case '\'':
			inSingleQuote = true
		case '"':
			inDoubleQuote = true
		case ';':
			sb.WriteByte(script[i])
			if stmt := strings.TrimSpace(sb.String()); stmt != "" {
				stmts = append(stmts, stmt)
			}
			sb.Reset()
			continue
		}

		sb.WriteByte(script[i])
	}

	if stmt := strings.TrimSpace(sb.String()); stmt != "" {
		stmts = append(stmts, stmt)
	}

	return stmts
}

func statementReturnsRows(stmt string) bool {
	keyword, next := nextTopLevelKeyword(stmt, 0)
	return statementReturnsRowsFromKeyword(stmt, keyword, next)
}

func statementReturnsRowsFromKeyword(stmt, keyword string, next int) bool {
	switch keyword {
	case "SELECT", "SHOW", "VALUES", "TABLE", "EXPLAIN":
		return true
	case "WITH":
		return withStatementReturnsRows(stmt, next)
	case "INSERT", "UPDATE", "DELETE", "MERGE":
		return hasTopLevelKeyword(stmt, "RETURNING", next)
	default:
		return false
	}
}

func withStatementReturnsRows(stmt string, start int) bool {
	pos := skipWhitespaceAndComments(stmt, start)
	if keyword, next := readKeyword(stmt, pos); keyword == "RECURSIVE" {
		pos = next
	}

	for {
		pos = skipWhitespaceAndComments(stmt, pos)
		next := skipIdentifierOrQuoted(stmt, pos)
		if next == pos {
			return false
		}
		pos = skipWhitespaceAndComments(stmt, next)

		if pos < len(stmt) && stmt[pos] == '(' {
			pos = skipBalanced(stmt, pos, '(', ')')
			pos = skipWhitespaceAndComments(stmt, pos)
		}

		keyword, next := readKeyword(stmt, pos)
		if keyword != "AS" {
			return false
		}
		pos = skipWhitespaceAndComments(stmt, next)

		if keyword, next := readKeyword(stmt, pos); keyword == "NOT" {
			pos = skipWhitespaceAndComments(stmt, next)
			keyword, next = readKeyword(stmt, pos)
			if keyword == "MATERIALIZED" {
				pos = skipWhitespaceAndComments(stmt, next)
			}
		} else if keyword == "MATERIALIZED" {
			pos = skipWhitespaceAndComments(stmt, next)
		}

		if pos >= len(stmt) || stmt[pos] != '(' {
			return false
		}
		pos = skipBalanced(stmt, pos, '(', ')')
		pos = skipWhitespaceAndComments(stmt, pos)

		if pos < len(stmt) && stmt[pos] == ',' {
			pos++
			continue
		}
		break
	}

	keyword, next := nextTopLevelKeyword(stmt, pos)
	return statementReturnsRowsFromKeyword(stmt, keyword, next)
}

func hasTopLevelKeyword(stmt, target string, start int) bool {
	for pos := start; pos < len(stmt); {
		keyword, next := nextTopLevelKeyword(stmt, pos)
		if keyword == "" {
			return false
		}
		if keyword == target {
			return true
		}
		pos = next
	}
	return false
}

func nextTopLevelKeyword(s string, start int) (string, int) {
	depth := 0
	var dollarTag string

	for i := start; i < len(s); {
		switch {
		case dollarTag != "":
			if strings.HasPrefix(s[i:], dollarTag) {
				i += len(dollarTag)
				dollarTag = ""
				continue
			}
			i++
			continue
		case s[i] == '-' && i+1 < len(s) && s[i+1] == '-':
			i += 2
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		case s[i] == '/' && i+1 < len(s) && s[i+1] == '*':
			i += 2
			for i+1 < len(s) && !(s[i] == '*' && s[i+1] == '/') {
				i++
			}
			if i+1 < len(s) {
				i += 2
			}
			continue
		case s[i] == '\'':
			i = skipQuoted(s, i, '\'')
			continue
		case s[i] == '"':
			i = skipQuoted(s, i, '"')
			continue
		case s[i] == '$':
			if tag, ok := readDollarQuoteTag(s[i:]); ok {
				dollarTag = tag
				i += len(tag)
				continue
			}
		case s[i] == '(':
			depth++
			i++
			continue
		case s[i] == ')':
			if depth > 0 {
				depth--
			}
			i++
			continue
		case depth == 0 && isIdentifierStart(s[i]):
			j := i + 1
			for j < len(s) && isIdentifierPart(s[j]) {
				j++
			}
			return strings.ToUpper(s[i:j]), j
		}
		i++
	}

	return "", len(s)
}

func skipWhitespaceAndComments(s string, start int) int {
	for i := start; i < len(s); {
		switch {
		case unicode.IsSpace(rune(s[i])):
			i++
		case s[i] == '-' && i+1 < len(s) && s[i+1] == '-':
			i += 2
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case s[i] == '/' && i+1 < len(s) && s[i+1] == '*':
			i += 2
			for i+1 < len(s) && !(s[i] == '*' && s[i+1] == '/') {
				i++
			}
			if i+1 < len(s) {
				i += 2
			}
		default:
			return i
		}
	}
	return len(s)
}

func readKeyword(s string, start int) (string, int) {
	start = skipWhitespaceAndComments(s, start)
	if start >= len(s) || !isIdentifierStart(s[start]) {
		return "", start
	}

	end := start + 1
	for end < len(s) && isIdentifierPart(s[end]) {
		end++
	}
	return strings.ToUpper(s[start:end]), end
}

func skipIdentifierOrQuoted(s string, start int) int {
	start = skipWhitespaceAndComments(s, start)
	if start >= len(s) {
		return start
	}
	if s[start] == '"' {
		return skipQuoted(s, start, '"')
	}
	if !isIdentifierStart(s[start]) {
		return start
	}

	end := start + 1
	for end < len(s) && isIdentifierPart(s[end]) {
		end++
	}
	return end
}

func skipBalanced(s string, start int, open, close byte) int {
	if start >= len(s) || s[start] != open {
		return start
	}

	depth := 0
	var dollarTag string
	for i := start; i < len(s); {
		switch {
		case dollarTag != "":
			if strings.HasPrefix(s[i:], dollarTag) {
				i += len(dollarTag)
				dollarTag = ""
				continue
			}
			i++
			continue
		case s[i] == '-' && i+1 < len(s) && s[i+1] == '-':
			i += 2
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		case s[i] == '/' && i+1 < len(s) && s[i+1] == '*':
			i += 2
			for i+1 < len(s) && !(s[i] == '*' && s[i+1] == '/') {
				i++
			}
			if i+1 < len(s) {
				i += 2
			}
			continue
		case s[i] == '\'':
			i = skipQuoted(s, i, '\'')
			continue
		case s[i] == '"':
			i = skipQuoted(s, i, '"')
			continue
		case s[i] == '$':
			if tag, ok := readDollarQuoteTag(s[i:]); ok {
				dollarTag = tag
				i += len(tag)
				continue
			}
		case s[i] == open:
			depth++
			i++
			continue
		case s[i] == close:
			depth--
			i++
			if depth == 0 {
				return i
			}
			continue
		}
		i++
	}

	return len(s)
}

func skipQuoted(s string, start int, quote byte) int {
	for i := start + 1; i < len(s); i++ {
		if s[i] != quote {
			continue
		}
		if i+1 < len(s) && s[i+1] == quote {
			i++
			continue
		}
		return i + 1
	}
	return len(s)
}

func readDollarQuoteTag(s string) (string, bool) {
	if len(s) < 2 || s[0] != '$' {
		return "", false
	}

	for i := 1; i < len(s); i++ {
		if s[i] == '$' {
			return s[:i+1], true
		}
		if !isIdentifierPart(s[i]) {
			return "", false
		}
	}

	return "", false
}

func isIdentifierStart(b byte) bool {
	return b == '_' || ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z')
}

func isIdentifierPart(b byte) bool {
	return isIdentifierStart(b) || ('0' <= b && b <= '9')
}
