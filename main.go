package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type TokenType int

const (
	KEYWORD TokenType = iota
	IDENTIFIER
	DATATYPE
	LITERAL
	OPERATOR
	DELIMITER
	UNKNOWN
)

type Token struct {
	Type  TokenType
	Value string
}

type ASTNode interface {
	String() string
}

type CreateTableNode struct {
	TableName   string
	Columns     []ColumnDef
	Constraints []Constraint
}

type SelectNode struct {
	Columns   []string
	TableName string
	Where     *WhereClause
	OrderBy   []OrderColumn
	Limit     int
}

type InsertNode struct {
	TableName string
	Columns   []string
	Values    [][]interface{}
}

type UpdateNode struct {
	TableName  string
	SetClauses map[string]interface{}
	Where      *WhereClause
}

type DeleteNode struct {
	TableName string
	Where     *WhereClause
}

type WhereClause struct {
	Column   string
	Operator string
	Value    interface{}
}

type OrderColumn struct {
	Column string
	Desc   bool
}

type ColumnDef struct {
	Name       string
	DataType   string
	Size       []int
	NotNull    bool
	PrimaryKey bool
	Default    string
	AutoIncr   bool
}

type Constraint struct {
	Type       string
	Columns    []string
	RefTable   string
	RefColumns []string
}

type Database struct {
	Name    string
	Tables  map[string]*Table
	Indexes map[string]*Index
}

type Table struct {
	Name     string
	Columns  []ColumnDef
	Data     []map[string]interface{}
	NextID   int
	FilePath string
}

type Index struct {
	Name      string
	TableName string
	Columns   []string
	Unique    bool
	Data      map[string][]int
}

type DBMS struct {
	CurrentDB *Database
	Databases map[string]*Database
	DataDir   string
}

func (c CreateTableNode) String() string {
	return fmt.Sprintf("CREATE TABLE %s (%d columns, %d constraints)",
		c.TableName, len(c.Columns), len(c.Constraints))
}

func (s SelectNode) String() string {
	cols := strings.Join(s.Columns, ", ")
	if cols == "" {
		cols = "*"
	}
	return fmt.Sprintf("SELECT %s FROM %s", cols, s.TableName)
}

func (i InsertNode) String() string {
	return fmt.Sprintf("INSERT INTO %s (%d columns, %d rows)",
		i.TableName, len(i.Columns), len(i.Values))
}

func (u UpdateNode) String() string {
	return fmt.Sprintf("UPDATE %s SET %d columns", u.TableName, len(u.SetClauses))
}

func (d DeleteNode) String() string {
	return fmt.Sprintf("DELETE FROM %s", d.TableName)
}

var keywords = map[string]bool{
	"CREATE": true, "TABLE": true, "PRIMARY": true, "KEY": true,
	"FOREIGN": true, "REFERENCES": true, "NOT": true, "NULL": true,
	"UNIQUE": true, "INDEX": true, "DEFAULT": true, "AUTO_INCREMENT": true,
	"INSERT": true, "INTO": true, "VALUES": true, "SELECT": true,
	"UPDATE": true, "DELETE": true, "SET": true, "FROM": true,
	"WHERE": true, "ORDER": true, "BY": true, "GROUP": true,
	"HAVING": true, "JOIN": true, "INNER": true, "LEFT": true,
	"RIGHT": true, "ON": true, "AS": true, "AND": true, "OR": true,
	"LIMIT": true, "OFFSET": true, "ASC": true, "DESC": true,
	"DATABASE": true, "USE": true, "SHOW": true, "TABLES": true,
	"DESCRIBE": true, "DROP": true, "ALTER": true,
}

var dataTypes = map[string]bool{
	"INT": true, "INTEGER": true, "VARCHAR": true, "CHAR": true,
	"TEXT": true, "DATE": true, "DATETIME": true, "TIMESTAMP": true,
	"DECIMAL": true, "FLOAT": true, "DOUBLE": true, "BOOLEAN": true,
	"BOOL": true, "BIGINT": true, "SMALLINT": true, "TINYINT": true,
}

func SQLQueryTokenizer(query string) []Token {
	clean := strings.ReplaceAll(query, "\n", " ")
	clean = strings.ReplaceAll(clean, "\t", " ")
	clean = regexp.MustCompile(`\s+`).ReplaceAllString(clean, " ")

	clean = strings.ReplaceAll(clean, ",", " , ")
	clean = strings.ReplaceAll(clean, "(", " ( ")
	clean = strings.ReplaceAll(clean, ")", " ) ")
	clean = strings.ReplaceAll(clean, ";", " ; ")
	clean = strings.ReplaceAll(clean, "=", " = ")
	clean = strings.ReplaceAll(clean, "<", " < ")
	clean = strings.ReplaceAll(clean, ">", " > ")

	tokenStrings := strings.Fields(clean)
	var tokens []Token

	for _, tokenStr := range tokenStrings {
		token := Token{Value: tokenStr}
		upperToken := strings.ToUpper(tokenStr)

		if keywords[upperToken] {
			token.Type = KEYWORD
		} else if dataTypes[upperToken] {
			token.Type = DATATYPE
		} else if tokenStr == "," || tokenStr == "(" || tokenStr == ")" || tokenStr == ";" {
			token.Type = DELIMITER
		} else if tokenStr == "=" || tokenStr == "<" || tokenStr == ">" || tokenStr == "<=" || tokenStr == ">=" || tokenStr == "!=" {
			token.Type = OPERATOR
		} else if regexp.MustCompile(`^[0-9]+$`).MatchString(tokenStr) ||
			regexp.MustCompile(`^[0-9]+\.[0-9]+$`).MatchString(tokenStr) ||
			regexp.MustCompile(`^'.*'$`).MatchString(tokenStr) {
			token.Type = LITERAL
		} else {
			token.Type = IDENTIFIER
		}

		tokens = append(tokens, token)
	}

	return tokens
}

type SQLParser struct {
	tokens []Token
	pos    int
}

func NewSQLParser(tokens []Token) *SQLParser {
	return &SQLParser{tokens: tokens, pos: 0}
}

func (p *SQLParser) current() *Token {
	if p.pos >= len(p.tokens) {
		return nil
	}
	return &p.tokens[p.pos]
}

func (p *SQLParser) consume() *Token {
	token := p.current()
	p.pos++
	return token
}

func (p *SQLParser) peek() *Token {
	if p.pos+1 >= len(p.tokens) {
		return nil
	}
	return &p.tokens[p.pos+1]
}

func (p *SQLParser) Parse() ASTNode {
	if p.current() == nil {
		return nil
	}

	switch strings.ToUpper(p.current().Value) {
	case "CREATE":
		return p.ParseCreateTable()
	case "SELECT":
		return p.ParseSelect()
	case "INSERT":
		return p.ParseInsert()
	case "UPDATE":
		return p.ParseUpdate()
	case "DELETE":
		return p.ParseDelete()
	default:
		return nil
	}
}

func (p *SQLParser) ParseCreateTable() *CreateTableNode {
	node := &CreateTableNode{}

	if token := p.consume(); token == nil || strings.ToUpper(token.Value) != "CREATE" {
		return nil
	}

	if token := p.consume(); token == nil || strings.ToUpper(token.Value) != "TABLE" {
		return nil
	}

	if token := p.consume(); token == nil || token.Type != IDENTIFIER {
		return nil
	} else {
		node.TableName = token.Value
	}

	if token := p.consume(); token == nil || token.Value != "(" {
		return nil
	}

	for p.current() != nil && p.current().Value != ")" {
		if strings.ToUpper(p.current().Value) == "PRIMARY" ||
			strings.ToUpper(p.current().Value) == "FOREIGN" ||
			strings.ToUpper(p.current().Value) == "UNIQUE" {
			constraint := p.parseConstraint()
			if constraint == nil {
				return nil
			}
			node.Constraints = append(node.Constraints, *constraint)
		} else {
			column := p.parseColumnDef()
			if column == nil {
				return nil
			}
			node.Columns = append(node.Columns, *column)
		}

		if p.current() != nil && p.current().Value == "," {
			p.consume()
		}
	}

	if token := p.consume(); token == nil || token.Value != ")" {
		return nil
	}

	return node
}

func (p *SQLParser) ParseSelect() *SelectNode {
	node := &SelectNode{}

	if token := p.consume(); token == nil || strings.ToUpper(token.Value) != "SELECT" {
		return nil
	}

	for p.current() != nil && strings.ToUpper(p.current().Value) != "FROM" {
		if p.current().Value == "*" {
			node.Columns = []string{"*"}
			p.consume()
		} else if p.current().Type == IDENTIFIER {
			node.Columns = append(node.Columns, p.current().Value)
			p.consume()
		} else {
			p.consume()
		}

		if p.current() != nil && p.current().Value == "," {
			p.consume()
		}
	}

	if token := p.consume(); token == nil || strings.ToUpper(token.Value) != "FROM" {
		return nil
	}

	if token := p.consume(); token == nil || token.Type != IDENTIFIER {
		return nil
	} else {
		node.TableName = token.Value
	}

	if p.current() != nil && strings.ToUpper(p.current().Value) == "WHERE" {
		node.Where = p.parseWhere()
	}

	if p.current() != nil && strings.ToUpper(p.current().Value) == "ORDER" {
		node.OrderBy = p.parseOrderBy()
	}

	if p.current() != nil && strings.ToUpper(p.current().Value) == "LIMIT" {
		p.consume()
		if p.current() != nil && p.current().Type == LITERAL {
			if limit, err := strconv.Atoi(p.current().Value); err == nil {
				node.Limit = limit
			}
			p.consume()
		}
	}

	return node
}

func (p *SQLParser) ParseInsert() *InsertNode {
	node := &InsertNode{}

	if token := p.consume(); token == nil || strings.ToUpper(token.Value) != "INSERT" {
		return nil
	}

	if token := p.consume(); token == nil || strings.ToUpper(token.Value) != "INTO" {
		return nil
	}

	if token := p.consume(); token == nil || token.Type != IDENTIFIER {
		return nil
	} else {
		node.TableName = token.Value
	}

	if p.current() != nil && p.current().Value == "(" {
		p.consume()
		for p.current() != nil && p.current().Value != ")" {
			if p.current().Type == IDENTIFIER {
				node.Columns = append(node.Columns, p.current().Value)
			}
			p.consume()
			if p.current() != nil && p.current().Value == "," {
				p.consume()
			}
		}
		if p.current() != nil && p.current().Value == ")" {
			p.consume()
		}
	}

	if token := p.consume(); token == nil || strings.ToUpper(token.Value) != "VALUES" {
		return nil
	}

	for p.current() != nil && p.current().Value != ";" {
		if p.current().Value == "(" {
			p.consume()
			var values []interface{}
			for p.current() != nil && p.current().Value != ")" {
				if p.current().Type == LITERAL {
					value := p.current().Value
					if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
						values = append(values, value[1:len(value)-1])
					} else if intVal, err := strconv.Atoi(value); err == nil {
						values = append(values, intVal)
					} else if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
						values = append(values, floatVal)
					} else {
						values = append(values, value)
					}
				}
				p.consume()
				if p.current() != nil && p.current().Value == "," {
					p.consume()
				}
			}
			if p.current() != nil && p.current().Value == ")" {
				p.consume()
				node.Values = append(node.Values, values)
			}
		} else {
			p.consume()
		}

		if p.current() != nil && p.current().Value == "," {
			p.consume()
		}
	}

	return node
}

func (p *SQLParser) ParseUpdate() *UpdateNode {
	node := &UpdateNode{SetClauses: make(map[string]interface{})}

	if token := p.consume(); token == nil || strings.ToUpper(token.Value) != "UPDATE" {
		return nil
	}

	if token := p.consume(); token == nil || token.Type != IDENTIFIER {
		return nil
	} else {
		node.TableName = token.Value
	}

	if token := p.consume(); token == nil || strings.ToUpper(token.Value) != "SET" {
		return nil
	}

	for p.current() != nil && strings.ToUpper(p.current().Value) != "WHERE" && p.current().Value != ";" {
		if p.current().Type == IDENTIFIER {
			column := p.current().Value
			p.consume()
			if p.current() != nil && p.current().Value == "=" {
				p.consume()
				if p.current() != nil && p.current().Type == LITERAL {
					value := p.current().Value
					if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
						node.SetClauses[column] = value[1 : len(value)-1]
					} else if intVal, err := strconv.Atoi(value); err == nil {
						node.SetClauses[column] = intVal
					} else if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
						node.SetClauses[column] = floatVal
					} else {
						node.SetClauses[column] = value
					}
					p.consume()
				}
			}
		} else {
			p.consume()
		}

		if p.current() != nil && p.current().Value == "," {
			p.consume()
		}
	}

	if p.current() != nil && strings.ToUpper(p.current().Value) == "WHERE" {
		node.Where = p.parseWhere()
	}

	return node
}

func (p *SQLParser) ParseDelete() *DeleteNode {
	node := &DeleteNode{}

	if token := p.consume(); token == nil || strings.ToUpper(token.Value) != "DELETE" {
		return nil
	}

	if token := p.consume(); token == nil || strings.ToUpper(token.Value) != "FROM" {
		return nil
	}

	if token := p.consume(); token == nil || token.Type != IDENTIFIER {
		return nil
	} else {
		node.TableName = token.Value
	}

	if p.current() != nil && strings.ToUpper(p.current().Value) == "WHERE" {
		node.Where = p.parseWhere()
	}

	return node
}

func (p *SQLParser) parseWhere() *WhereClause {

	if p.current() == nil || strings.ToUpper(p.current().Value) != "WHERE" {
		return nil
	}
	p.consume()

	clause := &WhereClause{}

	if token := p.consume(); token != nil && token.Type == IDENTIFIER {
		clause.Column = token.Value
	} else {
		return nil
	}

	if token := p.consume(); token != nil && token.Type == OPERATOR {
		clause.Operator = token.Value
	} else {
		return nil
	}

	if token := p.consume(); token != nil && token.Type == LITERAL {
		value := token.Value
		if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
			clause.Value = value[1 : len(value)-1]
		} else if intVal, err := strconv.Atoi(value); err == nil {
			clause.Value = intVal
		} else if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			clause.Value = floatVal
		} else {
			clause.Value = value
		}
	} else {
		return nil
	}

	return clause
}

func (p *SQLParser) parseOrderBy() []OrderColumn {
	if p.consume() == nil || strings.ToUpper(p.current().Value) != "ORDER" {
		return nil
	}

	if p.consume() == nil || strings.ToUpper(p.current().Value) != "BY" {
		return nil
	}

	var orderBy []OrderColumn

	for p.current() != nil && strings.ToUpper(p.current().Value) != "LIMIT" && p.current().Value != ";" {
		if p.current().Type == IDENTIFIER {
			col := OrderColumn{Column: p.current().Value}
			p.consume()

			if p.current() != nil && strings.ToUpper(p.current().Value) == "DESC" {
				col.Desc = true
				p.consume()
			} else if p.current() != nil && strings.ToUpper(p.current().Value) == "ASC" {
				p.consume()
			}

			orderBy = append(orderBy, col)
		} else {
			p.consume()
		}

		if p.current() != nil && p.current().Value == "," {
			p.consume()
		}
	}

	return orderBy
}

func (p *SQLParser) parseColumnDef() *ColumnDef {
	column := &ColumnDef{}

	if token := p.consume(); token == nil || token.Type != IDENTIFIER {
		return nil
	} else {
		column.Name = token.Value
	}

	if token := p.consume(); token == nil || token.Type != DATATYPE {
		return nil
	} else {
		column.DataType = strings.ToUpper(token.Value)
	}

	if p.current() != nil && p.current().Value == "(" {
		p.consume()
		var sizes []int
		for p.current() != nil && p.current().Value != ")" {
			if p.current().Type == LITERAL {
				if size, err := strconv.Atoi(p.current().Value); err == nil {
					sizes = append(sizes, size)
				}
			}
			p.consume()
			if p.current() != nil && p.current().Value == "," {
				p.consume()
			}
		}
		if p.current() != nil && p.current().Value == ")" {
			p.consume()
		}
		column.Size = sizes
	}

	for p.current() != nil && p.current().Value != "," && p.current().Value != ")" {
		upperValue := strings.ToUpper(p.current().Value)
		switch upperValue {
		case "NOT":
			p.consume()
			if p.current() != nil && strings.ToUpper(p.current().Value) == "NULL" {
				p.consume()
				column.NotNull = true
			}
		case "PRIMARY":
			p.consume()
			if p.current() != nil && strings.ToUpper(p.current().Value) == "KEY" {
				p.consume()
				column.PrimaryKey = true
			}
		case "AUTO_INCREMENT":
			p.consume()
			column.AutoIncr = true
		case "DEFAULT":
			p.consume()
			if p.current() != nil {
				column.Default = p.current().Value
				p.consume()
			}
		default:
			p.consume()
		}
	}

	return column
}

func (p *SQLParser) parseConstraint() *Constraint {
	constraint := &Constraint{}

	upperValue := strings.ToUpper(p.current().Value)
	switch upperValue {
	case "PRIMARY":
		p.consume()
		if p.current() != nil && strings.ToUpper(p.current().Value) == "KEY" {
			p.consume()
			constraint.Type = "PRIMARY KEY"
		}
	case "FOREIGN":
		p.consume()
		if p.current() != nil && strings.ToUpper(p.current().Value) == "KEY" {
			p.consume()
			constraint.Type = "FOREIGN KEY"

			if p.current() != nil && p.current().Value == "(" {
				p.consume()
				for p.current() != nil && p.current().Value != ")" {
					if p.current().Type == IDENTIFIER {
						constraint.Columns = append(constraint.Columns, p.current().Value)
					}
					p.consume()
					if p.current() != nil && p.current().Value == "," {
						p.consume()
					}
				}
				if p.current() != nil && p.current().Value == ")" {
					p.consume()
				}
			}

			if p.current() != nil && strings.ToUpper(p.current().Value) == "REFERENCES" {
				p.consume()
				if p.current() != nil && p.current().Type == IDENTIFIER {
					constraint.RefTable = p.current().Value
					p.consume()
				}

				if p.current() != nil && p.current().Value == "(" {
					p.consume()
					for p.current() != nil && p.current().Value != ")" {
						if p.current().Type == IDENTIFIER {
							constraint.RefColumns = append(constraint.RefColumns, p.current().Value)
						}
						p.consume()
						if p.current() != nil && p.current().Value == "," {
							p.consume()
						}
					}
					if p.current() != nil && p.current().Value == ")" {
						p.consume()
					}
				}
			}
		}
	case "UNIQUE":
		p.consume()
		constraint.Type = "UNIQUE"
	}

	return constraint
}

func NewDBMS(dataDir string) *DBMS {
	dbms := &DBMS{
		Databases: make(map[string]*Database),
		DataDir:   dataDir,
	}

	os.MkdirAll(dataDir, 0755)
	dbms.loadDatabases()
	return dbms
}

func (dbms *DBMS) loadDatabases() {
	files, err := ioutil.ReadDir(dbms.DataDir)
	if err != nil {
		return
	}

	for _, file := range files {
		if file.IsDir() {
			dbName := file.Name()
			db := &Database{
				Name:    dbName,
				Tables:  make(map[string]*Table),
				Indexes: make(map[string]*Index),
			}

			dbPath := filepath.Join(dbms.DataDir, dbName)
			tableFiles, err := ioutil.ReadDir(dbPath)
			if err != nil {
				continue
			}

			for _, tableFile := range tableFiles {
				if strings.HasSuffix(tableFile.Name(), ".json") {
					tableName := strings.TrimSuffix(tableFile.Name(), ".json")
					table := dbms.loadTable(dbPath, tableName)
					if table != nil {
						db.Tables[tableName] = table
					}
				}
			}

			dbms.Databases[dbName] = db
		}
	}
}

func (dbms *DBMS) loadTable(dbPath, tableName string) *Table {
	filePath := filepath.Join(dbPath, tableName+".json")
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil
	}

	var table Table
	err = json.Unmarshal(data, &table)
	if err != nil {
		return nil
	}

	table.FilePath = filePath
	return &table
}

func (dbms *DBMS) saveTable(table *Table) error {
	data, err := json.MarshalIndent(table, "", "  ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(table.FilePath, data, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (dbms *DBMS) CreateDatabase(name string) error {
	dbPath := filepath.Join(dbms.DataDir, name)
	err := os.MkdirAll(dbPath, 0755)
	if err != nil {
		return err
	}

	db := &Database{
		Name:    name,
		Tables:  make(map[string]*Table),
		Indexes: make(map[string]*Index),
	}

	dbms.Databases[name] = db
	return nil
}

func (dbms *DBMS) UseDatabase(name string) error {
	db, exists := dbms.Databases[name]
	if !exists {
		return fmt.Errorf("database '%s' does not exist", name)
	}
	dbms.CurrentDB = db
	return nil
}

func (dbms *DBMS) ExecuteSQL(query string) (interface{}, error) {
	if dbms.CurrentDB == nil {
		return nil, fmt.Errorf("no database selected")
	}

	tokens := SQLQueryTokenizer(query)
	parser := NewSQLParser(tokens)
	ast := parser.Parse()

	if ast == nil {
		return nil, fmt.Errorf("failed to parse SQL query")
	}

	switch node := ast.(type) {
	case *CreateTableNode:
		return dbms.executeCreateTable(node)
	case *SelectNode:
		return dbms.executeSelect(node)
	case *InsertNode:
		return dbms.executeInsert(node)
	case *UpdateNode:
		return dbms.executeUpdate(node)
	case *DeleteNode:
		return dbms.executeDelete(node)
	default:
		return nil, fmt.Errorf("unsupported query type")
	}
}

func (dbms *DBMS) executeCreateTable(node *CreateTableNode) (interface{}, error) {
	if _, exists := dbms.CurrentDB.Tables[node.TableName]; exists {
		return nil, fmt.Errorf("table '%s' already exists", node.TableName)
	}

	table := &Table{
		Name:     node.TableName,
		Columns:  node.Columns,
		Data:     make([]map[string]interface{}, 0),
		NextID:   1,
		FilePath: filepath.Join(dbms.DataDir, dbms.CurrentDB.Name, node.TableName+".json"),
	}

	dbms.CurrentDB.Tables[node.TableName] = table
	err := dbms.saveTable(table)
	if err != nil {
		return nil, err
	}

	return fmt.Sprintf("Table '%s' created successfully", node.TableName), nil
}

func (dbms *DBMS) executeSelect(node *SelectNode) (interface{}, error) {
	table, exists := dbms.CurrentDB.Tables[node.TableName]
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", node.TableName)
	}

	var result []map[string]interface{}

	for _, row := range table.Data {
		if node.Where == nil || dbms.evaluateWhere(row, node.Where) {
			if len(node.Columns) == 1 && node.Columns[0] == "*" {
				result = append(result, row)
			} else {
				filteredRow := make(map[string]interface{})
				for _, col := range node.Columns {
					if val, exists := row[col]; exists {
						filteredRow[col] = val
					}
				}
				result = append(result, filteredRow)
			}
		}
	}

	if len(node.OrderBy) > 0 {
		result = dbms.sortResult(result, node.OrderBy)
	}

	if node.Limit > 0 && len(result) > node.Limit {
		result = result[:node.Limit]
	}

	return result, nil
}

func (dbms *DBMS) executeInsert(node *InsertNode) (interface{}, error) {
	table, exists := dbms.CurrentDB.Tables[node.TableName]
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", node.TableName)
	}

	insertedRows := 0

	for _, values := range node.Values {
		row := make(map[string]interface{})

		for i, col := range table.Columns {
			if col.PrimaryKey && col.AutoIncr {
				row[col.Name] = table.NextID
				table.NextID++
			} else if i < len(node.Columns) {
				for j, colName := range node.Columns {
					if colName == col.Name && j < len(values) {
						row[col.Name] = values[j]
						break
					}
				}
			} else if i < len(values) {
				row[col.Name] = values[i]
			} else if col.Default != "" {
				row[col.Name] = col.Default
			} else if !col.NotNull {
				row[col.Name] = nil
			}
		}

		table.Data = append(table.Data, row)
		insertedRows++
	}

	err := dbms.saveTable(table)
	if err != nil {
		return nil, err
	}

	return fmt.Sprintf("Inserted %d rows into '%s'", insertedRows, node.TableName), nil
}

func (dbms *DBMS) executeUpdate(node *UpdateNode) (interface{}, error) {
	table, exists := dbms.CurrentDB.Tables[node.TableName]
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", node.TableName)
	}

	updatedRows := 0

	for i := range table.Data {
		if node.Where == nil || dbms.evaluateWhere(table.Data[i], node.Where) {
			for col, value := range node.SetClauses {
				table.Data[i][col] = value
			}
			updatedRows++
		}
	}

	err := dbms.saveTable(table)
	if err != nil {
		return nil, err
	}

	return fmt.Sprintf("Updated %d rows in '%s'", updatedRows, node.TableName), nil
}

func (dbms *DBMS) executeDelete(node *DeleteNode) (interface{}, error) {
	table, exists := dbms.CurrentDB.Tables[node.TableName]
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", node.TableName)
	}

	deletedRows := 0
	var newData []map[string]interface{}

	for _, row := range table.Data {
		if node.Where == nil || !dbms.evaluateWhere(row, node.Where) {
			newData = append(newData, row)
		} else {
			deletedRows++
		}
	}

	table.Data = newData
	dbms.saveTable(table)

	return fmt.Sprintf("Deleted %d rows from '%s'", deletedRows, node.TableName), nil
}

func (dbms *DBMS) evaluateWhere(row map[string]interface{}, where *WhereClause) bool {
	value, exists := row[where.Column]
	if !exists {
		return false
	}

	switch where.Operator {
	case "=":
		return fmt.Sprintf("%v", value) == fmt.Sprintf("%v", where.Value)
	case "!=":
		return fmt.Sprintf("%v", value) != fmt.Sprintf("%v", where.Value)
	case "<":
		return dbms.compareValues(value, where.Value) < 0
	case ">":
		return dbms.compareValues(value, where.Value) > 0
	case "<=":
		return dbms.compareValues(value, where.Value) <= 0
	case ">=":
		return dbms.compareValues(value, where.Value) >= 0
	default:
		return false
	}
}

func (dbms *DBMS) compareValues(a, b interface{}) int {
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)

	if aFloat, aErr := strconv.ParseFloat(aStr, 64); aErr == nil {
		if bFloat, bErr := strconv.ParseFloat(bStr, 64); bErr == nil {
			if aFloat < bFloat {
				return -1
			} else if aFloat > bFloat {
				return 1
			}
			return 0
		}
	}

	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

func (dbms *DBMS) sortResult(result []map[string]interface{}, orderBy []OrderColumn) []map[string]interface{} {
	if len(orderBy) == 0 {
		return result
	}

	for i := 0; i < len(result)-1; i++ {
		for j := 0; j < len(result)-i-1; j++ {
			shouldSwap := false
			for _, order := range orderBy {
				val1 := result[j][order.Column]
				val2 := result[j+1][order.Column]

				cmp := dbms.compareValues(val1, val2)
				if order.Desc {
					cmp = -cmp
				}

				if cmp > 0 {
					shouldSwap = true
					break
				} else if cmp < 0 {
					break
				}
			}

			if shouldSwap {
				result[j], result[j+1] = result[j+1], result[j]
			}
		}
	}
	return result
}

func (dbms *DBMS) ShowTables() []string {
	if dbms.CurrentDB == nil {
		return []string{}
	}

	var tables []string
	for name := range dbms.CurrentDB.Tables {
		tables = append(tables, name)
	}
	return tables
}

func (dbms *DBMS) DescribeTable(tableName string) (*Table, error) {
	if dbms.CurrentDB == nil {
		return nil, fmt.Errorf("no database selected")
	}

	table, exists := dbms.CurrentDB.Tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	return table, nil
}

func (dbms *DBMS) DropTable(tableName string) error {
	if dbms.CurrentDB == nil {
		return fmt.Errorf("no database selected")
	}

	table, exists := dbms.CurrentDB.Tables[tableName]
	if !exists {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}

	os.Remove(table.FilePath)
	delete(dbms.CurrentDB.Tables, tableName)

	return nil
}

type QueryResult struct {
	Success bool
	Message string
	Data    interface{}
	Error   string
}

func (dbms *DBMS) ExecuteQuery(query string) QueryResult {
	query = strings.TrimSpace(query)
	upperQuery := strings.ToUpper(query)

	if strings.HasPrefix(upperQuery, "CREATE DATABASE") {
		parts := strings.Fields(query)
		if len(parts) >= 3 {
			dbName := parts[2]
			if strings.HasSuffix(dbName, ";") {
				dbName = dbName[:len(dbName)-1]
			}
			err := dbms.CreateDatabase(dbName)
			if err != nil {
				return QueryResult{Success: false, Error: err.Error()}
			}
			return QueryResult{Success: true, Message: fmt.Sprintf("Database '%s' created", dbName)}
		}
		return QueryResult{Success: false, Error: "Invalid CREATE DATABASE syntax"}
	}

	if strings.HasPrefix(upperQuery, "USE") {
		parts := strings.Fields(query)
		if len(parts) >= 2 {
			dbName := parts[1]
			if strings.HasSuffix(dbName, ";") {
				dbName = dbName[:len(dbName)-1]
			}
			err := dbms.UseDatabase(dbName)
			if err != nil {
				return QueryResult{Success: false, Error: err.Error()}
			}
			return QueryResult{Success: true, Message: fmt.Sprintf("Using database '%s'", dbName)}
		}
		return QueryResult{Success: false, Error: "Invalid USE syntax"}
	}

	if strings.HasPrefix(upperQuery, "SHOW TABLES") {
		tables := dbms.ShowTables()
		return QueryResult{Success: true, Data: tables, Message: fmt.Sprintf("Found %d tables", len(tables))}
	}

	if strings.HasPrefix(upperQuery, "DESCRIBE") || strings.HasPrefix(upperQuery, "DESC") {
		parts := strings.Fields(query)
		if len(parts) >= 2 {
			tableName := parts[1]
			if strings.HasSuffix(tableName, ";") {
				tableName = tableName[:len(tableName)-1]
			}
			table, err := dbms.DescribeTable(tableName)
			if err != nil {
				return QueryResult{Success: false, Error: err.Error()}
			}
			return QueryResult{Success: true, Data: table.Columns, Message: fmt.Sprintf("Table '%s' description", tableName)}
		}
		return QueryResult{Success: false, Error: "Invalid DESCRIBE syntax"}
	}

	if strings.HasPrefix(upperQuery, "DROP TABLE") {
		parts := strings.Fields(query)
		if len(parts) >= 3 {
			tableName := parts[2]
			if strings.HasSuffix(tableName, ";") {
				tableName = tableName[:len(tableName)-1]
			}
			err := dbms.DropTable(tableName)
			if err != nil {
				return QueryResult{Success: false, Error: err.Error()}
			}
			return QueryResult{Success: true, Message: fmt.Sprintf("Table '%s' dropped", tableName)}
		}
		return QueryResult{Success: false, Error: "Invalid DROP TABLE syntax"}
	}

	result, err := dbms.ExecuteSQL(query)
	if err != nil {
		return QueryResult{Success: false, Error: err.Error()}
	}

	return QueryResult{Success: true, Data: result}
}

func printQueryResult(result QueryResult) {
	fmt.Println("----------------------------------------")
	if result.Success {
		fmt.Println("✓ Query executed successfully")
		if result.Message != "" {
			fmt.Printf("Message: %s\n", result.Message)
		}
		if result.Data != nil {
			fmt.Println("Result:")
			printData(result.Data)
		}
	} else {
		fmt.Printf("✗ Error: %s\n", result.Error)
	}
	fmt.Println("----------------------------------------")
}

func printData(data interface{}) {
	switch v := data.(type) {
	case []map[string]interface{}:
		if len(v) == 0 {
			fmt.Println("  No rows returned")
			return
		}

		var columns []string
		if len(v) > 0 {
			for col := range v[0] {
				columns = append(columns, col)
			}
		}

		fmt.Printf("  ")
		for _, col := range columns {
			fmt.Printf("%-15s ", col)
		}
		fmt.Println()

		fmt.Printf("  ")
		for range columns {
			fmt.Printf("%-15s ", strings.Repeat("-", 15))
		}
		fmt.Println()

		for _, row := range v {
			fmt.Printf("  ")
			for _, col := range columns {
				val := row[col]
				if val == nil {
					fmt.Printf("%-15s ", "NULL")
				} else {
					fmt.Printf("%-15v ", val)
				}
			}
			fmt.Println()
		}
		fmt.Printf("  (%d rows)\n", len(v))

	case []string:
		if len(v) == 0 {
			fmt.Println("  No tables found")
			return
		}
		for i, item := range v {
			fmt.Printf("  %d. %s\n", i+1, item)
		}

	case []ColumnDef:
		if len(v) == 0 {
			fmt.Println("  No columns found")
			return
		}
		fmt.Printf("  %-20s %-15s %-10s %-10s %-10s\n", "Column", "Type", "Null", "Key", "Default")
		fmt.Printf("  %-20s %-15s %-10s %-10s %-10s\n", strings.Repeat("-", 20), strings.Repeat("-", 15), strings.Repeat("-", 10), strings.Repeat("-", 10), strings.Repeat("-", 10))
		for _, col := range v {
			nullStr := "YES"
			if col.NotNull {
				nullStr = "NO"
			}
			keyStr := ""
			if col.PrimaryKey {
				keyStr = "PRI"
			}
			defaultStr := col.Default
			if defaultStr == "" {
				defaultStr = "NULL"
			}
			typeStr := col.DataType
			if len(col.Size) > 0 {
				typeStr += fmt.Sprintf("(%d)", col.Size[0])
			}
			fmt.Printf("  %-20s %-15s %-10s %-10s %-10s\n", col.Name, typeStr, nullStr, keyStr, defaultStr)
		}

	default:
		fmt.Printf("  %v\n", data)
	}
}

func getTokenTypeName(tokenType TokenType) string {
	switch tokenType {
	case KEYWORD:
		return "KEYWORD"
	case IDENTIFIER:
		return "IDENTIFIER"
	case DATATYPE:
		return "DATATYPE"
	case LITERAL:
		return "LITERAL"
	case OPERATOR:
		return "OPERATOR"
	case DELIMITER:
		return "DELIMITER"
	default:
		return "UNKNOWN"
	}
}

func main() {
	fmt.Println("🗃️  Welcome to DBMS - Database Management System")
	fmt.Println("Type 'help' for available commands or 'exit' to quit")
	fmt.Println("=====================================================")

	dbms := NewDBMS("./data")
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("dbms> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		switch strings.ToLower(input) {
		case "exit", "quit":
			fmt.Println("Goodbye!")
			return

		case "help":
			printHelp()
			continue

		case "clear":
			fmt.Print("\033[H\033[2J")
			continue

		case "show databases":
			fmt.Println("Available databases:")
			if len(dbms.Databases) == 0 {
				fmt.Println("  No databases found")
			} else {
				for i, name := range getMapKeys(dbms.Databases) {
					current := ""
					if dbms.CurrentDB != nil && dbms.CurrentDB.Name == name {
						current = " (current)"
					}
					fmt.Printf("  %d. %s%s\n", i+1, name, current)
				}
			}
			continue

		case "status":
			fmt.Printf("Current Database: ")
			if dbms.CurrentDB == nil {
				fmt.Println("None")
			} else {
				fmt.Printf("%s (%d tables)\n", dbms.CurrentDB.Name, len(dbms.CurrentDB.Tables))
			}
			fmt.Printf("Total Databases: %d\n", len(dbms.Databases))
			continue
		}

		if !strings.HasSuffix(input, ";") {
			input += ";"
		}

		start := time.Now()
		result := dbms.ExecuteQuery(input)
		duration := time.Since(start)

		printQueryResult(result)
		fmt.Printf("Query executed in %v\n\n", duration)
	}
}

func getMapKeys(m map[string]*Database) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func printHelp() {
	fmt.Println(`
Available Commands:
===================

Database Operations:
  CREATE DATABASE <name>;           - Create a new database
  USE <database>;                   - Switch to a database
  SHOW DATABASES                    - List all databases
  
Table Operations:
  CREATE TABLE <name> (...);        - Create a new table
  SHOW TABLES;                      - List tables in current database
  DESCRIBE <table>;                 - Show table structure
  DROP TABLE <table>;               - Delete a table
  
Data Operations:
  INSERT INTO <table> VALUES (...); - Insert data into table
  SELECT * FROM <table>;            - Query data from table
  SELECT <cols> FROM <table> WHERE <condition>;
  UPDATE <table> SET <col>=<val> WHERE <condition>;
  DELETE FROM <table> WHERE <condition>;
  
System Commands:
  STATUS                            - Show current database status
  HELP                              - Show this help message
  CLEAR                             - Clear screen
  EXIT/QUIT                         - Exit the application

Example Usage:
  CREATE DATABASE company;
  USE company;
  CREATE TABLE employees (
      id INT PRIMARY KEY AUTO_INCREMENT,
      name VARCHAR(100) NOT NULL,
      salary DECIMAL(10,2),
      hire_date DATE
  );
  INSERT INTO employees VALUES (1, 'John Doe', 50000.00, '2023-01-15');
  SELECT * FROM employees WHERE salary > 40000;
`)
}
