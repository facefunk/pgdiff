//
// Copyright (c) 2017 Jon Carlson.  All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//

package pgdiff

import (
	"bytes"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"text/template"

	"github.com/joncrlsn/misc"
	"github.com/joncrlsn/pgutil"
)

var (
	functionSqlTemplate = initFunctionSqlTemplate()
)

// Initializes the Sql template
func initFunctionSqlTemplate() *template.Template {
	query := `
    SELECT n.nspname                 AS schema_name
        , {{if eq $.DbSchema "*" }}n.nspname || '.' || {{end}}p.proname AS compare_name
        , p.proname                  AS function_name
        , p.oid::regprocedure        AS fancy
        , t.typname                  AS return_type
        , pg_get_functiondef(p.oid)  AS definition
    FROM pg_proc AS p
    JOIN pg_type t ON (p.prorettype = t.oid)
    JOIN pg_namespace n ON (n.oid = p.pronamespace)
    JOIN pg_language l ON (p.prolang = l.oid AND l.lanname IN ('c','plpgsql', 'sql'))
    WHERE true
	{{if eq $.DbSchema "*" }}
    AND n.nspname NOT LIKE 'pg_%' 
    AND n.nspname <> 'information_schema' 
    {{else}}
    AND n.nspname = '{{$.DbSchema}}'
    {{end}};
	`
	t := template.New("FunctionSqlTmpl")
	template.Must(t.Parse(query))
	return t
}

// ==================================
// FunctionRows definition
// ==================================

// FunctionRows is a sortable slice of string maps
type FunctionRows []map[string]string

func (slice FunctionRows) Len() int {
	return len(slice)
}

func (slice FunctionRows) Less(i, j int) bool {
	return slice[i]["compare_name"] < slice[j]["compare_name"]
}

func (slice FunctionRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// FunctionSchema holds a channel streaming function information from one of the databases as well as
// a reference to the current row of data we're viewing.
//
// FunctionSchema implements the Schema interface defined in pgdiff.go
type FunctionSchema struct {
	rows   FunctionRows
	rowNum int
	done   bool
	schema string
	other  *FunctionSchema
}

// get returns the value from the current row for the given key
func (c *FunctionSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *FunctionSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *FunctionSchema) Compare(obj Schema) (int, *Error) {
	c2, ok := obj.(*FunctionSchema)
	if !ok {
		err := Error(fmt.Sprint("compare(obj) needs a FunctionSchema instance", c2))
		return +999, &err
	}
	c.other = c2

	val := misc.CompareStrings(c.get("compare_name"), c.other.get("compare_name"))
	//strs = append(strs, Line(fmt.Sprintf("-- Compared %v: %s with %s \n", val, c.get("function_name"), c.other.get("function_name"))))
	return val, nil
}

// Add returns SQL to create the function
func (c *FunctionSchema) Add() []Stringer {
	// If we are comparing two different schemas against each other, we need to do some
	// modification of the first function definition, so we create it in the right dbSchema
	functionDef := c.get("definition")
	if c.schema != c.other.schema {
		functionDef = strings.Replace(
			functionDef,
			fmt.Sprintf("FUNCTION %s.%s(", c.get("schema_name"), c.get("function_name")),
			fmt.Sprintf("FUNCTION %s.%s(", c.other.schema, c.get("function_name")),
			-1)
	}

	return []Stringer{
		Notice("-- STATEMENT-BEGIN"),
		Line(functionDef + ";"),
		Notice("-- STATEMENT-END"),
	}
}

// Drop returns SQL to drop the function
func (c FunctionSchema) Drop() []Stringer {
	return []Stringer{
		Notice("-- Note that CASCADE in the statement below will also drop any triggers depending on this function."),
		Notice("-- Also, if there are two functions with this name, you will want to add arguments to identify the correct one to drop."),
		Notice("-- (See http://www.postgresql.org/docs/9.4/interactive/sql-dropfunction.html) "),
		Line(fmt.Sprintf("DROP FUNCTION %s.%s CASCADE;", c.get("schema_name"), c.get("function_name"))),
	}
}

// Change handles the case where the function names match, but the definition does not
func (c FunctionSchema) Change() []Stringer {
	if c.get("definition") == c.other.get("definition") {
		return nil
	}

	// If we are comparing two different schemas against each other, we need to do some
	// modification of the first function definition, so we create it in the right dbSchema
	functionDef := c.get("definition")
	if c.schema != c.other.schema {
		functionDef = strings.Replace(
			functionDef,
			fmt.Sprintf("FUNCTION %s.%s(", c.get("schema_name"), c.get("function_name")),
			fmt.Sprintf("FUNCTION %s.%s(", c.other.schema, c.get("function_name")),
			-1)
	}

	// The definition column has everything needed to rebuild the function
	return []Stringer{
		Notice("-- This function is different so we'll recreate it:"),
		Notice("-- STATEMENT-BEGIN"),
		Line(fmt.Sprintf("%s;", functionDef)),
		Notice("-- STATEMENT-END"),
	}
}

// ==================================
// Functions
// ==================================

// CompareFunctions outputs SQL to make the functions match between DBs
func CompareFunctions(conn1 *sql.DB, conn2 *sql.DB, dbInfo1 *pgutil.DbInfo, dbInfo2 *pgutil.DbInfo) []Stringer {
	var errs []Stringer
	buf1 := new(bytes.Buffer)
	err := functionSqlTemplate.Execute(buf1, dbInfo1)
	if err != nil {
		errs = append(errs, Error(err.Error()))
	}
	buf2 := new(bytes.Buffer)
	err = functionSqlTemplate.Execute(buf2, dbInfo2)
	if err != nil {
		errs = append(errs, Error(err.Error()))
	}
	if len(errs) > 0 {
		return errs
	}

	rowChan1, _ := pgutil.QueryStrings(conn1, buf1.String())
	rowChan2, _ := pgutil.QueryStrings(conn2, buf2.String())

	rows1 := make(FunctionRows, 0)
	for row := range rowChan1 {
		rows1 = append(rows1, row)
	}
	sort.Sort(rows1)

	rows2 := make(FunctionRows, 0)
	for row := range rowChan2 {
		rows2 = append(rows2, row)
	}
	sort.Sort(rows2)

	// We must explicitly type this as Schema here
	var schema1 Schema = &FunctionSchema{rows: rows1, rowNum: -1, schema: dbInfo1.DbSchema}
	var schema2 Schema = &FunctionSchema{rows: rows2, rowNum: -1, schema: dbInfo2.DbSchema}

	// Compare the functions
	return doDiff(schema1, schema2)
}
