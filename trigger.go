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
	triggerSqlTemplate = initTriggerSqlTemplate()
)

// Initializes the Sql template
func initTriggerSqlTemplate() *template.Template {
	query := `
    SELECT n.nspname AS schema_name
       , {{if eq $.DbSchema "*" }}n.nspname || '.' || {{end}}c.relname || '.' || t.tgname AS compare_name
       , c.relname AS table_name
       , t.tgname AS trigger_name
       , pg_catalog.pg_get_triggerdef(t.oid, true) AS trigger_def
       , t.tgenabled AS enabled
    FROM pg_catalog.pg_trigger t
    INNER JOIN pg_catalog.pg_class c ON (c.oid = t.tgrelid)
    INNER JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
	WHERE not t.tgisinternal
    {{if eq $.DbSchema "*" }}
    AND n.nspname NOT LIKE 'pg_%' 
    AND n.nspname <> 'information_schema' 
    {{else}}
    AND n.nspname = '{{$.DbSchema}}'
    {{end}}
	`
	t := template.New("TriggerSqlTmpl")
	template.Must(t.Parse(query))
	return t
}

// ==================================
// TriggerRows definition
// ==================================

// TriggerRows is a sortable slice of string maps
type TriggerRows []map[string]string

func (slice TriggerRows) Len() int {
	return len(slice)
}

func (slice TriggerRows) Less(i, j int) bool {
	return slice[i]["compare_name"] < slice[j]["compare_name"]
}

func (slice TriggerRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// TriggerSchema holds a channel streaming trigger information from one of the databases as well as
// a reference to the current row of data we're viewing.
//
// TriggerSchema implements the Schema interface defined in pgdiff.go
type TriggerSchema struct {
	rows     TriggerRows
	rowNum   int
	done     bool
	dbSchema string
	other    *TriggerSchema
}

// get returns the value from the current row for the given key
func (c *TriggerSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *TriggerSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *TriggerSchema) Compare(obj Schema) (int, *Error) {
	c2, ok := obj.(*TriggerSchema)
	if !ok {
		err := Error(fmt.Sprint("compare(obj) needs a TriggerSchema instance", c2))
		return +999, &err
	}
	c.other = c2

	val := misc.CompareStrings(c.get("compare_name"), c.other.get("compare_name"))
	return val, nil
}

// Add returns SQL to create the trigger
func (c TriggerSchema) Add() []Stringer {
	// If we are comparing two different schemas against each other, we need to do some
	// modification of the first trigger definition, so we create it in the right dbSchema
	triggerDef := c.get("trigger_def")
	schemaName := c.get("schema_name")
	if c.dbSchema != c.other.dbSchema {
		schemaName = c.other.dbSchema
		triggerDef = strings.Replace(
			triggerDef,
			fmt.Sprintf(" %s.%s ", c.get("schema_name"), c.get("table_name")),
			fmt.Sprintf(" %s.%s ", schemaName, c.get("table_name")),
			-1)
	}

	return []Stringer{Line(fmt.Sprintf("%s;", triggerDef))}
}

// Drop returns SQL to drop the trigger
func (c TriggerSchema) Drop() []Stringer {
	return []Stringer{Line(fmt.Sprintf("DROP TRIGGER %s ON %s.%s;", c.get("trigger_name"), c.get("schema_name"), c.get("table_name")))}
}

// Change handles the case where the trigger names match, but the definition does not
func (c *TriggerSchema) Change() []Stringer {
	if c.get("trigger_def") == c.other.get("trigger_def") {
		return nil
	}

	// If we are comparing two different schemas against each other, we need to do some
	// modification of the first trigger definition, so we create it in the right dbSchema
	triggerDef := c.get("trigger_def")
	schemaName := c.get("schema_name")
	if c.dbSchema != c.other.dbSchema {
		schemaName = c.other.dbSchema
		triggerDef = strings.Replace(
			triggerDef,
			fmt.Sprintf(" %s.%s ", c.get("schema_name"), c.get("table_name")),
			fmt.Sprintf(" %s.%s ", schemaName, c.get("table_name")),
			-1)
	}

	// The trigger_def column has everything needed to rebuild the function
	return []Stringer{
		Notice("-- This function looks different so we'll drop and recreate it:"),
		Line(fmt.Sprintf("DROP TRIGGER %s ON %s.%s;", c.get("trigger_name"), schemaName, c.get("table_name"))),
		Notice("-- STATEMENT-BEGIN"),
		Line(fmt.Sprintf("%s;", triggerDef)),
		Notice("-- STATEMENT-END"),
	}
}

// dBSourceTriggerSchema returns a TriggerSchema that outputs SQL to make the triggers match between DBs
func dBSourceTriggerSchema(conn1 *sql.DB, dbInfo *pgutil.DbInfo) (Schema, error) {
	buf1 := new(bytes.Buffer)
	err := triggerSqlTemplate.Execute(buf1, dbInfo)
	if err != nil {
		return nil, err
	}

	rowChan1, _ := pgutil.QueryStrings(conn1, buf1.String())

	rows1 := make(TriggerRows, 0)
	for row := range rowChan1 {
		rows1 = append(rows1, row)
	}
	sort.Sort(rows1)

	return &TriggerSchema{rows: rows1, rowNum: -1, dbSchema: dbInfo.DbSchema}, nil
}
