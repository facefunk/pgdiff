//
// Copyright (c) 2017 Jon Carlson.
// Copyright (c) 2022 Facefunk.
// All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.
//

package pgdiff

import (
	"fmt"

	"github.com/joncrlsn/misc"
)

// ==================================
// TableRows definition
// ==================================

// TableRows is a sortable slice of string maps
type TableRows []map[string]string

func (slice TableRows) Len() int {
	return len(slice)
}

func (slice TableRows) Less(i, j int) bool {
	return slice[i]["compare_name"] < slice[j]["compare_name"]
}

func (slice TableRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// TableSchema holds a channel streaming table information from one of the databases as well as
// a reference to the current row of data we're viewing.
//
// TableSchema implements the Schema interface defined in pgdiff.go
type TableSchema struct {
	rows     TableRows
	rowNum   int
	done     bool
	dbSchema string
	other    *TableSchema
}

func NewTableSchema(rows TableRows, dbSchema string) *TableSchema {
	return &TableSchema{rows: rows, rowNum: -1, dbSchema: dbSchema}
}

// get returns the value from the current row for the given key
func (c *TableSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *TableSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *TableSchema) Compare(obj Schema) (int, *Error) {
	c2, ok := obj.(*TableSchema)
	if !ok {
		return +999, NewError(fmt.Sprint("compare(obj) needs a TableSchema instance", c2))
	}
	c.other = c2

	val := misc.CompareStrings(c.get("compare_name"), c.other.get("compare_name"))
	//strs = append(strs, NewLine(fmt.Sprintf("-- Compared %v: %s with %s \n", val, c.get("table_name"), c.other.get("table_name"))))
	return val, nil
}

// Add returns SQL to add the table or view
func (c TableSchema) Add() []Stringer {
	schema := c.other.dbSchema
	if schema == "*" {
		schema = c.get("table_schema")
	}
	return []Stringer{NewLine(fmt.Sprintf("CREATE %s %s.%s();", c.get("table_type"), schema, c.get("table_name")))}
}

// Drop returns SQL to drop the table or view
func (c TableSchema) Drop() []Stringer {
	return []Stringer{NewLine(fmt.Sprintf("DROP %s %s.%s;", c.get("table_type"), c.get("table_schema"), c.get("table_name")))}
}

// Change handles the case where the table and column match, but the details do not
func (c TableSchema) Change() []Stringer {
	// There's nothing we need to do here
	return nil
}
