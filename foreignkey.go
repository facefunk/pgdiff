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
// ForeignKeyRows definition
// ==================================

// ForeignKeyRows is a sortable string map
type ForeignKeyRows []map[string]string

func (slice ForeignKeyRows) Len() int {
	return len(slice)
}

func (slice ForeignKeyRows) Less(i, j int) bool {
	if slice[i]["compare_name"] != slice[j]["compare_name"] {
		return slice[i]["compare_name"] < slice[j]["compare_name"]
	}
	return slice[i]["constraint_def"] < slice[j]["constraint_def"]
}

func (slice ForeignKeyRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// ==================================
// ForeignKeySchema definition
// (implements Schema -- defined in pgdiff.go)
// ==================================

// ForeignKeySchema holds a slice of rows from one of the databases as well as
// a reference to the current row of data we're viewing.
type ForeignKeySchema struct {
	rows     ForeignKeyRows
	rowNum   int
	done     bool
	dbSchema string
	other    *ForeignKeySchema
}

func NewForeignKeySchema(rows ForeignKeyRows, dbSchema string) *ForeignKeySchema {
	return &ForeignKeySchema{rows: rows, rowNum: -1, dbSchema: dbSchema}
}

// get returns the value from the current row for the given key
func (c *ForeignKeySchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

// get returns the current row for the given key
func (c *ForeignKeySchema) getRow() map[string]string {
	if c.rowNum >= len(c.rows) {
		return make(map[string]string)
	}
	return c.rows[c.rowNum]
}

// NextRow reads from the channel and tells you if there are (probably) more or not
func (c *ForeignKeySchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *ForeignKeySchema) Compare(obj Schema) (int, *Error) {
	c2, ok := obj.(*ForeignKeySchema)
	if !ok {
		return +999, NewError(fmt.Sprint("compare(obj) needs a ForeignKeySchema instance", c2))
	}
	c.other = c2

	//strs = append(strs, NewLine(fmt.Sprintf("Comparing %s with %s", c.get("table_name"), c.other.get("table_name"))))
	val := misc.CompareStrings(c.get("compare_name"), c.other.get("compare_name"))
	if val != 0 {
		return val, nil
	}

	val = misc.CompareStrings(c.get("constraint_def"), c.other.get("constraint_def"))
	return val, nil
}

// Add returns SQL to add the foreign key
func (c *ForeignKeySchema) Add() []Stringer {
	schema := c.other.dbSchema
	if schema == "*" {
		schema = c.get("schema_name")
	}
	return []Stringer{NewLine(fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s %s;", schema, c.get("table_name"), c.get("fk_name"), c.get("constraint_def")))}
}

// Drop returns SQL to drop the foreign key
func (c ForeignKeySchema) Drop() []Stringer {
	return []Stringer{NewLine(fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s; -- %s", c.get("schema_name"), c.get("table_name"), c.get("fk_name"), c.get("constraint_def")))}
}

// Change handles the case where the table and foreign key name, but the details do not
func (c *ForeignKeySchema) Change() []Stringer {
	// There is no "changing" a foreign key.  It either gets created or dropped (or left as-is).
	return nil
}
