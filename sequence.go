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
// SequenceRows definition
// ==================================

// SequenceRows is a sortable slice of string maps
type SequenceRows []map[string]string

func (slice SequenceRows) Len() int {
	return len(slice)
}

func (slice SequenceRows) Less(i, j int) bool {
	return slice[i]["compare_name"] < slice[j]["compare_name"]
}

func (slice SequenceRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// SequenceSchema holds a channel streaming sequence information from one of the databases as well as
// a reference to the current row of data we're viewing.
//
// SequenceSchema implements the Schema interface defined in pgdiff.go
type SequenceSchema struct {
	rows     SequenceRows
	rowNum   int
	done     bool
	dbSchema string
	other    *SequenceSchema
}

func NewSequenceSchema(rows SequenceRows, dbSchema string) *SequenceSchema {
	return &SequenceSchema{rows: rows, rowNum: -1, dbSchema: dbSchema}
}

// get returns the value from the current row for the given key
func (c *SequenceSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *SequenceSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *SequenceSchema) Compare(obj Schema) (int, *Error) {
	c2, ok := obj.(*SequenceSchema)
	if !ok {
		err := Error(fmt.Sprint("compare(obj) needs a SequenceSchema instance", c2))
		return +999, &err
	}
	c.other = c2

	val := misc.CompareStrings(c.get("compare_name"), c.other.get("compare_name"))
	return val, nil
}

// Add returns SQL to add the sequence
func (c *SequenceSchema) Add() []Stringer {
	schema := c.other.dbSchema
	if schema == "*" {
		schema = c.get("schema_name")
	}
	return []Stringer{Line(fmt.Sprintf("CREATE SEQUENCE %s.%s INCREMENT %s MINVALUE %s MAXVALUE %s START %s;", schema, c.get("sequence_name"), c.get("increment"), c.get("minimum_value"), c.get("maximum_value"), c.get("start_value")))}
}

// Drop returns SQL to drop the sequence
func (c SequenceSchema) Drop() []Stringer {
	return []Stringer{Line(fmt.Sprintf("DROP SEQUENCE %s.%s;", c.get("schema_name"), c.get("sequence_name")))}
}

// Change doesn't do anything right now.
func (c SequenceSchema) Change() []Stringer {
	// Don't know of anything helpful we should do here
	return nil
}
