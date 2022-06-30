//
// Copyright (c) 2016 Jon Carlson.  All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//

package pgdiff

import (
	"fmt"

	"github.com/joncrlsn/misc"
)

// ==================================
// MatViewRows definition
// ==================================

// MatViewRows is a sortable slice of string maps
type MatViewRows []map[string]string

func (slice MatViewRows) Len() int {
	return len(slice)
}

func (slice MatViewRows) Less(i, j int) bool {
	return slice[i]["matviewname"] < slice[j]["matviewname"]
}

func (slice MatViewRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// MatViewSchema holds a channel streaming matview information from one of the databases as well as
// a reference to the current row of data we're matviewing.
//
// MatViewSchema implements the Schema interface defined in pgdiff.go
type MatViewSchema struct {
	rows   MatViewRows
	rowNum int
	done   bool
	other  *MatViewSchema
}

func NewMatViewSchema(rows MatViewRows) *MatViewSchema {
	return &MatViewSchema{rows: rows, rowNum: -1}
}

// get returns the value from the current row for the given key
func (c *MatViewSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *MatViewSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *MatViewSchema) Compare(obj Schema) (int, *Error) {
	c2, ok := obj.(*MatViewSchema)
	if !ok {
		err := Error(fmt.Sprint("compare(obj) needs a MatViewSchema instance", c2))
		return +999, &err
	}
	c.other = c2

	val := misc.CompareStrings(c.get("matviewname"), c.other.get("matviewname"))
	//strs = append(strs, Line(fmt.Sprintf("-- Compared %v: %s with %s \n", val, c.get("matviewname"), c.other.get("matviewname"))))
	return val, nil
}

// Add returns SQL to create the matview
func (c MatViewSchema) Add() []Stringer {
	return []Stringer{
		Line(fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS %s", c.get("matviewname"), c.get("definition"))),
		Line(""),
		Line(c.get("indexdef")),
		Line(""),
	}
}

// Drop returns SQL to drop the matview
func (c MatViewSchema) Drop() []Stringer {
	return []Stringer{Line(fmt.Sprintf("DROP MATERIALIZED VIEW %s;", c.get("matviewname")))}
}

// Change handles the case where the names match, but the definition does not
func (c MatViewSchema) Change() []Stringer {
	var strs []Stringer

	if c.get("definition") != c.other.get("definition") {
		strs = append(strs,
			Line(fmt.Sprintf("DROP MATERIALIZED VIEW %s;", c.get("matviewname"))),
			Line(fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS %s", c.get("matviewname"), c.get("definition"))),
			Line(c.get("indexdef")),
		)
	}
	return strs
}
