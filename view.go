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
// ViewRows definition
// ==================================

// ViewRows is a sortable slice of string maps
type ViewRows []map[string]string

func (slice ViewRows) Len() int {
	return len(slice)
}

func (slice ViewRows) Less(i, j int) bool {
	return slice[i]["viewname"] < slice[j]["viewname"]
}

func (slice ViewRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// ViewSchema holds a channel streaming view information from one of the databases as well as
// a reference to the current row of data we're viewing.
//
// ViewSchema implements the Schema interface defined in pgdiff.go
type ViewSchema struct {
	rows   ViewRows
	rowNum int
	done   bool
	other  *ViewSchema
}

func NewViewSchema(rows ViewRows) *ViewSchema {
	return &ViewSchema{rows: rows, rowNum: -1}
}

// get returns the value from the current row for the given key
func (c *ViewSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *ViewSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *ViewSchema) Compare(obj Schema) (int, *Error) {
	c2, ok := obj.(*ViewSchema)
	if !ok {
		err := Error(fmt.Sprint("compare(obj) needs a ViewSchema instance", c2))
		return +999, &err
	}
	c.other = c2

	val := misc.CompareStrings(c.get("viewname"), c.other.get("viewname"))
	//strs = append(strs, Line(fmt.Sprintf("-- Compared %v: %s with %s \n", val, c.get("viewname"), c.other.get("viewname"))))
	return val, nil
}

// Add returns SQL to create the view
func (c ViewSchema) Add() []Stringer {
	return []Stringer{Line(fmt.Sprintf("CREATE VIEW %s AS %s", c.get("viewname"), c.get("definition")))}
}

// Drop returns SQL to drop the view
func (c ViewSchema) Drop() []Stringer {
	return []Stringer{Line(fmt.Sprintf("DROP VIEW %s;", c.get("viewname")))}
}

// Change handles the case where the names match, but the definition does not
func (c ViewSchema) Change() []Stringer {
	if c.get("definition") != c.other.get("definition") {
		return nil
	}

	return []Stringer{
		Line(fmt.Sprintf("DROP VIEW %s;", c.get("viewname"))),
		Line(fmt.Sprintf("CREATE VIEW %s AS %s", c.get("viewname"), c.get("definition"))),
	}
}
