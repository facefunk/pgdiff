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
// SchemataRows definition
// ==================================

// SchemataRows is a sortable slice of string maps
type SchemataRows []map[string]string

func (slice SchemataRows) Len() int {
	return len(slice)
}

func (slice SchemataRows) Less(i, j int) bool {
	return slice[i]["schema_name"] < slice[j]["schema_name"]
}

func (slice SchemataRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// SchemataSchema holds a channel streaming dbSchema meta information from one of the databases as well as
// a reference to the current row of data we're viewing.
//
// SchemataSchema implements the Schema interface defined in pgdiff.go
type SchemataSchema struct {
	rows   SchemataRows
	rowNum int
	done   bool
	other  *SchemataSchema
}

func NewSchemataSchema(rows SchemataRows) *SchemataSchema {
	return &SchemataSchema{rows: rows, rowNum: -1}
}

// get returns the value from the current row for the given key
func (c *SchemataSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *SchemataSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *SchemataSchema) Compare(obj Schema) (int, *Error) {
	c2, ok := obj.(*SchemataSchema)
	if !ok {
		err := Error(fmt.Sprint("compare(obj) needs a SchemataSchema instance", c2))
		return +999, &err
	}
	c.other = c2

	val := misc.CompareStrings(c.get("schema_name"), c.other.get("schema_name"))
	//strs = append(strs, Line(fmt.Sprintf("-- Compared %v: %s with %s \n", val, c.get("schema_name"), c.other.get("schema_name"))))
	return val, nil
}

// Add returns SQL to add the schemata
func (c SchemataSchema) Add() []Stringer {
	// CREATE SCHEMA schema_name [ AUTHORIZATION user_name
	return []Stringer{Line(fmt.Sprintf("CREATE SCHEMA %s AUTHORIZATION %s;", c.get("schema_name"), c.get("schema_owner")))}
}

// Drop returns SQL to drop the schemata
func (c SchemataSchema) Drop() []Stringer {
	// DROP SCHEMA [ IF EXISTS ] name [, ...] [ CASCADE | RESTRICT ]
	return []Stringer{Line(fmt.Sprintf("DROP SCHEMA IF EXISTS %s;", c.get("schema_name")))}
}

// Change handles the case where the dbSchema name matches, but the details do not
func (c SchemataSchema) Change() []Stringer {
	// There's nothing we need to do here
	return nil
}
