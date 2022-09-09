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
// OwnerRows definition
// ==================================

// OwnerRows is a sortable slice of string maps
type OwnerRows []map[string]string

func (slice OwnerRows) Len() int {
	return len(slice)
}

func (slice OwnerRows) Less(i, j int) bool {
	return slice[i]["compare_name"] < slice[j]["compare_name"]
}

func (slice OwnerRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// ==================================
// OwnerSchema definition
// (implements Schema -- defined in pgdiff.go)
// ==================================

// OwnerSchema holds a slice of rows from one of the databases as well as
// a reference to the current row of data we're viewing.
type OwnerSchema struct {
	rows   OwnerRows
	rowNum int
	done   bool
	other  *OwnerSchema
}

func NewOwnerSchema(rows OwnerRows) *OwnerSchema {
	return &OwnerSchema{rows: rows, rowNum: -1}
}

// get returns the value from the current row for the given key
func (c *OwnerSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

// get returns the current row for the given key
func (c *OwnerSchema) getRow() map[string]string {
	if c.rowNum >= len(c.rows) {
		return make(map[string]string)
	}
	return c.rows[c.rowNum]
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *OwnerSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *OwnerSchema) Compare(obj Schema) (int, *Error) {
	c2, ok := obj.(*OwnerSchema)
	if !ok {
		return +999, NewError(fmt.Sprint("compare needs a OwnerSchema instance", c2))
	}
	c.other = c2
	val := misc.CompareStrings(c.get("compare_name"), c.other.get("compare_name"))
	return val, nil
}

// Add generates SQL to add the table/view owner
func (c OwnerSchema) Add() []Stringer {
	return []Stringer{NewNotice(fmt.Sprintf("-- Notice!, db2 has no %s named %s.  First, run pgdiff with the %s option.", c.get("type"), c.get("relationship_name"), c.get("type")))}
}

// Drop generates SQL to drop the owner
func (c OwnerSchema) Drop() []Stringer {
	return []Stringer{NewNotice(fmt.Sprintf("-- Notice!, db2 has a %s that db1 does not: %s.   First, run pgdiff with the %s option.", c.get("type"), c.get("relationship_name"), c.get("type")))}
}

// Change handles the case where the relationship name matches, but the owner does not
func (c OwnerSchema) Change() []Stringer {
	if c.get("owner") != c.other.get("owner") {
		return []Stringer{NewLine(fmt.Sprintf("ALTER %s %s.%s OWNER TO %s;", c.get("type"), c.other.get("schema_name"), c.get("relationship_name"), c.get("owner")))}
	}
	return nil
}
