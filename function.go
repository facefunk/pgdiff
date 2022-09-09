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
	"strings"

	"github.com/joncrlsn/misc"
)

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
	rows     FunctionRows
	rowNum   int
	done     bool
	dbSchema string
	other    *FunctionSchema
}

func NewFunctionSchema(rows FunctionRows, dbSchema string) *FunctionSchema {
	return &FunctionSchema{rows: rows, rowNum: -1, dbSchema: dbSchema}
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
		return +999, NewError(fmt.Sprint("compare(obj) needs a FunctionSchema instance", c2))
	}
	c.other = c2

	val := misc.CompareStrings(c.get("compare_name"), c.other.get("compare_name"))
	//strs = append(strs, NewLine(fmt.Sprintf("-- Compared %v: %s with %s \n", val, c.get("function_name"), c.other.get("function_name"))))
	return val, nil
}

// Add returns SQL to create the function
func (c *FunctionSchema) Add() []Stringer {
	// If we are comparing two different schemas against each other, we need to do some
	// modification of the first function definition, so we create it in the right dbSchema
	functionDef := c.get("definition")
	if c.dbSchema != c.other.dbSchema {
		functionDef = strings.Replace(
			functionDef,
			fmt.Sprintf("FUNCTION %s.%s(", c.get("schema_name"), c.get("function_name")),
			fmt.Sprintf("FUNCTION %s.%s(", c.other.dbSchema, c.get("function_name")),
			-1)
	}

	return []Stringer{
		NewNotice("-- STATEMENT-BEGIN"),
		NewLine(functionDef + ";"),
		NewNotice("-- STATEMENT-END"),
	}
}

// Drop returns SQL to drop the function
func (c FunctionSchema) Drop() []Stringer {
	return []Stringer{
		NewNotice("-- Note that CASCADE in the statement below will also drop any triggers depending on this function."),
		NewNotice("-- Also, if there are two functions with this name, you will want to add arguments to identify the correct one to drop."),
		NewNotice("-- (See http://www.postgresql.org/docs/9.4/interactive/sql-dropfunction.html) "),
		NewLine(fmt.Sprintf("DROP FUNCTION %s.%s CASCADE;", c.get("schema_name"), c.get("function_name"))),
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
	if c.dbSchema != c.other.dbSchema {
		functionDef = strings.Replace(
			functionDef,
			fmt.Sprintf("FUNCTION %s.%s(", c.get("schema_name"), c.get("function_name")),
			fmt.Sprintf("FUNCTION %s.%s(", c.other.dbSchema, c.get("function_name")),
			-1)
	}

	// The definition column has everything needed to rebuild the function
	return []Stringer{
		NewNotice("-- This function is different so we'll recreate it:"),
		NewNotice("-- STATEMENT-BEGIN"),
		NewLine(fmt.Sprintf("%s;", functionDef)),
		NewNotice("-- STATEMENT-END"),
	}
}
