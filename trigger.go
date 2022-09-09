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

func NewTriggerSchema(rows TriggerRows, dbSchema string) *TriggerSchema {
	return &TriggerSchema{rows: rows, rowNum: -1, dbSchema: dbSchema}
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
		return +999, NewError(fmt.Sprint("compare(obj) needs a TriggerSchema instance", c2))
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

	return []Stringer{NewLine(fmt.Sprintf("%s;", triggerDef))}
}

// Drop returns SQL to drop the trigger
func (c TriggerSchema) Drop() []Stringer {
	return []Stringer{NewLine(fmt.Sprintf("DROP TRIGGER %s ON %s.%s;", c.get("trigger_name"), c.get("schema_name"), c.get("table_name")))}
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
		NewNotice("-- This function looks different so we'll drop and recreate it:"),
		NewLine(fmt.Sprintf("DROP TRIGGER %s ON %s.%s;", c.get("trigger_name"), schemaName, c.get("table_name"))),
		NewNotice("-- STATEMENT-BEGIN"),
		NewLine(fmt.Sprintf("%s;", triggerDef)),
		NewNotice("-- STATEMENT-END"),
	}
}
