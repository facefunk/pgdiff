//
// Copyright (c) 2017 Jon Carlson.  All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//

package pgdiff

import (
	"fmt"
	"strings"

	"github.com/joncrlsn/misc"
)

// ==================================
// IndexRows definition
// ==================================

// IndexRows is a sortable slice of string maps
type IndexRows []map[string]string

func (slice IndexRows) Len() int {
	return len(slice)
}

func (slice IndexRows) Less(i, j int) bool {
	//strs = append(strs, Line(fmt.Sprintf("--Less %s:%s with %s:%s", slice[i]["table_name"], slice[i]["column_name"], slice[j]["table_name"], slice[j]["column_name"])))
	return slice[i]["compare_name"] < slice[j]["compare_name"]
}

func (slice IndexRows) Swap(i, j int) {
	//strs = append(strs, Line(fmt.Sprintf("--Swapping %d/%s:%s with %d/%s:%s \n", i, slice[i]["table_name"], slice[i]["index_name"], j, slice[j]["table_name"], slice[j]["index_name"])))
	slice[i], slice[j] = slice[j], slice[i]
}

// ==================================
// IndexSchema definition
// (implements Schema -- defined in pgdiff.go)
// ==================================

// IndexSchema holds a slice of rows from one of the databases as well as
// a reference to the current row of data we're viewing.
type IndexSchema struct {
	rows     IndexRows
	rowNum   int
	done     bool
	dbSchema string
	other    *IndexSchema
}

func NewIndexSchema(rows IndexRows, dbSchema string) *IndexSchema {
	return &IndexSchema{rows: rows, rowNum: -1, dbSchema: dbSchema}
}

// get returns the value from the current row for the given key
func (c *IndexSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

// get returns the current row for the given key
func (c *IndexSchema) getRow() map[string]string {
	if c.rowNum >= len(c.rows) {
		return make(map[string]string)
	}
	return c.rows[c.rowNum]
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *IndexSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *IndexSchema) Compare(obj Schema) (int, *Error) {
	c2, ok := obj.(*IndexSchema)
	if !ok {
		err := Error(fmt.Sprint("change needs a IndexSchema instance", c2))
		return +999, &err
	}
	c.other = c2
	var err *Error
	if len(c.get("table_name")) == 0 || len(c.get("index_name")) == 0 {
		e := Error(fmt.Sprintf("--Comparing (table_name and/or index_name is empty): %v\n--           %v",
			c.getRow(), c.other.getRow()))
		err = &e
	}

	val := misc.CompareStrings(c.get("compare_name"), c.other.get("compare_name"))
	return val, err
}

// Add prints SQL to add the index
func (c *IndexSchema) Add() []Stringer {
	var strs []Stringer

	schema := c.other.dbSchema
	if schema == "*" {
		schema = c.get("schema_name")
	}

	// Assertion
	if c.get("index_def") == "null" || len(c.get("index_def")) == 0 {
		strs = append(strs, Notice(fmt.Sprintf("-- Add Unexpected situation in index.go: there is no index_def for %s.%s %s", schema, c.get("table_name"), c.get("index_name"))))
		return strs
	}

	// If we are comparing two different schemas against each other, we need to do some
	// modification of the first index_def, so we create the index in the dbSchema we're writing to.
	indexDef := c.get("index_def")
	if c.dbSchema != c.other.dbSchema {
		indexDef = strings.Replace(
			indexDef,
			fmt.Sprintf(" %s.%s ", c.get("schema_name"), c.get("table_name")),
			fmt.Sprintf(" %s.%s ", c.other.dbSchema, c.get("table_name")),
			-1)
	}

	strs = append(strs, Line(fmt.Sprintf("%v;", indexDef)))

	if c.get("constraint_def") != "null" {
		// Create the constraint using the index we just created
		if c.get("pk") == "true" {
			// Add primary key using the index
			strs = append(strs, Line(fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s PRIMARY KEY USING INDEX %s; -- (1)", schema, c.get("table_name"), c.get("index_name"), c.get("index_name"))))
		} else if c.get("uq") == "true" {
			// Add unique constraint using the index
			strs = append(strs, Line(fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s UNIQUE USING INDEX %s; -- (2)", schema, c.get("table_name"), c.get("index_name"), c.get("index_name"))))
		}
	}
	return strs
}

// Drop prints SQL to drop the index
func (c *IndexSchema) Drop() []Stringer {
	var strs []Stringer
	if c.get("constraint_def") != "null" {
		strs = append(strs, Notice("-- Warning, this may drop foreign keys pointing at this column.  Make sure you re-run the FOREIGN_KEY diff after running this SQL."),
			Line(fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s CASCADE; -- %s", c.get("schema_name"), c.get("table_name"), c.get("index_name"), c.get("constraint_def"))))
	}
	strs = append(strs, Line(fmt.Sprintf("DROP INDEX %s.%s;", c.get("schema_name"), c.get("index_name"))))
	return strs
}

// Change handles the case where the table and column match, but the details do not
func (c *IndexSchema) Change() []Stringer {
	var strs []Stringer

	// Table and constraint name matches... We need to make sure the details match

	// NOTE that there should always be an index_def for both c and c.other (but we're checking below anyway)
	if len(c.get("index_def")) == 0 {
		strs = append(strs, Notice(fmt.Sprintf("-- Change: Unexpected situation in index.go: index_def is empty for 1: %v  2:%v", c.getRow(), c.other.getRow())))
		return strs
	}
	if len(c.other.get("index_def")) == 0 {
		strs = append(strs, Notice(fmt.Sprintf("-- Change: Unexpected situation in index.go: index_def is empty for 2: %v 1: %v", c.other.getRow(), c.getRow())))
		return strs
	}

	if c.get("constraint_def") != c.other.get("constraint_def") {
		// c1.constraint and c.other.constraint are just different
		strs = append(strs,
			Notice(fmt.Sprintf("-- CHANGE: Different defs on %s:", c.get("table_name"))),
			Notice(fmt.Sprintf("--    %s", c.get("constraint_def"))),
			Notice(fmt.Sprintf("--    %s", c.other.get("constraint_def"))),
		)

		if c.get("constraint_def") == "null" {
			// c1.constraint does not exist, c.other.constraint does, so
			// Drop constraint
			strs = append(strs, Line(fmt.Sprintf("DROP INDEX %s; -- %s", c.other.get("index_name"), c.other.get("index_def"))))
		} else if c.other.get("constraint_def") == "null" {
			// c1.constraint exists, c.other.constraint does not, so
			// Add constraint
			if c.get("index_def") == c.other.get("index_def") {
				// Indexes match, so
				// Add constraint using the index
				if c.get("pk") == "true" {
					// Add primary key using the index
					strs = append(strs, Line(fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY USING INDEX %s; -- (3)", c.get("table_name"), c.get("index_name"), c.get("index_name"))))
				} else if c.get("uq") == "true" {
					// Add unique constraint using the index
					strs = append(strs, Line(fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE USING INDEX %s; -- (4)", c.get("table_name"), c.get("index_name"), c.get("index_name"))))
				} else {

				}
			} else {
				// Drop the c.other index, create a copy of the c1 index
				strs = append(strs, Line(fmt.Sprintf("DROP INDEX %s; -- %s", c.other.get("index_name"), c.other.get("index_def"))))
			}
			// WIP
			//strs = append(strs, Line(fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s;\n", c.get("table_name"), c.get("index_name"), c.get("constraint_def"))))

		} else if c.get("index_def") != c.other.get("index_def") {
			// The constraints match
		}

		return strs
	}

	// At this point, we know that the constraint_def matches.  Compare the index_def

	indexDef1 := c.get("index_def")
	indexDef2 := c.other.get("index_def")

	// If we are comparing two different schemas against each other, we need to do
	// some modification of the first index_def, so it looks more like the second
	if c.dbSchema != c.other.dbSchema {
		indexDef1 = strings.Replace(
			indexDef1,
			fmt.Sprintf(" %s.%s ", c.get("schema_name"), c.get("table_name")),
			fmt.Sprintf(" %s.%s ", c.other.get("schema_name"), c.other.get("table_name")),
			-1,
		)
	}

	if indexDef1 != indexDef2 {
		// Notice that, if we are here, then the two constraint_defs match (both may be empty)
		// The indexes do not match, but the constraints do
		if !strings.HasPrefix(c.get("index_def"), c.other.get("index_def")) &&
			!strings.HasPrefix(c.other.get("index_def"), c.get("index_def")) {
			strs = append(strs,
				Notice("--"),
				Notice("--CHANGE: index defs are different for identical constraint defs:"),
				Notice(fmt.Sprintf("--    %s", c.get("index_def"))),
				Notice(fmt.Sprintf("--    %s", c.other.get("index_def"))),
			)

			// Drop the index (and maybe the constraint) so we can recreate the index
			c.Drop()

			// Recreate the index (and a constraint if specified)
			c.Add()
		}
	}
	return strs
}
