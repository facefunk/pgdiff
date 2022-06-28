//
// Copyright (c) 2016 Jon Carlson.  All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//

package pgdiff

import (
	"database/sql"
	"fmt"
	"sort"

	"github.com/joncrlsn/misc"
	"github.com/joncrlsn/pgutil"
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

// CompareMatViews outputs SQL to make the matviews match between DBs
func CompareMatViews(conn1 *sql.DB, conn2 *sql.DB, dbInfo1 *pgutil.DbInfo, dbInfo2 *pgutil.DbInfo) []Stringer {
	sql := `
	WITH matviews as ( SELECT schemaname || '.' || matviewname AS matviewname,
	definition
	FROM pg_catalog.pg_matviews 
	WHERE schemaname NOT LIKE 'pg_%' 
	)
	SELECT
	matviewname,
	definition,
	COALESCE(string_agg(indexdef, ';' || E'\n\n') || ';', '')  as indexdef
	FROM matviews
	LEFT JOIN  pg_catalog.pg_indexes on matviewname = schemaname || '.' || tablename
	group by matviewname, definition
	ORDER BY
	matviewname;
	`

	rowChan1, _ := pgutil.QueryStrings(conn1, sql)
	rowChan2, _ := pgutil.QueryStrings(conn2, sql)

	rows1 := make(MatViewRows, 0)
	for row := range rowChan1 {
		rows1 = append(rows1, row)
	}
	sort.Sort(rows1)

	rows2 := make(MatViewRows, 0)
	for row := range rowChan2 {
		rows2 = append(rows2, row)
	}
	sort.Sort(rows2)

	// We have to explicitly type this as Schema here
	var schema1 Schema = &MatViewSchema{rows: rows1, rowNum: -1}
	var schema2 Schema = &MatViewSchema{rows: rows2, rowNum: -1}

	// Compare the matviews
	return doDiff(schema1, schema2)
}
