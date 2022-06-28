//
// Copyright (c) 2017 Jon Carlson.  All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//

package pgdiff

import (
	"bytes"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/joncrlsn/misc"
	"github.com/joncrlsn/pgutil"
)

var (
	columnSqlTemplate = initColumnSqlTemplate()
)

// Initializes the Sql template
func initColumnSqlTemplate() *template.Template {
	query := `
SELECT table_schema
    ,  {{if eq $.DbSchema "*" }}table_schema || '.' || {{end}}table_name || '.' ||lpad(cast (ordinal_position as varchar), 5, '0')|| column_name AS compare_name
	, table_name
    , column_name
    , data_type
    , is_nullable
    , column_default
    , character_maximum_length
    , is_identity
    , identity_generation
    , substring(udt_name from 2) AS array_type
FROM information_schema.columns
WHERE is_updatable = 'YES'
{{if eq $.DbSchema "*" }}
AND table_schema NOT LIKE 'pg_%' 
AND table_schema <> 'information_schema' 
{{else}}
AND table_schema = '{{$.DbSchema}}'
{{end}}
ORDER BY compare_name ASC;
`
	t := template.New("ColumnSqlTmpl")
	template.Must(t.Parse(query))
	return t
}

var (
	tableColumnSqlTemplate = initTableColumnSqlTemplate()
)

// Initializes the Sql template
func initTableColumnSqlTemplate() *template.Template {
	query := `
SELECT a.table_schema
    , {{if eq $.DbSchema "*" }}a.table_schema || '.' || {{end}}a.table_name || '.' || column_name  AS compare_name
	, a.table_name
    , column_name
    , data_type
    , is_nullable
    , column_default
    , character_maximum_length
FROM information_schema.columns a
INNER JOIN information_schema.tables b
    ON a.table_schema = b.table_schema AND
       a.table_name = b.table_name AND
       b.table_type = 'BASE TABLE'
WHERE is_updatable = 'YES'
{{if eq $.DbSchema "*" }}
AND a.table_schema NOT LIKE 'pg_%' 
AND a.table_schema <> 'information_schema' 
{{else}}
AND a.table_schema = '{{$.DbSchema}}'
{{end}}
ORDER BY compare_name ASC;
`
	t := template.New("ColumnSqlTmpl")
	template.Must(t.Parse(query))
	return t
}

// ==================================
// Column Rows definition
// ==================================

// ColumnRows is a sortable slice of string maps
type ColumnRows []map[string]string

func (slice ColumnRows) Len() int {
	return len(slice)
}

func (slice ColumnRows) Less(i, j int) bool {
	return slice[i]["compare_name"] < slice[j]["compare_name"]
}

func (slice ColumnRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// ==================================
// ColumnSchema definition
// (implements Schema -- defined in pgdiff.go)
// ==================================

// ColumnSchema holds a slice of rows from one of the databases as well as
// a reference to the current row of data we're viewing.
type ColumnSchema struct {
	rows     ColumnRows
	rowNum   int
	done     bool
	dbSchema string
	other    *ColumnSchema
}

// get returns the value from the current row for the given key
func (c *ColumnSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *ColumnSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *ColumnSchema) Compare(obj Schema) (int, *Error) {
	var err *Error
	c2, ok := obj.(*ColumnSchema)
	if !ok {
		e := Error(fmt.Sprint("Error!!!, Compare needs a ColumnSchema instance", c2))
		err = &e
	}
	c.other = c2
	val := misc.CompareStrings(c.get("compare_name"), c.other.get("compare_name"))
	return val, err
}

// Add prints SQL to add the column
func (c *ColumnSchema) Add() []Stringer {
	var strs []Stringer
	schema := c.other.dbSchema
	if schema == "*" {
		schema = c.get("table_schema")
	}

	// Knowing the version of db2 would eliminate the need for this warning
	if c.get("is_identity") == "YES" {
		strs = append(strs, Notice("-- WARNING: identity columns are not supported in PostgreSQL versions < 10."),
			Notice("-- Attempting to create identity columns in earlier versions will probably result in errors."))
	}

	var alter string
	if c.get("data_type") == "character varying" {
		maxLength, valid := getMaxLength(c.get("character_maximum_length"))
		if !valid {
			alter = fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s character varying", schema, c.get("table_name"), c.get("column_name"))
		} else {
			alter = fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s character varying(%s)", schema, c.get("table_name"), c.get("column_name"), maxLength)
		}
	} else {
		dataType := c.get("data_type")
		//if c.get("data_type") == "ARRAY" {
		//strs = append(strs, Notice(fmt.Sprintln("-- Note that adding of array data types are not yet generated properly.")))
		//}
		if dataType == "ARRAY" {
			dataType = c.get("array_type") + "[]"
		}
		//alter = mt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s %s", dbSchema, c.get("table_name"), c.get("column_name"), c.get("data_type"))
		alter = fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s %s", schema, c.get("table_name"), c.get("column_name"), dataType)
	}

	if c.get("is_nullable") == "NO" {
		alter += " NOT NULL"
	}
	if c.get("column_default") != "null" {
		alter += fmt.Sprintf(" DEFAULT %s", c.get("column_default"))
	}
	// NOTE: there are more identity column sequence options according to the PostgreSQL
	// CREATE TABLE docs, but these do not appear to be available as of version 10.1
	if c.get("is_identity") == "YES" {
		alter += fmt.Sprintf(" GENERATED %s AS IDENTITY", c.get("identity_generation"))
	}
	strs = append(strs, Line(alter+";"))
	return strs
}

// Drop prints SQL to drop the column
func (c *ColumnSchema) Drop() []Stringer {
	// if dropping column
	return []Stringer{Line(fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN IF EXISTS %s;", c.get("table_schema"), c.get("table_name"), c.get("column_name")))}
}

// Change handles the case where the table and column match, but the details do not
func (c *ColumnSchema) Change() []Stringer {
	var strs []Stringer
	// Adjust data type for array columns
	dataType1 := c.get("data_type")
	if dataType1 == "ARRAY" {
		dataType1 = c.get("array_type") + "[]"
	}
	dataType2 := c.other.get("data_type")
	if dataType2 == "ARRAY" {
		dataType2 = c.other.get("array_type") + "[]"
	}

	// Detect column type change (mostly varchar length, or number size increase)
	// (integer to/from bigint is OK)
	if dataType1 == dataType2 {
		if dataType1 == "character varying" {
			max1, max1Valid := getMaxLength(c.get("character_maximum_length"))
			max2, max2Valid := getMaxLength(c.other.get("character_maximum_length"))
			if !max1Valid && !max2Valid {
				// Leave them alone, they both have undefined max lengths
			} else if (max1Valid || !max2Valid) && (max1 != c.other.get("character_maximum_length")) {
				//if !max1Valid {
				//    strs = append(strs, Line(fmt.Sprintln("-- WARNING: varchar column has no maximum length.  Setting to 1024, which may result in data loss.")))
				//}
				max1Int, err1 := strconv.Atoi(max1)
				check("converting string to int", err1)
				max2Int, err2 := strconv.Atoi(max2)
				check("converting string to int", err2)
				if max1Int < max2Int {
					strs = append(strs, Notice("-- WARNING: The next statement will shorten a character varying column, which may result in data loss."))
				}
				strs = append(strs, Notice(fmt.Sprintf("-- max1Valid: %v  max2Valid: %v", max1Valid, max2Valid)),
					Line(fmt.Sprintf("ALTER TABLE %s.%s ALTER COLUMN %s TYPE character varying(%s);", c.other.get("table_schema"), c.get("table_name"), c.get("column_name"), max1)))
			}
		}
	}

	// Code and test a column change from integer to bigint
	if dataType1 != dataType2 {
		strs = append(strs, Notice(fmt.Sprintf("-- WARNING: This type change may not work well: (%s to %s).", dataType2, dataType1)))
		if strings.HasPrefix(dataType1, "character") {
			max1, max1Valid := getMaxLength(c.get("character_maximum_length"))
			if !max1Valid {
				strs = append(strs, Notice("-- WARNING: varchar column has no maximum length.  Setting to 1024"))
			}
			strs = append(strs, Line(fmt.Sprintf("ALTER TABLE %s.%s ALTER COLUMN %s TYPE %s(%s);", c.other.get("table_schema"), c.get("table_name"), c.get("column_name"), dataType1, max1)))
		} else {
			strs = append(strs, Line(fmt.Sprintf("ALTER TABLE %s.%s ALTER COLUMN %s TYPE %s;", c.other.get("table_schema"), c.get("table_name"), c.get("column_name"), dataType1)))
		}
	}

	// Detect column default change (or added, dropped)
	if c.get("column_default") == "null" {
		if c.other.get("column_default") != "null" {
			strs = append(strs, Line(fmt.Sprintf("ALTER TABLE %s.%s ALTER COLUMN %s DROP DEFAULT;", c.other.get("table_schema"), c.get("table_name"), c.get("column_name"))))
		}
	} else if c.get("column_default") != c.other.get("column_default") {
		strs = append(strs, Line(fmt.Sprintf("ALTER TABLE %s.%s ALTER COLUMN %s SET DEFAULT %s;", c.other.get("table_schema"), c.get("table_name"), c.get("column_name"), c.get("column_default"))))
	}

	// Detect identity column change
	// Save result to variable instead of printing because order for adding/removing
	// is_nullable affects identity columns
	var identitySql string
	if c.get("is_identity") != c.other.get("is_identity") {
		// Knowing the version of db2 would eliminate the need for this warning
		strs = append(strs, Notice("-- WARNING: identity columns are not supported in PostgreSQL versions < 10."),
			Notice("-- Attempting to create identity columns in earlier versions will probably result in errors."))
		if c.get("is_identity") == "YES" {
			identitySql = fmt.Sprintf("ALTER TABLE \"%s\".\"%s\" ALTER COLUMN \"%s\" ADD GENERATED %s AS IDENTITY;", c.other.get("table_schema"), c.get("table_name"), c.get("column_name"), c.get("identity_generation"))
		} else {
			identitySql = fmt.Sprintf("ALTER TABLE \"%s\".\"%s\" ALTER COLUMN \"%s\" DROP IDENTITY;", c.other.get("table_schema"), c.get("table_name"), c.get("column_name"))
		}
	}

	// Detect not-null and nullable change
	if c.get("is_nullable") != c.other.get("is_nullable") {
		if c.get("is_nullable") == "YES" {
			if identitySql != "" {
				strs = append(strs, Line(identitySql))
			}
			strs = append(strs, Line(fmt.Sprintf("ALTER TABLE %s.%s ALTER COLUMN %s DROP NOT NULL;", c.other.get("table_schema"), c.get("table_name"), c.get("column_name"))))
		} else {
			strs = append(strs, Line(fmt.Sprintf("ALTER TABLE %s.%s ALTER COLUMN %s SET NOT NULL;", c.other.get("table_schema"), c.get("table_name"), c.get("column_name"))))
			if identitySql != "" {
				strs = append(strs, Line(identitySql))
			}
		}
	} else {
		if identitySql != "" {
			strs = append(strs, Line(identitySql))
		}
	}
	return strs
}

// ==================================
// Standalone Functions
// ==================================

// compare outputs SQL to make the columns match between two databases or schemas
func compare(conn1 *sql.DB, conn2 *sql.DB, dbInfo1 *pgutil.DbInfo, dbInfo2 *pgutil.DbInfo, tpl *template.Template) []Stringer {
	var errs []Stringer
	buf1 := new(bytes.Buffer)
	err := tpl.Execute(buf1, dbInfo1)
	if err != nil {
		errs = append(errs, Error(err.Error()))
	}
	buf2 := new(bytes.Buffer)
	err = tpl.Execute(buf2, dbInfo2)
	if err != nil {
		errs = append(errs, Error(err.Error()))
	}
	if len(errs) > 0 {
		return errs
	}

	rowChan1, _ := pgutil.QueryStrings(conn1, buf1.String())
	rowChan2, _ := pgutil.QueryStrings(conn2, buf2.String())

	//rows1 := make([]map[string]string, 500)
	rows1 := make(ColumnRows, 0)
	for row := range rowChan1 {
		rows1 = append(rows1, row)
	}
	sort.Sort(rows1)

	//rows2 := make([]map[string]string, 500)
	rows2 := make(ColumnRows, 0)
	for row := range rowChan2 {
		rows2 = append(rows2, row)
	}
	sort.Sort(&rows2)

	// We have to explicitly type this as Schema here for some unknown reason
	var schema1 Schema = &ColumnSchema{rows: rows1, rowNum: -1, dbSchema: dbInfo1.DbSchema}
	var schema2 Schema = &ColumnSchema{rows: rows2, rowNum: -1, dbSchema: dbInfo2.DbSchema}

	// Compare the columns
	return doDiff(schema1, schema2)
}

// CompareColumns outputs SQL to make the columns match between two databases or schemas
func CompareColumns(conn1 *sql.DB, conn2 *sql.DB, dbInfo1 *pgutil.DbInfo, dbInfo2 *pgutil.DbInfo) []Stringer {

	return compare(conn1, conn2, dbInfo1, dbInfo2, columnSqlTemplate)

}

// CompareTableColumns outputs SQL to make the tables columns (without views columns) match between two databases or schemas
func CompareTableColumns(conn1 *sql.DB, conn2 *sql.DB, dbInfo1 *pgutil.DbInfo, dbInfo2 *pgutil.DbInfo) []Stringer {

	return compare(conn1, conn2, dbInfo1, dbInfo2, tableColumnSqlTemplate)

}

// getMaxLength returns the maximum length and whether or not it is valid
func getMaxLength(maxLength string) (string, bool) {

	if maxLength == "null" {
		// default to 1024
		return "1024", false
	}
	return maxLength, true
}
