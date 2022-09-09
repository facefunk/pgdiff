//
// Copyright (c) 2017 Jon Carlson.
// Copyright (c) 2022 Facefunk.
// All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.
//

package db

import (
	"bytes"
	"database/sql"
	"fmt"
	"sort"
	"text/template"

	"github.com/facefunk/pgdiff"
	"github.com/joncrlsn/pgutil"
	_ "github.com/lib/pq"
)

type SchemaFactory struct {
	conn   *sql.DB
	dbInfo *pgutil.DbInfo
}

func NewSchemaFactory(conn *sql.DB, dbInfo *pgutil.DbInfo) pgdiff.SchemaFactory {
	return &SchemaFactory{conn, dbInfo}
}

// columnSchema returns a Schema that outputs SQL to make the columns match between two databases or schemas
func columnSchema(conn *sql.DB, dbInfo *pgutil.DbInfo, tpl *template.Template) (*pgdiff.ColumnSchema, error) {
	buf := new(bytes.Buffer)
	err := tpl.Execute(buf, dbInfo)
	if err != nil {
		return nil, err
	}

	rowChan, _ := pgutil.QueryStrings(conn, buf.String())

	rows := make(pgdiff.ColumnRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewColumnSchema(rows, dbInfo.DbSchema), nil
}

// Column returns a ColumnSchema that outputs SQL to make the columns match between two databases or
// schemas
func (f *SchemaFactory) Column() (*pgdiff.ColumnSchema, error) {
	return columnSchema(f.conn, f.dbInfo, columnSqlTemplate)
}

// TableColumn returns a ColumnSchema that outputs SQL to make the tables columns (without views columns)
// match between two databases or schemas
func (f *SchemaFactory) TableColumn() (*pgdiff.ColumnSchema, error) {
	return columnSchema(f.conn, f.dbInfo, tableColumnSqlTemplate)
}

// ForeignKey returns a ForeignKeySchema that compares the foreign keys in the two databases.
func (f *SchemaFactory) ForeignKey() (*pgdiff.ForeignKeySchema, error) {
	buf := new(bytes.Buffer)
	err := foreignKeySqlTemplate.Execute(buf, f.dbInfo)
	if err != nil {
		return nil, err
	}

	rowChan, _ := pgutil.QueryStrings(f.conn, buf.String())

	rows := make(pgdiff.ForeignKeyRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewForeignKeySchema(rows, f.dbInfo.DbSchema), nil
}

// Function returns a FunctionSchema that outputs SQL to make the functions match between DBs
func (f *SchemaFactory) Function() (*pgdiff.FunctionSchema, error) {
	buf := new(bytes.Buffer)
	err := functionSqlTemplate.Execute(buf, f.dbInfo)
	if err != nil {
		return nil, err
	}

	rowChan, _ := pgutil.QueryStrings(f.conn, buf.String())

	rows := make(pgdiff.FunctionRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewFunctionSchema(rows, f.dbInfo.DbSchema), nil
}

// GrantAttribute returns a GrantAttributeSchema that outputs SQL to make the granted permissions match
// between DBs or schemas
func (f *SchemaFactory) GrantAttribute() (*pgdiff.GrantAttributeSchema, error) {
	buf := new(bytes.Buffer)
	err := grantAttributeSqlTemplate.Execute(buf, f.dbInfo)
	if err != nil {
		return nil, err
	}

	rowChan, _ := pgutil.QueryStrings(f.conn, buf.String())

	rows := make(pgdiff.GrantAttributeRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewGrantAttributeSchema(rows, f.dbInfo.DbSchema), nil
}

// GrantRelationship returns a GrantRelationshipSchema that outputs SQL to make the granted permissions
// match between DBs or schemas
func (f *SchemaFactory) GrantRelationship() (*pgdiff.GrantRelationshipSchema, error) {
	buf := new(bytes.Buffer)
	err := grantRelationshipSqlTemplate.Execute(buf, f.dbInfo)
	if err != nil {
		return nil, err
	}

	rowChan, _ := pgutil.QueryStrings(f.conn, buf.String())

	rows := make(pgdiff.GrantRelationshipRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewGrantRelationshipSchema(rows, f.dbInfo.DbSchema), nil
}

// Index returns an IndexSchema that outputs Sql to make the indexes match between to DBs or schemas
func (f *SchemaFactory) Index() (*pgdiff.IndexSchema, error) {
	buf := new(bytes.Buffer)
	err := indexSqlTemplate.Execute(buf, f.dbInfo)
	if err != nil {
		return nil, err
	}

	rowChan, _ := pgutil.QueryStrings(f.conn, buf.String())

	rows := make(pgdiff.IndexRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewIndexSchema(rows, f.dbInfo.DbSchema), nil
}

// MatView returns a MatViewSchema that outputs SQL to make the matviews match between DBs
func (f *SchemaFactory) MatView() (*pgdiff.MatViewSchema, error) {

	rowChan, _ := pgutil.QueryStrings(f.conn, matViewSql)

	rows := make(pgdiff.MatViewRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewMatViewSchema(rows), nil
}

// Owner returns an OwnerSchema that compares the ownership of tables, sequences, and views between two
// databases or schemas
func (f *SchemaFactory) Owner() (*pgdiff.OwnerSchema, error) {
	buf := new(bytes.Buffer)
	err := ownerSqlTemplate.Execute(buf, f.dbInfo)
	if err != nil {
		return nil, err
	}

	rowChan, _ := pgutil.QueryStrings(f.conn, buf.String())

	rows := make(pgdiff.OwnerRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewOwnerSchema(rows), nil
}

// Role returns a RoleSchema that compares the roles between two databases or schemas.
func (f *SchemaFactory) Role() (*pgdiff.RoleSchema, error) {

	rowChan, _ := pgutil.QueryStrings(f.conn, roleSql)

	rows := make(pgdiff.RoleRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewRoleSchema(rows), nil
}

// Schemata returns a SchemataSchema that outputs SQL to make the dbSchema names match between DBs
func (f *SchemaFactory) Schemata() (*pgdiff.SchemataSchema, error) {
	rowChan, _ := pgutil.QueryStrings(f.conn, schemataSql)

	rows := make(pgdiff.SchemataRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewSchemataSchema(rows), nil
}

// Sequence returns a SequenceSchema that outputs SQL to make the sequences match between DBs or schemas
func (f *SchemaFactory) Sequence() (*pgdiff.SequenceSchema, error) {
	buf := new(bytes.Buffer)
	err := sequenceSqlTemplate.Execute(buf, f.dbInfo)
	if err != nil {
		return nil, err
	}

	rowChan, _ := pgutil.QueryStrings(f.conn, buf.String())

	rows := make(pgdiff.SequenceRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewSequenceSchema(rows, f.dbInfo.DbSchema), nil
}

// Table returns a TableSchema that outputs SQL to make the table names match between DBs
func (f *SchemaFactory) Table() (*pgdiff.TableSchema, error) {
	buf := new(bytes.Buffer)
	err := tableSqlTemplate.Execute(buf, f.dbInfo)
	if err != nil {
		return nil, err
	}

	rowChan, _ := pgutil.QueryStrings(f.conn, buf.String())

	rows := make(pgdiff.TableRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewTableSchema(rows, f.dbInfo.DbSchema), nil
}

// Trigger returns a TriggerSchema that outputs SQL to make the triggers match between DBs
func (f *SchemaFactory) Trigger() (*pgdiff.TriggerSchema, error) {
	buf := new(bytes.Buffer)
	err := triggerSqlTemplate.Execute(buf, f.dbInfo)
	if err != nil {
		return nil, err
	}

	rowChan, _ := pgutil.QueryStrings(f.conn, buf.String())

	rows := make(pgdiff.TriggerRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewTriggerSchema(rows, f.dbInfo.DbSchema), nil
}

// View returns a ViewSchema that outputs SQL to make the views match between DBs
func (f *SchemaFactory) View() (*pgdiff.ViewSchema, error) {
	rowChan, _ := pgutil.QueryStrings(f.conn, viewSql)

	rows := make(pgdiff.ViewRows, 0)
	for row := range rowChan {
		rows = append(rows, row)
	}
	sort.Sort(rows)

	return pgdiff.NewViewSchema(rows), nil
}

func (f *SchemaFactory) Identify(num int) *pgdiff.Notice {
	return pgdiff.NewNotice(fmt.Sprintf("-- db%d: %v", num, *f.dbInfo))
}
