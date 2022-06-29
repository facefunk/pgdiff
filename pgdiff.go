//
// Copyright (c) 2017 Jon Carlson.  All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.
//

package pgdiff

import (
	"database/sql"
	"fmt"

	"github.com/joncrlsn/pgutil"
	_ "github.com/lib/pq"
)

const (
	SchemaSchemaType            = "SCHEMA"
	RoleSchemaType              = "ROLE"
	SequenceSchemaType          = "SEQUENCE"
	TableSchemaType             = "TABLE"
	ColumnSchemaType            = "COLUMN"
	TableColumnSchemaType       = "TABLE_COLUMN"
	IndexSchemaType             = "INDEX"
	ViewSchemaType              = "VIEW"
	MatViewSchemaType           = "MATVIEW"
	ForeignKeySchemaType        = "FOREIGN_KEY"
	FunctionSchemaType          = "FUNCTION"
	TriggerSchemaType           = "TRIGGER"
	OwnerSchemaType             = "OWNER"
	GrantRelationshipSchemaType = "GRANT_RELATIONSHIP"
	GrantAttributeSchemaType    = "GRANT_ATTRIBUTE"
)

// Schema is a database definition (table, column, constraint, indes, role, etc) that can be
// added, dropped, or changed to match another database.
type (
	Schema interface {
		Compare(otherSchema Schema) (int, *Error)
		Add() []Stringer
		Drop() []Stringer
		Change() []Stringer
		NextRow() bool
	}

	SchemaFactory interface {
		Schema() Schema
	}

	Stringer interface {
		String() string
	}

	Line   string
	Notice string
	Error  string

	dBSourceSchemaFunc func(conn *sql.DB, dbInfo *pgutil.DbInfo) (Schema, error)
)

var (
	dDSourceSchemaFuncs = map[string]dBSourceSchemaFunc{
		SchemaSchemaType:            dBSourceSchemataSchema,
		RoleSchemaType:              dBSourceRoleSchema,
		SequenceSchemaType:          dBSourceSequenceSchema,
		TableSchemaType:             dBSourceTableSchema,
		ColumnSchemaType:            dBSourceColumnSchema,
		TableColumnSchemaType:       dBSourceTableColumnSchema,
		IndexSchemaType:             dBSourceIndexSchema,
		ViewSchemaType:              dBSourceViewSchema,
		MatViewSchemaType:           dBSourceMatViewSchema,
		ForeignKeySchemaType:        dBSourceForeignKeySchema,
		FunctionSchemaType:          dBSourceFunctionSchema,
		TriggerSchemaType:           dBSourceTriggerSchema,
		OwnerSchemaType:             dBSourceOwnerSchema,
		GrantRelationshipSchemaType: dBSourceGrantRelationshipSchema,
		GrantAttributeSchemaType:    dBSourceGrantAttributeSchema,
	}
	AllSchemaTypes = []string{
		SchemaSchemaType,
		RoleSchemaType,
		SequenceSchemaType,
		TableSchemaType,
		ColumnSchemaType,
		IndexSchemaType,
		ViewSchemaType,
		MatViewSchemaType,
		ForeignKeySchemaType,
		FunctionSchemaType,
		TriggerSchemaType,
		OwnerSchemaType,
		GrantRelationshipSchemaType,
		GrantAttributeSchemaType,
	}
)

func (s Line) String() string {
	return string(s)
}

func (s Notice) String() string {
	return string(s)
}

func (s Error) String() string {
	return string(s)
}

func (s Error) Error() string {
	return string(s)
}

func DBSourceSchema(connection *sql.DB, dbInfo *pgutil.DbInfo, schemaType string) (Schema, error) {
	fun, ok := dDSourceSchemaFuncs[schemaType]
	if !ok {
		e := Error(fmt.Sprintf("DB Source does not support schema type: %s", schemaType))
		return nil, &e
	}
	return fun(connection, dbInfo)
}

func DBSourceCompare(conn1 *sql.DB, conn2 *sql.DB, dbInfo1 *pgutil.DbInfo, dbInfo2 *pgutil.DbInfo, schemaType string) []Stringer {
	schema1, err := DBSourceSchema(conn1, dbInfo1, schemaType)
	if err != nil {
		return []Stringer{Error(err.Error())}
	}
	schema2, err := DBSourceSchema(conn2, dbInfo2, schemaType)
	if err != nil {
		return []Stringer{Error(err.Error())}
	}
	return Diff(schema1, schema2)
}

// Diff is a generic diff function that compares tables, columns, indexes, roles, grants, etc.
// Different behaviors are specified by the Schema implementations
func Diff(db1 Schema, db2 Schema) []Stringer {
	var strs []Stringer
	var s []Stringer
	more1 := db1.NextRow()
	more2 := db2.NextRow()
	for more1 || more2 {
		compareVal, err := db1.Compare(db2)
		if err != nil {
			strs = append(strs, err)
		}
		if compareVal == 0 {
			// table and column match, look for non-identifying changes
			s = db1.Change()
			more1 = db1.NextRow()
			more2 = db2.NextRow()
		} else if compareVal < 0 {
			// db2 is missing a value that db1 has
			if more1 {
				s = db1.Add()
				more1 = db1.NextRow()
			} else {
				// db1 is at the end
				s = db2.Drop()
				more2 = db2.NextRow()
			}
		} else if compareVal > 0 {
			// db2 has an extra column that we don't want
			if more2 {
				s = db2.Drop()
				more2 = db2.NextRow()
			} else {
				// db2 is at the end
				s = db1.Add()
				more1 = db1.NextRow()
			}
		}
		strs = append(strs, s...)
	}
	return strs
}
