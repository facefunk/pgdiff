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
)

const (
	AllSchemaType               = "ALL"
	SchemataSchemaType          = "SCHEMA"
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

var schemaTypes = []string{
	AllSchemaType,
	SchemataSchemaType,
	RoleSchemaType,
	SequenceSchemaType,
	TableSchemaType,
	ColumnSchemaType,
	TableColumnSchemaType,
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

var SchemaTypes = strings.Join(schemaTypes, ", ")

// AllSchemaTypes is all the schema types necessary to generate full output. This is actually all the schema types minus
// TableColumnSchemaType which is a more restrictive output of ColumnSchemaType.
var AllSchemaTypes = []string{
	SchemataSchemaType,
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

type (
	// Schema is a database definition (table, column, constraint, index, role, etc) that can be added, dropped, or
	// changed to match another database.
	Schema interface {
		Compare(otherSchema Schema) (int, *Error)
		Add() []Stringer
		Drop() []Stringer
		Change() []Stringer
		NextRow() bool
	}

	// SchemaFactory instantiates each type of Schema based on a data source.
	SchemaFactory interface {
		Schemata() (*SchemataSchema, error)
		Role() (*RoleSchema, error)
		Sequence() (*SequenceSchema, error)
		Table() (*TableSchema, error)
		Column() (*ColumnSchema, error)
		TableColumn() (*ColumnSchema, error)
		Index() (*IndexSchema, error)
		View() (*ViewSchema, error)
		MatView() (*MatViewSchema, error)
		ForeignKey() (*ForeignKeySchema, error)
		Function() (*FunctionSchema, error)
		Trigger() (*TriggerSchema, error)
		Owner() (*OwnerSchema, error)
		GrantRelationship() (*GrantRelationshipSchema, error)
		GrantAttribute() (*GrantAttributeSchema, error)
		Identify(num int) *Notice
	}

	// Stringer simply returns a string. Allows a slice of Stringer to be returned from a function, which can then be
	// type-switched to filter the output.
	Stringer interface {
		String() string
	}

	Line   string
	Notice string
	Error  string
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

// SchemaByType returns a Schema from factory by schemaType.
func SchemaByType(factory SchemaFactory, schemaType string) (Schema, error) {
	switch schemaType {
	case SchemataSchemaType:
		return factory.Schemata()
	case RoleSchemaType:
		return factory.Role()
	case SequenceSchemaType:
		return factory.Sequence()
	case TableSchemaType:
		return factory.Table()
	case ColumnSchemaType:
		return factory.Column()
	case TableColumnSchemaType:
		return factory.TableColumn()
	case IndexSchemaType:
		return factory.Index()
	case ViewSchemaType:
		return factory.View()
	case MatViewSchemaType:
		return factory.MatView()
	case ForeignKeySchemaType:
		return factory.ForeignKey()
	case FunctionSchemaType:
		return factory.Function()
	case TriggerSchemaType:
		return factory.Trigger()
	case OwnerSchemaType:
		return factory.Owner()
	case GrantRelationshipSchemaType:
		return factory.GrantRelationship()
	case GrantAttributeSchemaType:
		return factory.GrantAttribute()
	}
	err := Error(fmt.Sprintf("unsupported schema type: %s", schemaType))
	return nil, &err
}

// CompareByFactories runs a single comparison of schemaType between sources represented by fac1 and fac2.
func CompareByFactories(fac1 SchemaFactory, fac2 SchemaFactory, schemaType string) []Stringer {
	var strs []Stringer
	schema1, err := SchemaByType(fac1, schemaType)
	if err != nil {
		strs = append(strs, Error(err.Error()))
	}
	schema2, err := SchemaByType(fac2, schemaType)
	if err != nil {
		strs = append(strs, Error(err.Error()))
	}
	diff := Diff(schema1, schema2)
	strs = append(strs, diff...)
	return strs
}

// CompareByFactoriesAndArgs is the main command-line compare function. It runs one comparison between sources
// represented by fac1 and fac2 for each schema type listed in args.
func CompareByFactoriesAndArgs(fac1 SchemaFactory, fac2 SchemaFactory, args []string) []Stringer {
	schemaType := strings.ToUpper(strings.Join(args, " "))
	strs := []Stringer{
		Notice("-- schemaType: " + schemaType),
		fac1.Identify(1),
		fac2.Identify(2),
		Notice("-- Run the following SQL against db2:"),
	}
	for _, arg := range args {
		if arg == AllSchemaType {
			for _, st := range AllSchemaTypes {
				strs = append(strs, CompareByFactories(fac1, fac2, st)...)
			}
			continue
		}
		strs = append(strs, CompareByFactories(fac1, fac2, arg)...)
	}
	return strs
}

// Diff is a generic diff function that compares tables, columns, indexes, roles, grants, etc.
// Different behaviors are specified by Schema implementations.
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
