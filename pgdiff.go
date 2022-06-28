//
// Copyright (c) 2017 Jon Carlson.  All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.
//

package pgdiff

import (
	"log"

	_ "github.com/lib/pq"
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

/*
 * This is a generic diff function that compares tables, columns, indexes, roles, grants, etc.
 * Different behaviors are specified the Schema implementations
 */
func doDiff(db1 Schema, db2 Schema) []Stringer {
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

func check(msg string, err error) {
	if err != nil {
		log.Fatal("Error "+msg, err)
	}
}
