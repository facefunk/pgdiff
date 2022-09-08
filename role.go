//
// Copyright (c) 2014 Jon Carlson.
// Copyright (c) 2022 Facefunk.
// All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.
//

package pgdiff

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/joncrlsn/misc"
)

var curlyBracketRegex = regexp.MustCompile("[{}]")

// RoleRows is a sortable slice of string maps
type RoleRows []map[string]string

func (slice RoleRows) Len() int {
	return len(slice)
}

func (slice RoleRows) Less(i, j int) bool {
	return slice[i]["rolname"] < slice[j]["rolname"]
}

func (slice RoleRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// ==================================
// RoleSchema definition
// (implements Schema -- defined in pgdiff.go)
// ==================================

// RoleSchema holds a slice of rows from one of the databases as well as
// a reference to the current row of data we're viewing.
type RoleSchema struct {
	rows   RoleRows
	rowNum int
	done   bool
	other  *RoleSchema
}

func NewRoleSchema(rows RoleRows) *RoleSchema {
	return &RoleSchema{rows: rows, rowNum: -1}
}

// get returns the value from the current row for the given key
func (c *RoleSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

// get returns the current row for the given key
func (c *RoleSchema) getRow() map[string]string {
	if c.rowNum >= len(c.rows) {
		return make(map[string]string)
	}
	return c.rows[c.rowNum]
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *RoleSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *RoleSchema) Compare(obj Schema) (int, *Error) {
	c2, ok := obj.(*RoleSchema)
	if !ok {
		err := Error(fmt.Sprint("change needs a RoleSchema instance", c2))
		return +999, &err
	}
	c.other = c2

	val := misc.CompareStrings(c.get("rolname"), c.other.get("rolname"))
	return val, nil
}

/*
CREATE ROLE name [ [ WITH ] option [ ... ] ]

where option can be:

      SUPERUSER | NOSUPERUSER
    | CREATEDB | NOCREATEDB
    | CREATEROLE | NOCREATEROLE
    | CREATEUSER | NOCREATEUSER
    | INHERIT | NOINHERIT
    | LOGIN | NOLOGIN
    | REPLICATION | NOREPLICATION
    | CONNECTION LIMIT connlimit
    | [ ENCRYPTED | UNENCRYPTED ] PASSWORD 'password'
    | VALID UNTIL 'timestamp'
    | IN ROLE role_name [, ...]
    | IN GROUP role_name [, ...]
    | ROLE role_name [, ...]
    | ADMIN role_name [, ...]
    | USER role_name [, ...]
    | SYSID uid
*/

// Add generates SQL to add the constraint/index
func (c RoleSchema) Add() []Stringer {
	var strs []Stringer

	// We don't care about efficiency here so we just concat strings
	options := " WITH PASSWORD 'changeme'"

	if c.get("rolcanlogin") == "true" {
		options += " LOGIN"
	} else {
		options += " NOLOGIN"
	}

	if c.get("rolsuper") == "true" {
		options += " SUPERUSER"
	}

	if c.get("rolcreatedb") == "true" {
		options += " CREATEDB"
	}

	if c.get("rolcreaterole") == "true" {
		options += " CREATEROLE"
	}

	if c.get("rolinherit") == "true" {
		options += " INHERIT"
	} else {
		options += " NOINHERIT"
	}

	if c.get("rolreplication") == "true" {
		options += " REPLICATION"
	} else {
		options += " NOREPLICATION"
	}

	if c.get("rolconnlimit") != "-1" && len(c.get("rolconnlimit")) > 0 {
		options += " CONNECTION LIMIT " + c.get("rolconnlimit")
	}
	if c.get("rolvaliduntil") != "null" {
		options += fmt.Sprintf(" VALID UNTIL '%s'", c.get("rolvaliduntil"))
	}

	strs = append(strs, Line(fmt.Sprintf("CREATE ROLE %s%s;", c.get("rolname"), options)))
	return strs
}

// Drop generates SQL to drop the role
func (c RoleSchema) Drop() []Stringer {
	return []Stringer{Line(fmt.Sprintf("DROP ROLE %s;", c.get("rolname")))}
}

// Change handles the case where the role name matches, but the details do not
func (c RoleSchema) Change() []Stringer {
	var strs []Stringer

	options := ""
	if c.get("rolsuper") != c.other.get("rolsuper") {
		if c.get("rolsuper") == "true" {
			options += " SUPERUSER"
		} else {
			options += " NOSUPERUSER"
		}
	}

	if c.get("rolcanlogin") != c.other.get("rolcanlogin") {
		if c.get("rolcanlogin") == "true" {
			options += " LOGIN"
		} else {
			options += " NOLOGIN"
		}
	}

	if c.get("rolcreatedb") != c.other.get("rolcreatedb") {
		if c.get("rolcreatedb") == "true" {
			options += " CREATEDB"
		} else {
			options += " NOCREATEDB"
		}
	}

	if c.get("rolcreaterole") != c.other.get("rolcreaterole") {
		if c.get("rolcreaterole") == "true" {
			options += " CREATEROLE"
		} else {
			options += " NOCREATEROLE"
		}
	}

	if c.get("rolcreateuser") != c.other.get("rolcreateuser") {
		if c.get("rolcreateuser") == "true" {
			options += " CREATEUSER"
		} else {
			options += " NOCREATEUSER"
		}
	}

	if c.get("rolinherit") != c.other.get("rolinherit") {
		if c.get("rolinherit") == "true" {
			options += " INHERIT"
		} else {
			options += " NOINHERIT"
		}
	}

	if c.get("rolreplication") != c.other.get("rolreplication") {
		if c.get("rolreplication") == "true" {
			options += " REPLICATION"
		} else {
			options += " NOREPLICATION"
		}
	}

	if c.get("rolconnlimit") != c.other.get("rolconnlimit") {
		if len(c.get("rolconnlimit")) > 0 {
			options += " CONNECTION LIMIT " + c.get("rolconnlimit")
		}
	}

	if c.get("rolvaliduntil") != c.other.get("rolvaliduntil") {
		if c.get("rolvaliduntil") != "null" {
			options += fmt.Sprintf(" VALID UNTIL '%s'", c.get("rolvaliduntil"))
		}
	}

	// Only alter if we have changes
	if len(options) > 0 {
		strs = append(strs, Line(fmt.Sprintf("ALTER ROLE %s%s;", c.get("rolname"), options)))
	}

	if c.get("memberof") != c.other.get("memberof") {
		strs = append(strs, Line(fmt.Sprintln(c.get("memberof"), "!=", c.other.get("memberof"))))

		// Remove the curly brackets
		memberof1 := curlyBracketRegex.ReplaceAllString(c.get("memberof"), "")
		memberof2 := curlyBracketRegex.ReplaceAllString(c.other.get("memberof"), "")

		// Split
		membersof1 := strings.Split(memberof1, ",")
		membersof2 := strings.Split(memberof2, ",")

		// TODO: Define INHERIT or not
		for _, mo1 := range membersof1 {
			if !misc.ContainsString(membersof2, mo1) {
				strs = append(strs, Line(fmt.Sprintf("GRANT %s TO %s;", mo1, c.get("rolename"))))
			}
		}

		for _, mo2 := range membersof2 {
			if !misc.ContainsString(membersof1, mo2) {
				strs = append(strs, Line(fmt.Sprintf("REVOKE %s FROM %s;", mo2, c.get("rolename"))))
			}
		}

	}
	return strs
}
