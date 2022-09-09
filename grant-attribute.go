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
// GrantAttributeRows definition
// ==================================

// GrantAttributeRows is a sortable slice of string maps
type GrantAttributeRows []map[string]string

func (slice GrantAttributeRows) Len() int {
	return len(slice)
}

func (slice GrantAttributeRows) Less(i, j int) bool {
	if slice[i]["compare_name"] != slice[j]["compare_name"] {
		return slice[i]["compare_name"] < slice[j]["compare_name"]
	}

	// Only compare the role part of the ACL
	// Not yet sure if this is absolutely necessary
	// (or if we could just compare the entire ACL string)
	role1, _ := parseAcl(slice[i]["attribute_acl"])
	role2, _ := parseAcl(slice[j]["attribute_acl"])
	if role1 != role2 {
		return role1 < role2
	}

	return false
}

func (slice GrantAttributeRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// ==================================
// GrantAttributeSchema definition
// (implements Schema -- defined in pgdiff.go)
// ==================================

// GrantAttributeSchema holds a slice of rows from one of the databases as well as
// a reference to the current row of data we're viewing.
type GrantAttributeSchema struct {
	rows     GrantAttributeRows
	rowNum   int
	done     bool
	dbSchema string
	other    *GrantAttributeSchema
}

func NewGrantAttributeSchema(rows GrantAttributeRows, dbSchema string) *GrantAttributeSchema {
	return &GrantAttributeSchema{rows: rows, rowNum: -1, dbSchema: dbSchema}
}

// get returns the value from the current row for the given key
func (c *GrantAttributeSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

// get returns the current row for the given key
func (c *GrantAttributeSchema) getRow() map[string]string {
	if c.rowNum >= len(c.rows) {
		return make(map[string]string)
	}
	return c.rows[c.rowNum]
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *GrantAttributeSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than,
// or greater than the second row.
func (c *GrantAttributeSchema) Compare(obj Schema) (int, *Error) {
	c2, ok := obj.(*GrantAttributeSchema)
	if !ok {
		return +999, NewError(fmt.Sprint("compare needs a GrantAttributeSchema instance", c2))
	}
	c.other = c2

	val := misc.CompareStrings(c.get("compare_name"), c.other.get("compare_name"))
	if val != 0 {
		return val, nil
	}

	role1, _ := parseAcl(c.get("attribute_acl"))
	role2, _ := parseAcl(c.other.get("attribute_acl"))
	val = misc.CompareStrings(role1, role2)
	return val, nil
}

// Add prints SQL to add the grant
func (c *GrantAttributeSchema) Add() []Stringer {
	schema := c.other.dbSchema
	if schema == "*" {
		schema = c.get("schema_name")
	}
	var strs []Stringer
	role, grants, errs := parseGrants(c.get("attribute_acl"))
	strs = append(strs, errs...)
	strs = append(strs, NewLine(fmt.Sprintf("GRANT %s (%s) ON %s.%s TO %s; -- Add", strings.Join(grants, ", "), c.get("attribute_name"), schema, c.get("relationship_name"), role)))
	return strs
}

// Drop prints SQL to drop the grant
func (c *GrantAttributeSchema) Drop() []Stringer {
	role, grants, errs := parseGrants(c.get("attribute_acl"))
	var strs []Stringer
	strs = append(strs, errs...)
	strs = append(strs, NewLine(fmt.Sprintf("REVOKE %s (%s) ON %s.%s FROM %s; -- Drop", strings.Join(grants, ", "), c.get("attribute_name"), c.get("schema_name"), c.get("relationship_name"), role)))
	return strs
}

// Change handles the case where the relationship and column match, but the grant does not
func (c *GrantAttributeSchema) Change() []Stringer {
	var strs []Stringer

	role, grants1, errs := parseGrants(c.get("attribute_acl"))
	strs = append(strs, errs...)
	_, grants2, errs := parseGrants(c.other.get("attribute_acl"))
	strs = append(strs, errs...)

	// Find grants in the first db that are not in the second
	// (for this relationship and owner)
	var grantList []string
	for _, g := range grants1 {
		if !misc.ContainsString(grants2, g) {
			grantList = append(grantList, g)
		}
	}
	if len(grantList) > 0 {
		strs = append(strs, NewLine(fmt.Sprintf("GRANT %s (%s) ON %s.%s TO %s; -- Change", strings.Join(grantList, ", "),
			c.get("attribute_name"), c.other.get("schema_name"), c.get("relationship_name"), role)))
	}

	// Find grants in the second db that are not in the first
	// (for this relationship and owner)
	var revokeList []string
	for _, g := range grants2 {
		if !misc.ContainsString(grants1, g) {
			revokeList = append(revokeList, g)
		}
	}
	if len(revokeList) > 0 {
		strs = append(strs, NewLine(fmt.Sprintf("REVOKE %s (%s) ON %s.%s FROM %s; -- Change", strings.Join(revokeList, ", "), c.get("attribute_name"), c.other.get("schema_name"), c.get("relationship_name"), role)))
	}

	//strs = append(strs, NewLine(fmt.Sprintf("--1 rel:%s, relAcl:%s, col:%s, colAcl:%s\n", c.get("attribute_name"), c.get("attribute_acl"), c.get("attribute_name"), c.get("attribute_acl"))))
	//strs = append(strs, NewLine(fmt.Sprintf("--2 rel:%s, relAcl:%s, col:%s, colAcl:%s\n", c.other.get("attribute_name"), c.other.get("attribute_acl"), c.other.get("attribute_name"), c.other.get("attribute_acl"))))
	return strs
}
