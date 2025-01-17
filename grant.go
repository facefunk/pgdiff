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
	"sort"
	"strings"
)

var aclRegex = regexp.MustCompile(`([a-zA-Z0-9]+)*=([rwadDxtXUCcT]+)/([a-zA-Z0-9]+)$`)

var permMap = map[string]string{
	"a": "INSERT",
	"r": "SELECT",
	"w": "UPDATE",
	"d": "DELETE",
	"D": "TRUNCATE",
	"x": "REFERENCES",
	"t": "TRIGGER",
	"X": "EXECUTE",
	"U": "USAGE",
	"C": "CREATE",
	"c": "CONNECT",
	"T": "TEMPORARY",
}

/*
parseGrants converts an ACL (access control list) line into a role and a slice of permission strings

Example of an ACL: user1=rwa/c42

rolename=xxxx -- privileges granted to a role
        =xxxx -- privileges granted to PUBLIC
            r -- SELECT ("read")
            w -- UPDATE ("write")
            a -- INSERT ("append")
            d -- DELETE
            D -- TRUNCATE
            x -- REFERENCES
            t -- TRIGGER
            X -- EXECUTE
            U -- USAGE
            C -- CREATE
            c -- CONNECT
            T -- TEMPORARY
      arwdDxt -- ALL PRIVILEGES (for tables, varies for other objects)
            * -- grant option for preceding privilege
        /yyyy -- role that granted this privilege
*/
func parseGrants(acl string) (string, []string, []Stringer) {
	role, perms := parseAcl(acl)
	if len(role) == 0 && len(acl) == 0 {
		return role, make([]string, 0), nil
	}
	// For each character in perms, convert it to a word found in permMap
	// e.g. 'a' maps to 'INSERT'
	permWords := make(sort.StringSlice, 0)
	var errs []Stringer
	for _, c := range strings.Split(perms, "") {
		permWord := permMap[c]
		if len(permWord) > 0 {
			permWords = append(permWords, permWord)
		} else {
			errs = append(errs, NewError(fmt.Sprintf("-- Error, found permission character we haven't coded for: %s", c)))
		}
	}
	permWords.Sort()
	return role, permWords, errs
}

// parseAcl parses an ACL (access control list) string (e.g. 'c42=aur/postgres') into a role and
// a string made up of one-character permissions
func parseAcl(acl string) (role string, perms string) {
	role, perms = "", ""
	matches := aclRegex.FindStringSubmatch(acl)
	if matches != nil {
		role = matches[1]
		perms = matches[2]
		if len(role) == 0 {
			role = "public"
		}
	}
	return role, perms
}
