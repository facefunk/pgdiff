package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/facefunk/pgdiff"

	"github.com/joncrlsn/pgutil"
	flag "github.com/ogier/pflag"
)

const (
	version = "0.9.3"
)

var (
	args       []string
	dbInfo1    *pgutil.DbInfo
	dbInfo2    *pgutil.DbInfo
	schemaType string
)

/*
 * Do the main logic
 */
func main() {

	var helpPtr = flag.BoolP("help", "?", false, "print help information")
	var versionPtr = flag.BoolP("version", "V", false, "print version information")

	dbInfo1, dbInfo2 = parseFlags()

	// Remaining args:
	args = flag.Args()

	if *helpPtr {
		usage()
	}

	if *versionPtr {
		fmt.Fprintf(os.Stderr, "%s - version %s\n", os.Args[0], version)
		fmt.Fprintln(os.Stderr, "Copyright (c) 2017 Jon Carlson.  All rights reserved.")
		fmt.Fprintln(os.Stderr, "Use of this source code is governed by the MIT license")
		fmt.Fprintln(os.Stderr, "that can be found here: http://opensource.org/licenses/MIT")
		os.Exit(1)
	}

	if len(args) == 0 {
		fmt.Println("The required first argument is SchemaType: SCHEMA, ROLE, SEQUENCE, TABLE, VIEW, MATVIEW, COLUMN, INDEX, FOREIGN_KEY, OWNER, GRANT_RELATIONSHIP, GRANT_ATTRIBUTE")
		os.Exit(1)
	}

	// Verify schemas
	schemas := dbInfo1.DbSchema + dbInfo2.DbSchema
	if schemas != "**" && strings.Contains(schemas, "*") {
		fmt.Println("If one schema is an asterisk, both must be.")
		os.Exit(1)
	}

	schemaType = strings.ToUpper(strings.Join(args, " "))
	fmt.Println("-- schemaType:", schemaType)

	fmt.Println("-- db1:", *dbInfo1)
	fmt.Println("-- db2:", *dbInfo2)

	conn1, err := dbInfo1.Open()
	check("opening database 1", err)

	conn2, err := dbInfo2.Open()
	check("opening database 2", err)

	var strs []pgdiff.Stringer

	for _, arg := range args {
		if arg == "ALL" {
			for _, st := range pgdiff.AllSchemaTypes {
				strs = append(strs, pgdiff.DBSourceCompare(conn1, conn2, dbInfo1, dbInfo2, st)...)
			}
			continue
		}
		strs = append(strs, pgdiff.DBSourceCompare(conn1, conn2, dbInfo1, dbInfo2, arg)...)
	}

	fmt.Println("-- Run the following SQL against db2:")
	for _, s := range strs {
		fmt.Println(s.String())
	}
}

func parseFlags() (*pgutil.DbInfo, *pgutil.DbInfo) {

	var dbUser1 = flag.StringP("user1", "U", "", "db user")
	var dbPass1 = flag.StringP("password1", "W", "", "db password")
	var dbHost1 = flag.StringP("host1", "H", "localhost", "db host")
	var dbPort1 = flag.IntP("port1", "P", 5432, "db port")
	var dbName1 = flag.StringP("dbname1", "D", "", "db name")
	var dbSchema1 = flag.StringP("schema1", "S", "*", "schema name or * for all schemas")
	var dbOptions1 = flag.StringP("options1", "O", "", "db options (eg. sslmode=disable)")

	var dbUser2 = flag.StringP("user2", "u", "", "db user")
	var dbPass2 = flag.StringP("password2", "w", "", "db password")
	var dbHost2 = flag.StringP("host2", "h", "localhost", "db host")
	var dbPort2 = flag.IntP("port2", "p", 5432, "db port")
	var dbName2 = flag.StringP("dbname2", "d", "", "db name")
	var dbSchema2 = flag.StringP("schema2", "s", "*", "schema name or * for all schemas")
	var dbOptions2 = flag.StringP("options2", "o", "", "db options (eg. sslmode=disable)")

	flag.Parse()

	dbInfo1 := pgutil.DbInfo{DbName: *dbName1, DbHost: *dbHost1, DbPort: int32(*dbPort1), DbUser: *dbUser1, DbPass: *dbPass1, DbSchema: *dbSchema1, DbOptions: *dbOptions1}

	dbInfo2 := pgutil.DbInfo{DbName: *dbName2, DbHost: *dbHost2, DbPort: int32(*dbPort2), DbUser: *dbUser2, DbPass: *dbPass2, DbSchema: *dbSchema2, DbOptions: *dbOptions2}

	return &dbInfo1, &dbInfo2
}

func usage() {
	fmt.Fprintf(os.Stderr, "%s - version %s\n", os.Args[0], version)
	fmt.Fprintf(os.Stderr, "usage: %s [<options>] <schemaType> \n", os.Args[0])
	fmt.Fprintln(os.Stderr, `
Compares the schema between two PostgreSQL databases and generates alter statements 
that can be *manually* run against the second database.

Options:
  -?, --help    : print help information
  -V, --version : print version information
  -v, --verbose : print extra run information
  -U, --user1   : first postgres user 
  -u, --user2   : second postgres user 
  -H, --host1   : first database host.  default is localhost 
  -h, --host2   : second database host. default is localhost 
  -P, --port1   : first port.  default is 5432 
  -p, --port2   : second port. default is 5432 
  -D, --dbname1 : first database name 
  -d, --dbname2 : second database name 
  -S, --schema1 : first schema.  default is all schemas
  -s, --schema2 : second schema. default is all schemas

<schemaTpe> can be: ALL, SCHEMA, ROLE, SEQUENCE, TABLE, TABLE_COLUMN, VIEW, MATVIEW, COLUMN, INDEX, FOREIGN_KEY, OWNER, GRANT_RELATIONSHIP, GRANT_ATTRIBUTE, TRIGGER, FUNCTION`)

	os.Exit(2)
}

func check(msg string, err error) {
	if err != nil {
		log.Fatal("Error "+msg, err)
	}
}
