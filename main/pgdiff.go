package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/facefunk/pgdiff"
	"github.com/facefunk/pgdiff/db"

	"github.com/joncrlsn/pgutil"
	_ "github.com/lib/pq"
	flag "github.com/ogier/pflag"
)

const (
	version = "1.0-fcfnk.1"
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
	flag.Usage = usage

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

	fac1 := db.NewDBSourceSchemaFactory(conn1, dbInfo1)
	fac2 := db.NewDBSourceSchemaFactory(conn2, dbInfo2)
	strs := pgdiff.CompareByFactoriesAndArgs(fac1, fac2, args)

	fmt.Println("-- Run the following SQL against db2:")
	for _, s := range strs {
		fmt.Println(s.String())
	}
}

func parseFlags() (*pgutil.DbInfo, *pgutil.DbInfo) {

	var dbUser1 = flag.StringP("user1", "U", "", "first postgres user")
	var dbPass1 = flag.StringP("password1", "W", "", "first database password")
	var dbHost1 = flag.StringP("host1", "H", "localhost", "first database host")
	var dbPort1 = flag.IntP("port1", "P", 5432, "first port")
	var dbName1 = flag.StringP("dbname1", "D", "", "first database name")
	var dbSchema1 = flag.StringP("schema1", "S", "*", "first schema name or * for all schemas")
	var dbOptions1 = flag.StringP("options1", "O", "", "first database options (eg. sslmode=disable)")

	var dbUser2 = flag.StringP("user2", "u", "", "second postgres user")
	var dbPass2 = flag.StringP("password2", "w", "", "second database password")
	var dbHost2 = flag.StringP("host2", "h", "localhost", "second postgres host")
	var dbPort2 = flag.IntP("port2", "p", 5432, "second port")
	var dbName2 = flag.StringP("dbname2", "d", "", "second database name")
	var dbSchema2 = flag.StringP("schema2", "s", "*", "second schema name or * for all schemas")
	var dbOptions2 = flag.StringP("options2", "o", "", "second database options (eg. sslmode=disable)")

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

Options:`)
	fmt.Println(alignedFlagDefaults())
	fmt.Println("<schemaTpe> can be: ALL, SCHEMA, ROLE, SEQUENCE, TABLE, TABLE_COLUMN, VIEW, MATVIEW, COLUMN, INDEX, FOREIGN_KEY, OWNER, GRANT_RELATIONSHIP, GRANT_ATTRIBUTE, TRIGGER, FUNCTION")
	os.Exit(2)
}

func check(msg string, err error) {
	if err != nil {
		log.Fatal("Error "+msg, err)
	}
}

func alignedFlagDefaults() string {
	var buf bytes.Buffer
	flag.CommandLine.SetOutput(&buf)
	flag.PrintDefaults()
	flag.CommandLine.SetOutput(os.Stderr)
	def := buf.String()
	lines := strings.Split(def, "\n")
	max := 0
	l := len(lines) - 1
	pos := make([]int, l)
	for i := 0; i < l; i++ {
		p := strings.Index(lines[i], ":")
		pos[i] = p
		if p > max {
			max = p
		}
	}
	max += 1
	for i := 0; i < l; i++ {
		lines[i] = lines[i][:pos[i]] + strings.Repeat(" ", max-pos[i]) + lines[i][pos[i]:]
	}
	return strings.Join(lines, "\n")
}
