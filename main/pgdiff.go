package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/facefunk/pgdiff"
	"github.com/facefunk/pgdiff/db"
	"gopkg.in/yaml.v3"

	_ "github.com/lib/pq"
	flag "github.com/ogier/pflag"
)

const (
	version = "1.0-fcfnk.1"
)

/*
 * Do the main logic
 */
func main() {
	var helpPtr = flag.BoolP("help", "?", false, "print help information")
	var versionPtr = flag.BoolP("version", "V", false, "print version information")
	var configPtr = flag.StringP("config", "c", "", "load configuration from YAML file")

	modules := []pgdiff.Module{
		&db.Module{},
	}
	for _, mod := range modules {
		mod.RegisterFlags(flag.CommandLine)
	}

	flag.Usage = usage
	flag.Parse()

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

	// Remaining args:
	args := flag.Args()
	if len(args) == 0 {
		log.Fatal("The required first argument is SchemaType: " + pgdiff.SchemaTypes)
	}

	if *configPtr != "" {
		file, err := os.Open(*configPtr)
		check("opening config file", err)
		decoder := yaml.NewDecoder(file)
		for _, mod := range modules {
			err = decoder.Decode(mod)
			check("decoding config file", err)
		}
	} else {
		for _, mod := range modules {
			mod.ConfigureFromFlags()
		}
	}

	facs := make([]pgdiff.SchemaFactory, 3)
	dbSchemas := make([]string, 3)
confNum:
	for i := 1; i <= 2; i++ {
		for _, mod := range modules {
			conf := mod.Config(i)
			if conf.Valid() {
				dbSchemas[i] = conf.DBSchema()
				fac, err := mod.Factory(conf)
				check("initialising SchemaFactory", err)
				facs[i] = fac
				continue confNum
			}
		}
		log.Fatal("Error: two properly configured datasources required.")
	}

	// Verify schemas
	schemas := dbSchemas[1] + dbSchemas[2]
	if schemas != "**" && strings.Contains(schemas, "*") {
		log.Fatal("If one schema is an asterisk, both must be.")
	}

	strs := pgdiff.CompareByFactoriesAndArgs(facs[1], facs[2], args)
	for _, s := range strs {
		fmt.Println(s.String())
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "%s - version %s\n", os.Args[0], version)
	fmt.Fprintf(os.Stderr, "usage: %s [<options>] <schemaType> \n", os.Args[0])
	fmt.Fprintln(os.Stderr, `
Compares the schema between two PostgreSQL databases and generates alter statements 
that can be *manually* run against the second database.

Options:`)
	fmt.Println(alignedFlagDefaults())
	fmt.Println("<schemaType> can be: " + pgdiff.SchemaTypes)
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
