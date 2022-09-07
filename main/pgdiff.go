package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/facefunk/pgdiff"
	"github.com/facefunk/pgdiff/db"
	flag "github.com/ogier/pflag"
)

const (
	versionFormat = `pgdiff - version %s
Copyright (c) 2017 Jon Carlson.
All rights reserved.
Use of this source code is governed by the MIT license
that can be found here: http://opensource.org/licenses/MIT
`
	usageFormat = `pgdiff - version %s
usage: %s [<options>] <schemaType>
Compares the schema between two PostgreSQL databases and generates alter statements 
that can be *manually* run against the second database.

Options:
%s
<schemaType> can be: %s
`
)

var commandLineModules []pgdiff.CommandLineModule

/*
 * Do the main logic
 */
func main() {

	initModule := &pgdiff.InitModule{}
	sourceModule := &pgdiff.SourceModule{}
	DBModule := &db.Module{}

	commandLineModules = []pgdiff.CommandLineModule{
		initModule,
		sourceModule,
		DBModule,
	}

	configModules := []pgdiff.ConfigModule{
		sourceModule,
		DBModule,
	}

	modules := []pgdiff.Module{
		DBModule,
	}

	for _, mod := range commandLineModules {
		mod.RegisterFlags(flag.CommandLine)
	}

	flag.Usage = usage
	flag.Parse()

	if initModule.Help {
		usage()
	}

	if initModule.Version {
		fmt.Fprintf(os.Stderr, versionFormat, pgdiff.Version)
		os.Exit(1)
	}

	// Remaining args:
	args := flag.Args()
	if len(args) == 0 {
		log.Fatal("Error: the required first argument is SchemaType: " + pgdiff.SchemaTypes)
	}

	if initModule.ConfigFile != "" {
		err := pgdiff.ConfigureModulesFromFile(configModules, initModule.ConfigFile)
		check("configuring Modules from config file", err)
	} else {
		for _, mod := range configModules {
			mod.ConfigureFromFlags()
		}
	}

	facs, err := pgdiff.FactoriesFromModules(modules, sourceModule)
	check("generating SchemaFactories", err)

	strs := pgdiff.CompareByFactoriesAndArgs(facs[1], facs[2], args)
	for _, s := range strs {
		fmt.Println(s.String())
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, usageFormat, pgdiff.Version, os.Args[0], alignedFlagDefaults(), pgdiff.SchemaTypes)
	os.Exit(2)
}

func check(msg string, err error) {
	if err != nil {
		log.Fatal("Error: ", msg, ": ", err)
	}
}

func alignedFlagDefaults() string {
	var def string
	for _, mod := range commandLineModules {
		flagSet := flag.NewFlagSet("defaults", flag.PanicOnError)
		var buf bytes.Buffer
		flagSet.SetOutput(&buf)
		mod.RegisterFlags(flagSet)
		flagSet.PrintDefaults()
		def += mod.Name() + "\n" + buf.String()
	}

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
		if pos[i] == -1 {
			lines[i] = "\n  " + lines[i] + ":"
			continue
		}
		lines[i] = "  " + lines[i][:pos[i]] + strings.Repeat(" ", max-pos[i]) + lines[i][pos[i]:]
	}
	return strings.Join(lines, "\n")
}
