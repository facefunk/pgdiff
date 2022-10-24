// Copyright (c) 2022 Facefunk. All rights reserved.
// Use of this source code is governed by the MIT license that can be found in the LICENSE file.

package test

import (
	"bytes"
	"database/sql"
	"io"
	"os"
	"testing"

	"github.com/prudnitskiy/pgdiff"
	"github.com/prudnitskiy/pgdiff/db"
	"github.com/joncrlsn/pgutil"
	"github.com/stretchr/testify/assert"
)

func TestDB(t *testing.T) {
	suites, err := tests()
	if err != nil {
		t.Fatal(err)
	}

	sfInfo := newDbInfo()
	mod := &db.Module{}
	for _, s := range suites {

		// Reset once per suite.
		err = singleBatchFromFile(sfInfo, inputData+"/start-fresh.sql")
		if err != nil {
			t.Fatal(err)
		}
		for _, te := range s.tests {

			// Load input.
			for _, in := range te.inputs {
				info := newDbInfo()
				info.DbName = in.db
				info.DbUser = "u1"
				err = singleBatchFromFile(info, in.fname)
				if err != nil {
					t.Fatalf("error with test: %s: %s", te.name, err)
				}
			}

			// Build factories.
			facs := make([]pgdiff.SchemaFactory, 2)
			for i, tt := range confs[te.conf] {
				conf := &db.Config{*newDbInfo()}
				conf.DbName = tt.db
				conf.DbSchema = tt.schema
				conf.DbUser = "u1"
				facs[i], err = mod.Factory(conf)
				if err != nil {
					t.Fatalf("error with test: %s: %s", te.name, err)
				}
			}

			// Generate output.
			strs := pgdiff.CompareByFactories(facs[0], facs[1], s.op)

			// Close factories every time to avoid collisions with input.
			for _, fac := range facs {
				if closer, ok := fac.(io.Closer); ok {
					err = closer.Close()
					if err != nil {
						t.Errorf("error with test: %s: %s", te.name, err)
					}
				}
			}

			// Print
			buf, eBuf := &bytes.Buffer{}, &bytes.Buffer{}
			pgdiff.PrintStringers(strs, pgdiff.OutputLine, buf, eBuf)
			bStr, eStr := buf.String(), eBuf.String()
			if eStr != "" {
				t.Errorf("error with test: %s: %s", te.name, eStr)
			}

			// Load expected output.
			out, err := os.ReadFile(te.output)
			if err != nil {
				t.Fatal(err)
			}

			// Compare
			assert.Equal(t, string(out), bStr, te.name)
		}
	}
}

func newDbInfo() *pgutil.DbInfo {
	return &pgutil.DbInfo{
		DbName:    "pgdiff_parent",
		DbHost:    "localhost",
		DbPort:    5432,
		DbUser:    "pgdiff_parent",
		DbPass:    "asdf",
		DbOptions: "sslmode=disable",
	}
}

func singleBatchFromFile(info *pgutil.DbInfo, fname string) error {
	d, err := info.Open()
	if err != nil {
		return err
	}
	err = runBatchFromFile(d, fname)
	if err != nil {
		return err
	}
	err = d.Close()
	if err != nil {
		return err
	}
	return nil
}

func runBatchFromFile(db *sql.DB, fname string) error {
	query, err := os.ReadFile(fname)
	if err != nil {
		return err
	}
	//fmt.Println(string(query))
	qs := splitSQL(string(query))
	for _, q := range qs {
		_, err = db.Exec(q)
		if err != nil {
			return err
		}
	}
	return nil
}

const (
	defaultState = iota
	quoteState
	dollarState
	commentState
	blockState
)

// splitSQL splits SQL batches up into separate queries because Postgres clients must send queries to the server one at
// a time. psql does this splitting but Go client libs don't.
// TODO: Possibly improve this simplistic interpretation.
func splitSQL(query string) []string {
	var out []string
	var state uint8
	var li int
	var prev rune
	for i, c := range query {
		switch state {
		case defaultState:
			switch c {
			case ';':
				out = append(out, query[li:i+1])
				li = i + 1
			case '\'':
				state = quoteState
			case '$':
				if prev == '$' {
					state = dollarState
				}
			case '-':
				if prev == '-' {
					state = commentState
				}
			case '*':
				if prev == '/' {
					state = blockState
				}
			}
		case quoteState:
			if c == '\'' {
				state = defaultState
			}
		case dollarState:
			if c == '$' && prev == '$' {
				state = defaultState
			}
		case commentState:
			if c == '\n' {
				state = defaultState
			}
		case blockState:
			if c == '/' && prev == '*' {
				state = defaultState
			}
		}
		prev = c
	}
	return out
}

func TestSplitSQL(t *testing.T) {
	query, err := os.ReadFile(inputData + "/test-FUNCTION-1.sql")
	if err != nil {
		t.Fatal(err)
	}
	strs := splitSQL(string(query))
	assert.Equal(t, 6, len(strs))

	query, err = os.ReadFile(inputData + "/test-COLUMN-3-1.sql")
	if err != nil {
		t.Fatal(err)
	}
	strs = splitSQL(string(query))
	assert.Equal(t, 4, len(strs))
}
