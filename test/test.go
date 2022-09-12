// Copyright (c) 2022 Facefunk. All rights reserved.
// Use of this source code is governed by the MIT license that can be found in the LICENSE file.

package test

import (
	"path/filepath"
	"strconv"
	"strings"
)

const (
	inputData  = "data/input"
	outputData = "data/output"
)

type (
	input struct {
		fname string
		db    string
	}
	test struct {
		name   string
		conf   int
		inputs []*input
		output string
	}
	suite struct {
		name  string
		op    string
		tests []*test
	}
)

var confs = [][]struct {
	db     string
	schema string
}{
	{{"db1", "s1"}, {"db1", "s2"}},
	{{"db1", "*"}, {"db2", "*"}},
	{{"db1", "s3"}, {"db1", "s4"}},
}

func tests() ([]*suite, error) {
	// Find test input files
	files, err := filepath.Glob(inputData + "/test*.sql")
	if err != nil {
		return nil, err
	}
	// Parse filenames into test structures
	suites := make([]*suite, 0, len(files))
	lt := &test{}
	ls := &suite{}
	for _, f := range files {
		name := f[strings.LastIndex(f, "/")+1 : len(f)-4]
		parts := strings.Split(name, "-")
		first := parts[0]
		op := parts[1]
		testNum := parts[2]
		dbNum := testNum
		if len(parts) > 3 {
			dbNum = parts[3]
		}
		suiteName := first + "-" + op
		testName := suiteName + "-" + testNum
		testInt, err := strconv.Atoi(testNum)
		if err != nil {
			return nil, err
		}
		if op != ls.op {
			ls = &suite{name: suiteName, op: op}
			suites = append(suites, ls)
		}
		if testName != lt.name {
			lt = &test{name: testName, conf: testInt - 1, output: outputData + "/" + testName + ".sql"}
			ls.tests = append(ls.tests, lt)
		}
		lt.inputs = append(lt.inputs, &input{fname: f, db: "db" + dbNum})
	}
	return suites, nil
}
