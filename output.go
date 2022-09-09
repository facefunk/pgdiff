// Copyright (c) 2022 Facefunk. All rights reserved.
// Use of this source code is governed by the MIT license that can be found in the LICENSE file.

package pgdiff

import (
	"fmt"
	"io"
)

const (
	OutputLine = 1 << iota
	OutputNotice
	OutputError
)

// outputStrings is an array of structs, instead of a map, to provide a canonical order.
var outputStrings = [3]struct {
	OutputSet
	String string
}{
	{OutputLine, "line"},
	{OutputNotice, "notice"},
	{OutputError, "error"},
}

type (
	// Stringer simply returns a string. Allows a slice of Stringer to be returned from a function, which can then be
	// type-switched to filter the output.
	Stringer interface {
		String() string
	}

	Line   string
	Notice string
	Error  string

	// OutputSet is bitmask determining how to filter Stringer outputs.
	OutputSet byte
)

func (s *Line) String() string {
	return string(*s)
}

func (s *Notice) String() string {
	return string(*s)
}

func (s *Error) String() string {
	return string(*s)
}

func (s *Error) Error() string {
	return string(*s)
}

func NewLine(str string) *Line {
	l := Line(str)
	return &l
}

func NewNotice(str string) *Notice {
	l := Notice(str)
	return &l
}

func NewError(str string) *Error {
	l := Error(str)
	return &l
}

// StringsFromOutputSet produces a slice of strings representing the OutputSet o.
func StringsFromOutputSet(o OutputSet) []string {
	strs := make([]string, 0, len(outputStrings))
	for _, out := range outputStrings {
		if o&out.OutputSet != 0 {
			strs = append(strs, out.String)
		}
	}
	return strs
}

// OutputSetFromStrings parses a slice of strings into an OutputSet.
func OutputSetFromStrings(strs []string) (OutputSet, error) {
	o := OutputSet(0)
	errs := ""
scan:
	for _, s := range strs {
		for _, out := range outputStrings {
			if s == out.String {
				o |= out.OutputSet
				continue scan
			}
		}
		errs += "|" + s
	}
	if errs != "" {
		return 0, NewError("invalid output type: " + errs[1:])
	}
	return o, nil
}

// PrintStringers prints every Stringer to out filtered by type based on the corresponding bits set in the OutputSet o.
// Every Error will be printed to err.
func PrintStringers(strs []Stringer, o OutputSet, out io.Writer, err io.Writer) {
	l := o&OutputLine == 0
	n := o&OutputNotice == 0
	e := o&OutputError == 0
	for _, s := range strs {
		switch s.(type) {
		case *Line:
			if l {
				continue
			}
		case *Notice:
			if n {
				continue
			}
		case *Error:
			fmt.Fprintln(err, s.String())
			if e {
				continue
			}
		}
		fmt.Fprintln(out, s.String())
	}
}
