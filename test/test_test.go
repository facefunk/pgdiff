// Copyright (c) 2022 Facefunk. All rights reserved.
// Use of this source code is governed by the MIT license that can be found in the LICENSE file.

package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTests(t *testing.T) {
	suites, err := tests()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 14, len(suites))
}
