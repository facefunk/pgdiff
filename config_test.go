// Copyright (c) 2022 Facefunk. All rights reserved.
// Use of this source code is governed by the MIT license that can be found in the LICENSE file.

package pgdiff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

var (
	_ CommandLineModule = (*InitModule)(nil)
	_ ConfigModule      = (*GlobalModule)(nil)
	_ ConfigModule      = (*SourceModule)(nil)
	_ Config            = (*SourceConfig)(nil)
)

func TestYAML(t *testing.T) {
	in := `
source1:
  user: testy
  schema: public
source2:
  user: jon
`
	out := SourceModule{}
	err := yaml.Unmarshal([]byte(in), &out)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "testy", out.conf1.User)
	assert.Equal(t, "public", out.conf1.Schema)
	assert.Equal(t, "jon", out.conf2.User)
	assert.Equal(t, "*", out.conf2.Schema)
}
