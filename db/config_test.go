package db

import (
	"testing"

	"github.com/facefunk/pgdiff"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

var (
	_ pgdiff.Module = (*Module)(nil)
	_ pgdiff.Config = (*Config)(nil)
)

func TestYAML(t *testing.T) {
	in := `
db1:
  name: testing
  pass: password
  host: testy.com
  port: 1234
  options: sslmode=disable
db2:
  name: pgdiff
  pass: secret
`
	out := Module{}
	err := yaml.Unmarshal([]byte(in), &out)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "testing", out.conf1.DbName)
	assert.Equal(t, "password", out.conf1.DbPass)
	assert.Equal(t, "testy.com", out.conf1.DbHost)
	assert.Equal(t, int32(1234), out.conf1.DbPort)
	assert.Equal(t, "sslmode=disable", out.conf1.DbOptions)

	assert.Equal(t, "pgdiff", out.conf2.DbName)
	assert.Equal(t, "secret", out.conf2.DbPass)
	assert.Equal(t, "localhost", out.conf2.DbHost)
	assert.Equal(t, int32(5432), out.conf2.DbPort)
	assert.Equal(t, "", out.conf2.DbOptions)
}
