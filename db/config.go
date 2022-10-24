//
// Copyright (c) 2017 Jon Carlson.
// Copyright (c) 2022 Facefunk.
// All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.
//

package db

import (
	"github.com/prudnitskiy/pgdiff"
	"github.com/joncrlsn/pgutil"
	flag "github.com/ogier/pflag"
)

const (
	defaultHost = "localhost"
	defaultPort = 5432
)

type (
	Module struct {
		vals1 configVals
		vals2 configVals
		conf1 Config
		conf2 Config
	}
	Config struct {
		pgutil.DbInfo `yaml:",inline"`
	}
	configVals struct {
		Name    string
		Pass    string
		Host    string
		Port    int
		Options string
	}
)

func (m *Module) Name() string {
	return "Database source"
}

func (m *Module) RegisterFlags(flagSet *flag.FlagSet) {
	flagSet.StringVarP(&m.vals1.Name, "dbname1", "D", "", "first database name")
	flagSet.StringVarP(&m.vals1.Pass, "password1", "W", "", "first database password")
	flagSet.StringVarP(&m.vals1.Host, "host1", "H", defaultHost, "first database host")
	flagSet.IntVarP(&m.vals1.Port, "port1", "P", defaultPort, "first port")
	flagSet.StringVarP(&m.vals1.Options, "options1", "O", "", "first database options (eg. sslmode=disable)")

	flagSet.StringVarP(&m.vals2.Name, "dbname2", "d", "", "second database name")
	flagSet.StringVarP(&m.vals2.Pass, "password2", "w", "", "second database password")
	flagSet.StringVarP(&m.vals2.Host, "host2", "h", defaultHost, "second postgres host")
	flagSet.IntVarP(&m.vals2.Port, "port2", "p", defaultPort, "second port")
	flagSet.StringVarP(&m.vals2.Options, "options2", "o", "", "second database options (eg. sslmode=disable)")
}

func (m *Module) ConfigureFromFlags() {
	setConf(&m.conf1, &m.vals1)
	setConf(&m.conf2, &m.vals2)
}

func setConf(conf *Config, vals *configVals) {
	conf.DbName = vals.Name
	conf.DbPass = vals.Pass
	conf.DbHost = vals.Host
	conf.DbPort = int32(vals.Port)
	conf.DbOptions = vals.Options
}

func (m *Module) UnmarshalYAML(unmarshal func(interface{}) error) error {
	conf := struct {
		DB1 *configVals
		DB2 *configVals
	}{
		defaultConfigVals(),
		defaultConfigVals(),
	}
	err := unmarshal(&conf)
	if err != nil {
		return err
	}
	setConf(&m.conf1, conf.DB1)
	setConf(&m.conf2, conf.DB2)
	return nil
}

func defaultConfigVals() *configVals {
	return &configVals{
		Name:    "",
		Pass:    "",
		Host:    defaultHost,
		Port:    defaultPort,
		Options: "",
	}
}

func (m *Module) Config(i int) pgdiff.Config {
	switch i {
	case 1:
		return &m.conf1
	case 2:
		return &m.conf2
	default:
		panic("there are only 2 possible configs.")
	}
}

func (m *Module) Factory(conf pgdiff.Config) (pgdiff.SchemaFactory, error) {
	c, ok := conf.(*Config)
	if !ok {
		return nil, pgdiff.NewError("Factory requires db.Config instance")
	}
	conn, err := c.DbInfo.Open()
	if err != nil {
		return nil, pgdiff.NewError("opening database: " + err.Error())
	}
	return NewSchemaFactory(conn, &c.DbInfo), nil
}

func (c *Config) SetSourceConfig(conf *pgdiff.SourceConfig) {
	c.DbUser = conf.User
	c.DbSchema = conf.Schema
}

func (c *Config) Valid() bool {
	return c.DbUser != "" && c.DbPass != "" && c.DbHost != "" && c.DbPort != 0 && c.DbName != "" && c.DbSchema != ""
}
