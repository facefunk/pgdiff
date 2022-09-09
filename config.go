//
// Copyright (c) 2017 Jon Carlson.
// Copyright (c) 2022 Facefunk.
// All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.
//

package pgdiff

import (
	"fmt"
	"io"
	"os"
	"strings"

	flag "github.com/ogier/pflag"
	"gopkg.in/yaml.v3"
)

const (
	defaultSchema = "*"
	defaultOutput = OutputLine | OutputNotice
)

type (
	// CommandLineModule reads config from the command line.
	CommandLineModule interface {
		Name() string
		RegisterFlags(flagSet *flag.FlagSet)
	}

	// ConfigModule decodes config from either command line arguments or config file.
	ConfigModule interface {
		CommandLineModule
		ConfigureFromFlags()
		UnmarshalYAML(unmarshal func(interface{}) error) error
	}

	// Module is a super-factory. Each Module instance decodes a different type of config from either command line
	// arguments or config file and produces SchemaFactory instances based on that config.
	Module interface {
		ConfigModule
		Config(i int) Config
		Factory(conf Config) (SchemaFactory, error)
	}

	// Config is a decoded configuration instance.
	Config interface {
		SetSourceConfig(conf *SourceConfig)
		Valid() bool
	}

	// InitModule is a CommandLineModule that reads the main command-line-only options.
	InitModule struct {
		Help       bool
		Version    bool
		ConfigFile string
	}

	// GlobalModule is a ConfigModule that loads GlobalConfig.
	GlobalModule struct {
		vals GlobalConfig
		conf GlobalConfig
	}

	// GlobalConfig is the Config that does not apply to any Module.
	GlobalConfig struct {
		Output OutputSet
	}

	// SourceModule is a ConfigModule that decodes SourceConfig.
	SourceModule struct {
		vals1 SourceConfig
		vals2 SourceConfig
		conf1 SourceConfig
		conf2 SourceConfig
	}

	// SourceConfig is a Config that applies to every Module.
	SourceConfig struct {
		User   string
		Schema string
	}
)

func (m *InitModule) Name() string {
	return "Initialisation"
}

func (m *InitModule) RegisterFlags(flagSet *flag.FlagSet) {
	flagSet.BoolVarP(&m.Help, "help", "?", false, "print help information")
	flagSet.BoolVarP(&m.Version, "version", "V", false, "print version information")
	flagSet.StringVarP(&m.ConfigFile, "config", "c", "", "load configuration from YAML file")
}

func (m *GlobalModule) Name() string {
	return "Global"
}

func (m *GlobalModule) RegisterFlags(flagSet *flag.FlagSet) {
	m.vals.Output = defaultOutput
	flagSet.VarP(&m.vals.Output, "output", "t", "combination of output types to output")
}

func (m *GlobalModule) ConfigureFromFlags() {
	m.conf = m.vals
}

func (m *GlobalModule) UnmarshalYAML(unmarshal func(interface{}) error) error {
	conf := struct {
		Global *GlobalConfig
	}{
		defaultGlobalConfig(),
	}
	err := unmarshal(&conf)
	if err != nil {
		return err
	}
	m.conf = *conf.Global
	return nil
}

func defaultGlobalConfig() *GlobalConfig {
	return &GlobalConfig{
		Output: defaultOutput,
	}
}

func (m *GlobalModule) Config() *GlobalConfig {
	return &m.conf
}

func (m *SourceModule) Name() string {
	return "All sources"
}

func (m *SourceModule) RegisterFlags(flagSet *flag.FlagSet) {
	flagSet.StringVarP(&m.vals1.User, "user1", "U", "", "first postgres user")
	flagSet.StringVarP(&m.vals1.Schema, "schema1", "S", defaultSchema, "first schema name or * for all schemas")
	flagSet.StringVarP(&m.vals2.User, "user2", "u", "", "second postgres user")
	flagSet.StringVarP(&m.vals2.Schema, "schema2", "s", defaultSchema, "second schema name or * for all schemas")
}

func (m *SourceModule) ConfigureFromFlags() {
	m.conf1 = m.vals1
	m.conf2 = m.vals2
}

func (m *SourceModule) UnmarshalYAML(unmarshal func(interface{}) error) error {
	conf := struct {
		Source1 *SourceConfig
		Source2 *SourceConfig
	}{
		defaultSourceConfig(),
		defaultSourceConfig(),
	}
	err := unmarshal(&conf)
	if err != nil {
		return err
	}
	m.conf1 = *conf.Source1
	m.conf2 = *conf.Source2
	return nil
}

func defaultSourceConfig() *SourceConfig {
	return &SourceConfig{
		User:   "",
		Schema: defaultSchema,
	}
}

func (m *SourceModule) Config(i int) Config {
	switch i {
	case 1:
		return &m.conf1
	case 2:
		return &m.conf2
	default:
		panic("there are only 2 possible configs.")
	}
}

func (o *OutputSet) String() string {
	strs := StringsFromOutputSet(*o)
	return "\"" + strings.Join(strs, "|") + "\""
}

func (o *OutputSet) Set(str string) error {
	strs := strings.Split(str, "|")
	var err error
	*o, err = OutputSetFromStrings(strs)
	return err
}

func (c *SourceConfig) SetSourceConfig(conf *SourceConfig) {
	panic("SourceConfig already is a SourceConfig")
}

func (c *SourceConfig) Valid() bool {
	return c.User != "" && c.Schema != ""
}

func ConfigureModulesFromFile(configModules []ConfigModule, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return NewError(fmt.Sprintf("opening config file: %s", err))
	}
	err = ConfigureModulesFromReadSeeker(configModules, file)
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return NewError(fmt.Sprintf("closing config file: %s", err))
	}
	return nil
}

func ConfigureModulesFromReadSeeker(configModules []ConfigModule, reader io.ReadSeeker) error {
	for _, mod := range configModules {
		_, err := reader.Seek(0, 0)
		if err != nil {
			return NewError(fmt.Sprintf("rewinding config file: %s", err))
		}
		decoder := yaml.NewDecoder(reader)
		err = decoder.Decode(mod)
		if err != nil {
			return NewError(fmt.Sprintf("decoding config file: %s", err))
		}
	}
	return nil
}

func FactoriesFromModules(modules []Module, sourceModule *SourceModule) (map[int]SchemaFactory, error) {
	facs := make(map[int]SchemaFactory, 2)
	var schemas string
confNum:
	for i := 1; i <= 2; i++ {
		sourceConf := sourceModule.Config(i).(*SourceConfig)
		schemas += sourceConf.Schema
		for _, mod := range modules {
			conf := mod.Config(i)
			conf.SetSourceConfig(sourceConf)
			if conf.Valid() {
				fac, err := mod.Factory(conf)
				if err != nil {
					return nil, NewError(fmt.Sprintf("initialising SchemaFactory: %s", err))
				}
				facs[i] = fac
				continue confNum
			}
		}
		return nil, NewError("two properly configured datasources required")
	}
	// Verify schemas
	if schemas != "**" && strings.Contains(schemas, "*") {
		return nil, NewError("If one schema is an asterisk, both must be")
	}
	return facs, nil
}
