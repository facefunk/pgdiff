package pgdiff

import (
	"fmt"
	"io"
	"os"
	"strings"

	flag "github.com/ogier/pflag"
	"gopkg.in/yaml.v3"
)

const defaultSchema = "*"

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
		Config(i int) Config
	}

	// Module is a super-factory. Each Module instance decodes a different type of config from either command line
	// arguments or config file and produces SchemaFactory instances based on that config.
	Module interface {
		ConfigModule
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

	// SourceModule is a ConfigModule that decodes SourceConfig.
	SourceModule struct {
		vals1 SourceConfig
		vals2 SourceConfig
		conf1 SourceConfig
		conf2 SourceConfig
	}

	// SourceConfig is the set of config options that apply to all source modules.
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
		defaultConfigVals(),
		defaultConfigVals(),
	}
	err := unmarshal(&conf)
	if err != nil {
		return err
	}
	m.conf1 = *conf.Source1
	m.conf2 = *conf.Source2
	return nil
}

func defaultConfigVals() *SourceConfig {
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

func (c *SourceConfig) SetSourceConfig(conf *SourceConfig) {
	panic("SourceConfig already is a SourceConfig")
}

func (c *SourceConfig) Valid() bool {
	return c.User != "" && c.Schema != ""
}

func ConfigureModulesFromFile(configModules []ConfigModule, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return Error(fmt.Sprintf("opening config file: %s", err))
	}
	err = ConfigureModulesFromReadSeeker(configModules, file)
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return Error(fmt.Sprintf("closing config file: %s", err))
	}
	return nil
}

func ConfigureModulesFromReadSeeker(configModules []ConfigModule, reader io.ReadSeeker) error {
	for _, mod := range configModules {
		_, err := reader.Seek(0, 0)
		if err != nil {
			return Error(fmt.Sprintf("rewinding config file: %s", err))
		}
		decoder := yaml.NewDecoder(reader)
		err = decoder.Decode(mod)
		if err != nil {
			return Error(fmt.Sprintf("decoding config file: %s", err))
		}
	}
	return nil
}

func FactoriesFromModules(modules []Module, sourceModule ConfigModule) (map[int]SchemaFactory, error) {
	facs := make(map[int]SchemaFactory, 2)
	var schemas string
confNum:
	for i := 1; i <= 2; i++ {
		defConf := sourceModule.Config(i).(*SourceConfig)
		schemas += defConf.Schema
		for _, mod := range modules {
			conf := mod.Config(i)
			conf.SetSourceConfig(defConf)
			if conf.Valid() {
				fac, err := mod.Factory(conf)
				if err != nil {
					return nil, Error(fmt.Sprintf("initialising SchemaFactory: %s", err))
				}
				facs[i] = fac
				continue confNum
			}
		}
		return nil, Error("two properly configured datasources required")
	}
	// Verify schemas
	if schemas != "**" && strings.Contains(schemas, "*") {
		return nil, Error("If one schema is an asterisk, both must be")
	}
	return facs, nil
}
