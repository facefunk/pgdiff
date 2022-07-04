package db

import (
	"github.com/facefunk/pgdiff"
	"github.com/joncrlsn/pgutil"
	flag "github.com/ogier/pflag"
)

const (
	defaultHost   = "localhost"
	defaultPort   = 5432
	defaultSchema = "*"
)

type (
	Module struct {
		dBIP1 dBInfoPtrs
		dBIP2 dBInfoPtrs
		conf1 Config
		conf2 Config
	}
	Config struct {
		pgutil.DbInfo `yaml:",inline"`
	}
	dBInfoPtrs struct {
		Name    *string
		Host    *string
		Port    *int
		User    *string
		Pass    *string
		Schema  *string
		Options *string
	}
)

func (m *Module) RegisterFlags(flagSet *flag.FlagSet) {
	m.dBIP1.User = flagSet.StringP("user1", "U", "", "first postgres user")
	m.dBIP1.Pass = flagSet.StringP("password1", "W", "", "first database password")
	m.dBIP1.Host = flagSet.StringP("host1", "H", defaultHost, "first database host")
	m.dBIP1.Port = flagSet.IntP("port1", "P", defaultPort, "first port")
	m.dBIP1.Name = flagSet.StringP("dbname1", "D", "", "first database name")
	m.dBIP1.Schema = flagSet.StringP("schema1", "S", defaultSchema, "first schema name or * for all schemas")
	m.dBIP1.Options = flagSet.StringP("options1", "O", "", "first database options (eg. sslmode=disable)")

	m.dBIP2.User = flagSet.StringP("user2", "u", "", "second postgres user")
	m.dBIP2.Pass = flagSet.StringP("password2", "w", "", "second database password")
	m.dBIP2.Host = flagSet.StringP("host2", "h", defaultHost, "second postgres host")
	m.dBIP2.Port = flagSet.IntP("port2", "p", defaultPort, "second port")
	m.dBIP2.Name = flagSet.StringP("dbname2", "d", "", "second database name")
	m.dBIP2.Schema = flagSet.StringP("schema2", "s", defaultSchema, "second schema name or * for all schemas")
	m.dBIP2.Options = flagSet.StringP("options2", "o", "", "second database options (eg. sslmode=disable)")
}

func (m *Module) ConfigureFromFlags() {
	setDBInfo(&m.conf1.DbInfo, &m.dBIP1)
	setDBInfo(&m.conf2.DbInfo, &m.dBIP2)
}

func setDBInfo(dbi *pgutil.DbInfo, dbip *dBInfoPtrs) {
	dbi.DbUser = *dbip.User
	dbi.DbPass = *dbip.Pass
	dbi.DbHost = *dbip.Host
	dbi.DbPort = int32(*dbip.Port)
	dbi.DbName = *dbip.Name
	dbi.DbSchema = *dbip.Schema
	dbi.DbOptions = *dbip.Options
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
		return nil, pgdiff.Error("Factory requires db.Config instance")
	}
	conn, err := c.DbInfo.Open()
	if err != nil {
		return nil, pgdiff.Error("opening database: " + err.Error())
	}
	return NewSchemaFactory(conn, &c.DbInfo), nil
}

func (m *Module) UnmarshalYAML(unmarshal func(interface{}) error) error {
	conf := struct {
		DB1 *dBInfoPtrs
		DB2 *dBInfoPtrs
	}{
		defaultDBInfoPtrs(),
		defaultDBInfoPtrs(),
	}
	err := unmarshal(&conf)
	if err != nil {
		return err
	}
	setDBInfo(&m.conf1.DbInfo, conf.DB1)
	setDBInfo(&m.conf2.DbInfo, conf.DB2)
	return nil
}

func defaultDBInfoPtrs() *dBInfoPtrs {
	name := ""
	host := defaultHost
	port := defaultPort
	user := ""
	pass := ""
	schema := defaultSchema
	options := ""
	return &dBInfoPtrs{&name, &host, &port, &user, &pass, &schema, &options}
}

func (c *Config) Valid() bool {
	return c.DbInfo.DbUser != "" && c.DbInfo.DbPass != "" && c.DbInfo.DbHost != "" && c.DbInfo.DbPort != 0 &&
		c.DbInfo.DbName != "" && c.DbInfo.DbSchema != ""
}

func (c *Config) DBSchema() string {
	return c.DbSchema
}
