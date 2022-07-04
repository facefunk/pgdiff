# pgdiff - PostgreSQL schema diff

pgdiff compares the schema between two PostgreSQL 9 databases and generates alter statements to be *manually* run against the second database to make them match.  The provided pgdiff.sh script helps automate the process.  

pgdiff is transparent in what it does, so it never modifies a database directly. You alone are responsible for verifying the generated SQL before running it against your database.  Go ahead and see what SQL gets generated.

pgdiff is written to be easy to expand and improve the accuracy of the diff.


### repository status
This repository represents an attempt to adapt pgdiff into a Go library that can be easily integrated into Go based devlopment pipelines, it is currently in flux and still very much in beta. If you'd like to download binaries or work with the included scripts please visit the original pgdiff repository at [github.com/joncrlsn/pgdiff](https://github.com/joncrlsn/pgdiff).

### build
    go build main/pgdiff.go

### usage
	pgdiff [options] <schemaType>

(where options and &lt;schemaType&gt; are listed below)

There seems to be an ideal order for running the different schema types.  This order should minimize the problems you encounter.  For example, you will always want to add new tables before you add new columns.

In addition, some types can have dependencies which are not in the right order.  A classic case is views which depend on other views.  The missing view SQL is generated in alphabetical order so if a view create fails due to a missing view, just run the view SQL file over again. The pgdiff.sh script will prompt you about running it again.
 
Schema type ordering:

1. SCHEMA
2. ROLE
3. SEQUENCE
4. TABLE
5. COLUMN
6. INDEX
7. VIEW
8. MATVIEW
9. FOREIGN\_KEY
10. FUNCTION
11. TRIGGER
12. OWNER
13. GRANT\_RELATIONSHIP
14. GRANT\_ATTRIBUTE

As well as the above, the following special schema types are also available

1. ALL (all above in one run)
2. TABLE\_COLUMN (table columns only, no view columns)

Any combination of schema types may be specified, separated by spaces.

### example
I have found it helpful to take ```--schema-only``` dumps of the databases in question, load them into a local postgres, then do my sql generation and testing there before running the SQL against a more official database. Your local postgres instance will need the correct users/roles populated because db dumps do not copy that information.

```shell
pgdiff -U dbuser -H localhost -D refDB  -O "sslmode=disable" -S public \
       -u dbuser -h localhost -d compDB -o "sslmode=disable" -s public \
       TABLE 
```
```shell
pgdiff -c config-file.yaml TABLE 
```
```yaml
db1:
  user: dbuser
  pass: password
  host: localhost
  port: 5432
  name: refDB
  schema: public
  options: sslmode=disable
db2:
  user: dbuser
  pass: password
  host: localhost
  port: 5432
  name: compDB
  schema: public
  options: sslmode=disable
```

### options

|         options | explanation                                               |
|----------------:|-----------------------------------------------------------|
|   -V, --version | prints the version of pgdiff being used                   |
|      -?, --help | displays helpful usage information                        |
|     -U, --user1 | first postgres user                                       |
|     -u, --user2 | second postgres user                                      |
| -W, --password1 | first db password                                         |
| -w, --password2 | second db password                                        |
|     -H, --host1 | first db host.  default is localhost                      |
|     -h, --host2 | second db host. default is localhost                      |
|     -P, --port1 | first db port number.  default is 5432                    |
|     -p, --port2 | second db port number. default is 5432                    |
|   -D, --dbname1 | first db name                                             |
|   -d, --dbname2 | second db name                                            |
|   -S, --schema1 | first schema name.  default is * (all non-system schemas) |
|   -s, --schema2 | second schema name. default is * (all non-system schemas) |
|   -O, --option1 | first db options. example: sslmode=disable                |
|   -o, --option2 | second db options. example: sslmode=disable               |
|    -c, --config | load configuration from YAML file                         |

### getting help
If you think you found a bug, it might help replicate it if you find the appropriate test script (in the test directory) and modify it to show the problem.  Attach the script to an Issue request.

### todo
* add module providing SQL file data source
* fix SQL for adding an array column
* create Windows version of pgdiff.sh (or even better: re-write it all in Go)
* allow editing of individual SQL lines after failure (this would probably be done in the script pgdiff.sh)
* store failed SQL statements in an error file for later fixing and rerunning?
