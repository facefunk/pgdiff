//
// Copyright (c) 2017 Jon Carlson.
// Copyright (c) 2022 Facefunk.
// All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.
//

package db

import "text/template"

var (
	columnSqlTemplate            = initColumnSqlTemplate()
	tableColumnSqlTemplate       = initTableColumnSqlTemplate()
	foreignKeySqlTemplate        = initForeignKeySqlTemplate()
	functionSqlTemplate          = initFunctionSqlTemplate()
	grantAttributeSqlTemplate    = initGrantAttributeSqlTemplate()
	grantRelationshipSqlTemplate = initGrantRelationshipSqlTemplate()
	indexSqlTemplate             = initIndexSqlTemplate()
	ownerSqlTemplate             = initOwnerSqlTemplate()
	sequenceSqlTemplate          = initSequenceSqlTemplate()
	tableSqlTemplate             = initTableSqlTemplate()
	triggerSqlTemplate           = initTriggerSqlTemplate()

	matViewSql = `
WITH matviews as ( SELECT schemaname || '.' || matviewname AS matviewname,
definition
FROM pg_catalog.pg_matviews 
WHERE schemaname NOT LIKE 'pg_%' 
)
SELECT
matviewname,
definition,
COALESCE(string_agg(indexdef, ';' || E'\n\n') || ';', '')  as indexdef
FROM matviews
LEFT JOIN  pg_catalog.pg_indexes on matviewname = schemaname || '.' || tablename
group by matviewname, definition
ORDER BY
matviewname;
`

	roleSql = `
SELECT r.rolname
    , r.rolsuper
    , r.rolinherit
    , r.rolcreaterole
    , r.rolcreatedb
    , r.rolcanlogin
    , r.rolconnlimit
    , r.rolvaliduntil
    , r.rolreplication
	, ARRAY(SELECT b.rolname 
	        FROM pg_catalog.pg_auth_members m  
			JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)  
	        WHERE m.member = r.oid) as memberof
FROM pg_catalog.pg_roles AS r
ORDER BY r.rolname;
`

	schemataSql = `
SELECT schema_name
    , schema_owner
    , default_character_set_schema
FROM information_schema.schemata
WHERE schema_name NOT LIKE 'pg_%' 
  AND schema_name <> 'information_schema' 
ORDER BY schema_name;`

	viewSql = `
SELECT schemaname || '.' || viewname AS viewname
	, definition 
FROM pg_views 
WHERE schemaname NOT LIKE 'pg_%' 
ORDER BY viewname;
`
)

func initColumnSqlTemplate() *template.Template {
	query := `
SELECT table_schema
    ,  {{if eq $.DbSchema "*" }}table_schema || '.' || {{end}}table_name || '.' ||lpad(cast (ordinal_position as varchar), 5, '0')|| column_name AS compare_name
	, table_name
    , column_name
    , data_type
    , is_nullable
    , column_default
    , character_maximum_length
    , is_identity
    , identity_generation
    , substring(udt_name from 2) AS array_type
FROM information_schema.columns
WHERE is_updatable = 'YES'
{{if eq $.DbSchema "*" }}
AND table_schema NOT LIKE 'pg_%' 
AND table_schema <> 'information_schema' 
{{else}}
AND table_schema = '{{$.DbSchema}}'
{{end}}
ORDER BY compare_name ASC;
`
	t := template.New("ColumnSqlTmpl")
	template.Must(t.Parse(query))
	return t
}

func initTableColumnSqlTemplate() *template.Template {
	query := `
SELECT a.table_schema
    , {{if eq $.DbSchema "*" }}a.table_schema || '.' || {{end}}a.table_name || '.' || column_name  AS compare_name
	, a.table_name
    , column_name
    , data_type
    , is_nullable
    , column_default
    , character_maximum_length
FROM information_schema.columns a
INNER JOIN information_schema.tables b
    ON a.table_schema = b.table_schema AND
       a.table_name = b.table_name AND
       b.table_type = 'BASE TABLE'
WHERE is_updatable = 'YES'
{{if eq $.DbSchema "*" }}
AND a.table_schema NOT LIKE 'pg_%' 
AND a.table_schema <> 'information_schema' 
{{else}}
AND a.table_schema = '{{$.DbSchema}}'
{{end}}
ORDER BY compare_name ASC;
`
	t := template.New("ColumnSqlTmpl")
	template.Must(t.Parse(query))
	return t
}

func initForeignKeySqlTemplate() *template.Template {
	query := `
SELECT {{if eq $.DbSchema "*" }}ns.nspname || '.' || {{end}}cl.relname || '.' || c.conname AS compare_name
    , ns.nspname AS schema_name
	, cl.relname AS table_name
    , c.conname AS fk_name
	, pg_catalog.pg_get_constraintdef(c.oid, true) as constraint_def
FROM pg_catalog.pg_constraint c
INNER JOIN pg_class AS cl ON (c.conrelid = cl.oid)
INNER JOIN pg_namespace AS ns ON (ns.oid = c.connamespace)
WHERE c.contype = 'f'
{{if eq $.DbSchema "*"}}
AND ns.nspname NOT LIKE 'pg_%' 
AND ns.nspname <> 'information_schema' 
{{else}}
AND ns.nspname = '{{$.DbSchema}}'
{{end}}
`
	t := template.New("ForeignKeySqlTmpl")
	template.Must(t.Parse(query))
	return t
}

func initFunctionSqlTemplate() *template.Template {
	query := `
SELECT n.nspname                 AS schema_name
	, {{if eq $.DbSchema "*" }}n.nspname || '.' || {{end}}p.proname AS compare_name
	, p.proname                  AS function_name
	, p.oid::regprocedure        AS fancy
	, t.typname                  AS return_type
	, pg_get_functiondef(p.oid)  AS definition
FROM pg_proc AS p
JOIN pg_type t ON (p.prorettype = t.oid)
JOIN pg_namespace n ON (n.oid = p.pronamespace)
JOIN pg_language l ON (p.prolang = l.oid AND l.lanname IN ('c','plpgsql', 'sql'))
WHERE true
{{if eq $.DbSchema "*" }}
AND n.nspname NOT LIKE 'pg_%' 
AND n.nspname <> 'information_schema' 
{{else}}
AND n.nspname = '{{$.DbSchema}}'
{{end}};
`
	t := template.New("FunctionSqlTmpl")
	template.Must(t.Parse(query))
	return t
}

func initGrantAttributeSqlTemplate() *template.Template {
	query := `
-- Attribute/Column ACL only
SELECT
  n.nspname AS schema_name
  , {{ if eq $.DbSchema "*" }}n.nspname || '.' || {{ end }}c.relkind || '.' || c.relname || '.' || a.attname AS compare_name
  , CASE c.relkind
    WHEN 'r' THEN 'TABLE'
    WHEN 'v' THEN 'VIEW'
    WHEN 'f' THEN 'FOREIGN TABLE'
    END as type
  , c.relname AS relationship_name
  , a.attname AS attribute_name
  , a.attacl  AS attribute_acl
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
INNER JOIN (SELECT attname, unnest(attacl) AS attacl, attrelid
           FROM pg_catalog.pg_attribute
           WHERE NOT attisdropped AND attacl IS NOT NULL)
      AS a ON (a.attrelid = c.oid)
WHERE c.relkind IN ('r', 'v', 'f')
--AND pg_catalog.pg_table_is_visible(c.oid)
{{ if eq $.DbSchema "*" }}
AND n.nspname NOT LIKE 'pg_%'
AND n.nspname <> 'information_schema'
{{ else }}
AND n.nspname = '{{ $.DbSchema }}'
{{ end }};
`

	t := template.New("GrantAttributeSqlTmpl")
	template.Must(t.Parse(query))
	return t
}

func initGrantRelationshipSqlTemplate() *template.Template {
	query := `
SELECT n.nspname AS schema_name
  , {{ if eq $.DbSchema "*" }}n.nspname || '.' || {{ end }}c.relkind || '.' || c.relname AS compare_name
  , CASE c.relkind
    WHEN 'r' THEN 'TABLE'
    WHEN 'v' THEN 'VIEW'
    WHEN 'S' THEN 'SEQUENCE'
    WHEN 'f' THEN 'FOREIGN TABLE'
    END as type
  , c.relname AS relationship_name
  , unnest(c.relacl) AS relationship_acl
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
WHERE c.relkind IN ('r', 'v', 'S', 'f')
--AND pg_catalog.pg_table_is_visible(c.oid)
{{ if eq $.DbSchema "*" }}
AND n.nspname NOT LIKE 'pg_%'
AND n.nspname <> 'information_schema'
{{ else }}
AND n.nspname = '{{ $.DbSchema }}'
{{ end }};
`

	t := template.New("GrantRelationshipSqlTmpl")
	template.Must(t.Parse(query))
	return t
}

func initIndexSqlTemplate() *template.Template {
	query := `
SELECT {{if eq $.DbSchema "*" }}n.nspname || '.' || {{end}}c.relname || '.' || c2.relname AS compare_name
    , n.nspname AS schema_name
    , c.relname AS table_name
    , c2.relname AS index_name
    , i.indisprimary AS pk
    , i.indisunique AS uq
    , pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) AS index_def
    , pg_catalog.pg_get_constraintdef(con.oid, true) AS constraint_def
    , con.contype AS typ
FROM pg_catalog.pg_index AS i
INNER JOIN pg_catalog.pg_class AS c ON (c.oid = i.indrelid)
INNER JOIN pg_catalog.pg_class AS c2 ON (c2.oid = i.indexrelid)
LEFT OUTER JOIN pg_catalog.pg_constraint con
    ON (con.conrelid = i.indrelid AND con.conindid = i.indexrelid AND con.contype IN ('p','u','x'))
INNER JOIN pg_catalog.pg_namespace AS n ON (c2.relnamespace = n.oid)
WHERE true
{{if eq $.DbSchema "*"}}
AND n.nspname NOT LIKE 'pg_%' 
AND n.nspname <> 'information_schema' 
{{else}}
AND n.nspname = '{{$.DbSchema}}'
{{end}}
`
	t := template.New("IndexSqlTmpl")
	template.Must(t.Parse(query))
	return t
}

func initOwnerSqlTemplate() *template.Template {
	query := `
SELECT n.nspname AS schema_name
    , {{if eq $.DbSchema "*" }}n.nspname || '.' || {{end}}c.relname || '.' || c.relname AS compare_name
    , c.relname AS relationship_name
    , a.rolname AS owner
    , CASE WHEN c.relkind = 'r' THEN 'TABLE' 
        WHEN c.relkind = 'S' THEN 'SEQUENCE' 
        WHEN c.relkind = 'v' THEN 'VIEW' 
        ELSE c.relkind::varchar END AS type
FROM pg_class AS c
INNER JOIN pg_roles AS a ON (a.oid = c.relowner)
INNER JOIN pg_namespace AS n ON (n.oid = c.relnamespace)
WHERE c.relkind IN ('r', 'S', 'v')
{{if eq $.DbSchema "*" }}
AND n.nspname NOT LIKE 'pg_%' 
AND n.nspname <> 'information_schema'
{{else}}
AND n.nspname = '{{$.DbSchema}}'
{{end}}
;`

	t := template.New("OwnerSqlTmpl")
	template.Must(t.Parse(query))
	return t
}

func initSequenceSqlTemplate() *template.Template {
	query := `
SELECT sequence_schema AS schema_name
    , {{if eq $.DbSchema "*" }}sequence_schema || '.' || {{end}}sequence_name AS compare_name
    , sequence_name 
	, data_type
	, start_value
	, minimum_value
	, maximum_value
	, increment
	, cycle_option 
FROM information_schema.sequences
WHERE true
{{if eq $.DbSchema "*" }}
AND sequence_schema NOT LIKE 'pg_%' 
AND sequence_schema <> 'information_schema' 
{{else}}
AND sequence_schema = '{{$.DbSchema}}'
{{end}}
`

	t := template.New("SequenceSqlTmpl")
	template.Must(t.Parse(query))
	return t
}

func initTableSqlTemplate() *template.Template {

	query := `
SELECT table_schema
    , {{if eq $.DbSchema "*" }}table_schema || '.' || {{end}}table_name AS compare_name
	, table_name
    , CASE table_type 
	  WHEN 'BASE TABLE' THEN 'TABLE' 
	  ELSE table_type END AS table_type
    , is_insertable_into
FROM information_schema.tables 
WHERE table_type = 'BASE TABLE'
{{if eq $.DbSchema "*" }}
AND table_schema NOT LIKE 'pg_%' 
AND table_schema <> 'information_schema' 
{{else}}
AND table_schema = '{{$.DbSchema}}'
{{end}}
ORDER BY compare_name;
`
	t := template.New("TableSqlTmpl")
	template.Must(t.Parse(query))
	return t
}

func initTriggerSqlTemplate() *template.Template {
	query := `
SELECT n.nspname AS schema_name
   , {{if eq $.DbSchema "*" }}n.nspname || '.' || {{end}}c.relname || '.' || t.tgname AS compare_name
   , c.relname AS table_name
   , t.tgname AS trigger_name
   , pg_catalog.pg_get_triggerdef(t.oid, true) AS trigger_def
   , t.tgenabled AS enabled
FROM pg_catalog.pg_trigger t
INNER JOIN pg_catalog.pg_class c ON (c.oid = t.tgrelid)
INNER JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
WHERE not t.tgisinternal
{{if eq $.DbSchema "*" }}
AND n.nspname NOT LIKE 'pg_%' 
AND n.nspname <> 'information_schema' 
{{else}}
AND n.nspname = '{{$.DbSchema}}'
{{end}}
`
	t := template.New("TriggerSqlTmpl")
	template.Must(t.Parse(query))
	return t
}
