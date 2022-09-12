GRANT UPDATE (name) ON s2.table1 TO u2; -- Change
REVOKE REFERENCES (name) ON s2.table1 FROM u2; -- Change
REVOKE SELECT (name) ON s2.table2 FROM u2; -- Drop
GRANT SELECT (name) ON s2.table3 TO u2; -- Add
