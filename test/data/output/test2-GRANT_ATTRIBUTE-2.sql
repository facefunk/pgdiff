REVOKE SELECT (id) ON s1.table1 FROM u2; -- Drop
GRANT UPDATE (name) ON s2.table1 TO u2; -- Change
REVOKE REFERENCES (name) ON s2.table1 FROM u2; -- Change
REVOKE UPDATE (name) ON s2.table3 FROM u2; -- Drop
GRANT UPDATE (name) ON s2.table4 TO u2; -- Add
