GRANT INSERT, UPDATE ON s1.table1 TO u2; -- Change
REVOKE SELECT ON s1.table1 FROM u2; -- Change
REVOKE UPDATE ON s2.table3 FROM u2; -- Drop
