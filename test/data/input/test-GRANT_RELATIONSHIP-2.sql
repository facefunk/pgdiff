/*
* Copyright (c) 2017 Jon Carlson. All rights reserved.
* Use of this source code is governed by the MIT license that can be found in the LICENSE file.
*/

CREATE SCHEMA s1;
CREATE TABLE s1.table1 (id integer);
GRANT SELECT ON s1.table1 TO u2;
CREATE TABLE s1.table2 (id integer);
GRANT SELECT ON s1.table2 TO u2;
CREATE TABLE s1.table3 (id integer);
GRANT SELECT ON s1.table3 TO u2;

CREATE SCHEMA s2;
CREATE TABLE s2.table1 (id integer);
GRANT SELECT ON s2.table1 TO u2;
CREATE TABLE s2.table2 (id integer);
GRANT SELECT ON s2.table2 TO u2;
CREATE TABLE s2.table3 (id integer);
GRANT UPDATE ON s2.table3 TO u2;  -- revoke
