/*
 * Copyright (c) 2017 Jon Carlson. All rights reserved.
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */

CREATE SCHEMA s1;
CREATE SCHEMA s2;
---------
CREATE TABLE s1.table1 (id integer, name varchar(30));
GRANT SELECT, UPDATE (name) ON s1.table1 TO u2;
GRANT SELECT (id) ON s1.table1 TO u2;

CREATE TABLE s2.table1 (id integer, name varchar(30));
GRANT REFERENCES (name) ON s2.table1 TO u2;

CREATE TABLE s2.table3 (id integer, name varchar(30));
GRANT UPDATE (name) ON s2.table3 TO u2;

CREATE TABLE s2.table4 (id integer, name varchar(30));
