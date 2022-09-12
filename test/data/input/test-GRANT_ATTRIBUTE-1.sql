/*
 * Copyright (c) 2017 Jon Carlson. All rights reserved.
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */

-- Available Column Privileges: SELECT, INSERT, UPDATE, REFERENCES

CREATE SCHEMA s1;
CREATE SCHEMA s2;

---------

CREATE TABLE s1.table1 (id integer, name varchar(30));
GRANT SELECT, UPDATE (name) ON s1.table1 TO u2;

-- Drop REFERENCES, Add UPDATE
CREATE TABLE s2.table1 (id integer, name varchar(30));
GRANT SELECT, REFERENCES (name) ON s2.table1 TO u2;

---------

CREATE TABLE s1.table2 (id integer, name varchar(30));
-- u2 has no privileges

-- Drop SELECT on s1.table2
CREATE TABLE s2.table2 (id integer, name varchar(30));
GRANT SELECT (name) ON s2.table2 TO u2;

---------

CREATE TABLE s1.table3 (id integer, name varchar(30));
GRANT SELECT (name) ON s1.table3 TO u2;

-- Add SELECT on s1.table3
CREATE TABLE s2.table3 (id integer, name varchar(30));
-- u2 has no privileges
