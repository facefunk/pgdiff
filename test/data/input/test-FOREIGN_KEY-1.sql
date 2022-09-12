/*
 * Copyright (c) 2017 Jon Carlson. All rights reserved.
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */

CREATE SCHEMA s1;
CREATE TABLE s1.table1 (
    id integer PRIMARY KEY
);
CREATE TABLE s1.table2 (
    id integer PRIMARY KEY,
    table1_id integer REFERENCES s1.table1(id)
);
CREATE TABLE s1.table3 (
    id integer,
    table2_id integer
);

CREATE SCHEMA s2;
CREATE TABLE s2.table1 (
    id integer PRIMARY KEY
);
CREATE TABLE s2.table2 (
    id integer PRIMARY KEY,
    table1_id integer
);
CREATE TABLE s2.table3 (
    id integer,
    table2_id integer REFERENCES s2.table2(id) -- This will be deleted
);
