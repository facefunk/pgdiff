/*
 * Copyright (c) 2017 Jon Carlson. All rights reserved.
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */

CREATE SCHEMA s1;
CREATE TABLE s1.table1 (
    id   integer PRIMARY KEY,
    name varchar(32),
    url  varchar(200)
);
CREATE INDEX ON s1.table1(name);
CREATE INDEX ON s1.table1(url);

CREATE SCHEMA s2;
CREATE TABLE s2.table1 (
    id   integer PRIMARY KEY,
    name varchar(32),
    url  varchar(200)
);
