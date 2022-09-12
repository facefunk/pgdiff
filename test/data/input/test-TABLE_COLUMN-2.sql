/*
 * Copyright (c) 2017 Jon Carlson. All rights reserved.
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */

CREATE SCHEMA s1;
CREATE TABLE s1.table9 (
    id integer,
    name varchar(40)
);
CREATE TABLE s1.table10 ();
CREATE TABLE s1.table11 (dropme integer);

CREATE SCHEMA s2;
CREATE TABLE s2.table9 (  -- Add name column
    id integer
);
CREATE TABLE s2.table10 (id integer);  -- change id to bigint
CREATE TABLE s2.table11 (id integer); -- drop id column
CREATE OR REPLACE VIEW s1.view1 AS
    SELECT *
    FROM s1.table10;
