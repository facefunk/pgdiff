/*
 * Copyright (c) 2017 Jon Carlson. All rights reserved.
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */

CREATE SCHEMA s1;
CREATE TABLE s1.table9 (
   id integer,
   name varchar(50)
);
CREATE TABLE s1.table10 (id bigint);
CREATE TABLE s1.table11 ();

CREATE SCHEMA s2;
CREATE TABLE s2.table9 (  -- Add name column
    id integer
);
CREATE TABLE s2.table10 (id integer); -- change id to bigint
CREATE TABLE s2.table11 (id integer); -- drop id column