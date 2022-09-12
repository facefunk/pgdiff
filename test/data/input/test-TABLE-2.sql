/*
 * Copyright (c) 2017 Jon Carlson. All rights reserved.
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */

CREATE SCHEMA s1;
CREATE TABLE s1.table9 (id integer);
-- table10 will be added in db2

CREATE SCHEMA s2;
CREATE TABLE s2.table10 (id integer);
CREATE TABLE s2.table11 (id integer);
CREATE TABLE s2.table12 (id integer); -- will be dropped (not in db1)

CREATE SCHEMA s3;
