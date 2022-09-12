/*
 * Copyright (c) 2017 Jon Carlson. All rights reserved.
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */

-- schema s1
CREATE SCHEMA s1;
CREATE TABLE s1.table1();
GRANT CREATE ON SCHEMA s1 TO u2;
ALTER TABLE s1.table1 OWNER TO u2;
CREATE TABLE s1.table2();
CREATE TABLE s1.table3();
ALTER TABLE s1.table3 OWNER TO u2;

-- schema s2
CREATE SCHEMA s2;
CREATE TABLE s2.table1();
CREATE TABLE s2.table2();
CREATE TABLE s2.table3();
