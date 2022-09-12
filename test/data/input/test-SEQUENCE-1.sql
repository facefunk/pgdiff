/*
 * Copyright (c) 2017 Jon Carlson. All rights reserved.
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */

CREATE SCHEMA s1;
CREATE TABLE s1.table1 (id integer PRIMARY KEY);  -- just for kicks
CREATE SEQUENCE s1.sequence_1
    INCREMENT BY 2
    MINVALUE 1024
    MAXVALUE 99998
    START WITH 2048
    NO CYCLE
    OWNED BY s1.table1.id;
CREATE SEQUENCE s1.sequence_2;

CREATE SCHEMA s2;
CREATE SEQUENCE s2.sequence_2;
CREATE SEQUENCE s2.sequence_3;
