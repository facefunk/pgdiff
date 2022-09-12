/*
 * Copyright (c) 2017 Jon Carlson. All rights reserved.
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */

CREATE SCHEMA s3;
CREATE TABLE s3.table12 (
    ids       integer[],
    bigids    bigint[],
    something text[][] -- dimensions don't seem to matter, so ignore them
);
CREATE SCHEMA s4;
CREATE TABLE s4.table12 ( -- add ids column
    bigids    integer[], -- change bigids to int8[]
    something text[]     -- no change
);
