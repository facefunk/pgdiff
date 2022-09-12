/*
 * Copyright (c) 2017 Jon Carlson. All rights reserved.
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */

-- Schema s1
CREATE SCHEMA s1;
CREATE TABLE s1.table1 (id integer);
CREATE OR REPLACE FUNCTION s1.validate1() RETURNS TRIGGER AS $$
    BEGIN
        SELECT 1; -- look like we are doing something ;^>
    END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trigger1 AFTER INSERT ON s1.table1 FOR EACH ROW EXECUTE PROCEDURE s1.validate1();
CREATE TRIGGER trigger2 AFTER INSERT ON s1.table1 FOR EACH ROW EXECUTE PROCEDURE s1.validate1();

-- Schema s2
CREATE SCHEMA s2;
CREATE TABLE s2.table1 (id integer);
CREATE TRIGGER trigger2 BEFORE INSERT ON s2.table1 FOR EACH ROW EXECUTE PROCEDURE s1.validate1();
CREATE TRIGGER trigger3 AFTER INSERT ON s2.table1 FOR EACH ROW EXECUTE PROCEDURE s1.validate1();
