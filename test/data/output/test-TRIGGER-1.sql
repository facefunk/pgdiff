CREATE TRIGGER trigger1 AFTER INSERT ON s2.table1 FOR EACH ROW EXECUTE FUNCTION s1.validate1();
DROP TRIGGER trigger2 ON s2.table1;
CREATE TRIGGER trigger2 AFTER INSERT ON s2.table1 FOR EACH ROW EXECUTE FUNCTION s1.validate1();
DROP TRIGGER trigger3 ON s2.table1;
