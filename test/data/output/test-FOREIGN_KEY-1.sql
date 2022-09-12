ALTER TABLE s2.table2 ADD CONSTRAINT table2_table1_id_fkey FOREIGN KEY (table1_id) REFERENCES s1.table1(id);
ALTER TABLE s2.table3 DROP CONSTRAINT table3_table2_id_fkey; -- FOREIGN KEY (table2_id) REFERENCES s2.table2(id)
