ALTER TABLE s1.table2 ADD CONSTRAINT table2_table1_id_fkey FOREIGN KEY (table1_id) REFERENCES s1.table1(id);
ALTER TABLE s2.table2 DROP CONSTRAINT table2_table1_id_fkey; -- FOREIGN KEY (table1_id) REFERENCES s2.table1(id)
