DROP INDEX s1.table1_url_idx;
CREATE INDEX table1_url_idx ON s2.table1 USING btree (url);
