/*
 * Copyright (c) 2017 Jon Carlson. All rights reserved.
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */

DROP DATABASE IF EXISTS db1;
DROP DATABASE IF EXISTS db2;

DROP USER IF EXISTS u1;
CREATE USER u1 PASSWORD 'asdf' INHERIT;
GRANT u1 to pgdiff_parent;

CREATE DATABASE db1 WITH OWNER = u1 TEMPLATE = template0;
CREATE DATABASE db2 WITH OWNER = u1 TEMPLATE = template0;

DROP USER IF EXISTS u2;
CREATE USER u2 PASSWORD 'asdf' INHERIT;
GRANT u2 TO u1;
