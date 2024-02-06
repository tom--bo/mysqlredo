# mysqlredo

This is a prototype of `mysqlredo` command.  
`mysqlredo` parse innodb redo log and dump redo log records.

Currently, this tool modify and utilize innodb original recv_scan_log_recs().
`mysqlredo` is a tool to survey or learn innodb redo log.
I tested only MySQL 8.0.32.


## How to build

You can compile just like mysql-client tools.
cf) https://dev.mysql.com/doc/refman/8.0/en/source-installation.html

```sh
git clone https://github.com/tom--bo/mysqlredo
cd mysqlredo
mkdir bld
cd bld
cmake .. -DWITH_BOOST=../boost_1_77_0
make mysqlredo
```

## How to run

`mysqlredo --start-lsn=NNN --stop-lsn=MMM /path/to/#ib_redoN`



# Copyright (original readme contents)

Copyright (c) 2000, 2023, Oracle and/or its affiliates.

This is a release of MySQL, an SQL database server.

License information can be found in the LICENSE file.

In test packages where this file is renamed README-test, the license
file is renamed LICENSE-test.

This distribution may include materials developed by third parties.
For license and attribution notices for these materials,
please refer to the LICENSE file.

For further information on MySQL or additional documentation, visit
  http://dev.mysql.com/doc/

For additional downloads and the source of MySQL, visit
  http://dev.mysql.com/downloads/

MySQL is brought to you by the MySQL team at Oracle.
