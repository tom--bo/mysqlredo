# mysqlredo

`mysqlredo` parse innodb redo log and dump redo log records.
This is an experimental program to help me understand innodb redo logs.
I do not intend to make this a production ready implementation.

Currently, this tool modify and utilize innodb original my_recv_scan_log_recs().  
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
# boost 1.77.0 is bundled
cmake .. -DWITH_BOOST=../boost_1_77_0
make mysqlredo
```

## How to run

```shell
$ mysqlredo --start-lsn=NNN --stop-lsn=MMM --file-path=/path/to/#ib_redoN
```

`--file-path`(or `-f`) option is required. 

## sample

### Query

For example, I want to see what redo log record one insert query generagtes.

```sql
mysql> select * from t1;
+----+
| id |
+----+
|  1 |
|  2 |
...
|  5 |
+----+
5 rows in set (0.00 sec)

-- check the current lsn
mysql> show global status like '%lsn%';
+-------------------------------------+----------+
| Variable_name                       | Value    |
+-------------------------------------+----------+
| Innodb_redo_log_checkpoint_lsn      | 19607762 |
| Innodb_redo_log_current_lsn         | 19607762 |
| Innodb_redo_log_flushed_to_disk_lsn | 19607762 |
+-------------------------------------+----------+
3 rows in set (0.01 sec)

-- insert a row (target query)
mysql> insert into t1 values(10);
Query OK, 1 row affected (0.01 sec)

-- check the current lsn
mysql> show global status like '%lsn%';
+-------------------------------------+----------+
| Variable_name                       | Value    |
+-------------------------------------+----------+
| Innodb_redo_log_checkpoint_lsn      | 19607874 |
| Innodb_redo_log_current_lsn         | 19607874 |
| Innodb_redo_log_flushed_to_disk_lsn | 19607874 |
+-------------------------------------+----------+
3 rows in set (0.00 sec)
```

Let's read the redo log by `mysqlredo` command!
You need to specify the redolog file and start/stop lsn of it.

```shell
$ mysqlredo  --start-lsn=19607762 --stop-lsn=19607874 --file-path=/mysql_remote/redo/#innodb_redo/#ib_redo18 -v --header
-- Header block
filesize: 1048576
m_format: 6
m_start_lsn: 18845696
m_creater_name: MySQL 8.0.32
-- Checkpoint blocks
checkpoint 1: 19607874
checkpoint 2: 19608118
first_block_offset: 764416
-- Normal blocks
start parsing from lsn of 19607762(19607552) to 19607874
- recv_multi_rec(), lsn: 19607622
  type: MLOG_1BYTES, space: 4294967294, page_no: 0, offset: 457, val: 171,
  type: MLOG_1BYTES, space: 4294967294, page_no: 0, offset: 457, val: 171,
  type: MLOG_4BYTES, space: 4294967294, page_no: 2, offset: 8698, val: 24,
  TYPE: MLOG_MULTI_REC_END
- recv_single_rec(), lsn: 19607646
  type: MLOG_REC_UPDATE_IN_PLACE, space: 4294967294, page_no: 5, index_log_ver: 1, index_flag: 1, cols: 5, inst_cols: 0, uniq_cols: 1, len[0]: 32776, len[1]: 32774, len[2]: 32775, len[3]: 32776, len[4]: 65535, pos: 1, roll_ptr: 0, trx_id0
- recv_single_rec(), lsn: 19607699
  type: MLOG_REC_UPDATE_IN_PLACE, space: 4294967294, page_no: 5, index_log_ver: 1, index_flag: 1, cols: 5, inst_cols: 0, uniq_cols: 1, len[0]: 32776, len[1]: 32774, len[2]: 32775, len[3]: 32776, len[4]: 65535, pos: 1, roll_ptr: 0, trx_id0
- recv_single_rec(), lsn: 19607752
  type: MLOG_8BYTES, space: 0, page_no: 5, offset: 15908, dval: 2321,
- recv_multi_rec(), lsn: 19607762
  type: MLOG_UNDO_HDR_REUSE, space: 4294967279, page_no: 264
  type: MLOG_2BYTES, space: 4294967279, page_no: 264, offset: 40, val: 272,
  type: MLOG_2BYTES, space: 4294967279, page_no: 264, offset: 42, val: 272,
  type: MLOG_2BYTES, space: 4294967279, page_no: 264, offset: 104, val: 272,
  TYPE: MLOG_MULTI_REC_END
- recv_single_rec(), lsn: 19607800
  type: MLOG_UNDO_INSERT, space: 4294967279, page_no: 264, len: 9
- recv_single_rec(), lsn: 19607816
  type: MLOG_REC_INSERT, space: 2, page_no: 4, index_log_ver: 1, index_flag: 1, cols: 3, inst_cols: 0, uniq_cols: 1, len[0]: 32772, len[1]: 32774, len[2]: 32775, offset: 213, end_seg_len: 44
- recv_single_rec(), lsn: 19607856
  type: MLOG_2BYTES, space: 4294967279, page_no: 264, offset: 56, val: 2,
- recv_single_rec(), lsn: 19607864
  type: MLOG_8BYTES, space: 0, page_no: 5, offset: 15908, dval: 2322,
- recv_multi_rec(), lsn: 19607874
  type: MLOG_4BYTES, space: 4294967279, page_no: 4, offset: 50, val: 4294967295,
  type: MLOG_2BYTES, space: 4294967279, page_no: 4, offset: 54, val: 0,
  type: MLOG_4BYTES, space: 4294967279, page_no: 4, offset: 56, val: 4294967295,
  type: MLOG_2BYTES, space: 4294967279, page_no: 4, offset: 60, val: 0,
  type: MLOG_4BYTES, space: 4294967279, page_no: 4, offset: 46, val: 0,
  TYPE: MLOG_MULTI_REC_END
Parse END
scanned_lsn: 19608118, recovered_lsn: 19607912
```


# Copyright (original README contents)

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
