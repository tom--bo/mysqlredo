#ifndef MYSQL_MYLOG0RECV_H
#define MYSQL_MYLOG0RECV_H

bool my_parse_begin(byte *buf, const lsn_t checkpoint_lsn, lsn_t end_lsn, uint64_t first_block_offset);

#endif //MYSQL_MYLOG0RECV_H
