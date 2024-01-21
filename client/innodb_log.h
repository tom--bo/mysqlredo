#ifndef MYSQL_INNODB_LOG_H
#define MYSQL_INNODB_LOG_H


/* Note that inside MySQL 'byte' is defined as char on Linux! */
#include <string>
#include "log0types.h"
#include "log0files_io.h"

extern uint opt_verbose_output;

class innodb_log {
public:
    byte *buf;
    uint64_t file_size;
    Log_file_header header{};

    int read_file(std::string filepath);
    int deserialize_header();
    uint64_t get_offset(lsn_t max_lsn, lsn_t start_lsn) {
        return uint64_t(Log_file::offset(max_lsn, start_lsn));
    }
};

class NullStream : public std::streambuf {
public:
  int overflow(int c) {
    return c;
  }
};

#endif //MYSQL_INNODB_LOG_H
