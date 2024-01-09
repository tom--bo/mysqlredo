#include <fstream>
#include <iostream>

#include "innodb_log.h"
#include "log0files_io.h"
#include "mylog0recv.h"
#include "mtr0log.h"
#include "page0zip.h"
#include "trx0rec.h"
#include "trx0undo.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "ibuf0ibuf.h"
#include "log0test.h"
#include "./mylog0recv.h"

// read file containts to innodb_log.buf
int innodb_log::read_file(std::string filepath) {
    std::ifstream file(filepath, std::ios::binary | std::ios::ate);
    if (!file) {
        std::cerr << "Error: Cannot open file " << filepath << "\n";
        return 1;
    }

    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);

    char *buffer = static_cast<char*>(malloc(size));
    if (buffer == nullptr) {
        std::cerr << "Error: Memory allocation failed\n";
        return 1;
    }

    if (!file.read(buffer, size)) {
        std::cerr << "Error: Cannot read file " << filepath << "\n";
        return 1;
    }

    file_size = size;
    buf = (byte *)buffer;
    std::cout << "Read file Size: " << size << "\n";

    return 0;
}

int innodb_log::deserialize_header() {
    if(!log_file_header_deserialize(this->buf, this->header)) {
        return 1;
    }
    return 0;
}

