#include <mysql.h>
#include <mysqld_error.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <sys/types.h>

#include "client/client_priv_mysqlredo.h"
#include "m_ctype.h"
#include "m_string.h"
#include "my_alloc.h"
#include "my_dbug.h"
#include "my_default.h"
#include "my_inttypes.h"
#include "my_macros.h"
#include "my_sys.h"
#include "mysql/service_mysql_alloc.h"
#include "print_version.h"
#include "typelib.h"
// /* test */ #include "welcome_copyright_notice.h" /* ORACLE_WELCOME_COPYRIGHT_NOTICE */

#include "innodb_log.h"
#include "mylog0recv.h"
#include "dict0dict.h"
#include "mysqlredo.h"

static char *filepath = nullptr;
uint opt_verbose_output = 0;
bool opt_with_header = false;
bool opt_header_only = false;
unsigned long opt_start_lsn = 0;
unsigned long opt_stop_lsn = 0;

static void get_options(int *argc, char ***argv);

static const char *load_default_groups[] = {"mysqlredo", "client", nullptr};

static struct my_option my_long_options[] = {
        {"header", 'h', "Display redo log file's header info.", &opt_header_only, nullptr, nullptr, GET_NO_ARG, NO_ARG, 0, 0, 0, nullptr, 0, nullptr},
        {"header-only", 'H', "Display redo log file's header info only.", &opt_with_header, nullptr, nullptr, GET_NO_ARG, NO_ARG, 0, 0, 0, nullptr, 0, nullptr},
        {"help", '?', "Display this help and exit.", nullptr, nullptr, nullptr, GET_NO_ARG, NO_ARG, 0, 0, 0, nullptr, 0, nullptr},
        {"start-lsn", 'b', "Set start lsn to print", &opt_start_lsn, &opt_start_lsn, nullptr, GET_ULONG, OPT_ARG, 0, 0, ULONG_MAX, nullptr, 0, nullptr},
        {"stop-lsn", 'e', "Set stop lsn to print", &opt_stop_lsn, &opt_stop_lsn, nullptr, GET_ULL, OPT_ARG, LONG_LONG_MAX, 0, ULONG_MAX, nullptr, 0, nullptr},
        {"verbose", 'v', "More verbose output; (you can use this multiple times to get even more verbose output.)",
                nullptr, nullptr, nullptr, GET_NO_ARG, NO_ARG, 0, 0, 0, nullptr, 0, nullptr},
        {"version", 'V', "Output version information and exit.", nullptr, nullptr, nullptr, GET_NO_ARG, NO_ARG, 0, 0, 0, nullptr, 0, nullptr},
        {nullptr, 0, nullptr, nullptr, nullptr, nullptr, GET_NO_ARG, NO_ARG, 0, 0, 0, nullptr, 0, nullptr}
};

static void usage(void) {
    print_version();
    // /* test */ puts(ORACLE_WELCOME_COPYRIGHT_NOTICE("2000"));
    puts(
            "Shows the redo log contents.\n");
    printf("Usage: %s <filepath>\n", my_progname);

    // /* test */ print_defaults("my", load_default_groups);
    my_print_help(my_long_options);
    my_print_variables(my_long_options);
}

extern "C" {
static bool get_one_option(int optid, const struct my_option *opt,
                           char *argument) {
    switch (optid) {
        case 'b':
            opt_start_lsn = *(unsigned long long*)(opt->value);
            break;
        case 'e':
            opt_stop_lsn = *(unsigned long long*)(opt->value);
            break;
        case 'h':
            opt_with_header = true;
            break;
        case 'H':
            opt_header_only = true;
            break;
        case 'v':
            opt_verbose_output++;
            break;
        case 'V':
            print_version();
            exit(0);
        case '?':
            usage();
            exit(0);
    }
    return false;
}
}  // extern "C"

static void get_options(int *argc, char ***argv) {
    int ho_error;

    if ((ho_error = handle_options(argc, argv, my_long_options, get_one_option)))
        exit(ho_error);
    return;
}

int main(int argc, char **argv) {
    MY_INIT(argv[0]);
    opt_verbose_output = 0;

    my_getopt_use_args_separator = true;
    MEM_ROOT alloc{PSI_NOT_INSTRUMENTED, 512};
    if (load_defaults("my", load_default_groups, &argc, &argv, &alloc)) exit(1);
    my_getopt_use_args_separator = false;

    Log_checkpoint_header chpt_header;
    lsn_t checkpoint_lsn1 = 0, checkpoint_lsn2 = 0;
    lsn_t max_chpt_lsn = 0, start_chpt_lsn = 0;
    uint64_t start_chpt_offset, first_block_offset;

    get_options(&argc, &argv);

    /* Need to call ut_crc32 functions in  log_file_header_deserialize() */
    ut_crc32_init();

    if (argc > 1) {
        fprintf(stderr, "%s: Too many arguments\n", my_progname);
        exit(1);
    } else if (argc != 1) {
        fprintf(stderr, "%s: redo-log file path is not specified\n", my_progname);
        exit(1);
    }

    filepath = argv[0];
    if(opt_verbose_output > 1) {
        std::cout << "filepath: " << filepath << std::endl;
    }

    innodb_log *iblog = new(innodb_log);
    int ret = iblog->read_file(filepath);
    if(opt_stop_lsn == 0) { /* Need to set default manually because this value is limited to longlong */
        opt_stop_lsn = ULONG_MAX;
    }
    if(ret) {
        std::cerr <<  "Error: Failed to read redolog file." << std::endl;
    }

    /* Deserialize header block */
    ret = iblog->deserialize_header();
    if(ret) {
        std::cerr <<  "Error: Failed to parse header." << std::endl;
    }

    /* Print header block info */
    if(opt_with_header || opt_header_only) {
      std::cout <<  "-- Header block" << std::endl;
      std::cout << "filesize: " << iblog->file_size << std::endl;
      std::cout << "m_format: " << iblog->header.m_format << std::endl;
      std::cout << "m_start_lsn: " << iblog->header.m_start_lsn << std::endl;
      std::cout << "m_creater_name: " << iblog->header.m_creator_name << std::endl;
    }

    /* Deserialize checkpoint block 1 */
    if (!log_checkpoint_header_deserialize(iblog->buf + OS_FILE_LOG_BLOCK_SIZE, chpt_header)) {
        std::cout <<  "Error: redo log corrupted." << std::endl;
    }
    checkpoint_lsn1 = chpt_header.m_checkpoint_lsn;

    /* Deserialize checkpoint block 2 */
    if (!log_checkpoint_header_deserialize(iblog->buf + OS_FILE_LOG_BLOCK_SIZE*3, chpt_header)) {
        std::cout <<  "Error: redo log corrupted." << std::endl;
    }
    checkpoint_lsn2 = chpt_header.m_checkpoint_lsn;

    // max_checkpoint_lsn
    if(checkpoint_lsn1 > checkpoint_lsn2) {
      max_chpt_lsn = checkpoint_lsn1;
    } else {
      max_chpt_lsn = checkpoint_lsn2;
    }

    /* Calculate start_lsn offset */
    first_block_offset = iblog->get_offset(ut_uint64_align_down(max_chpt_lsn, OS_FILE_LOG_BLOCK_SIZE), iblog->header.m_start_lsn);

    /* Print checkpoint blocks info */
    if(opt_with_header || opt_header_only) {
      std::cout <<  "-- Checkpoint blocks" << std::endl;
      std::cout << "checkpoint 1: " << checkpoint_lsn1 << std::endl;
      std::cout << "checkpoint 2: " << checkpoint_lsn2 << std::endl;
        std::cout << "first_block_offset: " << first_block_offset << std::endl;
    }
    if(opt_header_only) {
      return 0;
    }

    /* overwrite first_block_offset */
    if(opt_start_lsn) {
        first_block_offset = iblog->get_offset(ut_uint64_align_down(opt_start_lsn, OS_FILE_LOG_BLOCK_SIZE), iblog->header.m_start_lsn);
    }

    /* Read the block including checkpoint lsn */
    Log_data_block_header block_header;
    log_checksum_algorithm_ptr.store(log_block_calc_checksum_crc32);
    log_data_block_header_deserialize(iblog->buf+first_block_offset, block_header);

    /* Initialize some structures */
    os_event_global_init();
    sync_check_init(1);
    recv_sys_create(); /* create recv_sys */
    recv_sys_init(); /* initialize recv_sys */
    dict_persist_init();

    if(opt_verbose_output > 1) {
      std::cout << "recv_sys->parse_start_lsn: " << ut_uint64_align_down(max_chpt_lsn, OS_FILE_LOG_BLOCK_SIZE) + block_header.m_first_rec_group << std::endl;
    }
    recv_sys->stop_lsn = opt_stop_lsn;

    std::cout <<  "-- Normal blocks" << std::endl;

    // initialize recv_sys->buf related by myself
    recv_sys->buf_len = iblog->file_size;
    recv_sys->len = iblog->file_size;
    recv_sys->buf = static_cast<byte *>(
            ut::malloc_withkey(UT_NEW_THIS_FILE_PSI_KEY, recv_sys->buf_len));

    srv_log_buffer_size = iblog->file_size;
    // modify recv_recovery_begin
    if(opt_start_lsn != 0) {
        start_chpt_lsn = opt_start_lsn;
        start_chpt_offset = iblog->get_offset(start_chpt_lsn, iblog->header.m_start_lsn);
    } else {
        start_chpt_lsn = max_chpt_lsn;
        start_chpt_offset = iblog->get_offset(max_chpt_lsn, iblog->header.m_start_lsn);
    }
    if(opt_verbose_output) {
        std::cout << "start parsing from lsn of " << start_chpt_lsn << "(" << (start_chpt_lsn - start_chpt_lsn % 512) <<  ")";
        if(opt_stop_lsn) {
            std::cout << " to " << opt_stop_lsn;
        }
        std::cout << std::endl;
    }

    bool finished = my_parse_begin(iblog->buf, start_chpt_lsn, start_chpt_lsn + iblog->file_size - start_chpt_offset, first_block_offset);
    if(!finished) {
        std::cerr << "Parse finished in the middle of file."<< std::endl;
    }
    std::cout << "Parse END" << std::endl;
    std::cout << "scanned_lsn: " << recv_sys->scanned_lsn << ", recovered_lsn: " << recv_sys->recovered_lsn << std::endl;
    std::cout << std::flush;

    return 0;
}
