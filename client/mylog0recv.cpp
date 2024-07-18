#include "log0files_io.h"
#include "mylog0recv.h"
#include "mymtr0log.h"
#include "page0zip.h"
#include "trx0rec.h"
#include "trx0undo.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "ibuf0ibuf.h"
#include "log0test.h"
#include "log0types.h"
#include "mysqlredo.h"

// ====================== copied from log0recv.cc

/** Check if redo log is for encryption information.
@param[in]      page_no         Page number
@param[in]      space_id        Tablespace identifier
@param[in]      start           Redo log record body
@param[in]      end             End of buffer
@return true if encryption information. */
static inline bool check_encryption(page_no_t page_no, space_id_t space_id,
                                    const byte *start, const byte *end) {
  /* Only page zero contains encryption metadata. */
  if (page_no != 0 || fsp_is_system_or_temp_tablespace(space_id) ||
      end < start + 4) {
    return false;
  }

  bool found = false;

  const page_size_t &page_size = fil_space_get_page_size(space_id, &found);

  if (!found) {
    return false;
  }

  auto encryption_offset = fsp_header_get_encryption_offset(page_size);
  auto offset = mach_read_from_2(start);

  /* Encryption offset at page 0 is the only way we can identify encryption
  information as of today. Ideally we should have a separate redo type. */
  if (offset == encryption_offset) {
    auto len = mach_read_from_2(start + 2);
    ut_ad(len == Encryption::INFO_SIZE);

    if (len != Encryption::INFO_SIZE) {
      /* purecov: begin inspected */
      ib::warn(ER_IB_WRN_ENCRYPTION_INFO_SIZE_MISMATCH, size_t{len},
               Encryption::INFO_SIZE);
      return false;
      /* purecov: end */
    }
    return true;
  }

  return false;
}


/** Try to parse a single log record body and also applies it if
specified.
@param[in]      type            Redo log entry type
@param[in]      ptr             Redo log record body
@param[in]      end_ptr         End of buffer
@param[in]      space_id        Tablespace identifier
@param[in]      page_no         Page number
@param[in,out]  block           Buffer block, or nullptr if
                                a page log record should not be applied
                                or if it is a MLOG_FILE_ operation
@param[in,out]  mtr             Mini-transaction, or nullptr if
                                a page log record should not be applied
@param[in]      parsed_bytes    Number of bytes parsed so far
@param[in]      start_lsn       lsn for REDO record
@return log record end, nullptr if not a complete record */
static byte *recv_parse_or_apply_log_rec_body(
    mlog_id_t type, byte *ptr, byte *end_ptr, space_id_t space_id,
    page_no_t page_no, buf_block_t *block, mtr_t *mtr, ulint parsed_bytes,
    lsn_t start_lsn) {
  bool applying_redo = (block != nullptr);

  switch (type) {
#ifndef UNIV_HOTBACKUP
    case MLOG_FILE_DELETE:
      std::cout << "  type: MLOG_FILE_DELETE, space: " << space_id << ", page_no: " << page_no;
      return fil_tablespace_redo_delete(
          ptr, end_ptr, page_id_t(space_id, page_no), parsed_bytes,
          true);

    case MLOG_FILE_CREATE:
      std::cout << "  type: MLOG_FILE_CREATE, space: " << space_id << ", page_no: " << page_no;

      return fil_tablespace_redo_create(
          ptr, end_ptr, page_id_t(space_id, page_no), parsed_bytes,
          true);

    case MLOG_FILE_RENAME:
      std::cout << "  type: MLOG_FILE_RENAME, space: " << space_id << ", page_no: " << page_no;

      return fil_tablespace_redo_rename(
          ptr, end_ptr, page_id_t(space_id, page_no), parsed_bytes,
          true);

    case MLOG_FILE_EXTEND:
      std::cout << "  type: MLOG_FILE_EXTEND, space: " << space_id << ", page_no: " << page_no;

      return fil_tablespace_redo_extend(
          ptr, end_ptr, page_id_t(space_id, page_no), parsed_bytes,
          true);
#else  /* !UNIV_HOTBACKUP */
      // Mysqlbackup does not execute file operations. It cares for all
      // files to be at their final places when it applies the redo log.
      // The exception is the restore of an incremental_with_redo_log_only
      // backup.
    case MLOG_FILE_DELETE:

      return fil_tablespace_redo_delete(
          ptr, end_ptr, page_id_t(space_id, page_no), parsed_bytes,
          !recv_sys->apply_file_operations);

    case MLOG_FILE_CREATE:

      return fil_tablespace_redo_create(
          ptr, end_ptr, page_id_t(space_id, page_no), parsed_bytes,
          !recv_sys->apply_file_operations);

    case MLOG_FILE_RENAME:

      return fil_tablespace_redo_rename(
          ptr, end_ptr, page_id_t(space_id, page_no), parsed_bytes,
          !recv_sys->apply_file_operations);

    case MLOG_FILE_EXTEND:

      return fil_tablespace_redo_extend(
          ptr, end_ptr, page_id_t(space_id, page_no), parsed_bytes,
          !recv_sys->apply_file_operations);
#endif /* !UNIV_HOTBACKUP */

    case MLOG_INDEX_LOAD:
      std::cout << "  type: MLOG_INDEX_LOAD, space: " << space_id << ", page_no: " << page_no;
#ifdef UNIV_HOTBACKUP
      // While scanning redo logs during a backup operation a
      // MLOG_INDEX_LOAD type redo log record indicates, that a DDL
      // (create index, alter table...) is performed with
      // 'algorithm=inplace'. The affected tablespace must be re-copied
      // in the backup lock phase. Record it in the index_load_list.
      if (!recv_recovery_on) {
        index_load_list.emplace_back(
            std::pair<space_id_t, lsn_t>(space_id, recv_sys->recovered_lsn));
      }
#endif /* UNIV_HOTBACKUP */
      if (end_ptr < ptr + 8) {
        return nullptr;
      }
      std::cout << ", (dummy 8bytes)";

      return ptr + 8;

    case MLOG_WRITE_STRING:
      std::cout << "  type: MLOG_WRITE_STRING, space: " << space_id << ", page_no: " << page_no;

#ifdef UNIV_HOTBACKUP
      if (recv_recovery_on && meb_is_space_loaded(space_id)) {
#endif /* UNIV_HOTBACKUP */
        /* For encrypted tablespace, we need to get the encryption key
        information before the page 0 is recovered. Otherwise, redo will not
        find the key to decrypt the data pages. */
        if (page_no == 0 && !applying_redo &&
            !fsp_is_system_or_temp_tablespace(space_id) &&
            /* For cloned db header page has the encryption information. */
            !recv_sys->is_cloned_db) {
          ut_ad(LSN_MAX != start_lsn);
          return fil_tablespace_redo_encryption(ptr, end_ptr, space_id,
                                                start_lsn);
        }
#ifdef UNIV_HOTBACKUP
      }
#endif /* UNIV_HOTBACKUP */

      break;

    default:
      break;
  }

  page_t *page;
  page_zip_des_t *page_zip;
  dict_index_t *index = nullptr;

#ifdef UNIV_DEBUG
  ulint page_type;
#endif /* UNIV_DEBUG */

#if defined(UNIV_HOTBACKUP) && defined(UNIV_DEBUG)
  ib::trace_3() << "recv_parse_or_apply_log_rec_body: type "
                << get_mlog_string(type) << " space_id " << space_id
                << " page_nr " << page_no << " ptr "
                << static_cast<const void *>(ptr) << " end_ptr "
                << static_cast<const void *>(end_ptr) << " block "
                << static_cast<const void *>(block) << " mtr "
                << static_cast<const void *>(mtr);
#endif /* UNIV_HOTBACKUP && UNIV_DEBUG */

  if (applying_redo) {
    /* Applying a page log record. */
    ut_ad(mtr != nullptr);

    page = block->frame;
    page_zip = buf_block_get_page_zip(block);

    ut_d(page_type = fil_page_get_type(page));
#if defined(UNIV_HOTBACKUP) && defined(UNIV_DEBUG)
    if (page_type == 0) {
      meb_print_page_header(page);
    }
#endif /* UNIV_HOTBACKUP && UNIV_DEBUG */

  } else {
    /* Parsing a page log record. */
    ut_ad(mtr == nullptr);
    page = nullptr;
    page_zip = nullptr;

    ut_d(page_type = FIL_PAGE_TYPE_ALLOCATED);
  }

  const byte *old_ptr = ptr;

  switch (type) {
#ifdef UNIV_LOG_LSN_DEBUG
    case MLOG_LSN:
      /* The LSN is checked in recv_parse_log_rec(). */
      break;
#endif /* UNIV_LOG_LSN_DEBUG */
    case MLOG_4BYTES:
      std::cout << "  type: MLOG_4BYTES, space: " << space_id << ", page_no: " << page_no;

      ut_ad(page == nullptr || end_ptr > ptr + 2);

      /* Most FSP flags can only be changed by CREATE or ALTER with
      ALGORITHM=COPY, so they do not change once the file
      is created. The SDI flag is the only one that can be
      changed by a recoverable transaction. So if there is
      change in FSP flags, update the in-memory space structure
      (fil_space_t) */

      if (page != nullptr && page_no == 0 &&
          mach_read_from_2(ptr) == FSP_HEADER_OFFSET + FSP_SPACE_FLAGS) {
        ptr = mlog_parse_nbytes(MLOG_4BYTES, ptr, end_ptr, page, page_zip);

        /* When applying log, we have complete records.
        They can be incomplete (ptr=nullptr) only during
        scanning (page==nullptr) */

        ut_ad(ptr != nullptr);

        fil_space_t *space = fil_space_acquire(space_id);

        ut_ad(space != nullptr);

        fil_space_set_flags(space, mach_read_from_4(FSP_HEADER_OFFSET +
                                                    FSP_SPACE_FLAGS + page));
        fil_space_release(space);

        break;
      }
      if (page_no == 0 && page != nullptr && end_ptr >= ptr + 2) {
        ulint offs = mach_read_from_2(ptr);

        fil_space_t *space = fil_space_acquire(space_id);
        ut_ad(space != nullptr);
        ulint offset = fsp_header_get_encryption_progress_offset(
            page_size_t(space->flags));

        if (offs == offset) {
          ptr = mlog_parse_nbytes(MLOG_1BYTE, ptr, end_ptr, page, page_zip);
          byte op = mach_read_from_1(page + offset);
          switch (op) {
            case Encryption::ENCRYPT_IN_PROGRESS:
              space->encryption_op_in_progress =
                  Encryption::Progress::ENCRYPTION;
              break;
            case Encryption::DECRYPT_IN_PROGRESS:
              space->encryption_op_in_progress =
                  Encryption::Progress::DECRYPTION;
              break;
            default:
              space->encryption_op_in_progress = Encryption::Progress::NONE;
              break;
          }
        }
        fil_space_release(space);
      }
#ifdef UNIV_DEBUG
     if (page && page_type == FIL_PAGE_TYPE_ALLOCATED && end_ptr >= ptr + 2) {
         /* It is OK to set FIL_PAGE_TYPE and certain
         list node fields on an empty page.  Any other
         write is not OK. */

         /* NOTE: There may be bogus assertion failures for
         dict_hdr_create(), trx_rseg_header_create(),
         trx_sys_create_doublewrite_buf(), and
         trx_sysf_create().
         These are only called during database creation. */

         ulint offs = mach_read_from_2(ptr);

         switch (type) {
             default:
                 ut_error;
             case MLOG_2BYTES:
                 /* Note that this can fail when the
                 redo log been written with something
                 older than InnoDB Plugin 1.0.4. */
                 ut_ad(
                         offs == FIL_PAGE_TYPE ||
                         offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_OFFSET ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_BYTE ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_BYTE +
                                 FIL_ADDR_SIZE ||
                         offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_OFFSET ||
                         offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_OFFSET ||
                         offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                                 FIL_ADDR_BYTE + 0 /*FLST_PREV*/
                         || offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                                    FIL_ADDR_BYTE + FIL_ADDR_SIZE /*FLST_NEXT*/);
                 break;
             case MLOG_4BYTES:
                 /* Note that this can fail when the
                 redo log been written with something
                 older than InnoDB Plugin 1.0.4. */
                 ut_ad(
                         0 ||
                         offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_SPACE ||
                         offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_PAGE_NO ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER /* flst_init */
                         ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_PAGE ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_PAGE +
                                 FIL_ADDR_SIZE ||
                         offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_PAGE_NO ||
                         offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_SPACE ||
                         offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_PAGE_NO ||
                         offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_SPACE ||
                         offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                                 FIL_ADDR_PAGE + 0 /*FLST_PREV*/
                         || offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                                    FIL_ADDR_PAGE + FIL_ADDR_SIZE /*FLST_NEXT*/);
                 break;
         }
     }
#endif /* UNIV_DEBUG */

     ptr = mlog_parse_nbytes(type, ptr, end_ptr, page, page_zip);

     if (ptr != nullptr && page != nullptr && page_no == 0 &&
         type == MLOG_4BYTES) {
         ulint offs = mach_read_from_2(old_ptr);

         switch (offs) {
             fil_space_t *space;
             uint32_t val;
             default:
                 break;

             case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
             case FSP_HEADER_OFFSET + FSP_SIZE:
             case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
             case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:

                 space = fil_space_get(space_id);

                 ut_a(space != nullptr);

                 val = mach_read_from_4(page + offs);

                 switch (offs) {
                     case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
                         space->flags = val;
                         break;

                     case FSP_HEADER_OFFSET + FSP_SIZE:

                         space->size_in_header = val;

                         if (space->size >= val) {
                             break;
                         }

                         ib::info(ER_IB_MSG_718, ulong{space->id}, space->name,
                                  ulong{val});

                         if (fil_space_extend(space, val)) {
                             break;
                         }

                         ib::error(ER_IB_MSG_719, ulong{space->id}, space->name,
                                   ulong{val});
                         break;

                     case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
                         space->free_limit = val;
                         break;

                     case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:
                         space->free_len = val;
                         ut_ad(val == flst_get_len(page + offs));
                         break;
                 }
         }
     }
     break;

    case MLOG_1BYTE:
      std::cout << "  type: MLOG_1BYTES, space: " << space_id << ", page_no: " << page_no;
      /* If 'ALTER TABLESPACE ... ENCRYPTION' was in progress and page 0 has
      REDO entry for this, now while applying this entry, set
      encryption_op_in_progress flag now so that any other page of this
      tablespace in redo log is written accordingly. */
      if (page_no == 0 && page != nullptr && end_ptr >= ptr + 2) {
        ulint offs = mach_read_from_2(ptr);

        fil_space_t *space = fil_space_acquire(space_id);
        ut_ad(space != nullptr);
        ulint offset = fsp_header_get_encryption_progress_offset(
            page_size_t(space->flags));

        if (offs == offset) {
          ptr = mlog_parse_nbytes(MLOG_1BYTE, ptr, end_ptr, page, page_zip);
          byte op = mach_read_from_1(page + offset);
          switch (op) {
            case Encryption::ENCRYPT_IN_PROGRESS:
              space->encryption_op_in_progress =
                  Encryption::Progress::ENCRYPTION;
              break;
            case Encryption::DECRYPT_IN_PROGRESS:
              space->encryption_op_in_progress =
                  Encryption::Progress::DECRYPTION;
              break;
            default:
              space->encryption_op_in_progress = Encryption::Progress::NONE;
              break;
          }
        }
        fil_space_release(space);
      }
#ifdef UNIV_DEBUG
     if (page && page_type == FIL_PAGE_TYPE_ALLOCATED && end_ptr >= ptr + 2) {
         /* It is OK to set FIL_PAGE_TYPE and certain
         list node fields on an empty page.  Any other
         write is not OK. */

         /* NOTE: There may be bogus assertion failures for
         dict_hdr_create(), trx_rseg_header_create(),
         trx_sys_create_doublewrite_buf(), and
         trx_sysf_create().
         These are only called during database creation. */

         ulint offs = mach_read_from_2(ptr);

         switch (type) {
             default:
                 ut_error;
             case MLOG_2BYTES:
                 /* Note that this can fail when the
                 redo log been written with something
                 older than InnoDB Plugin 1.0.4. */
                 ut_ad(
                         offs == FIL_PAGE_TYPE ||
                         offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_OFFSET ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_BYTE ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_BYTE +
                                 FIL_ADDR_SIZE ||
                         offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_OFFSET ||
                         offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_OFFSET ||
                         offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                                 FIL_ADDR_BYTE + 0 /*FLST_PREV*/
                         || offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                                    FIL_ADDR_BYTE + FIL_ADDR_SIZE /*FLST_NEXT*/);
                 break;
             case MLOG_4BYTES:
                 /* Note that this can fail when the
                 redo log been written with something
                 older than InnoDB Plugin 1.0.4. */
                 ut_ad(
                         0 ||
                         offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_SPACE ||
                         offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_PAGE_NO ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER /* flst_init */
                         ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_PAGE ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_PAGE +
                                 FIL_ADDR_SIZE ||
                         offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_PAGE_NO ||
                         offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_SPACE ||
                         offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_PAGE_NO ||
                         offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_SPACE ||
                         offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                                 FIL_ADDR_PAGE + 0 /*FLST_PREV*/
                         || offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                                    FIL_ADDR_PAGE + FIL_ADDR_SIZE /*FLST_NEXT*/);
                 break;
         }
     }
#endif /* UNIV_DEBUG */

     ptr = mlog_parse_nbytes(type, ptr, end_ptr, page, page_zip);

     if (ptr != nullptr && page != nullptr && page_no == 0 &&
         type == MLOG_4BYTES) {
         ulint offs = mach_read_from_2(old_ptr);

         switch (offs) {
             fil_space_t *space;
             uint32_t val;
             default:
                 break;

             case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
             case FSP_HEADER_OFFSET + FSP_SIZE:
             case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
             case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:

                 space = fil_space_get(space_id);

                 ut_a(space != nullptr);

                 val = mach_read_from_4(page + offs);

                 switch (offs) {
                     case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
                         space->flags = val;
                         break;

                     case FSP_HEADER_OFFSET + FSP_SIZE:

                         space->size_in_header = val;

                         if (space->size >= val) {
                             break;
                         }

                         ib::info(ER_IB_MSG_718, ulong{space->id}, space->name,
                                  ulong{val});

                         if (fil_space_extend(space, val)) {
                             break;
                         }

                         ib::error(ER_IB_MSG_719, ulong{space->id}, space->name,
                                   ulong{val});
                         break;

                     case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
                         space->free_limit = val;
                         break;

                     case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:
                         space->free_len = val;
                         ut_ad(val == flst_get_len(page + offs));
                         break;
                 }
         }
     }
     break;

    case MLOG_2BYTES:
      std::cout << "  type: MLOG_2BYTES, space: " << space_id << ", page_no: " << page_no;
#ifdef UNIV_DEBUG
     if (page && page_type == FIL_PAGE_TYPE_ALLOCATED && end_ptr >= ptr + 2) {
         /* It is OK to set FIL_PAGE_TYPE and certain
         list node fields on an empty page.  Any other
         write is not OK. */

         /* NOTE: There may be bogus assertion failures for
         dict_hdr_create(), trx_rseg_header_create(),
         trx_sys_create_doublewrite_buf(), and
         trx_sysf_create().
         These are only called during database creation. */

         ulint offs = mach_read_from_2(ptr);

         switch (type) {
             default:
                 ut_error;
             case MLOG_2BYTES:
                 /* Note that this can fail when the
                 redo log been written with something
                 older than InnoDB Plugin 1.0.4. */
                 ut_ad(
                         offs == FIL_PAGE_TYPE ||
                         offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_OFFSET ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_BYTE ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_BYTE +
                                 FIL_ADDR_SIZE ||
                         offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_OFFSET ||
                         offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_OFFSET ||
                         offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                                 FIL_ADDR_BYTE + 0 /*FLST_PREV*/
                         || offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                                    FIL_ADDR_BYTE + FIL_ADDR_SIZE /*FLST_NEXT*/);
                 break;
             case MLOG_4BYTES:
                 /* Note that this can fail when the
                 redo log been written with something
                 older than InnoDB Plugin 1.0.4. */
                 ut_ad(
                         0 ||
                         offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_SPACE ||
                         offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_PAGE_NO ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER /* flst_init */
                         ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_PAGE ||
                         offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_PAGE +
                                 FIL_ADDR_SIZE ||
                         offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_PAGE_NO ||
                         offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_SPACE ||
                         offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_PAGE_NO ||
                         offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_SPACE ||
                         offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                                 FIL_ADDR_PAGE + 0 /*FLST_PREV*/
                         || offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                                    FIL_ADDR_PAGE + FIL_ADDR_SIZE /*FLST_NEXT*/);
                 break;
         }
     }
#endif /* UNIV_DEBUG */

     ptr = mlog_parse_nbytes(type, ptr, end_ptr, page, page_zip);

     if (ptr != nullptr && page != nullptr && page_no == 0 &&
         type == MLOG_4BYTES) {
         ulint offs = mach_read_from_2(old_ptr);

         switch (offs) {
             fil_space_t *space;
             uint32_t val;
             default:
                 break;

             case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
             case FSP_HEADER_OFFSET + FSP_SIZE:
             case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
             case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:

                 space = fil_space_get(space_id);

                 ut_a(space != nullptr);

                 val = mach_read_from_4(page + offs);

                 switch (offs) {
                     case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
                         space->flags = val;
                         break;

                     case FSP_HEADER_OFFSET + FSP_SIZE:

                         space->size_in_header = val;

                         if (space->size >= val) {
                             break;
                         }

                         ib::info(ER_IB_MSG_718, ulong{space->id}, space->name,
                                  ulong{val});

                         if (fil_space_extend(space, val)) {
                             break;
                         }

                         ib::error(ER_IB_MSG_719, ulong{space->id}, space->name,
                                   ulong{val});
                         break;

                     case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
                         space->free_limit = val;
                         break;

                     case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:
                         space->free_len = val;
                         ut_ad(val == flst_get_len(page + offs));
                         break;
                 }
         }
     }
     break;

      case MLOG_8BYTES:
      std::cout << "  type: MLOG_8BYTES, space: " << space_id << ", page_no: " << page_no;
#ifdef UNIV_DEBUG
      if (page && page_type == FIL_PAGE_TYPE_ALLOCATED && end_ptr >= ptr + 2) {
        /* It is OK to set FIL_PAGE_TYPE and certain
        list node fields on an empty page.  Any other
        write is not OK. */

        /* NOTE: There may be bogus assertion failures for
        dict_hdr_create(), trx_rseg_header_create(),
        trx_sys_create_doublewrite_buf(), and
        trx_sysf_create().
        These are only called during database creation. */

        ulint offs = mach_read_from_2(ptr);

        switch (type) {
          default:
            ut_error;
          case MLOG_2BYTES:
            /* Note that this can fail when the
            redo log been written with something
            older than InnoDB Plugin 1.0.4. */
            ut_ad(
                offs == FIL_PAGE_TYPE ||
                offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_OFFSET ||
                offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_BYTE ||
                offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_BYTE +
                            FIL_ADDR_SIZE ||
                offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_OFFSET ||
                offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_OFFSET ||
                offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                            FIL_ADDR_BYTE + 0 /*FLST_PREV*/
                || offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                               FIL_ADDR_BYTE + FIL_ADDR_SIZE /*FLST_NEXT*/);
            break;
          case MLOG_4BYTES:
            /* Note that this can fail when the
            redo log been written with something
            older than InnoDB Plugin 1.0.4. */
            ut_ad(
                0 ||
                offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_SPACE ||
                offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_PAGE_NO ||
                offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER /* flst_init */
                ||
                offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_PAGE ||
                offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_PAGE +
                            FIL_ADDR_SIZE ||
                offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_PAGE_NO ||
                offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_SPACE ||
                offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_PAGE_NO ||
                offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_SPACE ||
                offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                            FIL_ADDR_PAGE + 0 /*FLST_PREV*/
                || offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                               FIL_ADDR_PAGE + FIL_ADDR_SIZE /*FLST_NEXT*/);
            break;
        }
      }
#endif /* UNIV_DEBUG */

      ptr = mlog_parse_nbytes(type, ptr, end_ptr, page, page_zip);

      if (ptr != nullptr && page != nullptr && page_no == 0 &&
          type == MLOG_4BYTES) {
        ulint offs = mach_read_from_2(old_ptr);

        switch (offs) {
          fil_space_t *space;
          uint32_t val;
          default:
            break;

          case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
          case FSP_HEADER_OFFSET + FSP_SIZE:
          case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
          case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:

            space = fil_space_get(space_id);

            ut_a(space != nullptr);

            val = mach_read_from_4(page + offs);

            switch (offs) {
              case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
                space->flags = val;
                break;

              case FSP_HEADER_OFFSET + FSP_SIZE:

                space->size_in_header = val;

                if (space->size >= val) {
                  break;
                }

                ib::info(ER_IB_MSG_718, ulong{space->id}, space->name,
                         ulong{val});

                if (fil_space_extend(space, val)) {
                  break;
                }

                ib::error(ER_IB_MSG_719, ulong{space->id}, space->name,
                          ulong{val});
                break;

              case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
                space->free_limit = val;
                break;

              case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:
                space->free_len = val;
                ut_ad(val == flst_get_len(page + offs));
                break;
            }
        }
      }
      break;

    case MLOG_REC_INSERT:
      std::cout << "  type: MLOG_REC_INSERT, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, &index))) {
        ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = page_cur_parse_insert_rec(false, ptr, end_ptr, block, index, mtr);
      }
      break;

    case MLOG_REC_INSERT_8027:
        std::cout << "  type: MLOG_REC_INSERT_8027, space: " << space_id << ", page_no: " << page_no;
        [[fallthrough]];

    case MLOG_COMP_REC_INSERT_8027:
        std::cout << "  type: MLOG_COMP_REC_INSERT_8027, space: " << space_id << ", page_no: " << page_no;

        ut_ad(!page || fil_page_type_is_index(page_type));

        if (nullptr !=
            (ptr = mlog_parse_index_8027(
                 ptr, end_ptr, type == MLOG_COMP_REC_INSERT_8027, &index))) {
          ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

          ptr = page_cur_parse_insert_rec(false, ptr, end_ptr, block, index, mtr);
        }
        break;

    case MLOG_REC_CLUST_DELETE_MARK:
      std::cout << "  type: MLOG_REC_CLUST_DELETE_MARK, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, &index))) {
        ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = btr_cur_parse_del_mark_set_clust_rec(ptr, end_ptr, page, page_zip,
                                                   index);
      }

      break;

    case MLOG_REC_CLUST_DELETE_MARK_8027:
        std::cout << "  type: MLOG_REC_CLUST_DELETE_MARK_8027, space: " << space_id << ", page_no: " << page_no;
        [[fallthrough]];
    case MLOG_COMP_REC_CLUST_DELETE_MARK_8027:
        std::cout << "  type: MLOG_COMP_REC_CLUST_DELETE_MARK_8027, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr !=
          (ptr = mlog_parse_index_8027(
               ptr, end_ptr, type == MLOG_COMP_REC_CLUST_DELETE_MARK_8027,
               &index))) {
        ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = btr_cur_parse_del_mark_set_clust_rec(ptr, end_ptr, page, page_zip,
                                                   index);
      }

      break;

    case MLOG_COMP_REC_SEC_DELETE_MARK:
      std::cout << "  type: MLOG_COMP_REC_SEC_DELETE_MARK, space: " << space_id << ", page_no: " << page_no << std::endl;

      ut_ad(!page || fil_page_type_is_index(page_type));

      /* This log record type is obsolete, but we process it for
      backward compatibility with MySQL 5.0.3 and 5.0.4. */

      ut_a(!page || page_is_comp(page));
      ut_a(!page_zip);

      ptr = mlog_parse_index_8027(ptr, end_ptr, true, &index);

      if (ptr == nullptr) {
        break;
      }

      [[fallthrough]];

    case MLOG_REC_SEC_DELETE_MARK:
      std::cout << "  type: MLOG_REC_SEC_DELETE_MARK, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      ptr = btr_cur_parse_del_mark_set_sec_rec(ptr, end_ptr, page, page_zip);
      break;

    case MLOG_REC_UPDATE_IN_PLACE:
      std::cout << "  type: MLOG_REC_UPDATE_IN_PLACE, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, &index))) {
        ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

        ptr =
            btr_cur_parse_update_in_place(ptr, end_ptr, page, page_zip, index);
      }

      break;

    case MLOG_REC_UPDATE_IN_PLACE_8027:
        std::cout << "  type: MLOG_REC_UPDATE_IN_PLACE_8027, space: " << space_id << ", page_no: " << page_no;
        [[fallthrough]];
    case MLOG_COMP_REC_UPDATE_IN_PLACE_8027:
        std::cout << "  type: MLOG_COMP_REC_UPDATE_IN_PLACE_8027, space: " << space_id << ", page_no: " << page_no;

        ut_ad(!page || fil_page_type_is_index(page_type));

        if (nullptr !=
            (ptr = mlog_parse_index_8027(
                 ptr, end_ptr, type == MLOG_COMP_REC_UPDATE_IN_PLACE_8027,
                 &index))) {
          ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

          ptr =
              btr_cur_parse_update_in_place(ptr, end_ptr, page, page_zip, index);
        }

        break;

    case MLOG_LIST_END_DELETE:
      std::cout << "  type: MLOG_LIST_END_DELETE, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, &index))) {
          ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

          ptr = page_parse_delete_rec_list(type, ptr, end_ptr, block, index, mtr);
      }

      break;

    case MLOG_LIST_START_DELETE:
      std::cout << "  type: MLOG_LIST_START_DELETE, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, &index))) {
        ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = page_parse_delete_rec_list(type, ptr, end_ptr, block, index, mtr);
      }

      break;

    case MLOG_LIST_END_DELETE_8027:
    case MLOG_COMP_LIST_END_DELETE_8027:
    case MLOG_LIST_START_DELETE_8027:
    case MLOG_COMP_LIST_START_DELETE_8027:
      std::cout << "  type: MLOG_LIST_..._8027, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr != (ptr = mlog_parse_index_8027(
                          ptr, end_ptr,
                          type == MLOG_COMP_LIST_END_DELETE_8027 ||
                              type == MLOG_COMP_LIST_START_DELETE_8027,
                          &index))) {
        ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = page_parse_delete_rec_list(type, ptr, end_ptr, block, index, mtr);
      }

      break;

    case MLOG_LIST_END_COPY_CREATED:
      std::cout << "  type: MLOG_LIST_END_COPY_CREATED, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, &index))) {
        ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = page_parse_copy_rec_list_to_created_page(ptr, end_ptr, block,
                                                       index, mtr);
      }

      break;

    case MLOG_LIST_END_COPY_CREATED_8027:
    case MLOG_COMP_LIST_END_COPY_CREATED_8027:
      std::cout << "  type: MLOG_LIST_..._8027, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr !=
          (ptr = mlog_parse_index_8027(
               ptr, end_ptr, type == MLOG_COMP_LIST_END_COPY_CREATED_8027,
               &index))) {
        ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = page_parse_copy_rec_list_to_created_page(ptr, end_ptr, block,
                                                       index, mtr);
      }

      break;

    case MLOG_PAGE_REORGANIZE:
      std::cout << "  type: MLOG_PAGE_REORGANIZE, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, &index))) {
        ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = btr_parse_page_reorganize(ptr, end_ptr, index,
                                        type == MLOG_ZIP_PAGE_REORGANIZE_8027,
                                        block, mtr);
      }

      break;

    case MLOG_PAGE_REORGANIZE_8027:
      std::cout << "  type: MLOG_LIST_..._8027, space: " << space_id << ", page_no: " << page_no;
      ut_ad(!page || fil_page_type_is_index(page_type));
      /* Uncompressed pages don't have any payload in the
      MTR so ptr and end_ptr can be, and are nullptr */
      mlog_parse_index_8027(ptr, end_ptr, false, &index);
      ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

      ptr = btr_parse_page_reorganize(ptr, end_ptr, index, false, block, mtr);

      break;

    case MLOG_ZIP_PAGE_REORGANIZE:
      std::cout << "  type: MLOG_ZIP_PAGE_REORGANIZE, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, &index))) {
        ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = btr_parse_page_reorganize(ptr, end_ptr, index, true, block, mtr);
      }

      break;

    case MLOG_COMP_PAGE_REORGANIZE_8027:
    case MLOG_ZIP_PAGE_REORGANIZE_8027:
      std::cout << "  type: MLOG_LIST_..._8027, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr !=
          (ptr = mlog_parse_index_8027(ptr, end_ptr, true, &index))) {
        ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = btr_parse_page_reorganize(ptr, end_ptr, index,
                                        type == MLOG_ZIP_PAGE_REORGANIZE_8027,
                                        block, mtr);
      }

      break;

    case MLOG_PAGE_CREATE:
      std::cout << "  type: MLOG_PAGE_CREATE, space: " << space_id << ", page_no: " << page_no;

      /* Allow anything in page_type when creating a page. */
      ut_a(!page_zip);

      page_parse_create(block, type == MLOG_COMP_PAGE_CREATE, FIL_PAGE_INDEX);

      break;

      case MLOG_COMP_PAGE_CREATE:
      std::cout << "  type: MLOG_COMP_PAGE_CREATE, space: " << space_id << ", page_no: " << page_no;

      /* Allow anything in page_type when creating a page. */
      ut_a(!page_zip);

      page_parse_create(block, type == MLOG_COMP_PAGE_CREATE, FIL_PAGE_INDEX);

      break;

    case MLOG_PAGE_CREATE_RTREE:
      std::cout << "  type: MLOG_PAGE_CREATE_RTREE, space: " << space_id << ", page_no: " << page_no;
      [[fallthrough]];
    case MLOG_COMP_PAGE_CREATE_RTREE:
      std::cout << "  type: MLOG_COMP_PAGE_CREATE_RTREE, space: " << space_id << ", page_no: " << page_no;

      page_parse_create(block, type == MLOG_COMP_PAGE_CREATE_RTREE,
                        FIL_PAGE_RTREE);

      break;

    case MLOG_PAGE_CREATE_SDI:
      std::cout << "  type: MLOG_PAGE_CREATE_SDI, space: " << space_id << ", page_no: " << page_no;

      page_parse_create(block, type == MLOG_COMP_PAGE_CREATE_SDI, FIL_PAGE_SDI);

      break;

    case MLOG_COMP_PAGE_CREATE_SDI:
        std::cout << "  type: MLOG_COMP_PAGE_CREATE_SDI, space: " << space_id << ", page_no: " << page_no;

      page_parse_create(block, type == MLOG_COMP_PAGE_CREATE_SDI, FIL_PAGE_SDI);

      break;

    case MLOG_UNDO_INSERT:
      std::cout << "  type: MLOG_UNDO_INSERT, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);

      ptr = trx_undo_parse_add_undo_rec(ptr, end_ptr, page);

      break;

    case MLOG_UNDO_ERASE_END:
      std::cout << "  type: MLOG_UNDO_ERASE_END, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);

      ptr = trx_undo_parse_erase_page_end(ptr, end_ptr, page, mtr);

      break;

    case MLOG_UNDO_INIT:
      std::cout << "  type: MLOG_UNDO_INIT, space: " << space_id << ", page_no: " << page_no;

      /* Allow anything in page_type when creating a page. */

      ptr = trx_undo_parse_page_init(ptr, end_ptr, page, mtr);

      break;
    case MLOG_UNDO_HDR_CREATE:
      std::cout << "  type: MLOG_UNDO_HDR_CREATE, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);

      ptr = trx_undo_parse_page_header(type, ptr, end_ptr, page, mtr);

      break;

      case MLOG_UNDO_HDR_REUSE:
      std::cout << "  type: MLOG_UNDO_HDR_REUSE, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);

      ptr = trx_undo_parse_page_header(type, ptr, end_ptr, page, mtr);

      break;

    case MLOG_REC_MIN_MARK:
      std::cout << "  type: MLOG_REC_MIN_MARK, space: " << space_id << ", page_no: " << page_no;
      [[fallthrough]];

    case MLOG_COMP_REC_MIN_MARK:
      std::cout << "  type: MLOG_COMP_REC_MIN_MARK, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      /* On a compressed page, MLOG_COMP_REC_MIN_MARK
      will be followed by MLOG_COMP_REC_DELETE
      or MLOG_ZIP_WRITE_HEADER(FIL_PAGE_PREV, FIL_nullptr)
      in the same mini-transaction. */

      ut_a(type == MLOG_COMP_REC_MIN_MARK || !page_zip);

      ptr = btr_parse_set_min_rec_mark(
          ptr, end_ptr, type == MLOG_COMP_REC_MIN_MARK, page, mtr);

      break;

    case MLOG_REC_DELETE:
      std::cout << "  type: MLOG_REC_DELETE, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, &index))) {
        ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = page_cur_parse_delete_rec(ptr, end_ptr, block, index, mtr);
      }

      break;

    case MLOG_REC_DELETE_8027:
        std::cout << "  type: MLOG_REC_DELETE_8027, space: " << space_id << ", page_no: " << page_no;
        [[fallthrough]];
    case MLOG_COMP_REC_DELETE_8027:
        std::cout << "  type: MLOG_COMP_REC_DELETE_8027, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr !=
          (ptr = mlog_parse_index_8027(
               ptr, end_ptr, type == MLOG_COMP_REC_DELETE_8027, &index))) {
        ut_a(!page || page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = page_cur_parse_delete_rec(ptr, end_ptr, block, index, mtr);
      }

      break;

    case MLOG_IBUF_BITMAP_INIT:
      std::cout << "  type: MLOG_IBUF_BITMAP_INIT, space: " << space_id << ", page_no: " << page_no;

      /* Allow anything in page_type when creating a page. */

      ptr = ibuf_parse_bitmap_init(ptr, end_ptr, block, mtr);

      break;

    case MLOG_INIT_FILE_PAGE:
      std::cout << "  type: MLOG_INIT_FILE_PAGE, space: " << space_id << ", page_no: " << page_no;
      [[fallthrough]];
    case MLOG_INIT_FILE_PAGE2: {
      std::cout << "  type: MLOG_INIT_FILE_PAGE2, space: " << space_id << ", page_no: " << page_no;
      /* For clone, avoid initializing page-0. Page-0 should already have been
      initialized. This is to avoid erasing encryption information. We cannot
      update encryption information later with redo logged information for
      clone. Please check comments in MLOG_WRITE_STRING. */
      bool skip_init = (recv_sys->is_cloned_db && page_no == 0);

      if (!skip_init) {
        /* Allow anything in page_type when creating a page. */
        ptr = fsp_parse_init_file_page(ptr, end_ptr, block);
      }
      break;
    }

    case MLOG_WRITE_STRING: {
      std::cout << "  type: MLOG_WRITE_STRING, space: " << space_id << ", page_no: " << page_no;
      ut_ad(!page || page_type != FIL_PAGE_TYPE_ALLOCATED || page_no == 0);
      bool is_encryption = check_encryption(page_no, space_id, ptr, end_ptr);

#ifndef UNIV_HOTBACKUP
      /* Reset in-mem encryption information for the tablespace here if this
      is "resetting encryprion info" log. */
      if (is_encryption && !recv_sys->is_cloned_db) {
        byte buf[Encryption::INFO_SIZE] = {0};

        if (memcmp(ptr + 4, buf, Encryption::INFO_SIZE - 4) == 0) {
          ut_a(DB_SUCCESS == fil_reset_encryption(space_id));
        }
      }

#endif
      auto apply_page = page;

      /* For clone recovery, skip applying encryption information from
      redo log. It is already updated in page 0. Redo log encryption
      information is encrypted with donor master key and must be ignored. */
      if (recv_sys->is_cloned_db && is_encryption) {
        apply_page = nullptr;
      }

      ptr = mlog_parse_string(ptr, end_ptr, apply_page, page_zip);
      break;
    }

    case MLOG_ZIP_WRITE_NODE_PTR:
      std::cout << "  type: MLOG_ZIP_WRITE_NODE_PTR, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      ptr = page_zip_parse_write_node_ptr(ptr, end_ptr, page, page_zip);

      break;

    case MLOG_ZIP_WRITE_BLOB_PTR:
      std::cout << "  type: MLOG_ZIP_WRITE_BLOB_PTR, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      ptr = page_zip_parse_write_blob_ptr(ptr, end_ptr, page, page_zip);

      break;

    case MLOG_ZIP_WRITE_HEADER:
      std::cout << "  type: MLOG_ZIP_WRITE_HEADER, space: " << space_id << ", page_no: " << page_no;

      ut_ad(!page || fil_page_type_is_index(page_type));

      ptr = page_zip_parse_write_header(ptr, end_ptr, page, page_zip);

      break;

    case MLOG_ZIP_PAGE_COMPRESS:
        std::cout << "  type: MLOG_ZIP_PAGE_COMPRESS, space: " << space_id << ", page_no: " << page_no;

      /* Allow anything in page_type when creating a page. */
      ptr = page_zip_parse_compress(ptr, end_ptr, page, page_zip);
      break;

    case MLOG_ZIP_PAGE_COMPRESS_NO_DATA:
        std::cout << "  type: MLOG_ZIP_PAGE_COMPRESS_NO_DATA, space: " << space_id << ", page_no: " << page_no;

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, &index))) {
        ut_a(!page || (page_is_comp(page) == dict_table_is_comp(index->table)));

        ptr = page_zip_parse_compress_no_data(ptr, end_ptr, page, page_zip,
                                              index);
      }

      break;

    case MLOG_ZIP_PAGE_COMPRESS_NO_DATA_8027:

      if (nullptr !=
          (ptr = mlog_parse_index_8027(ptr, end_ptr, true, &index))) {
        ut_a(!page || (page_is_comp(page) == dict_table_is_comp(index->table)));

        ptr = page_zip_parse_compress_no_data(ptr, end_ptr, page, page_zip,
                                              index);
      }

      break;

    case MLOG_TEST:
      std::cout << "  type: MLOG_TEST, space: " << space_id << ", page_no: " << page_no;
#ifndef UNIV_HOTBACKUP
      if (log_test != nullptr) {
        ptr = log_test->parse_mlog_rec(ptr, end_ptr);
      } else {
        /* Just parse and ignore record to pass it and go forward. Note that
        this record is also used in the innodb.log_first_rec_group mtr test.
        The record is written in the buf0flu.cc when flushing page in that
        case. */
        Log_test::Key key;
        Log_test::Value value;
        lsn_t _start_lsn, end_lsn;

        ptr = Log_test::parse_mlog_rec(ptr, end_ptr, key, value, _start_lsn,
                                       end_lsn);
      }
      break;
#endif /* !UNIV_HOTBACKUP */
      /* Fall through. */

    default:
      ptr = nullptr;
      recv_sys->found_corrupt_log = true;
  }

  if (index != nullptr) {
    dict_table_t *table = index->table;

    dict_mem_index_free(index);
    dict_mem_table_free(table);
  }

  return ptr;
}

/** Tries to parse a single log record.
@param[out]     type            log record type
@param[in]      ptr             pointer to a buffer
@param[in]      end_ptr         end of the buffer
@param[out]     space_id        tablespace identifier
@param[out]     page_no         page number
@param[out]     body            start of log record body
@return length of the record, or 0 if the record was not complete */
static ulint recv_parse_log_rec(mlog_id_t *type, byte *ptr, byte *end_ptr,
                                space_id_t *space_id, page_no_t *page_no,
                                byte **body) {
  byte *new_ptr;

  *body = nullptr;

  UNIV_MEM_INVALID(type, sizeof *type);
  UNIV_MEM_INVALID(space_id, sizeof *space_id);
  UNIV_MEM_INVALID(page_no, sizeof *page_no);
  UNIV_MEM_INVALID(body, sizeof *body);

  if (ptr == end_ptr) {
    return 0;
  }

  switch (*ptr) {
#ifdef UNIV_LOG_LSN_DEBUG
    case MLOG_LSN | MLOG_SINGLE_REC_FLAG:
    case MLOG_LSN:

      new_ptr =
          mlog_parse_initial_log_record(ptr, end_ptr, type, space_id, page_no);

      if (new_ptr != nullptr) {
        const lsn_t lsn = static_cast<lsn_t>(*space_id) << 32 | *page_no;

        ut_a(lsn == recv_sys->recovered_lsn);
      }

      *type = MLOG_LSN;
      return new_ptr == nullptr ? 0 : new_ptr - ptr;
#endif /* UNIV_LOG_LSN_DEBUG */

    case MLOG_MULTI_REC_END:
      std::cout << "  type: MLOG_MULTI_REC_END" << std::endl;
      *page_no = FIL_NULL;
      *space_id = SPACE_UNKNOWN;
      *type = static_cast<mlog_id_t>(*ptr);
      return 1;
    case MLOG_DUMMY_RECORD:
      std::cout << "  type: MLOG_DUMMY_RECORD" << std::endl;
      *page_no = FIL_NULL;
      *space_id = SPACE_UNKNOWN;
      *type = static_cast<mlog_id_t>(*ptr);
      return 1;

    case MLOG_MULTI_REC_END | MLOG_SINGLE_REC_FLAG:
      std::cout << "  type: MLOG_MULTI_REC_END | MLOG_SINGLE_REC_FLAG" << " (This means corruption)" << std::endl;
      recv_sys->found_corrupt_log = true;
      return 0;
    case MLOG_DUMMY_RECORD | MLOG_SINGLE_REC_FLAG:
      std::cout << "  type: MLOG_DUMMY_RECORD | MLOG_SINGLE_REC_FLAG" << " (This means corruption)" << std::endl;
      recv_sys->found_corrupt_log = true;
      return 0;

    case MLOG_TABLE_DYNAMIC_META:
      std::cout << "  type: MLOG_TABLE_DYNAMIC_META";
      table_id_t id;
      uint64_t version;

      *page_no = FIL_NULL;
      *space_id = SPACE_UNKNOWN;

      new_ptr =
          mlog_parse_initial_dict_log_record(ptr, end_ptr, type, &id, &version);
      std::cout << ", table_id: " << id << ", version: " << version;

      if (new_ptr != nullptr) {
        new_ptr = recv_sys->metadata_recover->parseMetadataLog(
            id, version, new_ptr, end_ptr);
      }
      std::cout << std::endl;

      return new_ptr == nullptr ? 0 : new_ptr - ptr;
    case MLOG_TABLE_DYNAMIC_META | MLOG_SINGLE_REC_FLAG:
      std::cout << "MLOG_TABLE_DYNAMIC_META | MLOG_SINGLE_REC_FLAG";

      *page_no = FIL_NULL;
      *space_id = SPACE_UNKNOWN;

      new_ptr =
          mlog_parse_initial_dict_log_record(ptr, end_ptr, type, &id, &version);
      std::cout << ", table_id: " << id << ", version: " << version << " (TBD. metadata log follows)" << std::endl;

      if (new_ptr != nullptr) {
        new_ptr = recv_sys->metadata_recover->parseMetadataLog(
            id, version, new_ptr, end_ptr);
      }

      return new_ptr == nullptr ? 0 : new_ptr - ptr;
  }

  new_ptr =
      mlog_parse_initial_log_record(ptr, end_ptr, type, space_id, page_no);

  *body = new_ptr;

  if (new_ptr == nullptr) {
    return 0;
  }

  new_ptr = recv_parse_or_apply_log_rec_body(
      *type, new_ptr, end_ptr, *space_id, *page_no, nullptr, nullptr,
      new_ptr - ptr, recv_sys->recovered_lsn);
  std::cout << std::endl;

  if (new_ptr == nullptr) {
    return 0;
  }

  return new_ptr - ptr;
}

/** Tracks changes of recovered_lsn and tracks proper values for what
first_rec_group should be for consecutive blocks. Must be called when
recv_sys->recovered_lsn is changed to next lsn pointing at boundary
between consecutive parsed mini-transactions. */
void recv_track_changes_of_recovered_lsn() {
  if (recv_sys->parse_start_lsn == 0) {
    return;
  }
  /* If we have already found the first block with mtr beginning there,
  we started to track boundaries between blocks. Since then we track
  all proper values of first_rec_group for consecutive blocks.
  The reason for that is to ensure that the first_rec_group of the last
  block is correct. Even though we do not depend during this recovery
  on that value, it would become important if we crashed later, because
  the last recovered block would become the first used block in redo and
  since then we would depend on a proper value of first_rec_group there.
  The checksums of log blocks should detect if it was incorrect, but the
  checksums might be disabled in the configuration. */
  const auto old_block =
      recv_sys->previous_recovered_lsn / OS_FILE_LOG_BLOCK_SIZE;

  const auto new_block = recv_sys->recovered_lsn / OS_FILE_LOG_BLOCK_SIZE;

  if (old_block != new_block) {
    ut_a(new_block > old_block);

    recv_sys->last_block_first_rec_group =
        recv_sys->recovered_lsn % OS_FILE_LOG_BLOCK_SIZE;
  }

  recv_sys->previous_recovered_lsn = recv_sys->recovered_lsn;
}

/** Parse and store a single log record entry.
@param[in]      ptr             start of buffer
@param[in]      end_ptr         end of buffer
@return true if end of processing */
static bool recv_single_rec(byte *ptr, byte *end_ptr) {
  /* The mtr did not modify multiple pages */

  lsn_t old_lsn = recv_sys->recovered_lsn;

  /* Try to parse a log record, fetching its type, space id,
  page no, and a pointer to the body of the log record */

  byte *body;
  mlog_id_t type;
  page_no_t page_no;
  space_id_t space_id;

  std::cout << "- recv_single_rec()";
  if(opt_verbose_output) {
      std::cout << ", lsn: " << recv_sys->recovered_lsn;
  }
  std::cout << std::endl;

  ulint len =
      recv_parse_log_rec(&type, ptr, end_ptr, &space_id, &page_no, &body);

  if (recv_sys->found_corrupt_log) {
    // recv_report_corrupt_log(ptr, type, space_id, page_no);
  } else if (len == 0 || recv_sys->found_corrupt_fs) {
    return true;
  }

  lsn_t new_recovered_lsn;

  new_recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);

  if (new_recovered_lsn > recv_sys->scanned_lsn) {
    /* The log record filled a log block, and we
    require that also the next log block should
    have been scanned in */

    return true;
  }

  recv_sys->recovered_offset += len;
  recv_sys->recovered_lsn = new_recovered_lsn;

  recv_track_changes_of_recovered_lsn();

  // if (recv_update_bytes_to_ignore_before_checkpoint(len)) {
  //   return false;
  // }

  switch (type) {
    case MLOG_DUMMY_RECORD:
      /* Do nothing */
      break;

#ifdef UNIV_LOG_LSN_DEBUG
    case MLOG_LSN:
      /* Do not add these records to the hash table.
      The page number and space id fields are misused
      for something else. */
      break;
#endif /* UNIV_LOG_LSN_DEBUG */

    default:

      if (recv_recovery_on) {
#ifndef UNIV_HOTBACKUP
        if (space_id == TRX_SYS_SPACE ||
            fil_tablespace_lookup_for_recovery(space_id)) {
#endif /* !UNIV_HOTBACKUP */

          // recv_add_to_hash_table(type, space_id, page_no, body, ptr + len,
          //                        old_lsn, recv_sys->recovered_lsn);

#ifndef UNIV_HOTBACKUP
        } else {
          recv_sys->missing_ids.insert(space_id);
        }
#endif /* !UNIV_HOTBACKUP */
      }

      [[fallthrough]];

    case MLOG_INDEX_LOAD:
    case MLOG_FILE_DELETE:
    case MLOG_FILE_RENAME:
    case MLOG_FILE_CREATE:
    case MLOG_FILE_EXTEND:
    case MLOG_TABLE_DYNAMIC_META:

      /* These were already handled by
      recv_parse_log_rec() and
      recv_parse_or_apply_log_rec_body(). */

      DBUG_PRINT("ib_log",
                 ("scan " LSN_PF ": log rec %s"
                  " len " ULINTPF " " PAGE_ID_PF,
                  old_lsn, get_mlog_string(type), len, space_id, page_no));
      break;
  }

  return false;
}

/** Parse and store a multiple record log entry.
@param[in]      ptr             start of buffer
@param[in]      end_ptr         end of buffer
@return true if end of processing */
static bool recv_multi_rec(byte *ptr, byte *end_ptr) {
  /* Check that all the records associated with the single mtr
  are included within the buffer */

  ulint n_recs = 0;
  ulint total_len = 0;

  std::cout << "- recv_multi_rec()";
  if(opt_verbose_output) {
      std::cout << ", lsn: " << recv_sys->recovered_lsn;
  }
  std::cout << std::endl;

  for (;;) {
    mlog_id_t type = MLOG_BIGGEST_TYPE;
    byte *body;
    page_no_t page_no = 0;
    space_id_t space_id = 0;

    ulint len =
        recv_parse_log_rec(&type, ptr, end_ptr, &space_id, &page_no, &body);

    if (recv_sys->found_corrupt_log) {
      // recv_report_corrupt_log(ptr, type, space_id, page_no);
      return true;

    } else if (len == 0) {
      return true;

    } else if ((*ptr & MLOG_SINGLE_REC_FLAG)) {
      recv_sys->found_corrupt_log = true;
      // recv_report_corrupt_log(ptr, type, space_id, page_no);

      return true;

    } else if (recv_sys->found_corrupt_fs) {
      return true;
    }

    recv_sys->save_rec(n_recs, space_id, page_no, type, body, len);

    total_len += len;
    ++n_recs;

    ptr += len;

    if (type == MLOG_MULTI_REC_END) {
      DBUG_PRINT("ib_log", ("scan " LSN_PF ": multi-log end total_len " ULINTPF
                            " n=" ULINTPF,
                            recv_sys->recovered_lsn, total_len, n_recs));

      break;
    }

    DBUG_PRINT("ib_log",
               ("scan " LSN_PF ": multi-log rec %s len " ULINTPF " " PAGE_ID_PF,
                recv_sys->recovered_lsn, get_mlog_string(type), len, space_id,
                page_no));
  }

  lsn_t new_recovered_lsn =
      recv_calc_lsn_on_data_add(recv_sys->recovered_lsn, total_len);

  if (new_recovered_lsn > recv_sys->scanned_lsn) {
    /* The log record filled a log block, and we require
    that also the next log block should have been scanned in */

    return true;
  }

  /* Add all the records to the hash table */

  ptr = recv_sys->buf + recv_sys->recovered_offset;

  for (ulint i = 0; i < n_recs; i++) {
    lsn_t old_lsn = recv_sys->recovered_lsn;

    /* This will apply MLOG_FILE_ records. */
    space_id_t space_id = 0;
    page_no_t page_no = 0;

    mlog_id_t type = MLOG_BIGGEST_TYPE;

    byte *body = nullptr;
    size_t len = 0;

    /* Avoid parsing if we have the record saved already. */
    if (!recv_sys->get_saved_rec(i, space_id, page_no, type, body, len)) {
      len = recv_parse_log_rec(&type, ptr, end_ptr, &space_id, &page_no, &body);
    }

    if (recv_sys->found_corrupt_log) {
      return true;

    } else if (recv_sys->found_corrupt_fs) {
      return true;
    }

    ut_a(len != 0);
    ut_a(!(*ptr & MLOG_SINGLE_REC_FLAG));

    recv_sys->recovered_offset += len;

    recv_sys->recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);

    switch (type) {
      case MLOG_MULTI_REC_END:
        recv_track_changes_of_recovered_lsn();
        /* Found the end mark for the records */
        return false;

#ifdef UNIV_LOG_LSN_DEBUG
      case MLOG_LSN:
        /* Do not add these records to the hash table.
        The page number and space id fields are misused
        for something else. */
        break;
#endif /* UNIV_LOG_LSN_DEBUG */

      case MLOG_FILE_DELETE:
      case MLOG_FILE_CREATE:
      case MLOG_FILE_RENAME:
      case MLOG_FILE_EXTEND:
      case MLOG_TABLE_DYNAMIC_META:
        /* case MLOG_TRUNCATE: Disabled for WL6378 */
        /* These were already handled by
        recv_parse_or_apply_log_rec_body(). */
        break;
      default:
        break;
    }

    ptr += len;
  }

  return false;
}

ulint offset_limit = 50;

/** Parse log records from a buffer and optionally store them to a
hash table to wait merging to file pages. */
void recv_parse_log_recs() {
  ut_ad(recv_sys->parse_start_lsn != 0);

  for (;;) {
    if(recv_sys->recovered_lsn > recv_sys->stop_lsn) {
      return;
    }
    byte *ptr = recv_sys->buf + recv_sys->recovered_offset;

    byte *end_ptr = recv_sys->buf + recv_sys->len;

    if (ptr == end_ptr) {
      return;
    }

    bool single_rec;

    switch (*ptr) {
#ifdef UNIV_LOG_LSN_DEBUG
      case MLOG_LSN:
#endif /* UNIV_LOG_LSN_DEBUG */
      case MLOG_DUMMY_RECORD:
        single_rec = true;
        break;
      default:
        single_rec = !!(*ptr & MLOG_SINGLE_REC_FLAG);
    }

    if (single_rec) {
      if (recv_single_rec(ptr, end_ptr)) {
        return;
      }

    } else if (recv_multi_rec(ptr, end_ptr)) {
      return;
    }
  }
}

bool log_block_checksum_is_ok(const byte *block) {
  return !srv_log_checksums ||
         log_block_get_checksum(block) == log_block_calc_checksum(block);
}

/** Checks if a given log data block could be considered a next valid block,
with regards to the epoch_no it has stored in its header, during the recovery.
@param[in]  log_block_epoch_no  epoch_no of the log data block to check
@param[in]  last_epoch_no       epoch_no of the last data block scanned
@return true iff the provided log block has valid epoch_no */
bool log_block_epoch_no_is_valid(uint32_t log_block_epoch_no,
                                        uint32_t last_epoch_no) {
  const auto expected_next_epoch_no = last_epoch_no + 1;

  return log_block_epoch_no == last_epoch_no ||
         log_block_epoch_no == expected_next_epoch_no;
}


/** Adds data from a new log block to the parsing buffer of recv_sys if
recv_sys->parse_start_lsn is non-zero.
@param[in]      log_block               log block
@param[in]      scanned_lsn             lsn of how far we were able
                                        to find data in this log block
@return true if more data added */
bool recv_sys_add_to_parsing_buf(const byte *log_block,
                                        lsn_t scanned_lsn) {
  ut_ad(scanned_lsn >= recv_sys->scanned_lsn);

  if (!recv_sys->parse_start_lsn) {
    /* Cannot start parsing yet because no start point for
    it found */

    return false;
  }

  ulint more_len;
  ulint data_len = log_block_get_data_len(log_block);

  if (recv_sys->parse_start_lsn >= scanned_lsn) {
    return false;

  } else if (recv_sys->scanned_lsn >= scanned_lsn) {
    return false;

  } else if (recv_sys->parse_start_lsn > recv_sys->scanned_lsn) {
    more_len = (ulint)(scanned_lsn - recv_sys->parse_start_lsn);

  } else {
    more_len = (ulint)(scanned_lsn - recv_sys->scanned_lsn);
  }

  if (more_len == 0) {
    return false;
  }

  ut_ad(data_len >= more_len);

  ulint start_offset = data_len - more_len;

  if (start_offset < LOG_BLOCK_HDR_SIZE) {
    start_offset = LOG_BLOCK_HDR_SIZE;
  }

  ulint end_offset = data_len;

  if (end_offset > OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
    end_offset = OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE;
  }

  ut_ad(start_offset <= end_offset);

  if (start_offset < end_offset) {
    memcpy(recv_sys->buf + recv_sys->len, log_block + start_offset,
           end_offset - start_offset);

    recv_sys->len += end_offset - start_offset;

    ut_a(recv_sys->len <= recv_sys->buf_len);
  }

  return true;
}


/** Resize the recovery parsing buffer up to log_buffer_size */
bool recv_sys_resize_buf() {
  ut_ad(recv_sys->buf_len <= srv_log_buffer_size);

#ifndef UNIV_HOTBACKUP
  /* If the buffer cannot be extended further, return false. */
  if (recv_sys->buf_len == srv_log_buffer_size) {
    ib::error(ER_IB_MSG_723, srv_log_buffer_size);
    return false;
  }
#else  /* !UNIV_HOTBACKUP */
  if ((recv_sys->buf_len >= srv_log_buffer_size) ||
      (recv_sys->len >= srv_log_buffer_size)) {
    ib::fatal(UT_LOCATION_HERE, ER_IB_ERR_LOG_PARSING_BUFFER_OVERFLOW)
        << "Log parsing buffer overflow. Log parse failed. "
        << "Please increase --limit-memory above "
        << srv_log_buffer_size / 1024 / 1024 << " (MB)";
  }
#endif /* !UNIV_HOTBACKUP */

  /* Extend the buffer by double the current size with the resulting
  size not more than srv_log_buffer_size. */
  recv_sys->buf_len = ((recv_sys->buf_len * 2) >= srv_log_buffer_size)
                          ? srv_log_buffer_size
                          : recv_sys->buf_len * 2;

  /* Resize the buffer to the new size. */
  recv_sys->buf = static_cast<byte *>(ut::realloc_withkey(
      UT_NEW_THIS_FILE_PSI_KEY, recv_sys->buf, recv_sys->buf_len));

  ut_ad(recv_sys->buf != nullptr);

  /* Return error and fail the recovery if not enough memory available */
  if (recv_sys->buf == nullptr) {
    ib::error(ER_IB_MSG_740);
    return false;
  }

  ib::info(ER_IB_MSG_739, recv_sys->buf_len);
  return true;
}

/** Scans log from a buffer and stores new log data to the parsing buffer.
Parses and hashes the log records if new data found.  Unless
UNIV_HOTBACKUP is defined, this function will apply log records
automatically when the hash table becomes full.
@param[in,out]  log             redo log
@param[in]      max_memory      we let the hash table of recs to grow to
                                this size, at the maximum
@param[in]      buf             buffer containing a log segment or garbage
@param[in]      len             buffer length
@param[in]      start_lsn       buffer start lsn
@param[out]  read_upto_lsn  scanning succeeded up to this lsn
@param[out]  err             DB_SUCCESS when no dblwr corruptions.
@return true if not able to scan any more in this log */
static bool my_recv_scan_log_recs(const byte *buf, size_t len,
                               lsn_t start_lsn) {
  const byte *log_block = buf;
  lsn_t scanned_lsn = start_lsn;
  bool finished = false;
  bool more_data = false;

  ut_ad(start_lsn % OS_FILE_LOG_BLOCK_SIZE == 0);
  ut_ad(len % OS_FILE_LOG_BLOCK_SIZE == 0);
  ut_ad(len >= OS_FILE_LOG_BLOCK_SIZE);

  do {
    ut_ad(!finished);

    Log_data_block_header block_header;
    log_data_block_header_deserialize(log_block, block_header);

    const uint32_t expected_hdr_no =
        log_block_convert_lsn_to_hdr_no(scanned_lsn);

    if (block_header.m_hdr_no != expected_hdr_no) {
      /* Garbage or an incompletely written log block.
      We will not report any error, because this can
      happen when InnoDB was killed while it was
      writing redo log. We simply treat this as an
      abrupt end of the redo log. */
      finished = true;
      break;
    }

    if (!log_block_checksum_is_ok(log_block)) {
      uint32_t checksum1 = log_block_get_checksum(log_block);
      uint32_t checksum2 = log_block_calc_checksum(log_block);
      ib::error(ER_IB_MSG_720, ulong{block_header.m_hdr_no},
                ulonglong{scanned_lsn}, ulong{checksum1}, ulong{checksum2});

      finished = true;
      break;
    }

    const auto data_len = block_header.m_data_len;

    if (scanned_lsn + data_len > recv_sys->scanned_lsn &&
        recv_sys->scanned_epoch_no > 0 &&
        !log_block_epoch_no_is_valid(block_header.m_epoch_no,
                                     recv_sys->scanned_epoch_no)) {
      /* Garbage from a log buffer flush which was made
      before the most recent database recovery */

      finished = true;

      break;
    }

    if (!recv_sys->parse_start_lsn && block_header.m_first_rec_group > 0) {
      /* We found a point from which to start the parsing of log records */
      recv_sys->parse_start_lsn = scanned_lsn + block_header.m_first_rec_group;

      if (recv_sys->parse_start_lsn < recv_sys->checkpoint_lsn) {
        recv_sys->bytes_to_ignore_before_checkpoint =
            recv_sys->checkpoint_lsn - recv_sys->parse_start_lsn;

        ut_a(recv_sys->bytes_to_ignore_before_checkpoint <=
             OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE);

        ut_a(recv_sys->checkpoint_lsn % OS_FILE_LOG_BLOCK_SIZE +
                 LOG_BLOCK_TRL_SIZE <
             OS_FILE_LOG_BLOCK_SIZE);

        ut_a(recv_sys->parse_start_lsn % OS_FILE_LOG_BLOCK_SIZE >=
             LOG_BLOCK_HDR_SIZE);
      }

      recv_sys->scanned_lsn = recv_sys->parse_start_lsn;
      recv_sys->recovered_lsn = recv_sys->parse_start_lsn;

      recv_track_changes_of_recovered_lsn();
    }

    scanned_lsn += data_len;

    if (scanned_lsn > recv_sys->scanned_lsn) {
      if (!recv_needed_recovery && scanned_lsn > recv_sys->checkpoint_lsn) {
        ib::info(ER_IB_MSG_722, ulonglong{recv_sys->scanned_lsn});
      }

      /* We were able to find more log data: add it to the
      parsing buffer if parse_start_lsn is already
      non-zero */

      if (recv_sys->len + 4 * OS_FILE_LOG_BLOCK_SIZE >= recv_sys->buf_len) {
        if (!recv_sys_resize_buf()) {
          recv_sys->found_corrupt_log = true;
          std::cout << "resizing recv_sys.buf was failed." << std::endl;
        }
      }

      if (!recv_sys->found_corrupt_log) {
        more_data = recv_sys_add_to_parsing_buf(log_block, scanned_lsn);
      }

      recv_sys->scanned_lsn = scanned_lsn;
      recv_sys->scanned_epoch_no = block_header.m_epoch_no;
    }

    if (data_len < OS_FILE_LOG_BLOCK_SIZE) {
      /* Log data for this group ends here */
      finished = true;
      break;
    } else {
      log_block += OS_FILE_LOG_BLOCK_SIZE;
    }

  } while (log_block < buf + len);

  if (more_data && !recv_sys->found_corrupt_log) {
    /* Try to parse more log records */
    recv_parse_log_recs();
  }

  return finished;
}


/* ?? Modify recv_recovery_begin() */
bool my_parse_begin(byte *buf, const lsn_t checkpoint_lsn, lsn_t end_lsn, uint64_t first_block_offset) {
  mutex_enter(&recv_sys->mutex);

  recv_sys->len = 0;
  recv_sys->recovered_offset = 0;
  recv_sys->n_addrs = 0;

  /* Since 8.0, we can start recovery at checkpoint_lsn which points
  to the middle of log record. In such case we first to need to find
  the beginning of the first group of log records, which is at lsn
  greater than the checkpoint_lsn. */
  recv_sys->parse_start_lsn = 0;

  /* This is updated when we find value for parse_start_lsn. */
  recv_sys->bytes_to_ignore_before_checkpoint = 0;

  recv_sys->checkpoint_lsn = checkpoint_lsn;
  recv_sys->scanned_lsn = checkpoint_lsn;
  recv_sys->recovered_lsn = checkpoint_lsn;

  /* We have to trust that the first_rec_group in the first block is
  correct as we can't start parsing earlier to check it ourselves. */
  recv_sys->previous_recovered_lsn = checkpoint_lsn;
  recv_sys->last_block_first_rec_group = 0;

  recv_sys->scanned_epoch_no = 0;

  mutex_exit(&recv_sys->mutex);

  lsn_t start_lsn =
      ut_uint64_align_down(checkpoint_lsn, OS_FILE_LOG_BLOCK_SIZE);

  return my_recv_scan_log_recs(buf+first_block_offset, end_lsn - start_lsn, start_lsn);
}
