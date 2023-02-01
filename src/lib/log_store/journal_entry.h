#pragma once
#include <boost/uuid/uuid.hpp>
#include <sisl/utility/enum.hpp>
#include <sisl/fds/buffer.hpp>
#include <home_replication/repl_decls.h>

namespace home_replication {
VENUM(journal_type_t, uint16_t, DATA = 0);

static constexpr uint16_t JOURNAL_ENTRY_MAJOR{1};
static constexpr uint16_t JOURNAL_ENTRY_MINOR{0};

struct repl_journal_entry {
    // Major and minor version. For each major version underlying structures could change. Minor versions can only add
    // fields, not change any existing fields.
    uint16_t major_version{JOURNAL_ENTRY_MAJOR};
    uint16_t minor_version{JOURNAL_ENTRY_MINOR};

    journal_type_t code;
    uint16_t n_pbas;
    boost::uuids::uuid replica_id;
    uint32_t user_header_size;
    uint32_t key_size;
    // Followed by user_header, then key, then pbas

public:
    uint32_t total_size() const {
        return sizeof(repl_journal_entry) + (n_pbas * sizeof(pba_t)) + user_header_size + key_size;
    }
};

struct repl_req {
    sisl::blob header;
    sisl::blob key;
    sisl::sg_list value;
    void* user_ctx;
    int64_t lsn{0};
    raft_buf_ptr_t journal_entry;
    bool is_data_written{false};
    bool is_data_replicated{false};
};

} // namespace home_replication