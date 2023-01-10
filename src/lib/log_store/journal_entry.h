#pragma once
#include <sisl/utility/enum.hpp>
#include "engine.h"

namespace home_replication {
VENUM(journal_type_t, uint16_t, DATA = 0);

struct repl_journal_entry {
    uint16_t major_version; // Major and minor version. For each major version underlying structures could
    uint16_t minor_version; // change. Minor versions can only add fields, not change any existing fields.

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
} // namespace home_replication