#include <sisl/fds/obj_allocator.hpp>
#include <sisl/fds/vector_pool.hpp>
#include <home_replication/repl_service.h>
#include "state_machine/state_machine.h"
#include "log_store/repl_log_store.hpp"
#include "log_store/journal_entry.h"
#include "storage/storage_engine.h"

namespace home_replication {
ReplicaSet::ReplicaSet(const std::string& group_id, const std::shared_ptr< StateMachineStore >& sm_store,
                       const std::shared_ptr< nuraft::log_store >& log_store) :
        m_state_machine{std::make_shared< ReplicaStateMachine >(sm_store, this)},
        m_state_store{sm_store},
        m_data_journal{log_store},
        m_group_id{group_id} {}

void ReplicaSet::write(const sisl::blob& header, const sisl::blob& key, const sisl::sg_list& value, void* user_ctx) {
    // Step 1: Alloc PBAs
    auto pbas = m_state_store->alloc_pbas(uint32_cast(value.size));

    // Step 2: Send the data to all replicas
    // send_in_data_channel(pbas, value);

    // Step 3: Create the request structure containing all details essential for callback
    repl_req* req = sisl::ObjectAllocator< repl_req >::make_object();
    req->header = header;
    req->key = key;
    req->value = value;
    req->user_ctx = user_ctx;

    // Step 4: Allocate and populate the journal entry
    auto const entry_size = sizeof(repl_journal_entry) + (pbas.size() * sizeof(pba_t)) + header.size + key.size;
    nuraft::ptr< nuraft::buffer > buf = nuraft::buffer::alloc(entry_size);

    auto* entry = r_cast< repl_journal_entry* >(buf->data_begin());
    entry->code = journal_type_t::DATA;
    entry->n_pbas = s_cast< uint16_t >(pbas.size());
    entry->user_header_size = header.size;
    entry->key_size = key.size;

    // Step 5: Copy the header and key into the journal entry
    uint8_t* raw_ptr = uintptr_cast(entry) + sizeof(repl_journal_entry);
    std::memcpy(raw_ptr, header.bytes, header.size);
    raw_ptr += header.size;
    std::memcpy(raw_ptr, key.bytes, key.size);

    // Step 6: Append the entry to the raft group
    auto* vec = sisl::VectorPool< nuraft::ptr< nuraft::buffer > >::alloc();
    vec->push_back(buf);

    nuraft::raft_server::req_ext_params param;
    param.after_precommit_ = bind_this(ReplicaSet::after_precommit_in_leader, 1);
    param.expected_term_ = 0;
    param.context_ = voidptr_cast(req);
    // m_raft_server->append_entries_ext(*vec, param)
    sisl::VectorPool< nuraft::ptr< nuraft::buffer > >::free(vec);
}

void ReplicaSet::after_precommit_in_leader(const nuraft::raft_server::req_ext_cb_params& params) {
    repl_req* req = r_cast< repl_req* >(params.context);
    auto r = m_lsn_req_map.insert(params.log_idx, req);
    assert(r.second); // Duplicate log_idx, not expected
}

std::shared_ptr< nuraft::state_machine > ReplicaSet::get_state_machine() {
    return std::dynamic_pointer_cast< nuraft::state_machine >(m_state_machine);
}

} // namespace home_replication