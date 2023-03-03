#include <sisl/logging/logging.h>
#include <sisl/fds/utils.hpp>
#include <sisl/fds/obj_allocator.hpp>
#include <sisl/fds/vector_pool.hpp>
#include <home_replication/repl_service.h>
#include <home_replication/repl_set.h>
#include "state_machine.h"
#include "storage/storage_engine.h"
#include "log_store/journal_entry.h"
#include "service/repl_config.h"

SISL_LOGGING_DECL(home_replication)

namespace home_replication {

ReplicaStateMachine::ReplicaStateMachine(const std::shared_ptr< StateMachineStore >& state_store, ReplicaSet* rs) :
        m_state_store{state_store}, m_rs{rs}, m_group_id{rs->m_group_id} {
    m_success_ptr = nuraft::buffer::alloc(sizeof(int));
    m_success_ptr->put(0);
}

void ReplicaStateMachine::propose(const sisl::blob& header, const sisl::blob& key, const sisl::sg_list& value,
                                  void* user_ctx) {
    // Step 1: Alloc PBAs
    auto pbas = m_state_store->alloc_pbas(uint32_cast(value.size));

    // Step 2: Send the data to all replicas
    // m_rs->send_in_data_channel(pbas, value);

    // Step 3: Create the request structure containing all details essential for callback
    repl_req* req = sisl::ObjectAllocator< repl_req >::make_object();
    req->header = header;
    req->key = key;
    req->value = value;
    req->local_pbas = pbas;
    req->user_ctx = user_ctx;

    // Step 4: Write the data to underlying store
    m_state_store->async_write(value, pbas, [this, req]([[maybe_unused]] std::error_condition err) {
        assert(!err);
        ++req->num_pbas_written;
        check_and_commit(req);
    });

    // Step 5: Allocate and populate the journal entry
    auto const entry_size = sizeof(repl_journal_entry) + (pbas.size() * sizeof(pba_t)) + header.size + key.size;
    raft_buf_ptr_t buf = nuraft::buffer::alloc(entry_size);

    auto* entry = r_cast< repl_journal_entry* >(buf->data_begin());
    entry->code = journal_type_t::DATA;
    entry->n_pbas = s_cast< uint16_t >(pbas.size());
    entry->user_header_size = header.size;
    entry->key_size = key.size;

    // Step 6: Copy the header and key into the journal entry
    uint8_t* raw_ptr = uintptr_cast(entry) + sizeof(repl_journal_entry);
    std::memcpy(raw_ptr, header.bytes, header.size);
    raw_ptr += header.size;
    std::memcpy(raw_ptr, key.bytes, key.size);

    // Step 7: Append the entry to the raft group
    auto* vec = sisl::VectorPool< raft_buf_ptr_t >::alloc();
    vec->push_back(buf);

    nuraft::raft_server::req_ext_params param;
    param.after_precommit_ = bind_this(ReplicaStateMachine::after_precommit_in_leader, 1);
    param.expected_term_ = 0;
    param.context_ = voidptr_cast(req);
    // m_raft_server->append_entries_ext(*vec, param);
    sisl::VectorPool< raft_buf_ptr_t >::free(vec);
}

raft_buf_ptr_t ReplicaStateMachine::pre_commit_ext(const nuraft::state_machine::ext_op_params& params) {
    // Leader precommit is processed in next callback, since lsn would not have been known to repl layer till we get
    // the next callback.
    if (!m_rs->is_leader()) {
        int64_t lsn = s_cast< int64_t >(params.log_idx);
        raft_buf_ptr_t data = params.data;

        RS_LOG(DEBUG, "pre_commit: {}, size: {}", lsn, data->size());
        repl_req* req = lsn_to_req(lsn);

        m_rs->m_listener->on_pre_commit(req->lsn, req->header, req->key, req->user_ctx);
    }
    return m_success_ptr;
}

void ReplicaStateMachine::after_precommit_in_leader(const nuraft::raft_server::req_ext_cb_params& params) {
    repl_req* req = r_cast< repl_req* >(params.context);
    link_lsn_to_req(req, int64_cast(params.log_idx));

    m_rs->m_listener->on_pre_commit(req->lsn, req->header, req->key, req->user_ctx);
}

raft_buf_ptr_t ReplicaStateMachine::commit_ext(const nuraft::state_machine::ext_op_params& params) {
    int64_t lsn = s_cast< int64_t >(params.log_idx);
    raft_buf_ptr_t data = params.data;

    RS_LOG(DEBUG, "apply_commit: {}, size: {}", lsn, data->size());

    repl_req* req = lsn_to_req(lsn);
    if (m_rs->is_leader()) {
        // This is the time to ensure flushing of journal happens in leader
        if (m_rs->m_data_journal->last_durable_index() < uint64_cast(lsn)) { m_rs->m_data_journal->flush(); }
        req->is_raft_written.store(true);
    }
    check_and_commit(req);
    return m_success_ptr;
}

void ReplicaStateMachine::check_and_commit(repl_req* req) {
    if ((req->num_pbas_written.load() == req->local_pbas.size()) && req->is_raft_written.load()) {
        m_rs->m_listener->on_commit(req->lsn, req->header, req->key, req->local_pbas, req->user_ctx);
        m_state_store->commit_lsn(req->lsn);
        m_lsn_req_map.erase(req->lsn);
        sisl::ObjectAllocator< repl_req >::deallocate(req);
    }
}

uint64_t ReplicaStateMachine::last_commit_index() { return uint64_cast(m_state_store->get_last_commit_lsn()); }

repl_req* ReplicaStateMachine::transform_journal_entry(const raft_buf_ptr_t& raft_buf) {
    // Leader has nothing to transform or process
    if (m_rs->is_leader()) { return nullptr; }

    fq_pba_list_t remote_pbas;
    pba_list_t local_pbas;

    repl_journal_entry* entry = r_cast< repl_journal_entry* >(raft_buf->data_begin());
    repl_req* req = sisl::ObjectAllocator< repl_req >::make_object();
    req->header =
        sisl::blob{uintptr_cast(raft_buf->data_begin()) + sizeof(repl_journal_entry), entry->user_header_size};
    req->key = sisl::blob{req->header.bytes + req->header.size, req->key.size};
    pba_t* raw_pba_list = r_cast< pba_t* >(req->key.bytes + req->key.size);

    // Transform log_entry and also populate the pbas in a list
    for (uint16_t i{0}; i < entry->n_pbas; ++i) {
        auto const remote_pba = fully_qualified_pba{entry->replica_id, raw_pba_list[i]};
        remote_pbas.push_back(remote_pba);

        auto const [local_pba, written] = try_map_pba(remote_pba);
        raw_pba_list[i] = local_pba;
        local_pbas.push_back(local_pba);
    }
    // TODO: Should we leave this on senders id in journal or better to write local id?
    entry->replica_id = m_server_id;

    req->remote_fq_pbas = remote_pbas;
    req->local_pbas = local_pbas;
    req->journal_entry = raft_buf;

    return req;
}

void ReplicaStateMachine::link_lsn_to_req(repl_req* req, int64_t lsn) {
    req->lsn = lsn;
    [[maybe_unused]] auto r = m_lsn_req_map.insert(lsn, req);
    RS_DBG_ASSERT_EQ(r.second, true, "lsn={} already in precommit list", lsn);
}

repl_req* ReplicaStateMachine::lsn_to_req(int64_t lsn) {
    // Pull the req from the lsn
    auto const it = m_lsn_req_map.find(lsn);
    RS_DBG_ASSERT(it != m_lsn_req_map.cend(), "lsn req map missing lsn={}", lsn);

    repl_req* req = it->second;
    RS_DBG_ASSERT_EQ(lsn, req->lsn, "lsn req map mismatch");
    return req;
}

std::pair< pba_t, pba_state_t > ReplicaStateMachine::try_map_pba(const fully_qualified_pba& fq_pba) {
    // TODO: Implement them
    return std::make_pair(fq_pba.pba, pba_state_t::unknown);
}

bool ReplicaStateMachine::async_fetch_write_pbas(const std::vector< fully_qualified_pba >&, batch_completion_cb_t) {
    // TODO: Implement them
    return false;
}

pba_state_t ReplicaStateMachine::update_map_pba(const fully_qualified_pba&, pba_state_t&) {
    // TODO: Implement them
    return pba_state_t::unknown;
}

void ReplicaStateMachine::remove_map_pba(const fully_qualified_pba&) {
    // TODO: Implement them
    return;
}

void ReplicaStateMachine::create_snapshot(nuraft::snapshot& s, nuraft::async_result< bool >::handler_type& when_done) {
    RS_LOG(DEBUG, "create_snapshot {}/{}", s.get_last_log_idx(), s.get_last_log_term());
    auto null_except = std::shared_ptr< std::exception >();
    auto ret_val{false};
    if (when_done) when_done(ret_val, null_except);
}

} // namespace home_replication
