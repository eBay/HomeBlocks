#include "repl_set_impl.h"

#include <sisl/fds/obj_allocator.hpp>
#include <sisl/fds/vector_pool.hpp>
#include <sisl/grpc/generic_service.hpp>
#include "state_machine/state_machine.h"
#include "log_store/repl_log_store.hpp"
#include "log_store/journal_entry.h"
#include "storage/storage_engine.h"
#include "rpc_data_channel_include.h"

namespace home_replication {
ReplicaSetImpl::ReplicaSetImpl(const std::string& group_id, const std::shared_ptr< StateMachineStore >& sm_store,
                               const std::shared_ptr< nuraft::log_store >& log_store) :
        m_state_machine{nullptr}, m_state_store{sm_store}, m_data_journal{log_store}, m_group_id{group_id} {}

void ReplicaSetImpl::write(const sisl::blob& header, const sisl::blob& key, const sisl::sg_list& value,
                           void* user_ctx) {
    m_state_machine->propose(header, key, value, user_ctx);
}

void ReplicaSetImpl::transfer_pba_ownership(int64_t lsn, const pba_list_t& pbas) {
    m_state_store->add_free_pba_record(lsn, pbas);
}

std::shared_ptr< nuraft::state_machine > ReplicaSetImpl::get_state_machine() {
    if (!m_state_machine) m_state_machine = std::make_shared< ReplicaStateMachine >(m_state_store, this);
    return std::dynamic_pointer_cast< nuraft::state_machine >(m_state_machine);
}

bool ReplicaSetImpl::is_leader() const { return m_repl_svc_ctx->is_raft_leader(); }

void ReplicaSetImpl::send_data_service_response(sisl::io_blob_list_t const& outgoing_buf,
                                                boost::intrusive_ptr< sisl::GenericRpcData >& rpc_data) {
    m_repl_svc_ctx->send_data_service_response(outgoing_buf, rpc_data);
}

bool ReplicaSetImpl::register_data_service_apis(std::shared_ptr< nuraft_mesg::consensus_component >& messaging) {
    if (auto resp = messaging->bind_data_service_request(
            SEND_DATA, m_group_id,
            [this](sisl::io_blob const& incoming_buf, boost::intrusive_ptr< sisl::GenericRpcData >& rpc_data) {
                m_state_machine->on_data_received(incoming_buf, rpc_data);
            });
        !resp) {
        // LOG ERROR
        return false;
    }

    if (auto resp = messaging->bind_data_service_request(
            FETCH_DATA, m_group_id,
            [this](sisl::io_blob const& incoming_buf, boost::intrusive_ptr< sisl::GenericRpcData >& rpc_data) {
                m_state_machine->on_fetch_data_request(incoming_buf, rpc_data);
            });
        !resp) {
        // LOG ERROR
        return false;
    }

    return true;
}

void ReplicaSetImpl::send_in_data_channel(const pba_list_t& pbas, const sisl::sg_list& value) {
    m_repl_svc_ctx->data_service_request(
        SEND_DATA,
        data_rpc::serialize(data_channel_rpc_hdr{m_group_id, 0 /*replace with replica id*/}, pbas, m_state_store.get(),
                            value),
        nullptr); // response callback is null as this is fire and forget
}

void ReplicaSetImpl::fetch_pba_data_from_leader(const pba_list_t& remote_pbas) {
    m_repl_svc_ctx->data_service_request(
        FETCH_DATA,
        data_rpc::serialize(data_channel_rpc_hdr{m_group_id, 0 /*replace with replica id*/}, remote_pbas,
                            m_state_store.get(), {}),
        [this](sisl::io_blob const& incoming_buf) {
            auto null_rpc_data = boost::intrusive_ptr< sisl::GenericRpcData >(nullptr);
            m_state_machine->on_data_received(incoming_buf, null_rpc_data);
        });
}

} // namespace home_replication
