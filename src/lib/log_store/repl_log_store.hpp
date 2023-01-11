#pragma once

#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif
#include <libnuraft/nuraft.hxx>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif
#undef auto_lock

namespace home_replication {

template < typename LogStoreImplT >
class ReplicaLogStore : public LogStoreImplT {
    uint64_t append(ptr< nuraft::log_entry >& entry) override {
        // TODO: Identify the journal pba entry from the log_entry and then translate the fq_pba to local_pba and then
        // call the impl append.
        LogStoreImplT::append(entry);
    }

    void write_at(ulong index, ptr< nuraft::log_entry >& entry) override {
        // TODO: Identify the journal pba entry from the log_entry and then translate the fq_pba to local_pba and then
        // call the impl write_at.
        LogStoreImplT::write_at(index, entry);
    }
};

} // namespace home_replication