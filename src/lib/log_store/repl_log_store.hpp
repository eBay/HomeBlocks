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
public:
    template < typename... Args >
    ReplicaLogStore(Args&&... args) : LogStoreImplT{std::forward< Args >(args)...} {}

    uint64_t append(nuraft::ptr< nuraft::log_entry >& entry) override {
        // TODO: Identify the journal pba entry from the log_entry and then translate the fq_pba to local_pba and then
        // call the impl append.
        return LogStoreImplT::append(entry);
    }

    void write_at(ulong index, nuraft::ptr< nuraft::log_entry >& entry) override {
        // TODO: Identify the journal pba entry from the log_entry and then translate the fq_pba to local_pba and then
        // call the impl write_at.
        LogStoreImplT::write_at(index, entry);
    }
};

} // namespace home_replication