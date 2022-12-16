#pragma once

#include <libnuraft/nuraft.hxx>

namespace home_replication {

class hr_state_machine : public nuraft::state_machine {

public:
    hr_state_machine() = default;
    ~hr_state_machine() override = default;
    hr_state_machine(hr_state_machine const&) = delete;
    hr_state_machine& operator=(hr_state_machine const&) = delete;

    /// NuRaft overrides
    uint64_t last_commit_index() override;
    nuraft::ptr< nuraft::buffer > commit(uint64_t log_idx, nuraft::buffer& data) override;
    bool apply_snapshot(nuraft::snapshot&) override { return false; }
    void create_snapshot(nuraft::snapshot& s, nuraft::async_result< bool >::handler_type& when_done) override;
    nuraft::ptr< nuraft::snapshot > last_snapshot() override { return nullptr; }
};

}
