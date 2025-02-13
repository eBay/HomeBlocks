#include "mem_homeblks.hpp"

namespace homeblocks {

/// NOTE: We give ourselves the option to provide a different HR instance here than libhomeblocks.a
extern std::shared_ptr< HomeBlocks > init_homeblocks(std::weak_ptr< HomeBlocksApplication >&& application) {
    return std::make_shared< MemoryHomeBlocks >(std::move(application));
}

MemoryHomeBlocks::MemoryHomeBlocks(std::weak_ptr< HomeBlocksApplication >&& application) :
        HomeBlocksImpl::HomeBlocksImpl(std::move(application)) {
    _our_id = _application.lock()->discover_svcid(std::nullopt);
}

} // namespace homeblocks
