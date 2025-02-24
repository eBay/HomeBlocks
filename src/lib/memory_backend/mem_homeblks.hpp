#pragma once

#include <atomic>
#include <utility>

#include <folly/concurrency/ConcurrentHashMap.h>
#include "lib/homeblks_impl.hpp"

namespace homeblocks {

class MemoryHomeBlocks : public HomeBlocksImpl {
public:
    MemoryHomeBlocks(std::weak_ptr< HomeBlocksApplication >&& application);
    ~MemoryHomeBlocks() override = default;
};

} // namespace homeblocks
