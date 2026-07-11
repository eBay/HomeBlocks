/*********************************************************************************
 * Modifications Copyright 2026 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
#pragma once

// The craft_replica interface + its peer-only types now live in the standalone `craft_client` package
// (namespace `craft`). This is a thin re-export shim so homeblocks' backend (CraftReplDev) and volume glue keep
// referring to `homeblocks::craft_replica` / `JournalSlot` / `CraftPartitionState` unchanged. The interface's
// async_result<T> == homeblocks' `using homestore::async_result` (both sisl::async::result<T>), so a
// homeblocks-held craft_replica interoperates with zero conversion.

#include <craft/replica.hpp>

namespace homeblocks {
using craft::craft_replica;
using craft::CraftPartitionState;
using craft::JournalSlot;
} // namespace homeblocks
