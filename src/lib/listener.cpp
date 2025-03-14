
/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
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
#include "listener.hpp"

namespace homeblocks {

void HBListener::on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                           homestore::MultiBlkId const& blkids, cintrusive< homestore::repl_req_ctx >& ctx) {}

bool HBListener::on_pre_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                               cintrusive< homestore::repl_req_ctx >& ctx) {
    return true;
}

void HBListener::on_error(homestore::ReplServiceError error, const sisl::blob& header, const sisl::blob& key,
                          cintrusive< homestore::repl_req_ctx >& ctx) {}

homestore::ReplResult< homestore::blk_alloc_hints > HBListener::get_blk_alloc_hints(sisl::blob const& header,
                                                                                    uint32_t data_size) {
    return homestore::blk_alloc_hints();
}

void HBListener::on_destroy(const homestore::group_id_t& group_id) {}

} // namespace homeblocks
