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

homestore::ReplResult< homestore::blk_alloc_hints > get_blk_alloc_hints(sisl::blob const& header, uint32_t data_size) {
    return homestore::blk_alloc_hints();
}

void on_destroy(const homestore::group_id_t& group_id) {}

} // namespace homeblocks
