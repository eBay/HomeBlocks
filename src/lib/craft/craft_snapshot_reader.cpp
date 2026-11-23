#include "craft_snapshot_reader.hpp"

#include <algorithm>
#include <limits>

#include <homestore/crc.hpp>

#include "../hb_internal.hpp" // LOGE

namespace homeblocks::craft_snapshot {

namespace {
// A block whose checksum is wrong is looked up again this many times: a concurrent overwrite may have freed and reused
// it after the index was read. One that stays wrong under the same block id is corruption.
constexpr int k_reread_attempts = 3;
// The fewest LBAs one index read covers while a batch is collected.
constexpr uint64_t k_min_window = 16 * 1024;
} // namespace

snapshot_reader::snapshot_reader(snapshot_meta meta, snapshot_source& source, uint64_t batch_bytes,
                                 homestore::csum_t crc_seed) :
        meta_{std::move(meta)},
        meta_blob_{encode(meta_)},
        source_{source},
        batch_bytes_{std::max< uint64_t >(batch_bytes, 1)},
        crc_seed_{crc_seed} {}

std::optional< obj_id > snapshot_reader::expected_next() const {
    auto const regions = meta_.region_count;
    auto const trailer = obj_id{.region = regions + 1, .batch = 0};
    if (!cur_) return obj_id{};
    if (cur_->region == 0) return regions > 0 ? obj_id{.region = 1, .batch = 1} : trailer;
    if (cur_->region > regions) return std::nullopt; // after the trailer only LAST_OBJ_ID
    if (!cur_last_in_region_) return obj_id{.region = cur_->region, .batch = cur_->batch + 1};
    return cur_->region < regions ? obj_id{.region = cur_->region + 1, .batch = 1} : trailer;
}

bool snapshot_reader::accept(obj_id id) {
    if (cur_ && id == *cur_) {
        ++resends_;
        return true;
    }
    if (id == obj_id{}) return true;                                                     // from the start
    if (id.batch == 1 && id.region >= 1 && id.region <= meta_.region_count) return true; // (re)start a region
    auto const next = expected_next();
    return next && id == *next;
}

snapshot_reader::reply snapshot_reader::serve(uint64_t raw) {
    if (raw == LAST_OBJ_ID) return reply{.st = status::last};
    auto const id = obj_id::unpack(raw);
    if (!id || !accept(*id)) {
        cur_.reset();
        starts_.clear();
        return reply{.st = status::bad_cursor};
    }
    auto const regions = meta_.region_count;
    if (id->region == 0 && id->batch == 0) {
        cur_ = id;
        starts_.clear();
        return reply{.st = status::ok, .blob = meta_blob_};
    }
    if (id->region == regions + 1 && id->batch == 0) {
        cur_ = id;
        return reply{.st = status::ok, .blob = encode(snapshot_trailer{.w_hi = source_.last_append_lsn()})};
    }
    if (id->region < 1 || id->region > regions || id->batch < 1) {
        cur_.reset();
        starts_.clear();
        return reply{.st = status::bad_cursor};
    }
    auto batch = build_batch(id->region, id->batch);
    if (!batch) return reply{.st = status::error}; // cursor kept: the same id is asked again
    cur_ = id;
    cur_last_in_region_ = batch->last_in_region;
    return reply{.st = status::ok, .blob = encode(*batch)};
}

std::optional< region_batch > snapshot_reader::build_batch(uint64_t region, uint32_t batch) {
    auto const [region_start, region_end] = region_range(region, meta_.total_lbas, meta_.region_lbas);
    if (batch == 1) starts_.assign(1, region_start);
    if (starts_.size() < batch) return std::nullopt; // accept() only lets through a batch whose start is known
    auto const start = starts_[batch - 1];
    auto const lba_size = source_.lba_size();

    // At most `max_lbas` mapped LBAs per batch; the last batch a region may have takes all that is left.
    uint64_t max_lbas = std::max< uint64_t >(batch_bytes_ / lba_size, 1);
    if (batch >= k_max_batch) max_lbas = std::numeric_limits< uint64_t >::max();

    // Read the index in windows, so a batch costs about what it carries, not the rest of its region; at least
    // k_min_window LBAs each, so a sparse region isn't swept a few LBAs at a time.
    std::vector< index_entry > entries;
    lba_t cursor = start;
    auto const window = std::max< uint64_t >(std::min< uint64_t >(max_lbas, region_end), k_min_window);
    while (entries.size() <= max_lbas && cursor < region_end) {
        auto const window_end = std::min< lba_t >(region_end, cursor + window);
        auto found = source_.read_index(cursor, window_end);
        if (!found) return std::nullopt;
        entries.insert(entries.end(), found->begin(), found->end());
        cursor = window_end;
    }
    lba_t end = cursor;
    if (entries.size() > max_lbas) {
        end = entries[max_lbas].lba; // the next batch starts at the first LBA this one leaves out
        entries.resize(max_lbas);
    }

    region_batch out{.region = region,
                     .batch = batch,
                     .start = start,
                     .end = end,
                     .last_in_region = end >= region_end,
                     .lba_size = lba_size};
    if (!read_entries(entries, out.data)) return std::nullopt;
    out.lbas.reserve(entries.size());
    for (auto const& e : entries)
        out.lbas.push_back(snapshot_lba{.lba = e.lba, .csum = e.csum});
    if (!out.last_in_region) {
        starts_.resize(std::max< size_t >(starts_.size(), batch + 1));
        starts_[batch] = end;
    }
    return out;
}

bool snapshot_reader::read_entries(std::vector< index_entry >& entries, std::vector< uint8_t >& data) {
    auto const lba_size = source_.lba_size();
    data.assign(entries.size() * lba_size, 0);
    for (size_t i = 0; i < entries.size();) {
        // One read per run of contiguous blocks, as CraftReplDev's read path does.
        size_t j = i + 1;
        while (j < entries.size() && entries[j].blkid.chunk_num() == entries[j - 1].blkid.chunk_num() &&
               entries[j].blkid.blk_num() == entries[j - 1].blkid.blk_num() + 1 &&
               (j - i) < std::numeric_limits< homestore::blk_count_t >::max())
            ++j;
        homestore::blk_id const run{entries[i].blkid.blk_num(), static_cast< homestore::blk_count_t >(j - i),
                                    entries[i].blkid.chunk_num()};
        if (!source_.read_blocks(run, data.data() + i * lba_size)) return false;
        i = j;
    }

    std::vector< bool > dropped(entries.size(), false);
    for (size_t i = 0; i < entries.size(); ++i) {
        auto* buf = data.data() + i * lba_size;
        if (crc16_t10dif(crc_seed_, buf, lba_size) == entries[i].csum) continue;
        bool gone = false;
        if (!reread_one(entries[i], buf, gone)) return false;
        dropped[i] = gone;
    }

    // An LBA unmapped since the index was read is a hole now: a newer version than W, which a fuzzy region may carry.
    size_t kept = 0;
    for (size_t i = 0; i < entries.size(); ++i) {
        if (dropped[i]) continue;
        if (kept != i) {
            entries[kept] = entries[i];
            std::copy_n(data.begin() + i * lba_size, lba_size, data.begin() + kept * lba_size);
        }
        ++kept;
    }
    entries.resize(kept);
    data.resize(kept * lba_size);
    return true;
}

bool snapshot_reader::reread_one(index_entry& e, uint8_t* buf, bool& dropped) {
    auto const lba_size = source_.lba_size();
    for (int attempt = 0; attempt < k_reread_attempts; ++attempt) {
        ++checksum_retries_;
        auto const now = source_.read_index(e.lba, e.lba + 1);
        if (!now) return false;
        if (now->empty()) {
            dropped = true;
            return true;
        }
        auto const& fresh = now->front();
        if (!source_.read_blocks(fresh.blkid, buf)) return false;
        if (crc16_t10dif(crc_seed_, buf, lba_size) == fresh.csum) {
            e = fresh;
            return true;
        }
        if (fresh.blkid == e.blkid && fresh.csum == e.csum && attempt > 0) break; // the same block stays wrong
        e = fresh;
    }
    LOGE("snapshot: lba {} keeps failing its checksum (block {})", e.lba, e.blkid.to_string());
    return false;
}

} // namespace homeblocks::craft_snapshot
