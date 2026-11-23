#pragma once

// The donor's side of a CRAFT snapshot (docs/craft/resync.md, section 4.9, "The object stream"): answers nuraft's
// requests for objects by id, the way HomeObject's PGBlobIterator does. One reader lives in nuraft's user_ctx for
// the length of a transfer.
//
// It reads the volume through snapshot_source, so its cursor, batching and checksum handling are tested without
// HomeStore. CraftReplDev provides the real source: its LBA index and its block reads.

#include <cstdint>
#include <optional>
#include <vector>

#include <homestore/blk.hpp>

#include "craft_snapshot.hpp"

namespace homeblocks::craft_snapshot {

// One mapped LBA as the index holds it.
struct index_entry {
    lba_t lba{0};
    homestore::blk_id blkid{};
    homestore::csum_t csum{0};
};

// What the reader needs from the volume. Calls block (read_snapshot_obj is synchronous); they run on nuraft's
// snapshot thread, never on a reactor.
class snapshot_source {
public:
    virtual uint32_t lba_size() const = 0;
    virtual uint64_t total_lbas() const = 0;
    // The member's highest appended dLSN now.
    virtual int64_t last_append_lsn() const = 0;
    // The mapped LBAs in [start, end), ascending; nullopt if the index can't be read.
    virtual std::optional< std::vector< index_entry > > read_index(lba_t start, lba_t end) = 0;
    // Reads the contiguous run `blocks` into buf (blocks.blk_count() * lba_size() bytes). False on an IO error.
    virtual bool read_blocks(homestore::blk_id const& blocks, uint8_t* buf) = 0;
    virtual ~snapshot_source() = default;
};

class snapshot_reader {
public:
    // meta: object 0 | 0, built from the context's persisted capture (the same bytes on every retry). batch_bytes:
    // the data budget of one batch (a batch carries at least one LBA). crc_seed: the seed the volume's checksums use.
    snapshot_reader(snapshot_meta meta, snapshot_source& source, uint64_t batch_bytes, homestore::csum_t crc_seed);

    enum class status : uint8_t {
        ok,         // blob holds the object
        last,       // LAST_OBJ_ID: the end of the snapshot, blob empty
        bad_cursor, // an id the reader can't serve from where it is: it reset, blob empty (the receiver asks again)
        error,      // reading the volume failed: nuraft retries the same id later
    };
    struct reply {
        status st{status::error};
        std::vector< uint8_t > blob{};
    };

    // The object `id`. Accepted: the id served last (a resend, rebuilt over the same range), the next one, 0 | 0, the
    // first batch of any region (the receiver resuming or restarting it), and LAST_OBJ_ID.
    reply serve(uint64_t id);

    // Counters, for metrics and tests.
    uint64_t resends() const { return resends_; }
    uint64_t checksum_retries() const { return checksum_retries_; }

private:
    std::optional< obj_id > expected_next() const;
    bool accept(obj_id id);
    std::optional< region_batch > build_batch(uint64_t region, uint32_t batch);
    // Reads `entries` (mapped, ascending, one region's) into data; drops an LBA that turns out to be unmapped now, and
    // retries one whose block changed under it. False on an IO error or a checksum that stays wrong.
    bool read_entries(std::vector< index_entry >& entries, std::vector< uint8_t >& data);
    bool reread_one(index_entry& e, uint8_t* buf, bool& dropped);

    snapshot_meta meta_;
    std::vector< uint8_t > meta_blob_;
    snapshot_source& source_;
    uint64_t batch_bytes_;
    homestore::csum_t crc_seed_;

    std::optional< obj_id > cur_{}; // the id served last; nullopt before the first
    bool cur_last_in_region_{false};
    // Start LBA of each batch of the current region: starts_[b - 1] is batch b's. Kept so a resend rebuilds the same
    // range.
    std::vector< lba_t > starts_{};
    uint64_t resends_{0};
    uint64_t checksum_retries_{0};
};

} // namespace homeblocks::craft_snapshot
