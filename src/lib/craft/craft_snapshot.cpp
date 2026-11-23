#include "craft_snapshot.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <type_traits>

#include <homestore/crc.hpp>
#include <sisl/logging/logging.h>

namespace homeblocks::craft_snapshot {

namespace {

constexpr uint32_t k_crc32_seed = 0;

// Fixed-width little-endian fields, appended in order. Every payload is built and read with these two, so the layout
// is the order of the calls.
class writer {
public:
    template < typename T >
    void put(T v) {
        static_assert(std::is_trivially_copyable_v< T >);
        auto const off = buf_.size();
        buf_.resize(off + sizeof(T));
        std::memcpy(buf_.data() + off, &v, sizeof(T));
    }
    void put_bytes(uint8_t const* p, size_t n) { buf_.insert(buf_.end(), p, p + n); }
    std::vector< uint8_t > take() { return std::move(buf_); }

private:
    std::vector< uint8_t > buf_;
};

class reader {
public:
    reader(uint8_t const* p, size_t n) : p_{p}, n_{n} {}

    template < typename T >
    bool get(T& v) {
        static_assert(std::is_trivially_copyable_v< T >);
        if (n_ - off_ < sizeof(T)) return false;
        std::memcpy(&v, p_ + off_, sizeof(T));
        off_ += sizeof(T);
        return true;
    }
    bool get_bytes(std::vector< uint8_t >& out, size_t n) {
        if (n_ - off_ < n) return false;
        out.assign(p_ + off_, p_ + off_ + n);
        off_ += n;
        return true;
    }
    size_t left() const { return n_ - off_; }
    bool done() const { return off_ == n_; }

private:
    uint8_t const* p_;
    size_t n_;
    size_t off_{0};
};

void put_tuple(writer& w, control_tuple const& t) {
    w.put(t.session_term);
    w.put(t.client_token);
    w.put(t.fence_term);
    w.put(t.aborted_upto);
    w.put(t.data_watermark);
    w.put(t.sync_watermark);
    w.put(static_cast< uint32_t >(t.empty_lsns.size()));
    for (auto const lsn : t.empty_lsns)
        w.put(lsn);
}

bool get_tuple(reader& r, control_tuple& t) {
    uint32_t n{0};
    if (!(r.get(t.session_term) && r.get(t.client_token) && r.get(t.fence_term) && r.get(t.aborted_upto) &&
          r.get(t.data_watermark) && r.get(t.sync_watermark) && r.get(n)))
        return false;
    if (r.left() / sizeof(int64_t) < n) return false;
    t.empty_lsns.resize(n);
    for (auto& lsn : t.empty_lsns)
        if (!r.get(lsn)) return false;
    return true;
}

// The Empty verdicts are ascending and inside (W, S].
bool valid_tuple(control_tuple const& t) {
    if (t.sync_watermark < t.data_watermark && !t.empty_lsns.empty()) return false;
    int64_t prev = t.data_watermark;
    for (auto const lsn : t.empty_lsns) {
        if (lsn <= prev || lsn > t.sync_watermark) return false;
        prev = lsn;
    }
    return true;
}

std::vector< uint8_t > seal(msg_type type, std::vector< uint8_t > payload) {
    RELEASE_ASSERT(payload.size() <= std::numeric_limits< uint32_t >::max(), "snapshot object of {} bytes",
                   payload.size());
    object_header h;
    h.type = static_cast< uint8_t >(type);
    h.payload_size = static_cast< uint32_t >(payload.size());
    h.payload_crc = crc32_ieee(k_crc32_seed, payload.data(), payload.size());
    h.header_crc = 0;
    h.header_crc = crc32_ieee(k_crc32_seed, reinterpret_cast< unsigned char const* >(&h), sizeof(h));
    std::vector< uint8_t > out(sizeof(h) + payload.size());
    std::memcpy(out.data(), &h, sizeof(h));
    if (!payload.empty()) std::memcpy(out.data() + sizeof(h), payload.data(), payload.size());
    return out;
}

std::optional< snapshot_object > decode_meta(reader& r) {
    snapshot_meta m;
    uint32_t def_len{0};
    if (!(r.get(m.raft_idx) && r.get(m.digest) && get_tuple(r, m.tuple) && r.get(m.lba_size) && r.get(m.total_lbas) &&
          r.get(m.region_lbas) && r.get(m.region_count) && r.get(def_len) && r.get_bytes(m.volume_def, def_len) &&
          r.done()))
        return std::nullopt;
    if (!valid_tuple(m.tuple) || tuple_digest(m.tuple) != m.digest) return std::nullopt;
    if (m.lba_size == 0 || m.region_count != region_count(m.total_lbas, m.region_lbas)) return std::nullopt;
    return m;
}

std::optional< snapshot_object > decode_batch(reader& r) {
    region_batch b;
    uint8_t last{0};
    uint32_t n{0};
    if (!(r.get(b.region) && r.get(b.batch) && r.get(b.start) && r.get(b.end) && r.get(last) && r.get(b.lba_size) &&
          r.get(n)))
        return std::nullopt;
    if (b.region == 0 || b.region > k_max_region || b.batch == 0 || b.batch > k_max_batch || b.start > b.end ||
        last > 1 || b.lba_size == 0)
        return std::nullopt;
    b.last_in_region = last == 1;
    if (r.left() / (sizeof(lba_t) + sizeof(homestore::csum_t)) < n) return std::nullopt;
    b.lbas.resize(n);
    lba_t next = b.start;
    for (auto& e : b.lbas) {
        if (!(r.get(e.lba) && r.get(e.csum))) return std::nullopt;
        if (e.lba < next || e.lba >= b.end) return std::nullopt; // ascending, inside the batch
        next = e.lba + 1;
    }
    if (r.left() != static_cast< size_t >(n) * b.lba_size) return std::nullopt;
    if (!r.get_bytes(b.data, r.left())) return std::nullopt;
    return b;
}

std::optional< snapshot_object > decode_trailer(reader& r) {
    snapshot_trailer t;
    if (!(r.get(t.w_hi) && r.done())) return std::nullopt;
    return t;
}

} // namespace

// ── ids ─────────────────────────────────────────────────────────────────────

std::optional< uint64_t > obj_id::pack() const {
    if (region > k_max_region || batch > k_max_batch) return std::nullopt;
    auto const id = k_app_bit | (region << 15) | batch;
    if (id == LAST_OBJ_ID) return std::nullopt; // all ones is the end marker
    return id;
}

std::optional< obj_id > obj_id::unpack(uint64_t id) {
    if ((id & k_app_bit) == 0 || id == LAST_OBJ_ID) return std::nullopt;
    return obj_id{.region = (id >> 15) & k_max_region, .batch = static_cast< uint32_t >(id & k_max_batch)};
}

// ── payloads ────────────────────────────────────────────────────────────────

uint32_t tuple_digest(control_tuple const& t) {
    writer w;
    put_tuple(w, t);
    auto const bytes = w.take();
    return crc32_ieee(k_crc32_seed, bytes.data(), bytes.size());
}

std::vector< uint8_t > encode(snapshot_meta const& m) {
    writer w;
    w.put(m.raft_idx);
    w.put(m.digest);
    put_tuple(w, m.tuple);
    w.put(m.lba_size);
    w.put(m.total_lbas);
    w.put(m.region_lbas);
    w.put(m.region_count);
    w.put(static_cast< uint32_t >(m.volume_def.size()));
    w.put_bytes(m.volume_def.data(), m.volume_def.size());
    return seal(msg_type::meta, w.take());
}

std::vector< uint8_t > encode(region_batch const& b) {
    RELEASE_ASSERT(b.data.size() == b.lbas.size() * b.lba_size, "batch data of {} bytes for {} LBAs of {}",
                   b.data.size(), b.lbas.size(), b.lba_size);
    writer w;
    w.put(b.region);
    w.put(b.batch);
    w.put(b.start);
    w.put(b.end);
    w.put(static_cast< uint8_t >(b.last_in_region ? 1 : 0));
    w.put(b.lba_size);
    w.put(static_cast< uint32_t >(b.lbas.size()));
    for (auto const& e : b.lbas) {
        w.put(e.lba);
        w.put(e.csum);
    }
    w.put_bytes(b.data.data(), b.data.size());
    return seal(msg_type::region_batch, w.take());
}

std::vector< uint8_t > encode(snapshot_trailer const& t) {
    writer w;
    w.put(t.w_hi);
    return seal(msg_type::trailer, w.take());
}

std::optional< snapshot_object > decode(uint8_t const* buf, size_t len) {
    if (buf == nullptr || len < sizeof(object_header)) return std::nullopt;
    object_header h;
    std::memcpy(&h, buf, sizeof(h));
    auto const stored_crc = h.header_crc;
    h.header_crc = 0;
    if (crc32_ieee(k_crc32_seed, reinterpret_cast< unsigned char const* >(&h), sizeof(h)) != stored_crc)
        return std::nullopt;
    if (h.magic != k_magic || h.version != k_version || len - sizeof(h) != h.payload_size) return std::nullopt;
    auto const* payload = buf + sizeof(h);
    if (crc32_ieee(k_crc32_seed, payload, h.payload_size) != h.payload_crc) return std::nullopt;
    reader r{payload, h.payload_size};
    switch (static_cast< msg_type >(h.type)) {
    case msg_type::meta:
        return decode_meta(r);
    case msg_type::region_batch:
        return decode_batch(r);
    case msg_type::trailer:
        return decode_trailer(r);
    }
    return std::nullopt;
}

// ── geometry ────────────────────────────────────────────────────────────────

uint64_t region_count(uint64_t total_lbas, uint64_t region_lbas) {
    if (total_lbas == 0 || region_lbas == 0) return 0;
    return (total_lbas + region_lbas - 1) / region_lbas;
}

std::pair< lba_t, lba_t > region_range(uint64_t r, uint64_t total_lbas, uint64_t region_lbas) {
    if (r == 0 || r > region_count(total_lbas, region_lbas)) return {0, 0};
    auto const start = (r - 1) * region_lbas;
    return {start, std::min(start + region_lbas, total_lbas)};
}

} // namespace homeblocks::craft_snapshot
