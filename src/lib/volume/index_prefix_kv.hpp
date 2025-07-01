
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
#pragma once

#include <homestore/btree/btree_kv.hpp>
#include <homestore/index_service.hpp>
#include <homestore/index/index_internal.hpp>
#include <homestore/blk.h>
#include <sisl/fds/buffer.hpp>

using homestore::BlkId;
using homestore::BtreeKey;
using homestore::BtreeValue;

namespace homeblocks {
using lba_t = std::uint64_t;

struct BlockInfo {
    // Checksum calculated on new data and written to new_blkid.
    homestore::BlkId new_blkid;
    homestore::BlkId old_blkid;
    homestore::csum_t new_checksum;
};

struct IndexValueContext {
    std::unordered_map< lba_t, BlockInfo >* block_info;
    lba_t start_lba;
};

class VolumeIndexKey : public homestore::BtreeIntervalKey {
private:
#pragma pack(1)
    // We use lba as the key with 32 bit LSB used as suffix and 32 bit MSB used as prefix.
    uint32_t m_lba_base{0};
    uint32_t m_lba_offset{0};
#pragma pack()

public:
    VolumeIndexKey() = default;
    VolumeIndexKey(lba_t k) {
        m_lba_base = uint32_cast(k >> 32);
        m_lba_offset = uint32_cast(k & 0xFFFFFFFF);
    }

    VolumeIndexKey(uint32_t b, uint32_t o) : m_lba_base{b}, m_lba_offset{o} {}
    VolumeIndexKey(const VolumeIndexKey& other) = default;
    VolumeIndexKey(const BtreeKey& other) : VolumeIndexKey(other.serialize(), true) {}
    VolumeIndexKey(const sisl::blob& b, bool copy) : homestore::BtreeIntervalKey() {
        VolumeIndexKey const* other = r_cast< VolumeIndexKey const* >(b.cbytes());
        m_lba_base = other->m_lba_base;
        m_lba_offset = other->m_lba_offset;
    }

    VolumeIndexKey& operator=(VolumeIndexKey const& other) {
        m_lba_base = other.m_lba_base;
        m_lba_offset = other.m_lba_offset;
        return *this;
    };
    virtual ~VolumeIndexKey() = default;

    /////////////////// Overriding methods of BtreeKey /////////////////
    int compare(homestore::BtreeKey const& o) const override {
        VolumeIndexKey const& other = s_cast< VolumeIndexKey const& >(o);
        if (m_lba_base < other.m_lba_base) {
            return -1;
        } else if (m_lba_base > other.m_lba_base) {
            return 1;
        } else if (m_lba_offset < other.m_lba_offset) {
            return -1;
        } else if (m_lba_offset > other.m_lba_offset) {
            return 1;
        } else {
            return 0;
        }
    }

    sisl::blob serialize() const override {
        return sisl::blob{uintptr_cast(const_cast< VolumeIndexKey* >(this)), uint32_cast(sizeof(VolumeIndexKey))};
    }

    uint32_t serialized_size() const override { return sizeof(VolumeIndexKey); }

    void deserialize(sisl::blob const& b, bool copy) override {
        assert(b.size() == sizeof(VolumeIndexKey));
        VolumeIndexKey const* other = r_cast< VolumeIndexKey const* >(b.cbytes());
        m_lba_base = other->m_lba_base;
        m_lba_offset = other->m_lba_offset;
    }

    std::string to_string() const override { return fmt::format("{}", key()); }

    static uint32_t get_max_size() { return sizeof(VolumeIndexKey); }

    static bool is_fixed_size() { return true; }

    static uint32_t get_fixed_size() { return sizeof(VolumeIndexKey); }

    /////////////////// Overriding methods of BtreeIntervalKey /////////////////

    void shift(int n, void* app_ctx) override {
        // Increment the suffix part for lba.
        m_lba_offset += n;
    }

    int distance(BtreeKey const& f) const override {
        VolumeIndexKey const& from = s_cast< VolumeIndexKey const& >(f);
        DEBUG_ASSERT_EQ(m_lba_base, from.m_lba_base, "Invalid from key for distance");
        DEBUG_ASSERT_GE(m_lba_offset, from.m_lba_offset, "Invalid from key for distance");
        return m_lba_offset - from.m_lba_offset;
    }

    bool is_interval_key() const override { return true; }

    sisl::blob serialize_prefix() const override {
        return sisl::blob{uintptr_cast(const_cast< uint32_t* >(&m_lba_base)), uint32_cast(sizeof(uint32_t))};
    }

    sisl::blob serialize_suffix() const override {
        return sisl::blob{uintptr_cast(const_cast< uint32_t* >(&m_lba_offset)), uint32_cast(sizeof(uint32_t))};
    }

    uint32_t serialized_prefix_size() const override { return uint32_cast(sizeof(uint32_t)); }

    uint32_t serialized_suffix_size() const override { return uint32_cast(sizeof(uint32_t)); };

    void deserialize(sisl::blob const& prefix, sisl::blob const& suffix, bool) {
        DEBUG_ASSERT_EQ(prefix.size(), sizeof(uint32_t), "Invalid prefix size on deserialize");
        DEBUG_ASSERT_EQ(suffix.size(), sizeof(uint32_t), "Invalid suffix size on deserialize");
        uint32_t const* other_p = r_cast< uint32_t const* >(prefix.cbytes());
        m_lba_base = *other_p;

        uint32_t const* other_s = r_cast< uint32_t const* >(suffix.cbytes());
        m_lba_offset = *other_s;
    }

    /////////////////// Local methods for helping tests //////////////////
    bool operator<(const VolumeIndexKey& o) const { return (compare(o) < 0); }
    bool operator==(const VolumeIndexKey& other) const { return (compare(other) == 0); }

    lba_t key() const { return (uint64_cast(m_lba_base) << 32) | m_lba_offset; }
    lba_t start_key(const homestore::BtreeKeyRange< VolumeIndexKey >& range) const {
        const VolumeIndexKey& k = (const VolumeIndexKey&)(range.start_key());
        return k.key();
    }

    lba_t end_key(const homestore::BtreeKeyRange< VolumeIndexKey >& range) const {
        const VolumeIndexKey& k = (const VolumeIndexKey&)(range.end_key());
        return k.key();
    }

    friend std::ostream& operator<<(std::ostream& os, const VolumeIndexKey& k) {
        os << k.to_string();
        return os;
    }

    friend std::istream& operator>>(std::istream& is, VolumeIndexKey& k) {
        uint32_t m_lba_base;
        uint32_t m_lba_offset;
        char dummy;
        is >> m_lba_base >> dummy >> m_lba_offset;
        k = VolumeIndexKey{m_lba_base, m_lba_offset};
        return is;
    }
};

class VolumeIndexValue : public homestore::BtreeIntervalValue {
private:
#pragma pack(1)
    // Store blkid and checksum as the value. Most significant 32 bits of BlkId contains chunk_num
    // and num_blks which is same in a single blkid and used as the prefix. Checksum and least significant 32 bits which
    // contains blk num are unique and used as suffix. Ignore the multiblkid bit.
    uint32_t m_blkid_prefix;
    uint32_t m_blkid_suffix;
    homestore::csum_t m_checksum;
#pragma pack()

public:
    VolumeIndexValue(const BlkId& base_blkid, homestore::csum_t csum) : homestore::BtreeIntervalValue() {
        m_blkid_suffix = uint32_cast(base_blkid.to_integer() & 0xFFFFFFFF) >> 1;
        m_blkid_prefix = uint32_cast(base_blkid.to_integer() >> 32);
        m_checksum = csum;
    }
    VolumeIndexValue(const BlkId& base_blkid) : VolumeIndexValue(base_blkid, 0) {}
    VolumeIndexValue() = default;
    VolumeIndexValue(const VolumeIndexValue& other) :
            homestore::BtreeIntervalValue(),
            m_blkid_prefix(other.m_blkid_prefix),
            m_blkid_suffix(other.m_blkid_suffix),
            m_checksum(other.m_checksum) {}
    VolumeIndexValue(const sisl::blob& b, bool copy) : homestore::BtreeIntervalValue() { this->deserialize(b, copy); }
    virtual ~VolumeIndexValue() = default;

    homestore::BlkId blkid() const {
        homestore::blk_num_t blk_num = m_blkid_suffix;
        homestore::chunk_num_t chunk_num = m_blkid_prefix >> 16;
        homestore::blk_count_t nblks = m_blkid_prefix & 0xFFFF;
        return BlkId{blk_num, nblks, chunk_num};
    }

    homestore::csum_t checksum() const { return m_checksum; }

    ///////////////////////////// Overriding methods of BtreeValue //////////////////////////
    VolumeIndexValue& operator=(const VolumeIndexValue& other) = default;
    sisl::blob serialize() const override {
        sisl::blob b{r_cast< uint8_t const* >(this), sizeof(VolumeIndexValue)};
        return b;
    }

    uint32_t serialized_size() const override { return sizeof(VolumeIndexValue); }
    static uint32_t get_fixed_size() { return sizeof(VolumeIndexValue); }
    void deserialize(const sisl::blob& b, bool) {
        VolumeIndexValue const* other = r_cast< VolumeIndexValue const* >(b.cbytes());
        m_blkid_prefix = other->m_blkid_prefix;
        m_blkid_suffix = other->m_blkid_suffix;
        m_checksum = other->m_checksum;
    }

    std::string to_string() const override { return fmt::format("{} csum={}", blkid().to_string(), m_checksum); }

    friend std::ostream& operator<<(std::ostream& os, const VolumeIndexValue& v) {
        os << v.to_string();
        return os;
    }

    friend std::istream& operator>>(std::istream& is, VolumeIndexValue& v) {
        uint32_t base_val;
        uint32_t offset;
        char dummy;
        is >> base_val >> dummy >> offset;
        v = VolumeIndexValue{BlkId{}};
        return is;
    }

    ///////////////////////////// Overriding methods of BtreeIntervalValue //////////////////////////
    void shift(int n, void* app_ctx) override {
        auto ctx = r_cast< IndexValueContext* >(app_ctx);
        DEBUG_ASSERT(ctx, "Context null");

        // Get the next blk num and checksum
        m_blkid_suffix += n;
        auto curr_lba = ctx->start_lba + n;
        DEBUG_ASSERT(ctx->block_info->find(curr_lba) != ctx->block_info->end(), "Invalid index");
        m_checksum = (*ctx->block_info)[curr_lba].new_checksum;
    }

    sisl::blob serialize_prefix() const override {
        return sisl::blob{uintptr_cast(const_cast< uint32_t* >(&m_blkid_prefix)), uint32_cast(sizeof(uint32_t))};
    }
    sisl::blob serialize_suffix() const override {
        // Include both m_blkid_suffix and checksum in the suffix.
        return sisl::blob{uintptr_cast(const_cast< uint32_t* >(&m_blkid_suffix)),
                          uint32_cast(sizeof(uint32_t) + sizeof(homestore::csum_t))};
    }
    uint32_t serialized_prefix_size() const override { return uint32_cast(sizeof(uint32_t)); }
    uint32_t serialized_suffix_size() const override {
        return uint32_cast(sizeof(uint32_t) + sizeof(homestore::csum_t));
    }

    void deserialize(sisl::blob const& prefix, sisl::blob const& suffix, bool) override {
        DEBUG_ASSERT_EQ(prefix.size(), sizeof(uint32_t), "Invalid prefix size on deserialize");
        DEBUG_ASSERT_EQ(suffix.size(), sizeof(uint32_t) + sizeof(homestore::csum_t),
                        "Invalid suffix size on deserialize");
        m_blkid_prefix = *(r_cast< uint32_t const* >(prefix.cbytes()));
        m_blkid_suffix = *(r_cast< uint32_t const* >(suffix.cbytes()));
        m_checksum = *(r_cast< homestore::csum_t const* >(suffix.cbytes() + sizeof(uint32_t)));
    }

    bool operator==(VolumeIndexValue const& other) const {
        return ((m_blkid_prefix == other.m_blkid_prefix) && (m_blkid_suffix == other.m_blkid_suffix) &&
                (m_checksum == other.m_checksum));
    }
};
} // namespace homeblocks