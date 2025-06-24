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
    homestore::csum_t old_checksum{0};
};

class VolumeIndexKey : public homestore::BtreeKey {
private:
#pragma pack(1)
    lba_t m_lba{0};
#pragma pack()

public:
    VolumeIndexKey() = default;
    VolumeIndexKey(lba_t k) : m_lba(k) {}
    VolumeIndexKey(const VolumeIndexKey& other) = default;
    VolumeIndexKey(const BtreeKey& other) : VolumeIndexKey(other.serialize(), true) {}
    VolumeIndexKey(const sisl::blob& b, bool copy) : homestore::BtreeKey() {
        VolumeIndexKey const* other = r_cast< VolumeIndexKey const* >(b.cbytes());
        m_lba = other->m_lba;
    }

    VolumeIndexKey& operator=(VolumeIndexKey const& other) {
        m_lba = other.m_lba;
        return *this;
    };
    virtual ~VolumeIndexKey() = default;

    /////////////////// Overriding methods of BtreeKey /////////////////
    int compare(homestore::BtreeKey const& o) const override {
        VolumeIndexKey const& other = s_cast< VolumeIndexKey const& >(o);
        if (m_lba < other.m_lba) {
            return -1;
        } else if (m_lba > other.m_lba) {
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
        m_lba = other->m_lba;
    }

    std::string to_string() const override { return fmt::format("{}", key()); }

    static uint32_t get_max_size() { return sizeof(VolumeIndexKey); }

    static bool is_fixed_size() { return true; }

    static uint32_t get_fixed_size() { return sizeof(VolumeIndexKey); }

    
    /////////////////// Local methods for helping tests //////////////////
    bool operator<(const VolumeIndexKey& o) const { return (compare(o) < 0); }
    bool operator==(const VolumeIndexKey& other) const { return (compare(other) == 0); }

    lba_t key() const { return m_lba; }

    friend std::ostream& operator<<(std::ostream& os, const VolumeIndexKey& k) {
        os << k.to_string();
        return os;
    }

    friend std::istream& operator>>(std::istream& is, VolumeIndexKey& k) {
        lba_t m_lba;
        is >> m_lba;
        k = VolumeIndexKey{m_lba};
        return is;
    }
};

class VolumeIndexValue : public homestore::BtreeValue {
private:
#pragma pack(1)
    // Store blkid and checksum as the value. 
    BlkId m_blkid;
    homestore::csum_t m_checksum;
#pragma pack()

public:
    VolumeIndexValue(const BlkId& base_blkid, homestore::csum_t csum) : 
            homestore::BtreeValue(),
            m_blkid(base_blkid),
            m_checksum(csum) {}
    VolumeIndexValue(const BlkId& base_blkid) : VolumeIndexValue(base_blkid, 0) {}
    VolumeIndexValue() = default;
    VolumeIndexValue(const VolumeIndexValue& other) :
            homestore::BtreeValue(),
            m_blkid(other.m_blkid),
            m_checksum(other.m_checksum) {}
    VolumeIndexValue(const sisl::blob& b, bool copy) : homestore::BtreeValue() { this->deserialize(b, copy); }
    virtual ~VolumeIndexValue() = default;

    homestore::BlkId blkid() const {
        return m_blkid;
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
        m_blkid = other->m_blkid;
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

    bool operator==(VolumeIndexValue const& other) const {
        return ((m_blkid == other.m_blkid) && (m_checksum == other.m_checksum));
    }
};
} // namespace homeblocks