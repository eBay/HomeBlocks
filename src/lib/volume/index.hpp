#pragma once

#include <homestore/btree/btree_kv.hpp>
#include <homestore/index_service.hpp>
#include <homestore/index/index_internal.hpp>
#include <homestore/blk.h>
#include <sisl/fds/buffer.hpp>

namespace homeblocks {

class VolumeIndexKey : public homestore::BtreeIntervalKey {
private:
#pragma pack(1)
    uint32_t m_base{0};
    uint32_t m_offset{0};
#pragma pack()

public:
    VolumeIndexKey() = default;
    VolumeIndexKey(uint64_t k) {
        m_base = uint32_cast(k >> 32);
        m_offset = uint32_cast(k & 0xFFFFFFFF);
    }
    VolumeIndexKey(uint32_t b, uint32_t o) : m_base{b}, m_offset{o} {}
    VolumeIndexKey(const VolumeIndexKey& other) = default;
    VolumeIndexKey(const homestore::BtreeKey& other) : VolumeIndexKey(other.serialize(), true) {}
    VolumeIndexKey(const sisl::blob& b, bool copy) : homestore::BtreeIntervalKey() {
        VolumeIndexKey const* other = r_cast< VolumeIndexKey const* >(b.cbytes());
        m_base = other->m_base;
        m_offset = other->m_offset;
    }

    VolumeIndexKey& operator=(VolumeIndexKey const& other) {
        m_base = other.m_base;
        m_offset = other.m_offset;
        return *this;
    };
    virtual ~VolumeIndexKey() = default;

    /////////////////// Overriding methods of BtreeKey /////////////////
    int compare(homestore::BtreeKey const& o) const override {
        VolumeIndexKey const& other = s_cast< VolumeIndexKey const& >(o);
        if (m_base < other.m_base) {
            return -1;
        } else if (m_base > other.m_base) {
            return 1;
        } else if (m_offset < other.m_offset) {
            return -1;
        } else if (m_offset > other.m_offset) {
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
        m_base = other->m_base;
        m_offset = other->m_offset;
    }

    std::string to_string() const override { return fmt::format("{}.{}", m_base, m_offset); }

    static uint32_t get_max_size() { return sizeof(VolumeIndexKey); }

    static bool is_fixed_size() { return true; }

    static uint32_t get_fixed_size() { return sizeof(VolumeIndexKey); }

    /////////////////// Overriding methods of BtreeIntervalKey /////////////////
    void shift(int n) override { m_offset += n; }

    int distance(homestore::BtreeKey const& f) const override {
        VolumeIndexKey const& from = s_cast< VolumeIndexKey const& >(f);
        DEBUG_ASSERT_EQ(m_base, from.m_base, "Invalid from key for distance");
        DEBUG_ASSERT_GE(m_offset, from.m_offset, "Invalid from key for distance");
        return m_offset - from.m_offset;
    }

    bool is_interval_key() const override { return true; }

    sisl::blob serialize_prefix() const override {
        return sisl::blob{uintptr_cast(const_cast< uint32_t* >(&m_base)), uint32_cast(sizeof(uint32_t))};
    }

    sisl::blob serialize_suffix() const override {
        return sisl::blob{uintptr_cast(const_cast< uint32_t* >(&m_offset)), uint32_cast(sizeof(uint32_t))};
    }

    uint32_t serialized_prefix_size() const override { return uint32_cast(sizeof(uint32_t)); }

    uint32_t serialized_suffix_size() const override { return uint32_cast(sizeof(uint32_t)); };

    void deserialize(sisl::blob const& prefix, sisl::blob const& suffix, bool) {
        DEBUG_ASSERT_EQ(prefix.size(), sizeof(uint32_t), "Invalid prefix size on deserialize");
        DEBUG_ASSERT_EQ(suffix.size(), sizeof(uint32_t), "Invalid suffix size on deserialize");
        uint32_t const* other_p = r_cast< uint32_t const* >(prefix.cbytes());
        m_base = *other_p;

        uint32_t const* other_s = r_cast< uint32_t const* >(suffix.cbytes());
        m_offset = *other_s;
    }

    /////////////////// Local methods for helping tests //////////////////
    bool operator<(const VolumeIndexKey& o) const { return (compare(o) < 0); }
    bool operator==(const VolumeIndexKey& other) const { return (compare(other) == 0); }

    uint64_t key() const { return (uint64_cast(m_base) << 32) | m_offset; }
    uint64_t start_key(const homestore::BtreeKeyRange< VolumeIndexKey >& range) const {
        const VolumeIndexKey& k = (const VolumeIndexKey&)(range.start_key());
        return k.key();
    }
    uint64_t end_key(const homestore::BtreeKeyRange< VolumeIndexKey >& range) const {
        const VolumeIndexKey& k = (const VolumeIndexKey&)(range.end_key());
        return k.key();
    }
    friend std::ostream& operator<<(std::ostream& os, const VolumeIndexKey& k) {
        os << k.to_string();
        return os;
    }

    friend std::istream& operator>>(std::istream& is, VolumeIndexKey& k) {
        uint32_t m_base;
        uint32_t m_offset;
        char dummy;
        is >> m_base >> dummy >> m_offset;
        k = VolumeIndexKey{m_base, m_offset};
        return is;
    }
};

class VolumeIndexValue : public homestore::BtreeIntervalValue {
private:
#pragma pack(1)
    uint32_t m_base_val{0};
    uint16_t m_offset{0};
#pragma pack()

public:
    VolumeIndexValue(homestore::bnodeid_t val) { assert(0); }
    VolumeIndexValue(uint32_t val, uint16_t o) : homestore::BtreeIntervalValue(), m_base_val{val}, m_offset{o} {}
    VolumeIndexValue() = default;
    VolumeIndexValue(const VolumeIndexValue& other) :
            homestore::BtreeIntervalValue(), m_base_val{other.m_base_val}, m_offset{other.m_offset} {}
    VolumeIndexValue(const sisl::blob& b, bool copy) : homestore::BtreeIntervalValue() { this->deserialize(b, copy); }
    virtual ~VolumeIndexValue() = default;

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
        m_base_val = other->m_base_val;
        m_offset = other->m_offset;
    }

    std::string to_string() const override { return fmt::format("{}.{}", m_base_val, m_offset); }

    friend std::ostream& operator<<(std::ostream& os, const VolumeIndexValue& v) {
        os << v.to_string();
        return os;
    }

    friend std::istream& operator>>(std::istream& is, VolumeIndexValue& v) {
        uint32_t m_base_val;
        uint16_t m_offset;
        char dummy;
        is >> m_base_val >> dummy >> m_offset;
        v = VolumeIndexValue{m_base_val, m_offset};
        return is;
    }

    ///////////////////////////// Overriding methods of BtreeIntervalValue //////////////////////////
    void shift(int n) override { m_offset += n; }

    sisl::blob serialize_prefix() const override {
        return sisl::blob{uintptr_cast(const_cast< uint32_t* >(&m_base_val)), uint32_cast(sizeof(uint32_t))};
    }
    sisl::blob serialize_suffix() const override {
        return sisl::blob{uintptr_cast(const_cast< uint16_t* >(&m_offset)), uint32_cast(sizeof(uint16_t))};
    }
    uint32_t serialized_prefix_size() const override { return uint32_cast(sizeof(uint32_t)); }
    uint32_t serialized_suffix_size() const override { return uint32_cast(sizeof(uint16_t)); }

    void deserialize(sisl::blob const& prefix, sisl::blob const& suffix, bool) override {
        DEBUG_ASSERT_EQ(prefix.size(), sizeof(uint32_t), "Invalid prefix size on deserialize");
        DEBUG_ASSERT_EQ(suffix.size(), sizeof(uint16_t), "Invalid suffix size on deserialize");
        m_base_val = *(r_cast< uint32_t const* >(prefix.cbytes()));
        m_offset = *(r_cast< uint16_t const* >(suffix.cbytes()));
    }

    bool operator==(VolumeIndexValue const& other) const {
        return ((m_base_val == other.m_base_val) && (m_offset == other.m_offset));
    }
};
} // namespace homeblocks
