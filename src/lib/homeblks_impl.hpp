
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
#include <map>
#include <string>
#include <sisl/logging/logging.h>
#include <sisl/utility/obj_life_counter.hpp>
#include <homestore/crc.h>
#include <homestore/homestore.hpp>
#include <homestore/index/index_table.hpp>
#include <homestore/superblk_handler.hpp>
#include <homeblks/home_blks.hpp>
#include <homeblks/volume_mgr.hpp>
#include <homeblks/common.hpp>
#include "volume/volume.hpp"

namespace homeblocks {

class Volume;

ENUM(HomeBlksMsgType, uint8_t, READ, WRITE, UNMAP);

class HomeBlocksImpl : public HomeBlocks, public VolumeManager, public std::enable_shared_from_this< HomeBlocksImpl > {
    struct homeblks_sb_t {
        uint64_t magic;
        uint32_t version;
        uint32_t flag;
        uint64_t boot_cnt;

        void init_flag(uint32_t f) { flag = f; }
        void set_flag(uint32_t bit) { flag |= bit; }
        void clear_flag(uint32_t bit) { flag &= ~bit; }
        bool test_flag(uint32_t bit) { return flag & bit; }
    };

private:
    inline static auto const HB_META_NAME = std::string("HomeBlks2");
    static constexpr uint64_t HB_SB_MAGIC{0xCEEDDEEB};
    static constexpr uint32_t HB_SB_VER{0x1};
    static constexpr uint64_t HS_CHUNK_SIZE = 2 * Gi;
    static constexpr uint32_t DATA_BLK_SIZE = 4096;
    static constexpr uint32_t SB_FLAGS_GRACEFUL_SHUTDOWN{0x00000001};
    static constexpr uint32_t SB_FLAGS_RESTRICTED{0x00000002};

private:
    /// Our SvcId retrieval and SvcId->IP mapping
    std::weak_ptr< HomeBlocksApplication > _application;
    folly::Executor::KeepAlive<> executor_;

    ///
    mutable std::shared_mutex vol_lock_;
    std::map< volume_id_t, VolumePtr > vol_map_;

    // index table map which only used during recovery;
    mutable std::shared_mutex index_lock_;
    std::unordered_map< std::string, shared< VolumeIndexTable > > idx_tbl_map_;

    bool recovery_done_{false};
    superblk< homeblks_sb_t > sb_;

public:
    explicit HomeBlocksImpl(std::weak_ptr< HomeBlocksApplication >&& application);

    ~HomeBlocksImpl() override;
    HomeBlocksImpl(const HomeBlocksImpl&) = delete;
    HomeBlocksImpl(HomeBlocksImpl&&) noexcept = delete;
    HomeBlocksImpl& operator=(const HomeBlocksImpl&) = delete;
    HomeBlocksImpl& operator=(HomeBlocksImpl&&) noexcept = delete;

    shared< VolumeManager > volume_manager() final;

    /// HomeBlocks
    /// Returns the UUID of this HomeBlocks.
    HomeBlocksStats get_stats() const final;
    peer_id_t our_uuid() const final {
        // not expected to be called;
        return peer_id_t{};
    }

    /// VolumeManager
    NullAsyncResult create_volume(VolumeInfo&& vol_info) final;

    NullAsyncResult remove_volume(const volume_id_t& id) final;

    VolumeInfoPtr lookup_volume(const volume_id_t& id) final;

    NullAsyncResult write(const volume_id_t& id, const vol_interface_req_ptr& req, bool part_of_batch = false) override;

    void submit_io_batch() override;

    // see api comments in base class;
    bool get_stats(volume_id_t id, VolumeStats& stats) const final;
    void get_volume_ids(std::vector< volume_id_t >& vol_ids) const final;

    // Index
    shared< VolumeIndexTable > recover_index_table(homestore::superblk< homestore::index_table_sb >&& sb);

    // HomeStore
    void init_homestore();
    void init_cp();

    uint64_t get_current_timestamp();

    void on_init_complete();

    void on_write(int64_t lsn, const sisl::blob& header, const sisl::blob& key, const std::vector< homestore::MultiBlkId >& pbas,
                  cintrusive< homestore::repl_req_ctx >& ctx);

private:
    // Should only be called for first-time-boot
    void superblk_init();
    void register_metablk_cb();

    void get_dev_info(shared< HomeBlocksApplication > app, std::vector< homestore::dev_info >& device_info,
                      bool& has_data_dev, bool& has_fast_dev);

    DevType get_device_type(std::string const& devname);
    auto defer() const { return folly::makeSemiFuture().via(executor_); }

    // recovery apis
    void on_hb_meta_blk_found(sisl::byte_view const& buf, void* cookie);
    void on_vol_meta_blk_found(sisl::byte_view const& buf, void* cookie);
};

class HBIndexSvcCB : public homestore::IndexServiceCallbacks {
public:
    HBIndexSvcCB(HomeBlocksImpl* hb) : hb_(hb) {}
    shared< homestore::IndexTableBase >
    on_index_table_found(homestore::superblk< homestore::index_table_sb >&& sb) override {
        LOGI("Recovered index table to index service");
        return hb_->recover_index_table(std::move(sb));
    }

private:
    HomeBlocksImpl* hb_;
};

static constexpr uint64_t HOMEBLKS_MSGHEADER_MAGIC = 0x54edfce1ae98104a;
static constexpr uint32_t HOMEBLKS_MSGHEADER_PROTOCOL_VERSION_V1 = 0x01;
static constexpr uint32_t init_crc32 = 0;
static constexpr uint16_t init_crc_16 = 0x8005;

#pragma pack(1)
template < typename Header >
struct BaseMessageHeader {
    uint64_t magic_num{HOMEBLKS_MSGHEADER_MAGIC};
    uint32_t protocol_version;
    uint32_t payload_size;
    uint32_t payload_crc;
    mutable uint32_t header_crc;

    void seal() {
        header_crc = 0;
        header_crc = calculate_crc();
    }

    uint32_t calculate_crc() const {
        const auto* hdr = static_cast< const Header* >(this);
        return crc32_ieee(init_crc32, reinterpret_cast< const unsigned char* >(hdr), sizeof(*hdr));
    }

    bool corrupted() const {
        auto saved_crc = header_crc;
        header_crc = 0;
        bool is_corrupted = (saved_crc != calculate_crc());
        header_crc = saved_crc;
        return is_corrupted;
    }

    std::string to_string() const {
        return fmt::format("magic={:#x} version={} payload_size={} payload_crc={} header_crc={}\n", magic_num,
                           protocol_version, payload_size, payload_crc, header_crc);
    }
};

struct HomeBlksMessageHeader : public BaseMessageHeader< HomeBlksMessageHeader > {
    HomeBlksMessageHeader() : BaseMessageHeader() {
        magic_num = HOMEBLKS_MSGHEADER_MAGIC;
        protocol_version = HOMEBLKS_MSGHEADER_PROTOCOL_VERSION_V1;
    }
    HomeBlksMsgType msg_type;
};

struct HomeBlksMessageKey {
    lba_t lba;
    lba_count_t nlbas;
};

struct homeblks_repl_ctx : public homestore::repl_req_ctx {
    sisl::io_blob_safe hdr_buf_;
    sisl::io_blob_safe key_buf_;

    // Data bufs corresponding to data_sgs_. Since data_sgs are raw pointers, we need to keep the data bufs alive
    folly::small_vector< sisl::io_blob_safe, 3 > data_bufs_;
    sisl::sg_list data_sgs_;

    homeblks_repl_ctx(uint32_t hdr_extn_size, uint32_t key_size = 0) : homestore::repl_req_ctx{} {
        hdr_buf_ = std::move(sisl::io_blob_safe{uint32_cast(sizeof(HomeBlksMessageHeader) + hdr_extn_size), 0});
        new (hdr_buf_.bytes()) HomeBlksMessageHeader();

        if (key_size) { key_buf_ = std::move(sisl::io_blob_safe{key_size, 0}); }
        data_sgs_.size = 0;
    }

    ~homeblks_repl_ctx() {
        if (hdr_buf_.bytes()) { header()->~HomeBlksMessageHeader(); }
    }

    template < typename T >
    T* to() {
        return r_cast< T* >(this);
    }

    HomeBlksMessageHeader* header() { return r_cast< HomeBlksMessageHeader* >(hdr_buf_.bytes()); }
    uint8_t* header_extn() { return hdr_buf_.bytes() + sizeof(HomeBlksMessageHeader); }

    sisl::io_blob_safe& header_buf() { return hdr_buf_; }
    sisl::io_blob_safe const& cheader_buf() const { return hdr_buf_; }

    sisl::io_blob_safe& key_buf() { return key_buf_; }
    sisl::io_blob_safe const& ckey_buf() const { return key_buf_; }

    void add_data_sg(uint8_t* buf, uint32_t size) {
        data_sgs_.iovs.emplace_back(iovec{.iov_base = buf, .iov_len = size});
        data_sgs_.size += size;
    }

    void add_data_sg(sisl::io_blob_safe&& buf) {
        add_data_sg(buf.bytes(), buf.size());
        data_bufs_.emplace_back(std::move(buf));
    }

    sisl::sg_list& data_sgs() { return data_sgs_; }
    std::string data_sgs_string() const {
        fmt::memory_buffer buf;
        fmt::format_to(fmt::appender(buf), "total_size={} iovcnt={} [", data_sgs_.size, data_sgs_.iovs.size());
        for (auto const& iov : data_sgs_.iovs) {
            fmt::format_to(fmt::appender(buf), "<base={},len={}> ", iov.iov_base, iov.iov_len);
        }
        fmt::format_to(fmt::appender(buf), "]");
        return fmt::to_string(buf);
    }
};

template < typename T >
struct repl_result_ctx : public homeblks_repl_ctx {
    folly::Promise< T > promise_;

    template < typename... Args >
    static intrusive< repl_result_ctx< T > > make(Args&&... args) {
        return intrusive< repl_result_ctx< T > >{new repl_result_ctx< T >(std::forward< Args >(args)...)};
    }

    repl_result_ctx(uint32_t hdr_extn_size, uint32_t key_size = 0) : homeblks_repl_ctx{hdr_extn_size, key_size} {}
    folly::SemiFuture< T > result() { return promise_.getSemiFuture(); }
};

} // namespace homeblocks
