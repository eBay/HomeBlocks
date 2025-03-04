#pragma once
#include <string>
#include <boost/intrusive_ptr.hpp>
#include <sisl/logging/logging.h>
#include <homestore/homestore.hpp>
#include <homestore/index/index_table.hpp>
#include <homestore/superblk_handler.hpp>
#include <homeblks/home_blks.hpp>
#include <homeblks/volume_mgr.hpp>

#define LOGT(...) LOGTRACEMOD(homeblocks, ##__VA_ARGS__)
#define LOGD(...) LOGDEBUGMOD(homeblocks, ##__VA_ARGS__)
#define LOGI(...) LOGINFOMOD(homeblocks, ##__VA_ARGS__)
#define LOGW(...) LOGWARNMOD(homeblocks, ##__VA_ARGS__)
#define LOGE(...) LOGERRORMOD(homeblocks, ##__VA_ARGS__)
#define LOGC(...) LOGCRITICALMOD(homeblocks, ##__VA_ARGS__)

namespace homeblocks {

template < typename T >
using shared = std::shared_ptr< T >;

template < typename T >
using cshared = const std::shared_ptr< T >;

template < typename T >
using unique = std::unique_ptr< T >;

template < typename T >
using intrusive = boost::intrusive_ptr< T >;

template < typename T >
using cintrusive = const boost::intrusive_ptr< T >;
#if 0
class VolumeIndexKey;
class VolumeIndexValue;
using VolumeIndexTable = homestore::IndexTable< VolumeIndexKey, VolumeIndexValue >;
#endif
// TODO: move Volume to volume file
struct Volume {
    explicit Volume(VolumeInfo info) : volume_info_(std::move(info)) {}
    Volume(Volume const& volume) = delete;
    Volume(Volume&& volume) = default;
    Volume& operator=(Volume const& volume) = delete;
    Volume& operator=(Volume&& volume) = default;
    virtual ~Volume() = default;

    VolumeInfo volume_info_;
};

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
    inline static auto const VOL_META_NAME = std::string("Volume2");
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
    mutable std::shared_mutex _volume_lock;
    std::map< volume_id_t, unique< Volume > > _volume_map;

    bool recovery_done_{false};
    homestore::superblk< homeblks_sb_t > sb_;

public:
    explicit HomeBlocksImpl(std::weak_ptr< HomeBlocksApplication >&& application);

    ~HomeBlocksImpl() override = default;
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

    // see api comments in base class;
    bool get_stats(volume_id_t id, VolumeStats& stats) const final;
    void get_volume_ids(std::vector< volume_id_t >& vol_ids) const final;

    // Index
    // shared< VolumeIndexTable > recover_index_table(homestore::superblk< homestore::index_table_sb >&& sb);

    // HomeStore
    void init_homestore();
    void init_cp();

    uint64_t get_current_timestamp();

    void on_init_complete();

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
        // return hb_->recover_index_table(std::move(sb)); // TODO:
        return nullptr;
    }

private:
    HomeBlocksImpl* hb_;
};
} // namespace homeblocks
