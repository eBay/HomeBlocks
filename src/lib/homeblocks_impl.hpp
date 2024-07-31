#pragma once

#include <boost/intrusive_ptr.hpp>
#include <sisl/logging/logging.h>

#include <homeblocks/homeblocks.hpp>
#include <homeblocks/volume_manager.hpp>

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
    virtual VolumeManager::NullAsyncResult _create_volume(VolumeInfo&& volume_info) = 0;
    virtual bool _get_stats(volume_id_t id, VolumeStats& stats) const = 0;
    virtual void _get_volume_ids(std::vector< volume_id_t >& vol_ids) const = 0;

    virtual HomeBlocksStats _get_stats() const = 0;

protected:
    peer_id_t _our_id;

    /// Our SvcId retrieval and SvcId->IP mapping
    std::weak_ptr< HomeBlocksApplication > _application;

    folly::Executor::KeepAlive<> executor_;

    ///
    mutable std::shared_mutex _volume_lock;
    std::map< volume_id_t, unique< Volume > > _volume_map;


    auto _defer() const { return folly::makeSemiFuture().via(executor_); }

public:
    explicit HomeBlocksImpl(std::weak_ptr< HomeBlocksApplication >&& application);

    ~HomeBlocksImpl() override = default;
    HomeBlocksImpl(const HomeBlocksImpl&) = delete;
    HomeBlocksImpl(HomeBlocksImpl&&) noexcept = delete;
    HomeBlocksImpl& operator=(const HomeBlocksImpl&) = delete;
    HomeBlocksImpl& operator=(HomeBlocksImpl&&) noexcept = delete;

    std::shared_ptr< VolumeManager > volume_manager() final;

    /// HomeBlocks
    /// Returns the UUID of this HomeBlocks.
    peer_id_t our_uuid() const final { return _our_id; }
    HomeBlocksStats get_stats() const final { return _get_stats(); }

    /// VolumeManager
    VolumeManager::NullAsyncResult create_volume(VolumeInfo&& vol_info) final;

    // see api comments in base class;
    bool get_stats(volume_id_t id, VolumeStats& stats) const final;
    void get_volume_ids(std::vector< volume_id_t >& vol_ids) const final;

    uint64_t get_current_timestamp();
};

} // namespace homeblocks
