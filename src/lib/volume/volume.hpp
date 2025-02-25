#pragma once
#include <homeblks/volume_mgr.hpp>

namespace homeblocks {

class Volume;
#if 0
class VolumeIndexKey;
class VolumeIndexValue;
using VolumeIndexTable = homestore::IndexTable< VolumeIndexKey, VolumeIndexValue >;
#endif
using VolumePtr = shared< Volume >;

class Volume {
public:
    explicit Volume(VolumeInfo&& info) : volume_info_(std::move(info)) {}
    Volume(Volume const& volume) = delete;
    Volume(Volume&& volume) = default;
    Volume& operator=(Volume const& volume) = delete;
    Volume& operator=(Volume&& volume) = default;
    virtual ~Volume() = default;

    void init();

    // static APIs exposed to HomeBlks Implementation Layer;
    static VolumePtr make_volume(VolumeInfo&& info) {
        auto vol = std::make_shared< Volume >(std::move(info));
        vol->init();
        return vol;
    }

private:
    VolumeInfo volume_info_;
};

} // namespace homeblocks
