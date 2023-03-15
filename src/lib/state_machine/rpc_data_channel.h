#pragma once
#include <sisl/utility/enum.hpp>
#include <home_replication/repl_decls.h>

namespace home_replication {

class StateMachineStore;

#pragma pack(1)
struct data_channel_rpc_hdr {
    static constexpr uint16_t MAJOR_VERSION{0};
    static constexpr uint16_t MINOR_VERSION{1};
    static constexpr uint32_t max_hdr_size{512};

    uuid_t group_id;            // UUID of the replica set
    uint32_t issuer_replica_id; // Server ID it is initiated from
    uint16_t major_version{MAJOR_VERSION};
    uint16_t minor_version{MINOR_VERSION};
};
#pragma pack()

#pragma pack(1)
struct pbas_serialized {
public:
    struct _pba_info {
        pba_t pba;
        uint32_t data_size;
    };

    uint16_t n_pbas;
    _pba_info pinfo[0];

public:
    static pbas_serialized* serialize(const pba_list_t& pbas, StateMachineStore* store_ptr, uint8_t* raw_ptr);
};
#pragma pack()

#pragma pack(1)
struct data_rpc {
public:
    data_channel_rpc_hdr common_hdr;
    pbas_serialized pba_area[0];

public:
    data_rpc() = default;

    static constexpr uint16_t max_pbas() {
        return (data_channel_rpc_hdr::max_hdr_size - sizeof(data_rpc) - sizeof(pbas_serialized)) /
            sizeof(pbas_serialized::_pba_info);
    }

    static sisl::io_blob_list_t serialize(const data_channel_rpc_hdr& common_header, const pba_list_t& pbas,
                                          StateMachineStore* store_ptr, const sisl::sg_list& value);
    static void deserialize(sisl::io_blob const& incoming_buf, data_channel_rpc_hdr& common_header,
                            fq_pba_list_t& fq_pbas, sisl::sg_list& value);
};
#pragma pack()

} // namespace home_replication