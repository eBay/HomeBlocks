#include "storage/storage_engine.h"

#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
#include "rpc_data_channel.h"
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif

namespace home_replication {

pbas_serialized* pbas_serialized::serialize(const pba_list_t& pbas, StateMachineStore* store_ptr, uint8_t* raw_ptr) {
    pbas_serialized* pthis = new (raw_ptr) pbas_serialized();
    pthis->n_pbas = pbas.size();
    for (uint16_t i{0}; i < pthis->n_pbas; ++i) {
        pthis->pinfo[i].pba = pbas[i];
        pthis->pinfo[i].data_size = store_ptr->pba_to_size(pbas[i]);
    }
    return pthis;
}

sisl::io_blob_list_t data_rpc::serialize(const data_channel_rpc_hdr& common_header, const pba_list_t& pbas,
                                         StateMachineStore* store_ptr, const sisl::sg_list& value) {
    if (pbas.size() > max_pbas()) {
        LOGERROR("Exceeds max number of pbas that can be sent in this rpc");
        return {};
    }

    auto* bytes = new uint8_t[data_channel_rpc_hdr::max_hdr_size];
    data_rpc* rpc = new (bytes) data_rpc();

    rpc->common_hdr = common_header;
    pbas_serialized::serialize(pbas, store_ptr, uintptr_cast(rpc->pba_area));

    auto io_list = sisl::io_blob::sg_list_to_ioblob_list(value);
    sisl::io_blob hdr_blob(uintptr_cast(rpc), data_channel_rpc_hdr::max_hdr_size, false);
    io_list.insert(io_list.begin(), hdr_blob);
    return io_list;
}

void data_rpc::deserialize(sisl::io_blob const& incoming_buf, data_channel_rpc_hdr& common_header,
                           fq_pba_list_t& fq_pbas, sisl::sg_list& value) {
    // assert buf.size >= max header size
    data_rpc* rpc = r_cast< data_rpc* >(incoming_buf.bytes);
    common_header = rpc->common_hdr;
    for (uint16_t i{0}; i < rpc->pba_area[0].n_pbas; ++i) {
        fq_pbas.emplace_back(rpc->common_hdr.issuer_replica_id, rpc->pba_area[0].pinfo[i].pba,
                             rpc->pba_area[0].pinfo[i].data_size);
    }
    value.size = incoming_buf.size - data_channel_rpc_hdr::max_hdr_size;
    value.iovs.emplace_back(
        iovec{r_cast< void* >(incoming_buf.bytes + data_channel_rpc_hdr::max_hdr_size), value.size});
}

void data_rpc::deserialize(sisl::io_blob const& incoming_buf, data_channel_rpc_hdr& common_header, pba_list_t& pbas) {
    // assert buf.size >= max header size
    data_rpc* rpc = r_cast< data_rpc* >(incoming_buf.bytes);
    common_header = rpc->common_hdr;
    for (uint16_t i{0}; i < rpc->pba_area[0].n_pbas; ++i) {
        pbas.emplace_back(rpc->pba_area[0].pinfo[i].pba);
    }
}

} // namespace home_replication