#include <string>
#include <vector>
#include <filesystem>
#include <gtest/gtest.h>
#include <iomgr/io_environment.hpp>
#include <homestore/homestore.hpp>
#include <homestore/blkdata_service.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include "storage/home_storage_engine.h"
#include <home_replication/common.hpp>
using namespace home_replication;

SISL_LOGGING_INIT(HOMEREPL_LOG_MODS)

static std::random_device g_rd{};
static std::default_random_engine g_re{g_rd()};
static std::uniform_int_distribution< uint32_t > g_num_pbas_generator{2, 10};

static const std::string s_fpath_root{"/tmp/home_sm_store"};

static constexpr uint64_t Ki{1024};
static constexpr uint64_t Mi{Ki * Ki};
static constexpr uint64_t Gi{Ki * Mi};

static void remove_files(uint32_t ndevices) {
    for (uint32_t i{0}; i < ndevices; ++i) {
        const std::string fpath{s_fpath_root + std::to_string(i + 1)};
        if (std::filesystem::exists(fpath)) { std::filesystem::remove(fpath); }
    }
}

static void init_files(uint32_t ndevices, uint64_t dev_size) {
    remove_files(ndevices);
    for (uint32_t i{0}; i < ndevices; ++i) {
        const std::string fpath{s_fpath_root + std::to_string(i + 1)};
        std::ofstream ofs{fpath, std::ios::binary | std::ios::out | std::ios::trunc};
        std::filesystem::resize_file(fpath, dev_size);
    }
}

typedef std::function< void(std::error_condition err, std::shared_ptr< pba_list_t > out_bids) > after_write_cb_t;

class TestHomeStateMachineStore : public ::testing::Test {
public:
    void SetUp() {
        boost::uuids::random_generator gen;
        m_uuid = gen();
    }

    void start_homestore(bool restart = false, bool ds_test = false) {
        auto const ndevices = SISL_OPTIONS["num_devs"].as< uint32_t >();
        auto const dev_size = SISL_OPTIONS["dev_size_mb"].as< uint64_t >() * 1024 * 1024;
        auto nthreads = SISL_OPTIONS["num_threads"].as< uint32_t >();

        if (restart) {
            shutdown(false);
            std::this_thread::sleep_for(std::chrono::seconds{5});
        }

        std::vector< homestore::dev_info > device_info;
        if (SISL_OPTIONS.count("device_list")) {
            auto dev_names = SISL_OPTIONS["device_list"].as< std::vector< std::string > >();
            std::string dev_list_str;
            for (const auto& d : dev_names) {
                dev_list_str += d;
            }
            LOGINFO("Taking input dev_list: {}", dev_list_str);

            /* if user customized file/disk names */
            for (uint32_t i{0}; i < dev_names.size(); ++i) {
                // const std::filesystem::path fpath{m_dev_names[i]};
                device_info.emplace_back(dev_names[i], homestore::HSDevType::Data);
            }
        } else {
            /* create files */
            LOGINFO("creating {} device files with each of size {} ", ndevices, homestore::in_bytes(dev_size));
            if (!restart) { init_files(ndevices, dev_size); }
            for (uint32_t i{0}; i < ndevices; ++i) {
                const std::filesystem::path fpath{s_fpath_root + std::to_string(i + 1)};
                device_info.emplace_back(std::filesystem::canonical(fpath).string(), homestore::HSDevType::Data);
            }
        }

        LOGINFO("Starting iomgr with {} threads, spdk: {}", nthreads, false);
        ioenvironment.with_iomgr(nthreads, false);

        const uint64_t app_mem_size = ((ndevices * dev_size) * 15) / 100;
        LOGINFO("Initialize and start HomeStore with app_mem_size = {}", homestore::in_bytes(app_mem_size));

        homestore::hs_input_params params;
        params.app_mem_size = app_mem_size;
        params.data_devices = device_info;
        if (ds_test) {
            homestore::HomeStore::instance()
                ->with_params(params)
                .with_meta_service(5.0)
                .with_log_service(40.0, 5.0)
                .with_data_service(40.0)
                .before_init_devices([this]() {
                    homestore::meta_service().register_handler(
                        "replica_set",
                        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t) {
                            rs_super_blk_found(std::move(buf), voidptr_cast(mblk));
                        },
                        nullptr);
                })
                .init(true /* wait_for_init */);

        } else {
            homestore::HomeStore::instance()
                ->with_params(params)
                .with_meta_service(5.0)
                .with_log_service(80.0, 5.0)
                .before_init_devices([this]() {
                    homestore::meta_service().register_handler(
                        "replica_set",
                        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t) {
                            rs_super_blk_found(std::move(buf), voidptr_cast(mblk));
                        },
                        nullptr);
                })
                .init(true /* wait_for_init */);
        }

        if (!restart) { m_hsm = std::make_unique< HomeStateMachineStore >(m_uuid); }
    }

    void shutdown(bool cleanup = true) {
        if (cleanup) { m_hsm->destroy(); }

        m_hsm.reset();
        homestore::HomeStore::instance()->shutdown();
        homestore::HomeStore::reset_instance();
        iomanager.stop();

        if (cleanup) { remove_files(SISL_OPTIONS["num_devs"].as< uint32_t >()); }
    }

    void rs_super_blk_found(const sisl::byte_view& buf, void* meta_cookie) {
        homestore::superblk< home_rs_superblk > rs_sb;
        rs_sb.load(buf, meta_cookie);
        m_hsm = std::make_unique< HomeStateMachineStore >(rs_sb);
        m_uuid = rs_sb->uuid;
    }

    void add(int64_t lsn) {
        pba_list_t pbas;
        auto num_pbas = g_num_pbas_generator(g_re);
        for (size_t i{0}; i < num_pbas; ++i) {
            pbas.push_back(m_cur_pba.fetch_add(1));
        }
        m_shadow_log[lsn - 1] = pbas;
        m_hsm->add_free_pba_record(lsn, pbas);
    }

    void validate_all(int64_t from_lsn, int64_t to_lsn) {
        m_hsm->flush_free_pba_records();
        m_hsm->get_free_pba_records(from_lsn, to_lsn, [this](int64_t lsn, const pba_list_t& pbas) {
            if (pbas != m_shadow_log[lsn - 1]) {
                ASSERT_TRUE(false) << " Free pba record and shadow log pba list mismatch for lsn=" << lsn;
            }
        });
    }

    void remove_upto(int64_t lsn) {
        m_hsm->remove_free_pba_records_upto(lsn - 1);
        // m_shadow_log.erase(m_shadow_log.begin(), m_shadow_log.begin() + lsn);
    }

    void wait_for_all_io_complete() {
        std::unique_lock lk(m_mtx);
        m_cv.wait(lk, [this] { return this->m_io_job_done; });
    }

    void free_sg_buf(std::shared_ptr< sisl::sg_list > sg) {
        for (auto x : sg->iovs) {
            iomanager.iobuf_free(s_cast< uint8_t* >(x.iov_base));
            x.iov_base = nullptr;
            x.iov_len = 0;
        }

        sg->size = 0;
    }

    //
    // this api is for caller who is not interested with the write buffer and blkids;
    //
    void write_read_io_then_free_blk(const uint64_t io_size) {
        std::shared_ptr< sisl::sg_list > sg_write = std::make_shared< sisl::sg_list >();
        write_io(io_size, sg_write, 1 /* num_iovs */,
                 [this, sg_write](std::error_condition err, std::shared_ptr< pba_list_t > sout_bids) {
                     // this read is not interested with what was written
                     free_sg_buf(sg_write);

                     // this will be called in write io completion cb;
                     LOGINFO("after_write_cb: Write completed;");

                     const auto out_bids = *(sout_bids.get());

                     assert(out_bids.size() == 1);

                     std::shared_ptr< sisl::sg_list > sg_read = std::make_shared< sisl::sg_list >();

                     homestore::BlkId bid_to_read{out_bids[0]};
                     struct iovec iov;
                     iov.iov_len = bid_to_read.get_nblks() * ds_inst().get_page_size();
                     iov.iov_base = iomanager.iobuf_alloc(512, iov.iov_len);
                     sg_read->iovs.push_back(iov);
                     sg_read->size += iov.iov_len;

                     LOGINFO("Step 3: async read on blkid: {}", bid_to_read.to_string());
                     auto free_bid = bid_to_read;
                     m_hsm->async_read(bid_to_read.to_integer() /* pba */, *(sg_read.get()), sg_read->size,
                                       [sg_read, free_bid, this](std::error_condition err) {
                                           assert(!err);

                                           LOGINFO("Read completed;");
                                           free_sg_buf(sg_read);

                                           LOGINFO("Step 4: started async_free_blk: {}", free_bid.to_string());

                                           m_hsm->free_pba(free_bid.to_integer());

                                           // it is fine to declair job done here as we know there is no read pending on
                                           // this free blk and free should be done in sync mode;
                                           {
                                               std::lock_guard lk(this->m_mtx);
                                               this->m_io_job_done = true;
                                           }

                                           this->m_cv.notify_one();
                                       });
                 });
    }

    homestore::BlkDataService& ds_inst() { return homestore::data_service(); }

    void fill_data_buf(uint8_t* buf, uint64_t size) {
        for (uint64_t i = 0ul; i < size; ++i) {
            *(buf + i) = (i % 256);
        }
    }

private:
    void write_io(const uint64_t io_size, std::shared_ptr< sisl::sg_list > sg, const uint32_t num_iovs,
                  const after_write_cb_t& after_write_cb = nullptr) {
        assert(io_size % (4 * Ki * num_iovs) == 0);
        const auto iov_len = io_size / num_iovs;
        for (auto i = 0ul; i < num_iovs; ++i) {
            struct iovec iov;
            iov.iov_len = iov_len;
            iov.iov_base = iomanager.iobuf_alloc(512, iov_len);
            fill_data_buf(r_cast< uint8_t* >(iov.iov_base), iov.iov_len);
            sg->iovs.push_back(iov);
            sg->size += iov_len;
        }

        std::shared_ptr< pba_list_t > out_bids_ptr = std::make_shared< pba_list_t >();
        const auto pba_list = m_hsm->alloc_pbas(io_size);
        for (const auto& p : pba_list) {
            out_bids_ptr->emplace_back(p);
        }

        m_hsm->async_write(*(sg.get()), *(out_bids_ptr.get()),
                           [sg, this, after_write_cb, out_bids_ptr](std::error_condition err) {
                               LOGINFO("async_write completed, err: {}", err.message());
                               assert(!err);
                               const auto out_bids = *(out_bids_ptr.get());

                               for (auto i = 0ul; i < out_bids.size(); ++i) {
                                   LOGINFO("bid-{}: {}", i, out_bids[i]);
                               }

                               if (after_write_cb != nullptr) { after_write_cb(err, out_bids_ptr); }
                           });
    }

protected:
    std::unique_ptr< HomeStateMachineStore > m_hsm;
    homestore::logstore_id_t m_store_id{UINT32_MAX};
    sisl::sparse_vector< pba_list_t > m_shadow_log;
    std::atomic< uint64_t > m_cur_pba{0};
    boost::uuids::uuid m_uuid;
    // below is for data service testing;
    std::mutex m_mtx;
    std::condition_variable m_cv;
    bool m_io_job_done{false};
};

TEST_F(TestHomeStateMachineStore, ds_alloc_pbas) {
    LOGINFO("Step 1: Start HomeStore");
    this->start_homestore(false /* recovery */, true /* ds test*/);

    LOGINFO("Step 2: Start to allocate pbas");
    const auto io_size = 2 * Mi;
    const auto pba_list = m_hsm->alloc_pbas(io_size);

    for (auto i = 0ul; i < pba_list.size(); ++i) {
        LOGINFO("pba-{} ==> BlkId: {}", i, homestore::BlkId{pba_list[i]}.to_string());
    }

    LOGINFO("Step 3: Operation completed, do shutdown.");
    this->shutdown();
}

TEST_F(TestHomeStateMachineStore, ds_async_write_read_then_free_blk) {
    LOGINFO("Step 1: Start HomeStore");
    this->start_homestore(false /* recovery */, true /* ds test*/);

    // start io in worker thread;
    const auto io_size = 4 * Ki;
    LOGINFO("Step 2: run on worker thread to schedule write for {} Bytes.", io_size);
    iomanager.run_on(iomgr::thread_regex::random_worker,
                     [this, &io_size](iomgr::io_thread_addr_t a) { this->write_read_io_then_free_blk(io_size); });

    LOGINFO("Step 5: Wait for I/O to complete.");
    wait_for_all_io_complete();

    LOGINFO("Step 6: I/O completed, do shutdown.");
    this->shutdown();
}

TEST_F(TestHomeStateMachineStore, free_pba_record_maintanence) {
    LOGINFO("Step 1: Start HomeStore");
    this->start_homestore();

    LOGINFO("Step 2: Insert with gaps and validate insertions");
    this->add(10);
    this->add(20);
    this->add(30);
    this->add(90);
    this->add(100);
    this->add(110);
    this->validate_all(1, 110);

    LOGINFO("Step 3: Truncate in-between");
    this->remove_upto(92);
    this->validate_all(1, 110);

    LOGINFO("Step 4: Truncate empty set");
    this->remove_upto(98);
    this->validate_all(1, 110);

    LOGINFO("Step 5: Restart homestore and validate recovery");
    this->start_homestore(true /* restart */);
    this->validate_all(1, 110);

    LOGINFO("Step 6: Post restart insert after truncated lsn");
    this->add(105);
    this->add(99);
    this->add(101);
    this->validate_all(1, 110);

    this->shutdown();
}

SISL_OPTIONS_ENABLE(logging, test_home_sm_store)
SISL_OPTION_GROUP(test_home_sm_store,
                  (num_threads, "", "num_threads", "number of threads",
                   ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
                  (num_devs, "", "num_devs", "number of devices to create",
                   ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
                  (dev_size_mb, "", "dev_size_mb", "size of each device in MB",
                   ::cxxopts::value< uint64_t >()->default_value("1024"), "number"),
                  (device_list, "", "device_list", "Device List instead of default created",
                   ::cxxopts::value< std::vector< std::string > >(), "path [...]"),
                  (num_records, "", "num_records", "number of record to test",
                   ::cxxopts::value< uint32_t >()->default_value("1000"), "number"),
                  (iterations, "", "iterations", "Iterations", ::cxxopts::value< uint32_t >()->default_value("1"),
                   "the number of iterations to run each test"));

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_home_sm_store);
    sisl::logging::SetLogger("test_home_local_journal");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%t] %v");

    return RUN_ALL_TESTS();
}
