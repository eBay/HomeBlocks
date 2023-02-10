#include <string>
#include <vector>
#include <filesystem>
#include <gtest/gtest.h>
#include <iomgr/io_environment.hpp>
#include <homestore/homestore.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include "storage/home_storage_engine.h"
#include <home_replication/repl_decls.h>
using namespace home_replication;

SISL_LOGGING_INIT(HOMEREPL_LOG_MODS)

static std::random_device g_rd{};
static std::default_random_engine g_re{g_rd()};
static std::uniform_int_distribution< uint32_t > g_num_pbas_generator{2, 10};

static const std::string s_fpath_root{"/tmp/home_sm_store"};

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

class TestHomeStateMachineStore : public ::testing::Test {
public:
    void SetUp() {
        boost::uuids::random_generator gen;
        m_uuid = gen();
    }

    void start_homestore(bool restart = false) {
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

protected:
    std::unique_ptr< HomeStateMachineStore > m_hsm;
    homestore::logstore_id_t m_store_id{UINT32_MAX};
    sisl::sparse_vector< pba_list_t > m_shadow_log;
    std::atomic< uint64_t > m_cur_pba{0};
    boost::uuids::uuid m_uuid;
};

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
