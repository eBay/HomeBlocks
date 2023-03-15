#include <string>
#include <vector>
#include <iostream>
#include <iomgr/io_environment.hpp>
#include <homestore/homestore.hpp>
#include <homestore/blkdata_service.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include "storage/home_storage_engine.h"
#include <home_replication/repl_service.h> // includes repl_set.h
#include <gtest/gtest.h>

#include <home_replication/repl_decls.h>
#include "state_machine/state_machine.h"

using namespace home_replication;

SISL_LOGGING_INIT(HOMEREPL_LOG_MODS)

static const std::string s_fpath_root{"/tmp/repl_state_machine"};
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

class TestReplStateMachine : public ::testing::Test {
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

        if (!restart) {
            m_hsm = std::make_shared< HomeStateMachineStore >(m_uuid);
            //  m_rs = std::make_shared< home_replication::ReplicaSet >("Test_Group_Id", m_hsm, nullptr /*log store*/);
            m_rs = new home_replication::ReplicaSet("Test_Group_Id", m_hsm, nullptr /*log store*/);
            m_sm = std::dynamic_pointer_cast< ReplicaStateMachine >(m_rs->get_state_machine());
        }
    }

    void shutdown(bool cleanup = true) {
        if (cleanup) { m_hsm->destroy(); }

        delete m_rs;
        m_hsm.reset();
        homestore::HomeStore::instance()->shutdown();
        homestore::HomeStore::reset_instance();
        iomanager.stop();

        if (cleanup) { remove_files(SISL_OPTIONS["num_devs"].as< uint32_t >()); }
    }

    void rs_super_blk_found(const sisl::byte_view& buf, void* meta_cookie) {
        homestore::superblk< home_rs_superblk > rs_sb;
        rs_sb.load(buf, meta_cookie);
        m_hsm = std::make_shared< HomeStateMachineStore >(rs_sb);
        m_uuid = rs_sb->uuid;
    }

public:
    std::shared_ptr< ReplicaStateMachine > m_sm{nullptr}; // state machine

private:
    home_replication::ReplicaSet* m_rs{nullptr}; // dummy replica set, it is just initialized for unit test purpose;
#if 0
    std::shared_ptr< home_replication::ReplicaSet > m_rs{nullptr};
#endif
    std::shared_ptr< HomeStateMachineStore > m_hsm{nullptr}; // Home SM Store
    boost::uuids::uuid m_uuid;
};

TEST_F(TestReplStateMachine, map_pba_basic_test) {
    LOGINFO("Step 1: Start HomeStore");
    this->start_homestore();

    LOGINFO("Step 2: try_map_pba");
    fully_qualified_pba fq_pba{1 /* server_id */, 100 /* remote_pba */, 4096 /* size */};
    auto const [local_pba_list, state] = m_sm->try_map_pba(fq_pba);

    ASSERT_EQ(state, pba_state_t::allocated);
    ASSERT_EQ(local_pba_list.size(), 1);

    LOGINFO("Step 3: updaet_map_pba state written");
    m_sm->update_map_pba(fq_pba, pba_state_t::written);

    LOGINFO("Step 4: updaet_map_pba state completed");
    m_sm->update_map_pba(fq_pba, pba_state_t::completed);

    LOGINFO("Step 5: remove_map_pba");
    const auto num_removed = m_sm->remove_map_pba(fq_pba);
    ASSERT_EQ(num_removed, 1);

    LOGINFO("Step 6: shutdown");
    this->shutdown();
}
#if 0 // TODO: disable for now because of known issue not related to this test;
TEST_F(TestReplStateMachine, async_fetch_pba_test_wait_no_timeout) {
    LOGINFO("Step 1: Start HomeStore");
    this->start_homestore();

    LOGINFO("Step 2: try_map_pba");
    fully_qualified_pba fq_pba{1 /* server_id */, 100 /* remote_pba */, 4096 /* size */};
    auto const [local_pba_list, state] = m_sm->try_map_pba(fq_pba);

    LOGINFO("Step 3: updaet_map_pba state written");
    m_sm->update_map_pba(fq_pba, pba_state_t::written);

    LOGINFO("Step 4: async_fetch_write_pbas");
    std::vector< fully_qualified_pba > fq_pbas{fq_pba};
    bool called{false};
    const auto need_to_wait = m_sm->async_fetch_write_pbas(fq_pbas, [&called]() {
        called = true;
        LOGINFO("callback called");
    });

    ASSERT_EQ(need_to_wait, true);
    ASSERT_EQ(called, false);

    LOGINFO("Step 5: updaet_map_pba state written");
    m_sm->update_map_pba(fq_pba, pba_state_t::completed);

    ASSERT_EQ(called, true);

    LOGINFO("Step 6: shutdown");
    this->shutdown();
}

TEST_F(TestReplStateMachine, async_fetch_pba_test_no_wait) {
    LOGINFO("Step 1: Start HomeStore");
    this->start_homestore();

    LOGINFO("Step 2: try_map_pba");
    fully_qualified_pba fq_pba{1 /* server_id */, 100 /* remote_pba */, 4096 /* size */};
    auto const [local_pba_list, state] = m_sm->try_map_pba(fq_pba);

    LOGINFO("Step 3: updaet_map_pba state written");
    m_sm->update_map_pba(fq_pba, pba_state_t::written);

    LOGINFO("Step 4: updaet_map_pba state completed");
    m_sm->update_map_pba(fq_pba, pba_state_t::completed);

    LOGINFO("Step 5: async_fetch_write_pbas");
    std::vector< fully_qualified_pba > fq_pbas{fq_pba};
    const auto need_to_wait =
        m_sm->async_fetch_write_pbas(fq_pbas, []() { ASSERT_TRUE(false) << "Should not be called. "; });

    ASSERT_EQ(need_to_wait, false);

    m_sm->stop_write_wait_timer();

    LOGINFO("Step 6: shutdown");
    this->shutdown();
}
#endif

TEST_F(TestReplStateMachine, async_fetch_pba_test_wait_timeout_fetch_remote) {
    // To be implemented;
}

SISL_OPTIONS_ENABLE(logging, test_repl_state_machine)
SISL_OPTION_GROUP(test_repl_state_machine,
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
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_repl_state_machine);
    sisl::logging::SetLogger("test_repl_state_machine");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%t] %v");

    return RUN_ALL_TESTS();
}
