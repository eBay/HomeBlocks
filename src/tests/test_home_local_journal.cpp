#include "local_journal/home_journal.h"
#include <string>
#include <vector>
#include <filesystem>
#include <gtest/gtest.h>
#include <iomgr/io_environment.hpp>
#include <homestore/homestore.hpp>

using namespace home_replication;

SISL_LOGGING_INIT(HOMESTORE_LOG_MODS, home_replication)

static constexpr uint32_t g_max_logsize{512};
static std::random_device g_rd{};
static std::default_random_engine g_re{g_rd()};
static std::uniform_int_distribution< uint32_t > g_randlogsize_generator{2, g_max_logsize};

static constexpr std::array< const char, 62 > alphanum{
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
    'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
    'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

static std::string gen_random_string(size_t len, uint64_t preamble = std::numeric_limits< uint32_t >::max()) {
    std::string str;
    if (preamble != std::numeric_limits< uint64_t >::max()) {
        std::stringstream ss;
        ss << std::setw(8) << std::setfill('0') << std::hex << preamble;
        str += ss.str();
    }

    std::uniform_int_distribution< size_t > rand_char{0, alphanum.size() - 1};
    for (size_t i{0}; i < len; ++i) {
        str += alphanum[rand_char(g_re)];
    }
    str += '\0';
    return str;
}

static const std::string s_fpath_root{"/tmp/log_store_dev_"};

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

class TestHomeJournal : public ::testing::Test {
public:
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
                m_hj = std::make_unique< HomeLocalJournal >(bind_this(TestHomeJournal::entry_found, 2), m_store_id);
            })
            .init(true /* wait_for_init */);

        if (!restart) {
            m_hj->create_store();
        } else {
            ASSERT_EQ(m_restart_log, m_shadow_log) << "All entries expected not found after restart";
            m_restart_log.clear();
        }
        m_store_id = m_hj->logstore_id();
    }

    void shutdown(bool cleanup = true) {
        if (cleanup) { m_hj->remove_store(); }

        homestore::HomeStore::instance()->shutdown();
        homestore::HomeStore::reset_instance();
        iomanager.stop();

        if (cleanup) { remove_files(SISL_OPTIONS["num_devs"].as< uint32_t >()); }
    }

    void insert(int64_t lsn) {
        m_shadow_log[lsn - 1] = gen_random_string(g_randlogsize_generator(g_re));
        auto& s = m_shadow_log[lsn - 1];
        m_hj->insert_async(lsn, sisl::io_blob::from_string(s));
    }

    void validate_all() {
        size_t lsn{1};
        for (const auto& s : m_shadow_log) {
            if (!s.empty()) {
                auto b = m_hj->read_sync(lsn);
                ASSERT_EQ(b.get_string(), s) << "Mismatch data for lsn=" << lsn;
            }
            ++lsn;
        }
    }

    void truncate(int64_t lsn) {
        m_hj->truncate(lsn);
        m_shadow_log.erase(m_shadow_log.begin(), m_shadow_log.begin() + lsn);
    }

private:
    void entry_found(int64_t lsn, const sisl::byte_view& data) {
        ASSERT_EQ(m_shadow_log[lsn - 1].empty(), false)
            << "Restart found lsn=" << lsn << " which was not never inserted";
        ASSERT_EQ(data.get_string(), m_shadow_log[lsn - 1]) << "Restart data mismatch for lsn=" << lsn;
        m_restart_log[lsn - 1] = m_shadow_log[lsn - 1];
    }

protected:
    std::unique_ptr< HomeLocalJournal > m_hj;
    homestore::logstore_id_t m_store_id{UINT32_MAX};
    sisl::sparse_vector< std::string > m_shadow_log;
    sisl::sparse_vector< std::string > m_restart_log;
};

TEST_F(TestHomeJournal, lifecycle_test) {
    LOGINFO("Step 1: Start HomeStore");
    this->start_homestore();

    LOGINFO("Step 2: Insert with gaps and validate insertions");
    this->insert(10);
    this->insert(20);
    this->insert(30);
    this->insert(90);
    this->insert(100);
    this->insert(110);
    this->validate_all();

    LOGINFO("Step 3: Truncate in-between");
    this->truncate(92);
    this->validate_all();

    LOGINFO("Step 4: Truncate empty set");
    this->truncate(98);
    this->validate_all();

    LOGINFO("Step 5: Restart homestore and validate recovery");
    this->start_homestore(true /* restart */);
    this->validate_all();

    LOGINFO("Step 6: Post restart insert after truncated lsn");
    this->insert(99);
    this->insert(101);
    this->insert(105);

    this->shutdown();
}

SISL_OPTIONS_ENABLE(logging, test_home_local_journal)
SISL_OPTION_GROUP(test_home_local_journal,
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
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_home_local_journal);
    sisl::logging::SetLogger("test_home_local_journal");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%t] %v");

    return RUN_ALL_TESTS();
}