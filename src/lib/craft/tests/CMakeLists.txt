cmake_minimum_required(VERSION 3.11)

# Unit test for CraftReplDev::truncate (S4). Compiles craft_repl_dev.cpp directly to avoid
# dragging in craft_api.cpp → volume.hpp (heavy HomeStore volume plumbing not needed here).
add_executable(test_craft_truncate)
target_sources(test_craft_truncate PRIVATE
    test_craft_truncate.cpp
    ../craft_repl_dev.cpp
)
target_compile_definitions(test_craft_truncate PRIVATE _PRERELEASE)
target_link_libraries(test_craft_truncate
    ${COMMON_TEST_DEPS}
    -rdynamic
)

add_test(NAME CraftTruncateTest COMMAND test_craft_truncate)

# Unit tests for CraftReplDev::get_lsns(), get_rs_commit_lsn(), and fetch_data() (S6).
# Same pattern as test_craft_truncate: compile craft_repl_dev.cpp directly.
add_executable(test_craft_peer_exchange)
target_sources(test_craft_peer_exchange PRIVATE
    test_craft_peer_exchange.cpp
    ../craft_repl_dev.cpp
)
target_compile_definitions(test_craft_peer_exchange PRIVATE _PRERELEASE)
target_link_libraries(test_craft_peer_exchange
    ${COMMON_TEST_DEPS}
    -rdynamic
)

add_test(NAME CraftPeerExchangeTest COMMAND test_craft_peer_exchange)

# Unit tests for CraftReplDev::write (S2: Write Path). Compiles craft_repl_dev.cpp directly to avoid
# dragging in craft_api.cpp → volume.hpp (heavy HomeStore volume plumbing not needed here).
add_executable(test_craft_write)
target_sources(test_craft_write PRIVATE
    test_craft_write.cpp
    ../craft_repl_dev.cpp
)
target_compile_definitions(test_craft_write PRIVATE _PRERELEASE)
target_link_libraries(test_craft_write
    ${COMMON_TEST_DEPS}
    -rdynamic
)

add_test(NAME CraftWriteTest COMMAND test_craft_write)

# Exercises HomeStoreCraftJournalBackend against a REAL HomeStore home_log_store -- the
# production backend was previously never executed by any test. Links the full homeblocks
# library, unlike the tests above, because a real home_log_store requires a running HomeStore
# instance -- this is the heaviest CRAFT test in the suite.
add_executable(test_craft_homestore_backend)
target_sources(test_craft_homestore_backend PRIVATE
    test_craft_homestore_backend.cpp
)
target_include_directories(test_craft_homestore_backend PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../volume/tests
)
target_link_libraries(test_craft_homestore_backend
    ${PROJECT_NAME}
    ${COMMON_TEST_DEPS}
    -rdynamic
)

add_test(NAME CraftHomeStoreBackendTest COMMAND test_craft_homestore_backend)

# Verifies homeblocks::JournalSlot stays wire-compatible with craft_client's craft::JournalSlot:
# per-field compile-time layout checks, plus round-trips through the REAL peer-plane codec
# (craft::peer::encode_fetch_data_rsp / decode_fetch_data_rsp in peer_codec.cpp). This is the only
# craft test binary that needs craft_client::craft_client (the component peer_codec.cpp is built
# into) in addition to craft_client::craft_types.
add_executable(test_craft_journal_slot_wire)
target_sources(test_craft_journal_slot_wire PRIVATE
    test_craft_journal_slot_wire.cpp
)
target_link_libraries(test_craft_journal_slot_wire
    ${COMMON_TEST_DEPS}
    craft_client::craft_client
    -rdynamic
)

add_test(NAME CraftJournalSlotWireTest COMMAND test_craft_journal_slot_wire)
