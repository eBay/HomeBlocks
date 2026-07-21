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
