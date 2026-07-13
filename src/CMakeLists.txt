cmake_minimum_required (VERSION 3.11)

add_flags("-Wall -Wextra -Werror -Wpedantic")
include_directories (BEFORE include/)
include_directories (BEFORE lib)
include_directories (AFTER .)

find_package(Threads QUIET REQUIRED)
find_package(homestore QUIET REQUIRED)
find_package(sisl QUIET REQUIRED)
find_package(craft_client QUIET REQUIRED) # the extracted CRAFT wire + client + reference

# HomeBlocks is the CRAFT *backend*, so it takes exactly one thing from craft_client: the header-only vocabulary
# the wire is defined in (craft_types -- client_hdr, lsn_pair, read_result, craft_error). Not the client, not the
# craft_replica interface, not the codec. A CRAFT server (CraftConnector) will add craft_client::craft_wire.
list(APPEND COMMON_DEPS homestore::homestore sisl::sisl craft_client::craft_types)

list(APPEND COMMON_TEST_DEPS
    ${COMMON_DEPS}
    GTest::gmock
)

add_subdirectory(lib)
add_subdirectory(test)
