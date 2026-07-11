cmake_minimum_required (VERSION 3.11)

add_flags("-Wall -Wextra -Werror -Wpedantic")
include_directories (BEFORE include/)
include_directories (BEFORE lib)
include_directories (AFTER .)

find_package(Threads QUIET REQUIRED)
find_package(homestore QUIET REQUIRED)
find_package(sisl QUIET REQUIRED)
find_package(craft_client QUIET REQUIRED) # the extracted CRAFT wire + client + reference

# The product links only the header-only vocab + craft_replica interface (craft_types). Tests that need the
# in-memory reference model / cluster server add craft_client::craft_reference themselves.
list(APPEND COMMON_DEPS homestore::homestore sisl::sisl craft_client::craft_types)

list(APPEND COMMON_TEST_DEPS
    ${COMMON_DEPS}
    GTest::gmock
)

add_subdirectory(lib)
add_subdirectory(test)
