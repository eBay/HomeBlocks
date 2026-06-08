cmake_minimum_required (VERSION 3.11)

add_flags("-Wall -Wextra -Werror -Wpedantic")
include_directories (BEFORE include/)
include_directories (BEFORE lib)
include_directories (AFTER .)

find_package(Threads QUIET REQUIRED)
find_package(homestore QUIET REQUIRED)
find_package(sisl QUIET REQUIRED)

list(APPEND COMMON_DEPS homestore::homestore sisl::sisl)

list(APPEND COMMON_TEST_DEPS
    ${COMMON_DEPS}
    GTest::gmock
)

add_subdirectory(lib)
add_subdirectory(test)
