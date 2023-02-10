cmake_minimum_required (VERSION 3.11)

find_package(Threads REQUIRED)
find_package(homestore REQUIRED)
find_package(nuraft_mesg REQUIRED)

set (COMMON_DEPS
        homestore::homestore
        nuraft_mesg::nuraft_mesg
    )

set(COMMON_TEST_DEPS
        ${COMMON_DEPS}
        ${spdk_LIBRARY_LIST}
        ${dpdk_LIBRARY_LIST}
    )

include_directories (BEFORE lib/)
include_directories (BEFORE include/)
include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(BEFORE ${CMAKE_CURRENT_BINARY_DIR}/lib)

add_subdirectory(lib)
add_subdirectory(examples)
add_subdirectory(tests)
