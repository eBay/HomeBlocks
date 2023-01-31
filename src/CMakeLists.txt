cmake_minimum_required (VERSION 3.11)

# add warning options
add_flags("-Wall -Wextra -Werror -Wpedantic")

find_package(nuraft REQUIRED)
find_package(sisl REQUIRED)
find_package(Threads REQUIRED)
find_package(homestore REQUIRED)
find_package(GTest REQUIRED)
find_package(nuraft_mesg REQUIRED)

set (COMMON_DEPS
        homestore::homestore
        sisl::sisl
        nuraft::nuraft
    )

set(COMMON_TEST_DEPS
        ${COMMON_DEPS}
        ${spdk_LIBRARY_LIST}
        ${dpdk_LIBRARY_LIST}
        GTest::gmock
    )

include_directories (BEFORE lib/)
include_directories (BEFORE include/)
include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_subdirectory(lib/state_machine)
add_subdirectory(lib/log_store)
add_subdirectory(lib/storage)
add_subdirectory(lib/service)
add_subdirectory(tests)

add_library(${PROJECT_NAME})
target_sources(${PROJECT_NAME} PRIVATE
            $<TARGET_OBJECTS:service>
            $<TARGET_OBJECTS:state_machine>
            $<TARGET_OBJECTS:log_store>
            $<TARGET_OBJECTS:storage_engine>
        )
target_link_libraries(${PROJECT_NAME}
            ${COMMON_DEPS}
        )
