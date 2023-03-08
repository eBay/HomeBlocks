cmake_minimum_required (VERSION 3.11)

# add warning options
add_flags("-Wall -Wextra -Werror -Wno-unused-parameter")

add_library(example_lib OBJECT)
target_sources(example_lib PRIVATE example_lib.cpp)
target_link_libraries(example_lib ${COMMON_DEPS})

link_directories(${spdk_LIB_DIRS} ${dpdk_LIB_DIRS})

add_executable(example_obj_store)
target_sources(example_obj_store PRIVATE obj_store.cpp)
target_link_libraries(example_obj_store
            $<TARGET_OBJECTS:example_lib>
            home_replication
            ${COMMON_TEST_DEPS}
        )
