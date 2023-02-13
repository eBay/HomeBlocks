cmake_minimum_required (VERSION 3.11)

# add warning options
add_flags("-Wall -Wextra -Werror -Wno-unused-parameter")

link_directories(${spdk_LIB_DIRS} ${dpdk_LIB_DIRS})

add_executable(example_obj_store)
target_sources(example_obj_store PRIVATE
            obj_store.cpp
        )
target_link_libraries(example_obj_store
            home_replication
            ${COMMON_TEST_DEPS}
        )
