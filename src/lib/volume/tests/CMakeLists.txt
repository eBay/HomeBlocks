cmake_minimum_required (VERSION 3.11)

# These tests exercise the legacy byte-based async_read/async_write, now [[deprecated]] in favor of
# the CRAFT overloads. Demote (not silence) the warning so it stays visible without failing -Werror.
add_flags("-Wno-error=deprecated-declarations")

add_executable(test_volume)
target_sources(test_volume PRIVATE
    test_volume.cpp
)

target_link_libraries(test_volume
    ${PROJECT_NAME}
    ${COMMON_TEST_DEPS}
    -rdynamic
)

add_executable(test_volume_io)
target_sources(test_volume_io PRIVATE
    test_volume_io.cpp
)

target_link_libraries(test_volume_io
    ${PROJECT_NAME}
    ${COMMON_TEST_DEPS}
    -rdynamic
)

add_executable(test_volume_chunk_selector)
target_sources(test_volume_chunk_selector PRIVATE
test_volume_chunk_selector.cpp
)
target_link_libraries(test_volume_chunk_selector
    ${PROJECT_NAME}
    ${COMMON_TEST_DEPS}
    -rdynamic
)

add_executable(test_craft_volume)
target_sources(test_craft_volume PRIVATE
    test_craft_volume.cpp
)
target_link_libraries(test_craft_volume
    ${PROJECT_NAME}
    ${COMMON_TEST_DEPS}
    -rdynamic
)

add_test(NAME VolumeTest COMMAND test_volume --gc_timer_nsecs=3 --index_chunk_size_mb=128 --data_chunk_size_mb=128)
# HomeStore's repl dev reaper (which runs a destroyed group's on_destroy) every second instead of every minute. Small
# index chunks: each volume takes at least one, and these tests keep most of the volumes they create.
add_test(NAME CraftVolumeTest COMMAND test_craft_volume --index_chunk_size_mb=32 --data_chunk_size_mb=128
         --repl_port=47451 --config_path=craft_volume_cfg
         --override_config homestore_config.generic.repl_dev_cleanup_interval_sec:1)
add_test(NAME VolumeIOTest COMMAND test_volume_io --index_chunk_size_mb=128 --data_chunk_size_mb=128)
add_test(NAME VolumeChunkSelectorTest COMMAND test_volume_chunk_selector)
