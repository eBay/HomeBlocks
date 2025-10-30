cmake_minimum_required (VERSION 3.11)

add_executable(test_volume)
target_sources(test_volume PRIVATE
    test_volume.cpp
)

target_link_libraries(test_volume
    ${PROJECT_NAME}_volume
    ${COMMON_TEST_DEPS}
    -rdynamic
)

add_executable(test_volume_io)
target_sources(test_volume_io PRIVATE
    test_volume_io.cpp
)

target_link_libraries(test_volume_io
    ${PROJECT_NAME}_volume
    ${COMMON_TEST_DEPS}
    -rdynamic
)

add_executable(test_volume_chunk_selector)
target_sources(test_volume_chunk_selector PRIVATE
test_volume_chunk_selector.cpp
)
target_link_libraries(test_volume_chunk_selector
    ${PROJECT_NAME}_volume
    ${COMMON_TEST_DEPS}
    -rdynamic
)

add_test(NAME VolumeTest COMMAND test_volume --gc_timer_nsecs=3 --index_chunk_size_mb=128 --data_chunk_size_mb=128)
add_test(NAME VolumeIOTest COMMAND test_volume_io --index_chunk_size_mb=128 --data_chunk_size_mb=128 --gtest_filter=-VolumeIOTest.LongRunningRandomIO:VolumeIOTest.WriteCrash:VolumeIOTest.IndexPutFailure) # FIXME: turn on after io issue is fixed;
add_test(NAME VolumeChunkSelectorTest COMMAND test_volume_chunk_selector)
