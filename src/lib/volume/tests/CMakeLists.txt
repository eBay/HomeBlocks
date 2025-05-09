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

add_test(NAME VolumeTest COMMAND test_volume)
add_test(NAME VolumeIOTest COMMAND test_volume_io)