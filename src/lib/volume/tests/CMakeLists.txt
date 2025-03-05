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

