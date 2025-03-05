cmake_minimum_required (VERSION 3.11)

add_flags("-Wno-unused-parameter")

add_library("${PROJECT_NAME}_volume")

target_sources("${PROJECT_NAME}_volume" PRIVATE
    volume.cpp
    $<TARGET_OBJECTS:${PROJECT_NAME}_core>
)

target_link_libraries("${PROJECT_NAME}_volume" PUBLIC
    homestore::homestore
    ${COMMON_DEPS}
)

add_subdirectory(tests)
