cmake_minimum_required (VERSION 3.11)

# Model unit test. It links the HomeStore-free model objects + sisl + gtest, deliberately WITHOUT
# homestore::homestore: a successful link is the proof that the model references no homestore symbol
# (an undefined-symbol error here would mean the model touched the storage engine).
add_executable(test_craft_memory)
target_sources(test_craft_memory PRIVATE
    test_craft_memory.cpp
    craft_test_main.cpp
    $<TARGET_OBJECTS:${PROJECT_NAME}_craft_mem>
)
target_link_libraries(test_craft_memory
    sisl::sisl
    GTest::gmock
    -rdynamic
)
# home_blocks.hpp (pulled in transitively) needs homestore/iomgr HEADERS only (the header-only
# error.hpp / iomgr_types.hpp). Supply their include dirs WITHOUT linking the engine.
target_include_directories(test_craft_memory SYSTEM PRIVATE
    $<TARGET_PROPERTY:homestore::homestore,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:iomgr::iomgr,INTERFACE_INCLUDE_DIRECTORIES>
)
add_test(NAME CraftMemoryModelTest COMMAND test_craft_memory)

# End-to-end test: drives the model through the PUBLIC volume_handle API (login/async_write/
# async_read/commit in libhomeblocks) via create_memory_volume. This one DOES link HomeStore (through
# libhomeblocks + volume), as expected for the integration surface.
add_executable(test_craft_e2e)
target_sources(test_craft_e2e PRIVATE
    test_craft_e2e.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../mem_craft_volume.cpp # volume<->model bridge (needs volume.hpp)
    $<TARGET_OBJECTS:${PROJECT_NAME}_craft_mem>
)
target_link_libraries(test_craft_e2e
    ${PROJECT_NAME}
    ${COMMON_TEST_DEPS}
    -rdynamic
)
add_test(NAME CraftMemoryE2ETest COMMAND test_craft_e2e)
