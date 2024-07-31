cmake_minimum_required (VERSION 3.11)

add_library ("${PROJECT_NAME}_memory")
target_sources("${PROJECT_NAME}_memory" PRIVATE
    mem_homeblocks.cpp
    $<TARGET_OBJECTS:${PROJECT_NAME}_core>
)
target_link_libraries("${PROJECT_NAME}_memory"
    ${COMMON_DEPS}
)

add_executable (memory_test)
target_sources(memory_test PRIVATE
    $<TARGET_OBJECTS:test_fixture>
)
target_link_libraries(memory_test
    ${PROJECT_NAME}_memory
    ${COMMON_TEST_DEPS}
    -rdynamic
)
add_test(NAME MemoryTestCPU COMMAND memory_test -csv error --executor cpu)
