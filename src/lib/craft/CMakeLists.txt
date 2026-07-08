cmake_minimum_required (VERSION 3.11)

add_library(${PROJECT_NAME}_craft OBJECT)
target_sources(${PROJECT_NAME}_craft PRIVATE
    craft_repl_dev.cpp
    craft_api.cpp
)
target_link_libraries(${PROJECT_NAME}_craft
    ${COMMON_DEPS}
)

# In-memory CRAFT reference model (HomeStore-free). Built by default; toggle off to exclude it and
# its tests from the build.
option(HB_BUILD_CRAFT_MEMORY_MODEL "Build the in-memory CRAFT reference model + tests" ON)
if (HB_BUILD_CRAFT_MEMORY_MODEL)
    add_subdirectory(memory)
endif()
