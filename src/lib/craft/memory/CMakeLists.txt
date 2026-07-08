cmake_minimum_required (VERSION 3.11)

# The HomeStore-FREE CRAFT reference model (MemCraftReplica + MemTransport). It is compiled with
# COMMON_DEPS' include paths (the header-only homestore/error.hpp + iomgr_types.hpp, plus sisl / fmt /
# boost) but references NO homestore symbol -- COMMON_DEPS is PRIVATE so nothing is propagated, and the
# model unit test links WITHOUT homestore to enforce that. This OBJECT lib is intentionally NOT
# aggregated into libhomeblocks; only the tests (and any reference driver) pull its objects.
add_library(${PROJECT_NAME}_craft_mem OBJECT)
target_sources(${PROJECT_NAME}_craft_mem PRIVATE
    mem_craft_replica.cpp
    mem_craft_cluster.cpp
)
target_link_libraries(${PROJECT_NAME}_craft_mem PRIVATE ${COMMON_DEPS})

add_subdirectory(tests)
