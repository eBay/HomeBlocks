cmake_minimum_required (VERSION 3.11)

# Match src/lib's relaxation: the homeblocks lib headers (volume.hpp -> index_fixed_kv/table.hpp) that
# mem_craft_volume.cpp pulls in have unused parameters; src/lib/CMakeLists.txt relaxes this dir-wide, so
# the model (which lived under src/lib) always built with it. Keep the same relaxation here.
add_flags("-Wno-unused-parameter")

# CRAFT development harness. NONE of this is aggregated into libhomeblocks -- it is all test-local so it
# can be dropped as a unit. Three sections:
#   model/  -- the in-memory reference model (HomeStore-free; throwaway once production CRAFT lands)
#   client/ -- the reference craft_client + ublk driver (the keepers; they graduate to src/lib once the
#              network shim replaces the in-memory volume_handle)
#   tests/  -- model unit test, public-API e2e, and client-sequencing test (no ublk device needed)
#
# Put this dir on the include path so "model/foo.hpp" / "client/foo.hpp" resolve from anywhere below.
# UblkPP_FOUND is inherited from the parent (src/test) scope; the ublk driver + CLI are gated on it.
include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})

# ── model: the HomeStore-free reference model (strict flags; references no homestore symbol) ──
add_library(${PROJECT_NAME}_craft_mem OBJECT)
target_sources(${PROJECT_NAME}_craft_mem PRIVATE
    model/mem_craft_replica.cpp
    model/mem_craft_cluster.cpp
)
target_link_libraries(${PROJECT_NAME}_craft_mem PRIVATE ${COMMON_DEPS})

# ── client: the reference CRAFT client over the public volume_handle API (ublkpp-free, strict) ──
add_library(craft_client OBJECT)
target_sources(craft_client PRIVATE
    client/craft_client.cpp
    client/dlsn_tracker.cpp
)
target_link_libraries(craft_client ${COMMON_DEPS})

# ── tests that need no ublk device (built regardless of ublkpp) ──

# Model unit test: links the HomeStore-free model objects + sisl + gtest, deliberately WITHOUT
# homestore::homestore -- a successful link proves the model references no homestore symbol.
add_executable(test_craft_memory)
target_sources(test_craft_memory PRIVATE
    tests/test_craft_memory.cpp
    tests/craft_test_main.cpp
    $<TARGET_OBJECTS:${PROJECT_NAME}_craft_mem>
)
target_link_libraries(test_craft_memory
    sisl::sisl
    GTest::gmock
    -rdynamic
)
# home_blocks.hpp (pulled in transitively) needs homestore/iomgr HEADERS only. Supply their include dirs
# WITHOUT linking the engine.
target_include_directories(test_craft_memory SYSTEM PRIVATE
    $<TARGET_PROPERTY:homestore::homestore,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:iomgr::iomgr,INTERFACE_INCLUDE_DIRECTORIES>
)
add_test(NAME CraftMemoryModelTest COMMAND test_craft_memory)

# dLSN tracker unit test: drives reserve/resolve directly (no replicas, no model), so every branch of the
# read-horizon rule is deterministic. Like test_craft_memory it links WITHOUT homestore::homestore.
add_executable(test_craft_dlsn_tracker)
target_sources(test_craft_dlsn_tracker PRIVATE
    tests/test_dlsn_tracker.cpp
    tests/craft_test_main.cpp
    client/dlsn_tracker.cpp
)
target_link_libraries(test_craft_dlsn_tracker
    sisl::sisl
    GTest::gmock
    -rdynamic
)
target_include_directories(test_craft_dlsn_tracker SYSTEM PRIVATE
    $<TARGET_PROPERTY:homestore::homestore,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:iomgr::iomgr,INTERFACE_INCLUDE_DIRECTORIES>
)
add_test(NAME CraftDlsnTrackerTest COMMAND test_craft_dlsn_tracker)

# End-to-end test: drives the model through the PUBLIC volume_handle API via create_memory_volume. This
# one DOES link HomeStore (through libhomeblocks + volume). mem_craft_volume.cpp is compiled directly
# (it needs volume.hpp and is not aggregated into libhomeblocks).
add_executable(test_craft_e2e)
target_sources(test_craft_e2e PRIVATE
    tests/test_craft_e2e.cpp
    model/mem_craft_volume.cpp
    $<TARGET_OBJECTS:${PROJECT_NAME}_craft_mem>
)
target_link_libraries(test_craft_e2e
    ${PROJECT_NAME}
    ${COMMON_TEST_DEPS}
    -rdynamic
)
add_test(NAME CraftMemoryE2ETest COMMAND test_craft_e2e)

# Client sequencing test: drives craft_client (dLSN assignment / broadcast + quorum / commit frontier /
# read horizon) against the model at n=1 and n=3. No ublk device -- a fast CI check of the client logic.
add_executable(test_craft_client)
target_sources(test_craft_client PRIVATE
    tests/test_craft_client.cpp
    model/mem_craft_volume.cpp
    $<TARGET_OBJECTS:craft_client>
    $<TARGET_OBJECTS:${PROJECT_NAME}_craft_mem>
)
target_link_libraries(test_craft_client
    ${PROJECT_NAME}
    ${COMMON_TEST_DEPS}
    -rdynamic
)
add_test(NAME CraftClientTest COMMAND test_craft_client)

# ── CRAFT ublk driver + CLI (need ublkpp; kernel UAPI headers need pedantic/unused relaxed per target) ──
if (UblkPP_FOUND)
    add_library(craft_ublk_disk OBJECT)
    target_sources(craft_ublk_disk PRIVATE
        client/craft_ublk_disk.cpp
    )
    target_compile_options(craft_ublk_disk PRIVATE -Wno-pedantic -Wno-unused-parameter)
    target_link_libraries(craft_ublk_disk
        ${COMMON_DEPS}
        UblkPP::UblkPP
    )

    # CLI: stands up an in-memory replica set, logs a craft_client in, exposes it as /dev/ublkbN. No
    # HomeStore -- only iomgr (for the ublk<->client completion bridge). Compiles mem_craft_volume.cpp +
    # the model objects directly (as test_craft_e2e does).
    add_executable(craft_ublk)
    target_sources(craft_ublk PRIVATE
        client/craft_ublk_cli.cpp
        model/mem_craft_volume.cpp
        $<TARGET_OBJECTS:craft_client>
        $<TARGET_OBJECTS:craft_ublk_disk>
        $<TARGET_OBJECTS:${PROJECT_NAME}_craft_mem>
    )
    target_compile_options(craft_ublk PRIVATE -Wno-pedantic -Wno-unused-parameter)
    target_link_libraries(craft_ublk
        ${PROJECT_NAME}
        ${COMMON_TEST_DEPS}
        UblkPP::UblkPP
        -rdynamic
    )
endif()
