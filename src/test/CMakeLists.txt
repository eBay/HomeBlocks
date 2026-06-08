cmake_minimum_required (VERSION 3.11)

# Test-only ublk adapter. Relax pedantic/unused-parameter for the kernel UAPI headers (ublk_cmd.h,
# ublksrv.h) pulled in transitively via ublkpp; keep -Werror/-Wall/-Wextra.
add_flags("-Wno-pedantic -Wno-unused-parameter")

# ublkpp is a test_requires; if it isn't available (e.g. a tests-disabled build) skip the adapter+CLI.
find_package(UblkPP QUIET)
if (NOT UblkPP_FOUND)
    message(STATUS "ublkpp not found; skipping homeblk_ublk adapter/CLI")
    return()
endif()

# Adapter: exposes a homeblocks volume as a ublkpp::ublk_disk.
add_library(homeblk_ublk_disk OBJECT)
target_sources(homeblk_ublk_disk PRIVATE
    homeblk_disk.cpp
)
target_link_libraries(homeblk_ublk_disk
    ${COMMON_DEPS}
    UblkPP::UblkPP
)

# CLI: brings up homeblocks and exposes a volume as /dev/ublkbN (ublkpp_disk-style).
add_executable(homeblk_ublk)
target_sources(homeblk_ublk PRIVATE
    homeblk_ublk_cli.cpp
    $<TARGET_OBJECTS:homeblk_ublk_disk>
)
target_link_libraries(homeblk_ublk
    ${PROJECT_NAME}
    ${COMMON_TEST_DEPS}
    UblkPP::UblkPP
    -rdynamic
)
