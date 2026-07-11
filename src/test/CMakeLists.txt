cmake_minimum_required (VERSION 3.11)

# Test-local, non-shipped code:
#   legacy_ublk/ -- the original ublk driver over the deprecated volume block API (drop with that API).
# (The CRAFT reference model + client + ublk driver moved out to the craft_client and ublkpp packages;
#  homeblocks now only declares the volume_handle CRAFT API and the homestore-backed CraftReplDev.)
#
# ublkpp (a test_requires) backs the ublk driver; detect it once here and gate the ublk target on
# UblkPP_FOUND (inherited into the child scope).
find_package(UblkPP QUIET)
if (NOT UblkPP_FOUND)
    message(STATUS "ublkpp not found; the ublk driver will be skipped")
endif()

if (UblkPP_FOUND)
    add_subdirectory(legacy_ublk)
endif()
