cmake_minimum_required (VERSION 3.11)
project (homeblocks LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)

if (NOT DEFINED CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE "Debug")
endif()

include (cmake/Flags.cmake)

enable_testing()
if ((DEFINED CODE_COVERAGE) AND (${CODE_COVERAGE}))
  include (cmake/CodeCoverage.cmake)
  APPEND_COVERAGE_COMPILER_FLAGS()
elseif ((DEFINED MEMORY_SANITIZER_ON) AND (${MEMORY_SANITIZER_ON}))
  message(WARNING "********* Running with Memory Sanitizer *********")
  add_flags("-fsanitize=address                 \
             -fsanitize=undefined               \
             -fsanitize-address-use-after-scope \
             -fno-sanitize=alignment            \
             -DCDS_ADDRESS_SANITIZER_ENABLED    \
             -fno-omit-frame-pointer            \
             -fno-optimize-sibling-calls        "
             )
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize=undefined")
endif()
find_package(GTest QUIET REQUIRED)

find_program(CCACHE_FOUND ccache QUIET)
if (CCACHE_FOUND)
   set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
   set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif ()

# add component version information
add_flags("-DPACKAGE_NAME=${PROJECT_NAME}")
add_flags("-DPACKAGE_VERSION=${PROJECT_VERSION}")

add_subdirectory(src)
