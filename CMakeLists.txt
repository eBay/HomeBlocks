cmake_minimum_required (VERSION 3.11)
project (home_replication)

include (cmake/Flags.cmake)
set(CMAKE_CXX_STANDARD 17)

if(EXISTS ${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
    include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
    conan_basic_setup(TARGETS)
else ()
    message("The file conanbuildinfo.cmake doesn't exist, some properties will be unavailable")
endif ()

if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    include (cmake/debug_flags.cmake)
endif()

if (DEFINED MEMORY_SANITIZER_ON)
    if (${MEMORY_SANITIZER_ON}) 
        include (cmake/mem_sanitizer.cmake)
        message(STATUS "********* WARNING: Running with Memory Sanitizer ON *********")
    endif()
else ()
    message(STATUS "********* WARNING: Running with Memory Sanitizer OFF *********")
endif()

find_program(CCACHE_FOUND ccache)
if (CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif ()

enable_testing()

# add conan information 
add_flags("-DPACKAGE_NAME=${PROJECT_NAME}")
if (DEFINED CONAN_PACKAGE_VERSION)
  message("Package Version: [${CONAN_PACKAGE_VERSION}]")
  add_flags("-DPACKAGE_VERSION=\\\"${CONAN_PACKAGE_VERSION}\\\"")
else ()
  message("Unknown Package Version")
  add_flags("-DPACKAGE_VERSION=\\\"${CONAN_PACKAGE_VERSION}\\\"")
endif ()

add_subdirectory(src)
