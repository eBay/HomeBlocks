cmake_minimum_required (VERSION 3.11)

add_flags("-Wno-unused-parameter")

list(GET sisl_INCLUDE_DIRS 0 SISL_TOP_INCLUDE)
include ("${SISL_TOP_INCLUDE}/../cmake/settings_gen.cmake")

if(NOT ${CMAKE_CURRENT_BINARY_DIR} STREQUAL ${CMAKE_CURRENT_SOURCE_DIR})
  include_directories(BEFORE ${CMAKE_CURRENT_BINARY_DIR})
endif()

add_library(${PROJECT_NAME}_core OBJECT)
target_sources(${PROJECT_NAME}_core PRIVATE
    homeblks_impl.cpp
    volume_mgr.cpp
    listener.cpp
    common.cpp
)
target_link_libraries(${PROJECT_NAME}_core
    ${COMMON_DEPS}
)

set(FLATBUFFERS_FLATC_EXECUTABLE ${flatbuffers_INCLUDE_DIRS}/../bin/flatc)
settings_gen_cpp(
    ${FLATBUFFERS_FLATC_EXECUTABLE}
    ${CMAKE_CURRENT_BINARY_DIR}/generated/
    "${PROJECT_NAME}_core"
	home_blks_config.fbs
  )

#add_subdirectory(homestore_backend)
#add_subdirectory(memory_backend)
add_subdirectory(volume)
add_subdirectory(tests)
