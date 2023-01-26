cmake_minimum_required (VERSION 3.11)

find_package(sisl REQUIRED)
find_package(Threads REQUIRED)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_subdirectory(lib)
add_subdirectory(examples)
