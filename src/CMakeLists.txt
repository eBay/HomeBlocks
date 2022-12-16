cmake_minimum_required (VERSION 3.11)

# add warning options
add_flags("-Wall -Wextra -Werror -Wpedantic")

find_package(sisl REQUIRED)
find_package(Threads REQUIRED)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_subdirectory(state_machine)

add_library(${PROJECT_NAME})
target_sources(${PROJECT_NAME} PRIVATE
            $<TARGET_OBJECTS:state_machine>
        )
target_link_libraries(${PROJECT_NAME}
            sisl::sisl
        )
