cmake_minimum_required (VERSION 3.11)

# add warning options
add_flags("-Wall -Wextra -Werror -Wpedantic")

add_subdirectory(log_store)
add_subdirectory(service)
add_subdirectory(storage)
add_subdirectory(state_machine)

add_library(${PROJECT_NAME})
target_sources(${PROJECT_NAME} PRIVATE
            $<TARGET_OBJECTS:service>
            $<TARGET_OBJECTS:state_machine>
            $<TARGET_OBJECTS:log_store>
            $<TARGET_OBJECTS:storage_engine>
        )
target_link_libraries(${PROJECT_NAME}
            ${COMMON_DEPS}
        )
