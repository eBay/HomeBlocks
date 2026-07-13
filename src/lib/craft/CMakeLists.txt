cmake_minimum_required (VERSION 3.11)

add_library(${PROJECT_NAME}_craft OBJECT)
target_sources(${PROJECT_NAME}_craft PRIVATE
    craft_repl_dev.cpp
    craft_api.cpp
)
target_link_libraries(${PROJECT_NAME}_craft
    ${COMMON_DEPS}
)

# This is the CRAFT BACKEND -- the far side of the wire. craft_api.cpp defines the public per-replica surface
# (the volume_handle free functions) and craft_repl_dev.cpp is the engine behind it. The CRAFT client, the wire
# codec, the in-memory reference model and the reference server all live in the standalone craft_client package;
# the ublk driver lives in ublkpp. None of them are built or linked here.
