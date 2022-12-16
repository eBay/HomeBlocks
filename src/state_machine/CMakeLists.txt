cmake_minimum_required (VERSION 3.11)

add_library(state_machine OBJECT)
target_sources(state_machine PRIVATE
            state_machine.cpp
        )
target_link_libraries(state_machine
            sisl::sisl
        )
target_compile_features(state_machine PUBLIC cxx_std_17)
