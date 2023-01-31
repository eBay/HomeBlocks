cmake_minimum_required (VERSION 3.11)

add_library(service OBJECT)
target_sources(service PRIVATE
            repl_service.cpp
            home_repl_backend.cpp
        )
target_link_libraries(service
            nuraft::nuraft
            sisl::sisl
            homestore::homestore
            nuraft_mesg::nuraft_mesg
        )
target_compile_features(service PUBLIC cxx_std_17)
