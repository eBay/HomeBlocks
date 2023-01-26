cmake_minimum_required (VERSION 3.11)

add_library(local_journal OBJECT)
target_sources(local_journal PRIVATE
            home_journal.cpp
        )
target_link_libraries(local_journal
            homestore::homestore
            sisl::sisl
        )
target_compile_features(local_journal PUBLIC cxx_std_17)
