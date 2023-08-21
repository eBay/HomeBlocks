cmake_minimum_required(VERSION 3.13)

include_directories (BEFORE ../include/)
include_directories (BEFORE ../lib/)
include_directories (BEFORE .)

find_package(GTest QUIET REQUIRED)

if (NOT ${BUILD_COVERAGE})
add_executable(test_home_raft_log_store)
target_sources(test_home_raft_log_store PRIVATE test_home_raft_log_store.cpp)
target_link_libraries(test_home_raft_log_store
            home_replication
            ${COMMON_TEST_DEPS}
            GTest::gmock
        )
    add_test(NAME HomeRaftLogStore COMMAND ${CMAKE_BINARY_DIR}/bin/test_home_raft_log_store)
    set_property(TEST HomeRaftLogStore PROPERTY RUN_SERIAL 1)
    message(WARNING "HomeRaftLogStore test is still disabled!")
endif()

add_executable(test_home_sm_store)
target_sources(test_home_sm_store PRIVATE test_home_sm_store.cpp)
target_link_libraries(test_home_sm_store 
            home_replication 
            ${COMMON_TEST_DEPS} 
            GTest::gmock)
#TODO Re-enable
#add_test(NAME HomeStateStore COMMAND ${CMAKE_BINARY_DIR}/bin/test_home_sm_store)
#set_property(TEST HomeStateStore PROPERTY RUN_SERIAL 1)
message(WARNING "HomeStateStore test is still disabled!")

add_executable(test_repl_state_machine)
target_sources(test_repl_state_machine PRIVATE test_repl_state_machine.cpp)
target_link_libraries(test_repl_state_machine 
            home_replication 
            ${COMMON_TEST_DEPS} 
            GTest::gmock)
add_test(NAME ReplStateMachine COMMAND ${CMAKE_BINARY_DIR}/bin/test_repl_state_machine)
set_property(TEST ReplStateMachine PROPERTY RUN_SERIAL 1)

add_executable(test_rpc_data_channel)
target_sources(test_rpc_data_channel PRIVATE test_rpc_data_channel.cpp)
target_link_libraries(test_rpc_data_channel 
            home_replication 
            ${COMMON_TEST_DEPS} 
            GTest::gmock)
add_test(NAME RpcDataChannel COMMAND ${CMAKE_BINARY_DIR}/bin/test_rpc_data_channel)
