add_executable(keystone_example keystone_example.cpp)
target_link_libraries(keystone_example blackbird)

target_compile_definitions(keystone_example PRIVATE GLOG_NO_ABBREVIATED_SEVERITIES)

set_target_properties(keystone_example PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

add_executable(simple_client_test simple_client_test.cpp)
target_link_libraries(simple_client_test blackbird)

target_compile_definitions(simple_client_test PRIVATE GLOG_NO_ABBREVIATED_SEVERITIES)

set_target_properties(simple_client_test PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

add_executable(worker_example worker_example.cpp)
target_link_libraries(worker_example blackbird)

target_compile_definitions(worker_example PRIVATE GLOG_NO_ABBREVIATED_SEVERITIES)

set_target_properties(worker_example PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

add_executable(ucx_client ../clients/ucx_client.cpp)

target_link_libraries(ucx_client PRIVATE blackbird)

add_executable(benchmark_client ../clients/benchmark_client.cpp)

target_link_libraries(benchmark_client PRIVATE blackbird)

install(TARGETS keystone_example simple_client_test worker_example ucx_client benchmark_client
    RUNTIME DESTINATION bin
) 