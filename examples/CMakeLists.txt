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

install(TARGETS keystone_example simple_client_test worker_example
    RUNTIME DESTINATION bin
) 