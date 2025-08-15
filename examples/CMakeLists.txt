# Examples directory for Blackbird

# Find required packages
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED liburing)
find_package(glog REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(YAMLCPP REQUIRED yaml-cpp)

# Benchmark disk backends example
add_executable(benchmark_disk_backends
    benchmark_disk_backends.cpp
)

target_link_libraries(benchmark_disk_backends
    blackbird
    ${LIBURING_LIBRARIES}
    glog::glog
    ${YAMLCPP_LIBRARIES}
    pthread
)

target_include_directories(benchmark_disk_backends PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${LIBURING_INCLUDE_DIRS}
    ${YAMLCPP_INCLUDE_DIRS}
)

target_compile_options(benchmark_disk_backends PRIVATE
    ${LIBURING_CFLAGS_OTHER}
    ${YAMLCPP_CFLAGS_OTHER}
)

# Test disk allocation example
add_executable(test_disk_allocation
    test_disk_allocation.cpp
)

target_link_libraries(test_disk_allocation
    blackbird
    ${LIBURING_LIBRARIES}
    glog::glog
    ${YAMLCPP_LIBRARIES}
    pthread
)

target_include_directories(test_disk_allocation PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${LIBURING_INCLUDE_DIRS}
    ${YAMLCPP_INCLUDE_DIRS}
)

target_compile_options(test_disk_allocation PRIVATE
    ${LIBURING_CFLAGS_OTHER}
    ${YAMLCPP_CFLAGS_OTHER}
)

# UCX transport benchmark
add_executable(benchmark_ucx_transports
    benchmark_ucx_transports.cpp
)

target_link_libraries(benchmark_ucx_transports
    blackbird
    ${LIBURING_LIBRARIES}
    glog::glog
    ${YAMLCPP_LIBRARIES}
    pthread
)

target_include_directories(benchmark_ucx_transports PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${LIBURING_INCLUDE_DIRS}
    ${YAMLCPP_INCLUDE_DIRS}
)

target_compile_options(benchmark_ucx_transports PRIVATE
    ${LIBURING_CFLAGS_OTHER}
    ${YAMLCPP_CFLAGS_OTHER}
)


add_executable(simple_keystone_client
    simple_keystone_client.cpp
)

target_link_libraries(simple_keystone_client
    blackbird
    ${LIBURING_LIBRARIES}
    glog::glog
    ${YAMLCPP_LIBRARIES}
    pthread
)

target_include_directories(simple_keystone_client PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${LIBURING_INCLUDE_DIRS}
    ${YAMLCPP_INCLUDE_DIRS}
)

target_compile_options(simple_keystone_client PRIVATE
    ${LIBURING_CFLAGS_OTHER}
    ${YAMLCPP_CFLAGS_OTHER}
)

# Worker example
add_executable(worker_example
    worker_example.cpp
)

target_link_libraries(worker_example
    blackbird
    ${LIBURING_LIBRARIES}
    glog::glog
    ${YAMLCPP_LIBRARIES}
    pthread
)

target_include_directories(worker_example PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${LIBURING_INCLUDE_DIRS}
    ${YAMLCPP_INCLUDE_DIRS}
)

target_compile_options(worker_example PRIVATE
    ${LIBURING_CFLAGS_OTHER}
    ${YAMLCPP_CFLAGS_OTHER}
)

# Keystone example
add_executable(keystone_example
    keystone_example.cpp
)

target_link_libraries(keystone_example
    blackbird
    ${LIBURING_LIBRARIES}
    glog::glog
    ${YAMLCPP_LIBRARIES}
    pthread
)

target_include_directories(keystone_example PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${LIBURING_INCLUDE_DIRS}
    ${YAMLCPP_INCLUDE_DIRS}
)

target_compile_options(keystone_example PRIVATE
    ${LIBURING_CFLAGS_OTHER}
    ${YAMLCPP_CFLAGS_OTHER}
)

# Simple client test
add_executable(simple_client_test
    simple_client_test.cpp
)

target_link_libraries(simple_client_test
    blackbird
    ${LIBURING_LIBRARIES}
    glog::glog
    ${YAMLCPP_LIBRARIES}
    pthread
)

target_include_directories(simple_client_test PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${LIBURING_INCLUDE_DIRS}
    ${YAMLCPP_INCLUDE_DIRS}
)

target_compile_options(simple_client_test PRIVATE
    ${LIBURING_CFLAGS_OTHER}
    ${YAMLCPP_CFLAGS_OTHER}
)