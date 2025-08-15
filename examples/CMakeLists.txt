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