cmake_minimum_required(VERSION 3.20)
project(blackbird VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

# Find packages
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)

# UCX
pkg_check_modules(UCX REQUIRED ucx)

# etcd-cpp-apiv3 (required for service discovery and HA)
find_package(etcd-cpp-apiv3 REQUIRED)

# glog for logging
find_package(PkgConfig REQUIRED)
pkg_check_modules(GLOG REQUIRED libglog)

# ylt for RPC (assuming it's installed)
find_path(YLT_INCLUDE_DIR ylt/coro_rpc/coro_rpc_server.hpp)
if(NOT YLT_INCLUDE_DIR)
    message(FATAL_ERROR "YLT library not found. Please install yaLanTingLibs.")
endif()

# nlohmann json
find_package(nlohmann_json QUIET)
if(NOT nlohmann_json_FOUND)
    # Fallback to find it manually
    find_path(NLOHMANN_JSON_INCLUDE_DIR nlohmann/json.hpp)
    if(NOT NLOHMANN_JSON_INCLUDE_DIR)
        message(FATAL_ERROR "nlohmann/json not found")
    endif()
endif()

# Include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${UCX_INCLUDE_DIRS}
    ${GLOG_INCLUDE_DIRS}
    ${YLT_INCLUDE_DIR}
)

# Link directories
link_directories(
    ${UCX_LIBRARY_DIRS}
    ${GLOG_LIBRARY_DIRS}
)

# Add subdirectories
add_subdirectory(src)

# Optional: Build examples
option(BUILD_EXAMPLES "Build example programs" ON)
if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# Optional: Build tests
option(BUILD_TESTS "Build tests" OFF)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif() 