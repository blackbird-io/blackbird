find_package(glog REQUIRED)

# Explicit source file listing - no wildcards for maintainable builds
set(BLACKBIRD_SOURCES
    keystone_service.cpp
    rpc_service.cpp
    etcd_service.cpp
    types.cpp
    error/error_codes.cpp
)

# Create the blackbird library
add_library(blackbird STATIC ${BLACKBIRD_SOURCES})

# Include directories
target_include_directories(blackbird PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

# Link libraries
target_link_libraries(blackbird PUBLIC
    glog::glog
    etcd-cpp-api-core
    cpprest
    nlohmann_json::nlohmann_json
)

# Compiler definitions
target_compile_definitions(blackbird PRIVATE 
    GLOG_NO_ABBREVIATED_SEVERITIES
    BLACKBIRD_USE_ETCD
)

# Set target properties
set_target_properties(blackbird PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
) 