find_package(glog REQUIRED)

# Source files for blackbird library
set(BLACKBIRD_SOURCES
    keystone/keystone_service.cpp
    rpc/rpc_service.cpp
    etcd/etcd_service.cpp
    worker/worker_service.cpp
    worker/storage/ram_backend.cpp
    transport/ucx_engine.cpp
    common/error/error_codes.cpp
    common/types.cpp
)

# Build the main blackbird library
add_library(blackbird ${BLACKBIRD_SOURCES})

# Include directories
target_include_directories(blackbird PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

# Link libraries
target_link_libraries(blackbird PUBLIC
    glog::glog
    nlohmann_json::nlohmann_json
    etcd-cpp-api-core
    yalantinglibs::yalantinglibs
    yaml-cpp
    ${UCX_LIBRARIES}
)

# Preprocessor definitions
target_compile_definitions(blackbird PUBLIC
    BLACKBIRD_USE_ETCD
)

# Compiler features
target_compile_features(blackbird PUBLIC cxx_std_20) 