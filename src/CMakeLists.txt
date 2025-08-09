find_package(Protobuf REQUIRED)

# Generate protobuf sources
set(PROTO_SRC ${CMAKE_SOURCE_DIR}/proto/worker.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_SRC})

set(BLACKBIRD_SOURCES
    types.cpp
    etcd_service.cpp
    master_service.cpp
    error/error_codes.cpp
    ${PROTO_SRCS}
    proto_adapters.cpp
    rpc_service.cpp
)

# Create the main blackbird library
add_library(blackbird STATIC ${BLACKBIRD_SOURCES})

# Ensure glog does not export abbreviated macro severities (INFO/ERROR/etc.)
target_compile_definitions(blackbird PRIVATE GLOG_NO_ABBREVIATED_SEVERITIES)

target_include_directories(blackbird PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

# Link libraries
target_link_libraries(blackbird
    ${UCX_LIBRARIES}
    ${GLOG_LIBRARIES}
    Threads::Threads
    etcd-cpp-api
    protobuf::libprotobuf
)

target_compile_definitions(blackbird PRIVATE BLACKBIRD_USE_ETCD)

# Link nlohmann json if found as target
if(nlohmann_json_FOUND)
    target_link_libraries(blackbird nlohmann_json::nlohmann_json)
endif()

# Set target properties
set_target_properties(blackbird PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
)

# Install library and headers
install(TARGETS blackbird
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/blackbird
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
)

# Create executables
add_subdirectory(executables) 