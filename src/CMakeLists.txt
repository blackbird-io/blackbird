# Source files for blackbird library
set(BLACKBIRD_SOURCES
    types.cpp
    etcd_service.cpp
    master_service.cpp
    rpc_service.cpp
    error/error_codes.cpp
)

# Create the main blackbird library
add_library(blackbird STATIC ${BLACKBIRD_SOURCES})

# Link libraries
target_link_libraries(blackbird
    ${UCX_LIBRARIES}
    ${GLOG_LIBRARIES}
    Threads::Threads
    etcd-cpp-apiv3
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