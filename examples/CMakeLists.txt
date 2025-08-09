# Examples CMakeLists.txt

# Keystone service example
add_executable(keystone_example setup_example.cpp)
target_link_libraries(keystone_example blackbird)

# Ensure proper linking and compilation flags
target_compile_definitions(keystone_example PRIVATE GLOG_NO_ABBREVIATED_SEVERITIES)

# Set additional properties
set_target_properties(keystone_example PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

install(TARGETS keystone_example
    RUNTIME DESTINATION bin
) 