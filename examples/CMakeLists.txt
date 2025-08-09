# Examples CMakeLists.txt

# Master service example
add_executable(master_example master_example.cpp)
target_link_libraries(master_example blackbird)

# Avoid glog abbreviated macro severities on this target as well
target_compile_definitions(master_example PRIVATE GLOG_NO_ABBREVIATED_SEVERITIES)

# Set properties
set_target_properties(master_example PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

# Install the example
install(TARGETS master_example
    RUNTIME DESTINATION bin/examples
) 