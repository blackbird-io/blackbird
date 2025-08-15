cmake_minimum_required(VERSION 3.20)

include(FetchContent)

# Fetch GoogleTest
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

file(GLOB_RECURSE TEST_SOURCES
  allocation/*.cpp
)

add_executable(blackbird_tests ${TEST_SOURCES})

target_include_directories(blackbird_tests PRIVATE
  ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(blackbird_tests PRIVATE
  blackbird
  GTest::gtest
  GTest::gtest_main
  Threads::Threads
)

include(GoogleTest)
gtest_discover_tests(blackbird_tests)

