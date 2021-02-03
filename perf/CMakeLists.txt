set(CMAKE_CXX_STANDARD 11)

find_package(benchmark QUIET)
if (NOT ${benchmark_FOUND})
    message(AUTHOR_WARNING "Google Benchmark submodule not found")
    return()
endif()

include_directories("${PROJECT_SOURCE_DIR}")

add_executable(small.perftest small.cc)
target_link_libraries(small.perftest small benchmark::benchmark)
