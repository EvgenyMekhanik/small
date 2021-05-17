project(small C CXX)
cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

include(CheckFunctionExists)
include(CheckSymbolExists)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

if(NOT DEFINED SMALL_EMBEDDED)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -Wall -Wextra")
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -Werror")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -Werror")
endif()

# Enable GNU glibc extentions.
add_definitions("-D_GNU_SOURCE")

set(CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE")

check_symbol_exists(MAP_ANON sys/mman.h TARANTOOL_SMALL_HAVE_MAP_ANON)
check_symbol_exists(MAP_ANONYMOUS sys/mman.h TARANTOOL_SMALL_HAVE_MAP_ANONYMOUS)

check_function_exists(madvise TARANTOOL_SMALL_HAVE_MADVISE)
check_symbol_exists(MADV_DONTDUMP sys/mman.h TARANTOOL_SMALL_HAVE_MADV_DONTDUMP)

set(config_h "${CMAKE_CURRENT_BINARY_DIR}/src/include/small_config.h")
configure_file(
    "src/small_config.h.cmake"
    "${config_h}"
    )
message (STATUS "")

include_directories("${CMAKE_CURRENT_BINARY_DIR}/src/include")

# Valgrind
include_directories(third_party)

set(lib_headers
    "${config_h}"
    src/small_features.h
    src/ibuf.h
    src/lf_lifo.h
    src/lifo.h
    src/matras.h
    src/mempool.h
    src/obuf.h
    src/quota.h
    src/rb.h
    src/region.h
    src/rlist.h
    src/slab_arena.h
    src/slab_cache.h
    src/small_class.h
    src/small.h
    src/lsregion.h
    src/static.h)

set(lib_sources
    src/small_features.c
    src/slab_cache.c
    src/region.c
    src/mempool.c
    src/slab_arena.c
    src/small_class.c
    src/small.c
    src/matras.c
    src/ibuf.c
    src/obuf.c
    src/lsregion.c
    src/static.c)

add_library(${PROJECT_NAME} STATIC ${lib_sources})
target_link_libraries(${PROJECT_NAME} m)

enable_testing()
add_subdirectory(test)
add_subdirectory(perf)

if(DEFINED SMALL_EMBEDDED)
    # Don't build shared library and skip INSTALL() targets if this
    # library is used as submodule in other project.
    return()
endif()

option(ENABLE_VALGRIND "Enable integration with valgrind, a memory analyzing tool" OFF)
if (NOT ENABLE_VALGRIND)
    add_definitions(-DNVALGRIND=1)
endif()

add_library(${PROJECT_NAME}_shared SHARED ${lib_sources})
target_link_libraries(${PROJECT_NAME}_shared m)
set_target_properties(${PROJECT_NAME}_shared PROPERTIES VERSION 1.0 SOVERSION 1)
set_target_properties(${PROJECT_NAME}_shared PROPERTIES OUTPUT_NAME ${PROJECT_NAME})

include(GNUInstallDirs)

install(TARGETS ${PROJECT_NAME}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    COMPONENT library)

install(TARGETS ${PROJECT_NAME}_shared
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    COMPONENT library)

install(FILES ${lib_headers} DESTINATION include/${PROJECT_NAME})
install(DIRECTORY third_party DESTINATION include/${PROJECT_NAME} FILES_MATCHING PATTERN "*.h")
