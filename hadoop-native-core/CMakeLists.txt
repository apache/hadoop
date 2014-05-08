cmake_minimum_required(VERSION 2.6)
set(CMAKE_BUILD_TYPE, Release) # Default to release builds
enable_testing()
MESSAGE(STATUS "Building hadoop-native-core, the native Hadoop core libraries.")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -Wextra -O2 -fno-strict-aliasing")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_REENTRANT -D_GNU_SOURCE")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -Wno-unused")

macro(add_utest utest)
    add_test(${utest} ${CMAKE_CURRENT_BINARY_DIR}/${utest} ${utest})
endmacro(add_utest)

# Find libuv
find_library(LIBUV_LIB NAMES uv PATHS lib/libuv)
find_path(LIBUV_HEADER_PATH NAMES uv.h PATHS lib/libuv/include uv/include)
if (NOT (LIBUV_LIB AND LIBUV_HEADER_PATH))
    MESSAGE(FATAL_ERROR "Failed to find libuv.  Please install libuv.  LIBUV_LIB=${LIBUV_LIB}, LIBUV_HEADER_PATH=${LIBUV_HEADER_PATH}") 
endif ()
include_directories(
    lib/libuv/include
    uv/include)

# Find protobuf-c
find_library(PROTOBUFC_LIB NAMES protobuf-c 
    HINTS /usr/lib64 /usr/lib)
find_program(PROTOBUFC_EXE NAMES protoc-c)
if (NOT (PROTOBUFC_LIB AND PROTOBUFC_EXE))
    MESSAGE(FATAL_ERROR "Failed to find protobuf-c.  Please install protobuf-c.  PROTOBUFC_LIB=${PROTOBUFC_LIB}, PROTOBUFC_EXE=${PROTOBUFC_EXE}")
endif()

# Find protobuf
find_library(PROTOC_LIB NAMES protoc
    HINTS /usr/lib /usr/lib64)
find_library(PROTOBUF_LIB NAMES protobuf
    HINTS /usr/lib /usr/lib64)
find_program(PROTOC_EXE NAMES protoc)
find_path(PROTOC_HEADER_PATH NAMES 
    google/protobuf/compiler/command_line_interface.h
    HINTS /usr/include)
if (NOT (PROTOC_LIB AND PROTOBUF_LIB AND PROTOC_EXE AND PROTOC_HEADER_PATH))
    MESSAGE(FATAL_ERROR "Failed to find the C++ protobuf libraries, which are needed for RPC code generation.  PROTOC_LIB=${PROTOC_LIB}, PROTOBUF_LIB=${PROTOBUF_LIB}, PROTOC_EXE=${PROTOC_EXE}, PROTOC_HEADER_PATH=${PROTOC_HEADER_PATH}")
endif ()

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}
    ${PROTOBUF_HEADER_PATH})

include(GenerateProtobufs.cmake NO_POLICY_SCOPE)

set(COMMON_SRCS
    common/hadoop_err.c
    common/net.c
    common/string.c
    common/test.c
)
set(COMMON_DEPS
    pthread
)

set(RPC_SRCS
    rpc/call.c
    rpc/client_id.c
    rpc/conn.c
    rpc/messenger.c
    rpc/proxy.c
    rpc/reactor.c
    rpc/varint.c
    ${COMMON_PROTOBUF_SRCS}
)
set(RPC_DEPS
    ${LIBUV_LIB}
    ${PROTOBUFC_LIB}
)

add_executable(varint-unit rpc/varint-unit.c
    rpc/varint.c common/test.c)
add_utest(varint-unit)

add_executable(hadoop_err-unit common/hadoop_err-unit.c
    common/hadoop_err.c common/test.c)
add_utest(hadoop_err-unit)
target_link_libraries(hadoop_err-unit uv)

add_executable(namenode-rpc-unit hdfs/namenode-rpc-unit.c)
target_link_libraries(namenode-rpc-unit hdfs-core)

add_library(hdfs-core SHARED
    ${COMMON_SRCS}
    ${RPC_SRCS}
    ${HDFS_PROTOBUF_SRCS}
)
target_link_libraries(hdfs-core 
    ${COMMON_DEPS}
    ${RPC_DEPS}
)
set(HDFS_CORE_VERSION_MAJOR 1)
set(HDFS_CORE_VERSION_MINOR 0)
set(HDFS_CORE_VERSION_PATCH 0)
set(HDFS_CORE_VERSION_STRING "${HDFS_CORE_VERSION_MAJOR}.${HDFS_CORE_VERSION_MINOR}.${HDFS_CORE_VERSION_PATCH}")
set_target_properties(hdfs-core PROPERTIES
    VERSION ${HDFS_CORE_VERSION_STRING}
    SOVERSION ${HDFS_CORE_VERSION_MAJOR})

add_library(yarn-core SHARED
    ${COMMON_SRCS}
    ${RPC_SRCS}
    ${YARN_PROTOBUF_SRCS}
)
target_link_libraries(yarn-core 
    ${COMMON_DEPS}
    ${RPC_DEPS}
)
set(YARN_CORE_VERSION_MAJOR 1)
set(YARN_CORE_VERSION_MINOR 0)
set(YARN_CORE_VERSION_PATCH 0)
set(YARN_CORE_VERSION_STRING ${YARN_CORE_VERSION_MAJOR}.${YARN_CORE_VERSION_MINOR}.${YARN_CORE_VERSION_PATCH})
set_target_properties(yarn-core PROPERTIES
    VERSION ${YARN_CORE_VERSION_STRING}
    SOVERSION ${YARN_CORE_VERSION_MAJOR})
