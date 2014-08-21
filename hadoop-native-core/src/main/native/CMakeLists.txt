cmake_minimum_required(VERSION 2.6)
set(CMAKE_BUILD_TYPE, Release) # Default to release builds
enable_testing()
MESSAGE(STATUS "Building hadoop-native-core, the native Hadoop core libraries.")

include(../../../../hadoop-common-project/hadoop-common/src/JNIFlags.cmake NO_POLICY_SCOPE)
GET_FILENAME_COMPONENT(JNI_LIBRARY_NAME ${JAVA_JVM_LIBRARY} NAME)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -Wextra -O2 -fno-strict-aliasing")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_REENTRANT -D_GNU_SOURCE")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -Wno-unused")

macro(add_utest utest)
    add_test(${utest} ${CMAKE_CURRENT_BINARY_DIR}/${utest} ${utest})
endmacro(add_utest)

# Check to see if our compiler and linker support the __thread attribute.
# On Linux and some other operating systems, this is a more efficient
# alternative to POSIX thread local storage.
INCLUDE(CheckCSourceCompiles)
CHECK_C_SOURCE_COMPILES("int main(void) { static __thread int i = 0; return 0; }" HAVE_BETTER_TLS)

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
find_library(PROTOC_LIB NAMES libprotoc.a protoc
    HINTS /usr/lib /usr/lib64)
find_library(PROTOBUF_LIB NAMES libprotobuf.a protobuf
    HINTS /usr/lib /usr/lib64)
find_program(PROTOC_EXE NAMES protoc)
find_path(PROTOC_HEADER_PATH NAMES
    google/protobuf/compiler/command_line_interface.h
    HINTS /usr/include)
if (NOT (PROTOC_LIB AND PROTOBUF_LIB AND PROTOC_EXE AND PROTOC_HEADER_PATH))
    MESSAGE(FATAL_ERROR "Failed to find the C++ protobuf libraries, which are needed for RPC code generation.  PROTOC_LIB=${PROTOC_LIB}, PROTOBUF_LIB=${PROTOBUF_LIB}, PROTOC_EXE=${PROTOC_EXE}, PROTOC_HEADER_PATH=${PROTOC_HEADER_PATH}")
endif ()

# Find libexpat
find_library(EXPAT_LIB NAMES expat
    HINTS /usr/lib /usr/lib64)
find_path(EXPAT_HEADER_PATH NAMES expat.h
    HINTS /usr/include)
if (NOT (EXPAT_LIB AND EXPAT_HEADER_PATH))
    MESSAGE(FATAL_ERROR "Failed to find libexpat, which is needed for parsing configuration XML files. EXPAT_LIB=${XEXPAT_LIB}, EXPAT_HEADER_PATH=${EXPAT_HEADER_PATH}")
endif ()

# Find liburiparser
find_library(URIPARSER_LIB NAMES uriparser
    HINTS /usr/lib /usr/lib64)
find_path(URIPARSER_HEADER_PATH NAMES uriparser/Uri.h
    HINTS /usr/include)
if (NOT (URIPARSER_LIB AND URIPARSER_HEADER_PATH))
    MESSAGE(FATAL_ERROR "Failed to find liburiparser, which is needed for parsing URIs. URIPARSER_LIB=${URIPARSER_LIB}, URIPARSER_HEADER_PATH=${URIPARSER_HEADER_PATH}")
endif ()

include_directories(
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${EXPAT_HEADER_PATH}
    ${JNI_INCLUDE_DIRS}
    ${PROTOBUF_HEADER_PATH}
    ${URIPARSER_HEADER_PATH})

include(GenerateProtobufs.cmake NO_POLICY_SCOPE)

set(COMMON_SRCS
    common/hadoop_err.c
    common/hconf.c
    common/htable.c
    common/net.c
    common/string.c
    common/user.c
    common/uri.c
)
set(COMMON_DEPS
    ${EXPAT_LIB}
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

set(FS_SRCS
    fs/common.c
    fs/copy.c
    fs/fs.c
    ndfs/file.c
    ndfs/meta.c
    ndfs/ops.c
    ndfs/permission.c
    ndfs/util.c
    ${HDFS_PROTOBUF_SRCS}
)

set(FS_DEPS
    ${URIPARSER_LIB}
)

set(JNI_SRCS
    jni/exception.c
    jni/jni_helper.c
    jni/jnifs.c
)
set(JNI_DEPS
    ${JAVA_JVM_LIBRARY}
    uv
    pthread
)

set(HCONF_XML_TEST_PATH "${CMAKE_SOURCE_DIR}/test/common/conf")
CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

add_library(fstest STATIC
    test/native_mini_dfs.c
    test/posix_util.c
    test/test.c
    ${COMMON_SRCS}
    ${RPC_SRCS}
    ${JNI_SRCS}
    ${FS_SRCS}
)
target_link_libraries(fstest
    ${COMMON_DEPS}
    ${RPC_DEPS}
    ${FS_DEPS}
)

add_executable(varint-unit rpc/varint-unit.c
    rpc/varint.c test/test.c)
add_utest(varint-unit)

add_executable(string-unit common/string-unit.c
    common/hadoop_err.c common/string.c test/test.c)
add_utest(string-unit)
target_link_libraries(string-unit ${LIBUV_LIB})

add_executable(htable-unit common/htable-unit.c
    common/htable.c test/test.c)
add_utest(htable-unit)

add_executable(hconf-unit common/hconf-unit.c
    common/hconf.c common/htable.c common/hadoop_err.c test/test.c)
add_utest(hconf-unit)
target_link_libraries(hconf-unit ${EXPAT_LIB} ${LIBUV_LIB})

add_executable(net-unit common/hadoop_err.c common/net-unit.c
    common/net.c common/test.c)
add_utest(net-unit)
target_link_libraries(net-unit uv)

add_executable(hadoop_err-unit common/hadoop_err-unit.c
    common/hadoop_err.c test/test.c)
add_utest(hadoop_err-unit)
target_link_libraries(hadoop_err-unit ${LIBUV_LIB})

add_executable(uri-unit common/uri-unit.c
    common/uri.c common/hadoop_err.c common/string.c test/test.c)
add_utest(uri-unit)
target_link_libraries(uri-unit ${URIPARSER_LIB} ${LIBUV_LIB})

add_executable(client_id-unit rpc/client_id-unit.c
    rpc/client_id.c common/hadoop_err.c test/test.c)
add_utest(client_id-unit)
target_link_libraries(client_id-unit fstest)

add_executable(call-unit rpc/call-unit.c
    rpc/call.c common/hadoop_err.c test/test.c)
add_utest(call-unit)
target_link_libraries(call-unit fstest)

add_executable(namenode-rpc-unit
    ndfs/namenode-rpc-unit.c)
target_link_libraries(namenode-rpc-unit fstest)

add_executable(test_libhdfs_threaded
    test/fs/test_libhdfs_threaded.c
)
target_link_libraries(test_libhdfs_threaded
    fstest
)

add_executable(test_libhdfs_zerocopy
    test/fs/test_libhdfs_zerocopy.c
)
target_link_libraries(test_libhdfs_zerocopy
    fstest
)

add_executable(test_libhdfs_meta_ops
    test/fs/test_libhdfs_meta_ops.c
)
target_link_libraries(test_libhdfs_meta_ops
    fstest
)

# When we generate our shared libraries, we want to hide symbols by default,
# exporting only a few carefully chosen symbols.  This prevents symbol name
# conflicts between our library and client programs.  It also prevents client
# programs from calling internal APIs they shouldn't.
# TODO: figure out what flag should be used here for Windows.
IF(UNIX)
    SET(VISIBILITY_FLAGS "-fvisibility=hidden")
ENDIF(UNIX)

add_library(hdfs SHARED
    ${COMMON_SRCS}
    ${FS_SRCS}
    ${JNI_SRCS}
    ${RPC_SRCS}
)
target_link_libraries(hdfs
    ${COMMON_DEPS}
    ${FS_DEPS}
    ${LIB_DL}
    ${RPC_DEPS}
)
set(HDFS_CORE_VERSION_MAJOR 1)
set(HDFS_CORE_VERSION_MINOR 0)
set(HDFS_CORE_VERSION_PATCH 0)
set(HDFS_CORE_VERSION_STRING "${HDFS_CORE_VERSION_MAJOR}.${HDFS_CORE_VERSION_MINOR}.${HDFS_CORE_VERSION_PATCH}")
set_target_properties(hdfs PROPERTIES
    VERSION ${HDFS_CORE_VERSION_STRING}
    SOVERSION ${HDFS_CORE_VERSION_MAJOR})
SET_TARGET_PROPERTIES(hdfs PROPERTIES COMPILE_FLAGS ${VISIBILITY_FLAGS})

#add_library(yarn SHARED
#    ${COMMON_SRCS}
#    ${RPC_SRCS}
#    ${YARN_PROTOBUF_SRCS}
#)
#target_link_libraries(yarn
#    ${COMMON_DEPS}
#    ${RPC_DEPS}
#)
#set(YARN_VERSION_MAJOR 1)
#set(YARN_VERSION_MINOR 0)
#set(YARN_VERSION_PATCH 0)
#set(YARN_VERSION_STRING ${YARN_VERSION_MAJOR}.${YARN_VERSION_MINOR}.${YARN_VERSION_PATCH})
#set_target_properties(yarn PROPERTIES
#    VERSION ${YARN_VERSION_STRING}
#    SOVERSION ${YARN_VERSION_MAJOR})
