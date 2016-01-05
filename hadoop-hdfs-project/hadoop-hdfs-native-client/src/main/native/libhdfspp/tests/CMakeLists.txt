#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Delegate some functionality to libhdfs, until libhdfspp is complete.
set (LIBHDFS_SRC_DIR ../../libhdfs)
set (LIBHDFS_TESTS_DIR ../../libhdfs-tests)
set (LIBHDFSPP_SRC_DIR ..)
set (LIBHDFSPP_LIB_DIR ${LIBHDFSPP_SRC_DIR}/lib)
set (LIBHDFSPP_BINDING_C ${LIBHDFSPP_LIB_DIR}/bindings/c)
include_directories(
    ${GENERATED_JAVAH}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
    ${JNI_INCLUDE_DIRS}
    ${LIBHDFS_SRC_DIR}/include
    ${LIBHDFS_SRC_DIR}/..
    ${LIBHDFS_SRC_DIR}
    ${OS_DIR}
)
add_library(test_common_obj OBJECT mock_connection.cc)
add_library(test_common $<TARGET_OBJECTS:test_common_obj>)

set(PROTOBUF_IMPORT_DIRS ${PROTO_HADOOP_TEST_DIR})

protobuf_generate_cpp(PROTO_TEST_SRCS PROTO_TEST_HDRS
  ${PROTO_HADOOP_TEST_DIR}/test.proto
  ${PROTO_HADOOP_TEST_DIR}/test_rpc_service.proto
)

# Shamelessly stolen from
#    http://stackoverflow.com/questions/9303711/how-do-i-make-ctest-run-a-program-with-valgrind-without-dart
function(add_memcheck_test name binary)
  add_test(${name} ${binary} ${ARGN})

  if(NOT MEMORYCHECK_COMMAND MATCHES "MEMORYCHECK_COMMAND-NOTFOUND" AND NOT SKIP_VALGRIND)
    set(memcheck_command "${MEMORYCHECK_COMMAND} ${MEMORYCHECK_COMMAND_OPTIONS}")
    separate_arguments(memcheck_command)
    add_test(memcheck_${name} ${memcheck_command} ./${binary} ${ARGN})
  endif()
endfunction(add_memcheck_test)


add_executable(remote_block_reader_test remote_block_reader_test.cc)
target_link_libraries(remote_block_reader_test test_common reader proto common connection ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES} gmock_main ${CMAKE_THREAD_LIBS_INIT})
add_memcheck_test(remote_block_reader remote_block_reader_test)

add_executable(sasl_digest_md5_test sasl_digest_md5_test.cc)
target_link_libraries(sasl_digest_md5_test common ${OPENSSL_LIBRARIES} gmock_main ${CMAKE_THREAD_LIBS_INIT})
add_memcheck_test(sasl_digest_md5 sasl_digest_md5_test)

add_executable(retry_policy_test retry_policy_test.cc)
target_link_libraries(retry_policy_test common gmock_main ${CMAKE_THREAD_LIBS_INIT})
add_memcheck_test(retry_policy retry_policy_test)

include_directories(${CMAKE_CURRENT_BINARY_DIR})
add_executable(rpc_engine_test rpc_engine_test.cc ${PROTO_TEST_SRCS} ${PROTO_TEST_HDRS})
target_link_libraries(rpc_engine_test test_common rpc proto common ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES} gmock_main ${CMAKE_THREAD_LIBS_INIT})
add_memcheck_test(rpc_engine rpc_engine_test)

add_executable(bad_datanode_test bad_datanode_test.cc)
target_link_libraries(bad_datanode_test rpc reader proto fs bindings_c rpc proto common reader connection ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES} gmock_main ${CMAKE_THREAD_LIBS_INIT})
add_memcheck_test(bad_datanode bad_datanode_test)

add_executable(node_exclusion_test node_exclusion_test.cc)
target_link_libraries(node_exclusion_test fs gmock_main common ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
add_memcheck_test(node_exclusion node_exclusion_test)

add_executable(configuration_test configuration_test.cc)
target_link_libraries(configuration_test common gmock_main ${CMAKE_THREAD_LIBS_INIT})
add_memcheck_test(configuration configuration_test)

add_executable(hdfs_configuration_test hdfs_configuration_test.cc)
target_link_libraries(hdfs_configuration_test common gmock_main ${CMAKE_THREAD_LIBS_INIT})
add_test(hdfs_configuration hdfs_configuration_test)

#This test requires a great deal of Hadoop Java infrastructure to run.
if(HADOOP_BUILD)
add_library(hdfspp_test_shim_static STATIC hdfs_shim.c libhdfs_wrapper.c libhdfspp_wrapper.cc ${LIBHDFSPP_BINDING_C}/hdfs.cc)

build_libhdfs_test(libhdfs_threaded hdfspp_test_shim_static expect.c test_libhdfs_threaded.c ${OS_DIR}/thread.c)
link_libhdfs_test(libhdfs_threaded hdfspp_test_shim_static fs reader rpc proto common connection ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES} native_mini_dfs ${JAVA_JVM_LIBRARY})
add_libhdfs_test(libhdfs_threaded hdfspp_test_shim_static)
endif(HADOOP_BUILD)
