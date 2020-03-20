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

find_package(Protobuf REQUIRED)
find_package(GTest REQUIRED)

include (ProtobufGenerate)

set(PROTOBUF_IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR})

protobuf_generate_cpp(PROTO_TEST_SRCS PROTO_TEST_HDRS
  ${CMAKE_CURRENT_SOURCE_DIR}/test.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/test_rpc_service.proto
)

find_program(MEMORYCHECK_COMMAND valgrind HINTS ${VALGRIND_DIR} )
set(MEMORYCHECK_COMMAND_OPTIONS
  "--trace-children=no --leak-check=full --error-exitcode=1 --suppressions=${PROJECT_SOURCE_DIR}/tests/memcheck.supp")
message(STATUS "valgrind location: ${MEMORYCHECK_COMMAND}")

if (REQUIRE_VALGRIND AND
    MEMORYCHECK_COMMAND MATCHES "MEMORYCHECK_COMMAND-NOTFOUND" )
  message(FATAL_ERROR "valgrind was required but not found."
          "The path can be included via a -DVALGRIND_DIR=... flag passed to CMake.")
endif ()

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

#
#
#   UNIT TESTS - TEST SELECTED PARTS OF THE LIBRARY
#
#

include_directories(
  ${CMAKE_BINARY_DIR}/test
  )

set (TEST_LIBS
  hdfspp
  ${SASL_LIBRARIES}
  ssl crypto
  gmock_main gmock gtest
  dl
  ${CMAKE_THREAD_LIBS_INIT})

function (define_test name)
  add_executable(${name}_test ${ARGN})
  target_link_libraries(${name}_test ${TEST_LIBS})
  add_memcheck_test(${name} ${name}_test)
endfunction (define_test)

define_test(remote_block_reader
  remote_block_reader_test.cc
  mock_connection.cc)

define_test(sasl_digest_md5 sasl_digest_md5_test.cc)
define_test(retry_policy retry_policy_test.cc)
define_test(bad_datanode bad_datanode_test.cc)
define_test(node_exclusion node_exclusion_test.cc)
define_test(configuration configuration_test.cc)
define_test(hdfs_configuration hdfs_configuration_test.cc)
define_test(hdfspp_errors hdfspp_errors.cc)
define_test(hdfs_builder hdfs_builder_test.cc)
define_test(logging logging_test.cc)
define_test(hdfs_ioservice hdfs_ioservice_test.cc)
define_test(user_lock user_lock_test.cc)
define_test(hdfs_config_connect_bugs hdfs_config_connect_bugs.cc)

define_test(rpc_engine
  rpc_engine_test.cc
  ${PROTO_TEST_SRCS}
  mock_connection.cc)
