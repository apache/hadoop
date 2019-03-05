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

cmake_minimum_required(VERSION 2.8.12)
cmake_policy(SET CMP0022 NEW) # support interface_link_libraries
if (POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW) # suppress warning about mac rpath
endif ()

project (HDFSPP_TESTS CXX)

enable_testing()
INCLUDE (CTest)

find_package(Protobuf REQUIRED)
find_package(HDFSPP REQUIRED)
find_package(GTest REQUIRED)

include(HdfsppCompilerOptions)

# Delegate some functionality to libhdfs, until libhdfspp is complete.
set (LIBHDFS_SRC_DIR ${CMAKE_SOURCE_DIR}/../../libhdfs)
set (LIBHDFS_TESTS_DIR ${CMAKE_SOURCE_DIR}/../../libhdfs-tests)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-missing-field-initializers")

find_program(MEMORYCHECK_COMMAND valgrind HINTS ${VALGRIND_DIR} )
set(MEMORYCHECK_COMMAND_OPTIONS
  "--trace-children=no --leak-check=full --error-exitcode=1\
   --suppressions=${LIBHDFS_SRC_DIR}/tests/memcheck.supp")
message(STATUS "valgrind location: ${MEMORYCHECK_COMMAND}")

if (REQUIRE_VALGRIND AND
    MEMORYCHECK_COMMAND MATCHES "MEMORYCHECK_COMMAND-NOTFOUND" )
  message(FATAL_ERROR "valgrind was required but not found."
          "The path can be included via a -DVALGRIND_DIR=...\
           flag passed to CMake.")
endif ()

# Set the rpath so that we find the shared libraries.
if (BUILD_SHARED_LIBS AND APPLE)
  include (RpathUtils)
  set(CMAKE_BUILD_RPATH ${CMAKE_PREFIX_PATH})
  appendToEach(CMAKE_BUILD_RPATH "/lib")
endif ()

# Put the protobuf stuff first, since the version has to match between
# the library, generated code, and the include files.
include_directories(BEFORE ${PROTOBUF_INCLUDE_DIR})

install(TARGETS)
include_directories(
    ${GENERATED_JAVAH}
    ${JNI_INCLUDE_DIRS}
    ${LIBHDFS_SRC_DIR}/include
    ${LIBHDFS_SRC_DIR}/..
    ${LIBHDFS_SRC_DIR}
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

#
#
#   INTEGRATION TESTS - TESTS THE FULL LIBRARY AGAINST ACTUAL SERVERS
#
#
# This test requires a great deal of Hadoop Java infrastructure to run.
#
include_directories (
  #TODO: Put this in a variable up top and pull it out here
    ${CMAKE_CURRENT_SOURCE_DIR}/../../libhdfs-tests/
)

add_library(hdfspp_test_shim_static STATIC hdfs_shim.c libhdfs_wrapper.c libhdfspp_wrapper.cc ${LIBHDFSPP_BINDING_C}/hdfs.cc)
add_library(hdfspp_test_static STATIC ${LIBHDFSPP_BINDING_C}/hdfs.cc)

# Add dependencies
add_dependencies(hdfspp_test_shim_static proto)
add_dependencies(hdfspp_test_static proto)

# TODO: get all of the mini dfs library bits here in one place
# add_library(hdfspp_mini_cluster     native_mini_dfs ${JAVA_JVM_LIBRARY} )

#TODO: Link against full library rather than just parts

build_libhdfs_test(libhdfs_threaded hdfspp_test_shim_static expect.c test_libhdfs_threaded.c ${OS_DIR}/thread.c)
link_libhdfs_test(libhdfs_threaded hdfspp_test_shim_static fs reader rpc proto common connection ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES} native_mini_dfs ${JAVA_JVM_LIBRARY} ${SASL_LIBRARIES} )
add_libhdfs_test  (libhdfs_threaded hdfspp_test_shim_static)

build_libhdfs_test(hdfspp_mini_dfs_smoke hdfspp_test_shim_static ${CMAKE_CURRENT_LIST_DIR}/hdfspp_mini_dfs_smoke.cc)
link_libhdfs_test (hdfspp_mini_dfs_smoke hdfspp_test_shim_static fs reader rpc proto common connection gmock_main ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES} native_mini_dfs ${JAVA_JVM_LIBRARY} ${SASL_LIBRARIES})
add_libhdfs_test  (hdfspp_mini_dfs_smoke hdfspp_test_shim_static)

build_libhdfs_test(libhdfs_mini_stress_valgrind hdfspp_test_static expect.c test_libhdfs_mini_stress.c ${OS_DIR}/thread.c)
link_libhdfs_test(libhdfs_mini_stress_valgrind hdfspp_test_static fs reader rpc proto common connection ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES} native_mini_dfs ${JAVA_JVM_LIBRARY} ${SASL_LIBRARIES})
add_memcheck_test(libhdfs_mini_stress_valgrind_hdfspp_test_static libhdfs_mini_stress_valgrind_hdfspp_test_static)
set_target_properties(libhdfs_mini_stress_valgrind_hdfspp_test_static PROPERTIES COMPILE_DEFINITIONS "VALGRIND")

build_libhdfs_test(libhdfs_mini_stress hdfspp_test_shim_static expect.c test_libhdfs_mini_stress.c ${OS_DIR}/thread.c)
link_libhdfs_test(libhdfs_mini_stress hdfspp_test_shim_static fs reader rpc proto common connection ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES} native_mini_dfs ${JAVA_JVM_LIBRARY} ${SASL_LIBRARIES})
add_libhdfs_test(libhdfs_mini_stress hdfspp_test_shim_static)

build_libhdfs_test(hdfs_ext hdfspp_test_shim_static ${CMAKE_CURRENT_LIST_DIR}/hdfs_ext_test.cc)
link_libhdfs_test (hdfs_ext hdfspp_test_shim_static hdfspp_static gmock_main native_mini_dfs ${JAVA_JVM_LIBRARY}  ${SASL_LIBRARIES})
add_libhdfs_test  (hdfs_ext hdfspp_test_shim_static)

#build_libhdfs_test(hdfs_config_connect_bugs hdfspp_test_shim_static ${CMAKE_CURRENT_LIST_DIR}/hdfs_config_connect_bugs.cc)
#link_libhdfs_test (hdfs_config_connect_bugs hdfspp_test_shim_static hdfspp_static gmock_main native_mini_dfs ${JAVA_JVM_LIBRARY}  ${SASL_LIBRARIES})
#add_libhdfs_test  (hdfs_config_connect_bugs hdfspp_test_shim_static)
