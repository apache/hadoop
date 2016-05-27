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

project (libhdfspp)

cmake_minimum_required(VERSION 2.8)

enable_testing()
include (CTest)
SET(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
SET(CMAKE_DISABLE_FIND_PACKAGE_CyrusSASL TRUE) # Until development is done.

find_package(Doxygen)
find_package(OpenSSL REQUIRED)
find_package(Protobuf REQUIRED)
find_package(GSasl)
find_package(CyrusSASL)
find_package(Threads)

find_program(MEMORYCHECK_COMMAND valgrind HINTS ${VALGRIND_DIR} )
set(MEMORYCHECK_COMMAND_OPTIONS "--trace-children=yes --leak-check=full --error-exitcode=1")
message(STATUS "valgrind location: ${MEMORYCHECK_COMMAND}")

if (REQUIRE_VALGRIND AND MEMORYCHECK_COMMAND MATCHES "MEMORYCHECK_COMMAND-NOTFOUND" )
  message(FATAL_ERROR "valgrind was required but not found.  "
                      "The path can be included via a -DVALGRIND_DIR=... flag passed to CMake.")
endif (REQUIRE_VALGRIND AND MEMORYCHECK_COMMAND MATCHES "MEMORYCHECK_COMMAND-NOTFOUND" )

# Prefer Cyrus SASL, but use GSASL if it is found
# Note that the packages can be disabled by setting CMAKE_DISABLE_FIND_PACKAGE_GSasl or
#    CMAKE_DISABLE_FIND_PACKAGE_CyrusSASL, respectively (case sensitive)
set (SASL_LIBRARIES)
set (SASL_INCLUDE_DIR)
if (CYRUS_SASL_FOUND)
    message(STATUS "Using Cyrus SASL; link with ${CYRUS_SASL_LIBRARIES}")
    set (SASL_INCLUDE_DIR ${CYRUS_SASL_INCLUDE_DIR})
    set (SASL_LIBRARIES ${CYRUS_SASL_SHARED_LIB})
    add_definitions(-DUSE_SASL -DUSE_CYRUS_SASL)
else (CYRUS_SASL_FOUND)
    if (REQUIRE_CYRUS_SASL)
      message(FATAL_ERROR "Cyrus SASL was required but not found.  "
                            "The path can be included via a -DCYRUS_SASL_DIR=... flag passed to CMake.")
    endif (REQUIRE_CYRUS_SASL)

    # If we didn't pick Cyrus, use GSASL instead
    if (GSASL_FOUND)
      message(STATUS "Using GSASL; link with ${GSASL_LIBRARIES}")
      set (SASL_INCLUDE_DIR ${GSASL_INCLUDE_DIR})
      set (SASL_LIBRARIES ${GSASL_LIBRARIES})
      add_definitions(-DUSE_SASL -DUSE_GSASL)
    else (GSASL_FOUND)
      if (REQUIRE_GSASL)
        message(FATAL_ERROR "GSASL was required but not found.  "
                            "The path can be included via a -DGSASL_DIR=... flag passed to CMake.")
      endif (REQUIRE_GSASL)
      message(STATUS "Not using SASL")
    endif (GSASL_FOUND)
endif (CYRUS_SASL_FOUND)

add_definitions(-DASIO_STANDALONE -DASIO_CPP11_DATE_TIME)

# Disable optimizations if compiling debug
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0")

if(UNIX)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -std=c++11 -g -fPIC -fno-strict-aliasing")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -fPIC -fno-strict-aliasing")
endif()

# Mac OS 10.7 and later deprecates most of the methods in OpenSSL.
# Add -Wno-deprecated-declarations to avoid the warnings.
if(APPLE)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -Wno-deprecated-declarations -Wno-unused-local-typedef")
endif()

if(DOXYGEN_FOUND)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/doc/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/doc/Doxyfile @ONLY)
add_custom_target(doc ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/doc/Doxyfile
                  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                  COMMENT "Generating API documentation with Doxygen" VERBATIM)
endif(DOXYGEN_FOUND)


# Copy files from the hadoop tree into the output/extern directory if
#    they've changed
function (copy_on_demand input_src_glob input_dest_dir)
  get_filename_component(src_glob ${input_src_glob} REALPATH)
  get_filename_component(dest_dir ${input_dest_dir} REALPATH)
  get_filename_component(src_dir ${src_glob} PATH)
  message(STATUS "Syncing ${src_glob} to ${dest_dir}")

  file(GLOB_RECURSE src_files ${src_glob})
  foreach(src_path ${src_files})
    file(RELATIVE_PATH relative_src ${src_dir} ${src_path})
    set(dest_path "${dest_dir}/${relative_src}")
    add_custom_command(TARGET copy_hadoop_files
     COMMAND ${CMAKE_COMMAND} -E copy_if_different "${src_path}" "${dest_path}"
    )
  endforeach()
endfunction()

# If we're building in the hadoop tree, pull the Hadoop files that
#     libhdfspp depends on.  This allows us to ensure that
#     the distribution will have a consistent set of headers and
#     .proto files
if(HADOOP_BUILD)
    set(HADOOP_IMPORT_DIR ${PROJECT_BINARY_DIR}/extern)
    get_filename_component(HADOOP_IMPORT_DIR ${HADOOP_IMPORT_DIR} REALPATH)

  add_custom_target(copy_hadoop_files ALL)

  # Gather the Hadoop files and resources that libhdfs++ needs to build
  copy_on_demand(../libhdfs/include/*.h* ${HADOOP_IMPORT_DIR}/include)
  copy_on_demand(${CMAKE_CURRENT_LIST_DIR}/../../../../../hadoop-hdfs-client/src/main/proto/*.proto ${HADOOP_IMPORT_DIR}/proto/hdfs)
  copy_on_demand(${CMAKE_CURRENT_LIST_DIR}/../../../../../../hadoop-common-project/hadoop-common/src/main/proto/*.proto  ${HADOOP_IMPORT_DIR}/proto/hadoop)
  copy_on_demand(${CMAKE_CURRENT_LIST_DIR}/../../../../../../hadoop-common-project/hadoop-common/src/test/proto/*.proto  ${HADOOP_IMPORT_DIR}/proto/hadoop_test)
else(HADOOP_BUILD)
  set(HADOOP_IMPORT_DIR ${CMAKE_CURRENT_LIST_DIR}/extern)
endif(HADOOP_BUILD)

# Paths to find the imported files
set(PROTO_HDFS_DIR         ${HADOOP_IMPORT_DIR}/proto/hdfs)
set(PROTO_HADOOP_DIR       ${HADOOP_IMPORT_DIR}/proto/hadoop)
set(PROTO_HADOOP_TEST_DIR  ${HADOOP_IMPORT_DIR}/proto/hadoop_test)

include_directories(
  include
  lib
  ${HADOOP_IMPORT_DIR}/include
)

include_directories( SYSTEM
  ${PROJECT_BINARY_DIR}/lib/proto
  third_party/asio-1.10.2/include
  third_party/rapidxml-1.13
  third_party/gmock-1.7.0
  third_party/tr2
  third_party/protobuf
  third_party/uriparser2
  ${OPENSSL_INCLUDE_DIR}
  ${SASL_INCLUDE_DIR}
)


add_subdirectory(third_party/gmock-1.7.0)
add_subdirectory(third_party/uriparser2)
add_subdirectory(lib)
add_subdirectory(tests)
add_subdirectory(examples)

# create an empty file; hadoop_add_dual_library wraps add_library which
# requires at least one file as an argument
set(EMPTY_FILE_CC ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/empty.cc)
file(WRITE ${EMPTY_FILE_CC} "")

# Build the output libraries
if(NEED_LINK_DL)
   set(LIB_DL dl)
endif()

set(LIBHDFSPP_ALL_OBJECTS $<TARGET_OBJECTS:bindings_c_obj> $<TARGET_OBJECTS:fs_obj> $<TARGET_OBJECTS:rpc_obj> $<TARGET_OBJECTS:reader_obj> $<TARGET_OBJECTS:proto_obj> $<TARGET_OBJECTS:connection_obj> $<TARGET_OBJECTS:common_obj> $<TARGET_OBJECTS:uriparser2_obj>)
if (HADOOP_BUILD)
  hadoop_add_dual_library(hdfspp ${EMPTY_FILE_CC} ${LIBHDFSPP_ALL_OBJECTS})
  hadoop_target_link_dual_libraries(hdfspp
    ${LIB_DL}
    ${PROTOBUF_LIBRARY}
    ${OPENSSL_LIBRARIES}
    ${SASL_LIBRARIES}
  )
else (HADOOP_BUILD)
  add_library(hdfspp_static STATIC ${EMPTY_FILE_CC} ${LIBHDFSPP_ALL_OBJECTS})
  target_link_libraries(hdfspp_static
    ${LIB_DL}
    ${PROTOBUF_LIBRARY}
    ${OPENSSL_LIBRARIES}
    ${SASL_LIBRARIES}
    )
  add_library(hdfspp SHARED ${EMPTY_FILE_CC} ${LIBHDFSPP_ALL_OBJECTS})
  target_link_libraries(hdfspp_static
    ${LIB_DL}
    ${PROTOBUF_LIBRARY}
    ${OPENSSL_LIBRARIES}
    ${SASL_LIBRARIES}
    )
endif (HADOOP_BUILD)
set(LIBHDFSPP_VERSION "0.1.0")
set_target_properties(hdfspp PROPERTIES
  SOVERSION ${LIBHDFSPP_VERSION})

# Set up make install targets
# Can be installed to a particular location via "make DESTDIR=... install"
file(GLOB_RECURSE LIBHDFSPP_HEADER_FILES "${CMAKE_CURRENT_LIST_DIR}/include/*.h*")
file(GLOB_RECURSE LIBHDFS_HEADER_FILES "${HADOOP_IMPORT_DIR}/include/*.h*")
install(FILES ${LIBHDFSPP_HEADER_FILES} DESTINATION /include/hdfspp)
install(FILES ${LIBHDFS_HEADER_FILES} DESTINATION /include/hdfs)

install(TARGETS hdfspp_static ARCHIVE DESTINATION /lib)
install(TARGETS hdfspp LIBRARY DESTINATION /lib)

add_custom_target(
    InstallToBuildDirectory
    COMMAND "${CMAKE_MAKE_PROGRAM}" install DESTDIR=${PROJECT_BINARY_DIR}/output
)
set(LIBHDFSPP_DIR ${PROJECT_BINARY_DIR}/output)
