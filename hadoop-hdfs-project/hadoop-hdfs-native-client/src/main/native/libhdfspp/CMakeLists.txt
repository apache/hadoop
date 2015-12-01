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

enable_testing()

find_package(Doxygen)
find_package(OpenSSL REQUIRED)
find_package(Protobuf REQUIRED)
find_package(Threads)

add_definitions(-DASIO_STANDALONE -DASIO_CPP11_DATE_TIME)

if(UNIX)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -std=c++11 -g -fPIC -fno-strict-aliasing")
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

include_directories(
  include
  lib
  ${PROJECT_BINARY_DIR}/lib/proto
  third_party/asio-1.10.2/include
  third_party/rapidxml-1.13
  third_party/gmock-1.7.0
  third_party/tr2
  ${OPENSSL_INCLUDE_DIR}
  ../libhdfs/include
)

set(PROTO_HDFS_DIR ${CMAKE_CURRENT_LIST_DIR}/../../../../../hadoop-hdfs-client/src/main/proto)
set(PROTO_HADOOP_DIR ${CMAKE_CURRENT_LIST_DIR}/../../../../../../hadoop-common-project/hadoop-common/src/main/proto)
set(PROTO_HADOOP_TEST_DIR ${CMAKE_CURRENT_LIST_DIR}/../../../../../../hadoop-common-project/hadoop-common/src/test/proto)

add_subdirectory(third_party/gmock-1.7.0)
add_subdirectory(lib)
add_subdirectory(tests)

# create an empty file; hadoop_add_dual_library wraps add_library which
# requires at least one file as an argument
set(EMPTY_FILE_CC ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/empty.cc)
file(WRITE ${EMPTY_FILE_CC} "")

hadoop_add_dual_library(hdfspp ${EMPTY_FILE_CC})
if(NEED_LINK_DL)
   set(LIB_DL dl)
endif()

SET(LIBHDFSPP_SUBLIBS bindings_c fs rpc reader proto common)
IF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  # linking a shared library from static ones requires --whole-archive
  SET(LIBHDFSPP_SUBLIBS -Wl,--whole-archive ${LIBHDFSPP_SUBLIBS} -Wl,--no-whole-archive)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

hadoop_target_link_dual_libraries(hdfspp
    ${LIBHDFSPP_SUBLIBS}
    ${LIB_DL}
    ${PROTOBUF_LIBRARY}
    ${OPENSSL_LIBRARIES}
)
