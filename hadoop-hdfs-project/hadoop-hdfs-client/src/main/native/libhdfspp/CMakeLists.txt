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

find_package(Doxygen)
find_package(Protobuf REQUIRED)
find_package(Threads)

add_definitions(-DASIO_STANDALONE -DASIO_CPP11_DATE_TIME)

if(UNIX)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -std=c++11 -g -fPIC -fno-strict-aliasing")
endif()

# Mac OS 10.7 and later deprecates most of the methods in OpenSSL.
# Add -Wno-deprecated-declarations to avoid the warnings.
if(APPLE)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -Wno-deprecated-declarations")
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
  third_party/gmock-1.7.0
)

add_subdirectory(third_party/gmock-1.7.0)
add_subdirectory(lib)
add_subdirectory(tests)
