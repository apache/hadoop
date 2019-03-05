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

# This build script is set up as top-level driver and four modules:
#
#   * lib      - the library
#   * tests    - the integration tests that depend on hadoop & libhdfs
#   * tools    - the tools built upon the library
#   * examples - additional examples using the library

# It is designed so that the dependencies are used from the host if they
# are available. If they are not found or explicitly specified via *_HOME,
# they will be downloaded and built using CMake's external project support.

# A side effect of using external projects for the dependencies is
# that our code also needs to be built as external projects, because
# of the CMake's life cycle. (This enables the find_package to find
# the version that we built if necessary.) Another important point is
# that CMAKE_PREFIX_PATH is a list and when we pass it down to the
# modules via external project's CMAKE_ARGS, we need to change the
# quote character via LIST_SEPARATOR so that they aren't interpreted
# as additional args.

# The build directory is organized as:
#   * projects/Build/${projectname}    - the build directory for each project
#   * projects/Download/${projectname} - the download for each project
#   * projects/Source/${projectname}   - the source for each project
#   * projects/Stamp/${projectname}    - the timestamp files for each project
#   * staging/{include,bin,lib}        - the files that will be installed

# If cmake variable HDFSPP_LIBRARY_ONLY is set, then tests, examples, and
# tools will not be built. This allows for faster builds of the libhdfspp
# library alone, avoids looking for a JDK, valgrind, and gmock, and
# prevents the generation of multiple binaries that might not be relevant
# to other projects during normal use.
# Example of cmake invocation with HDFSPP_LIBRARY_ONLY enabled:
#   * cmake -DHDFSPP_LIBRARY_ONLY=1

cmake_minimum_required(VERSION 2.8.12)
cmake_policy(SET CMP0022 NEW) # support interface_link_libraries
if (POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW) # suppress warning about mac rpath
endif ()

project (HDFSPP C CXX)

set (CMAKE_PROJECT_VERSION_MAJOR 3)
set (CMAKE_PROJECT_VERSION_MINOR 2)
set (CMAKE_PROJECT_VERSION_PATCH "0-SNAPSHOT")
set (HDFSPP_VERSION
    ${CMAKE_PROJECT_VERSION_MAJOR}.${CMAKE_PROJECT_VERSION_MINOR}.${CMAKE_PROJECT_VERSION_PATCH})
set (PROJECT_DESCRIPTION "A native C++ implementation of the HDFS client")
set (PROJECT_HOMEPAGE_URL https://hadoop.apache.org/)

option(BUILD_SHARED_LIBS
       "Build shared libraries instead of static ones"
       ON)

option(HADOOP_BUILD
       "The package is being built as part of Hadoop"
       OFF)

option(HDFSPP_LIBRARY_ONLY
       "Only build the library and not the test, examples, and tools"
       OFF)

set(HDFSPP_SASL_IMPL CYRUS
    CACHE STRING "Which SASL implementation to use (NONE, CYRUS, GSASL).")

SET(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
set_directory_properties(PROPERTIES EP_BASE
  "${CMAKE_CURRENT_BINARY_DIR}/projects")
set(STAGING_DIR ${CMAKE_CURRENT_BINARY_DIR}/staging)
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${STAGING_DIR})
string(REPLACE ";" "|" quoted_prefix_path "${CMAKE_PREFIX_PATH}")

enable_testing()
INCLUDE (CTest)
INCLUDE (ExternalProject)

# Set the rpath so that we find the shared libraries.
if (BUILD_SHARED_LIBS)
  if (APPLE)
    set(CMAKE_MACOSX_RPATH ON)
    set(CMAKE_INSTALL_RPATH "@loader_path/../lib")
  elseif (UNIX)
    set(CMAKE_INSTALL_RPATH "\\$ORIGIN/../lib")
  endif ()
endif ()

add_subdirectory(third_party)

set (HDFSPP_CMAKE_ARGS
     -DBUILD_SHARED_LIBS=${BUILD_SHARED_LIBS}
     -DCMAKE_INSTALL_PREFIX=${STAGING_DIR}
     -DCMAKE_INSTALL_RPATH=${CMAKE_INSTALL_RPATH}
     -DCMAKE_MACOSX_RPATH=${CMAKE_MACOSX_RPATH}
     -DCMAKE_MODULE_PATH=${CMAKE_MODULE_PATH}
     -DCMAKE_PREFIX_PATH=${quoted_prefix_path}
     -DHDFSPP_SASL_IMPL=${HDFSPP_SASL_IMPL}
     -DHDFSPP_VERSION=${HDFSPP_VERSION}
     -DHDFSPP_LIB_VERSION=${CMAKE_PROJECT_VERSION_MAJOR}.${CMAKE_PROJECT_VERSION_MINOR}
     )

ExternalProject_Add(hdfspp
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lib
  CMAKE_ARGS ${HDFSPP_CMAKE_ARGS}
  INSTALL_DIR ${STAGING_DIR}
  LIST_SEPARATOR |
  TEST_BEFORE_INSTALL 1
  TEST_COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
                                      --output-on-failure
  DEPENDS third_party
  )

if (HADOOP_BUILD AND NOT HDFSPP_LIBRARY_ONLY)
  set (TESTS_CMAKE_ARGS ${HDFSPP_CMAKE_ARGS})
  list (APPEND TESTS_CMAKE_ARGS -DVALGRIND_REQUIRED=${VALGRIND_REQUIRED})
  ExternalProject_Add(hdfspp_tests
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests
    CMAKE_ARGS ${TESTS_CMAKE_ARGS}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
    TEST_BEFORE_INSTALL 1
    TEST_COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
                                        --output-on-failure
    DEPENDS hdfspp
  )
endif ()

if (NOT HDFSPP_LIBRARY_ONLY)
  ExternalProject_Add(hdfspp_examples
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/examples
    CMAKE_ARGS ${HDFSPP_CMAKE_ARGS}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
    DEPENDS hdfspp
  )
  ExternalProject_Add(hdfspp_tools
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tools
    CMAKE_ARGS ${HDFSPP_CMAKE_ARGS}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
    TEST_BEFORE_INSTALL 1
    TEST_COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
                                        --output-on-failure
    DEPENDS hdfspp
  )
endif ()

INSTALL(DIRECTORY
  ${STAGING_DIR}/bin ${STAGING_DIR}/include ${STAGING_DIR}/lib
  DESTINATION .
  OPTIONAL)

set(CPACK_GENERATOR "TGZ")
set(CPACK_PACKAGE_VENDOR "Apache")
set(CPACK_PACKAGE_CONTACT "Apache Hadoop <hdfs-dev@hadoop.apache.org>")
INCLUDE (CPack)
