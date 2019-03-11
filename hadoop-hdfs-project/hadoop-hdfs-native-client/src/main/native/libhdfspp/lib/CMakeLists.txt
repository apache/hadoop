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

project(libhdfspp)

enable_testing()
include (CTest)

find_package(ASIO REQUIRED)
find_package(Doxygen)
find_package(OpenSSL REQUIRED)
find_package(Protobuf REQUIRED)
find_package(RapidXML REQUIRED)
find_package(Threads REQUIRED)
find_package(URIparser REQUIRED)

include(DecideSasl)
include(CheckCXXSourceCompiles)

include(HdfsppCompilerOptions)

# Check if thread_local is supported
unset (THREAD_LOCAL_SUPPORTED CACHE)
set (CMAKE_REQUIRED_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
check_cxx_source_compiles(
    "#include <thread>
    int main(void) {
      thread_local int s;
      return 0;
    }"
    THREAD_LOCAL_SUPPORTED)
if (NOT THREAD_LOCAL_SUPPORTED)
  message(FATAL_ERROR "FATAL ERROR: The required feature thread_local storage is not supported by your compiler. Known compilers that support this feature: GCC 4.8+, Visual Studio 2015+, Clang (community version 3.3+), Clang (version for Xcode 8+ and iOS 9+).")
endif (NOT THREAD_LOCAL_SUPPORTED)

# Check if PROTOC library was compiled with the compatible compiler by trying
# to compile some dummy code
unset (PROTOC_IS_COMPATIBLE CACHE)
set (CMAKE_REQUIRED_LIBRARIES protobuf protoc)
check_cxx_source_compiles(
    "#include <google/protobuf/io/printer.h>
    #include <string>
    int main(void) {
      ::google::protobuf::io::ZeroCopyOutputStream *out = NULL;
      ::google::protobuf::io::Printer printer(out, '$');
      printer.PrintRaw(std::string(\"test\"));
      return 0;
    }"
    PROTOC_IS_COMPATIBLE)
if (NOT PROTOC_IS_COMPATIBLE)
  message(WARNING "WARNING: the Protocol Buffers Library and the hdfs++ Library must both be compiled with the same (or compatible) compiler. Normally only the same major versions of the same compiler are compatible with each other.")
endif (NOT PROTOC_IS_COMPATIBLE)

if(DOXYGEN_FOUND)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/doc/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/doc/Doxyfile @ONLY)
add_custom_target(doc ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/doc/Doxyfile
                  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                  COMMENT "Generating API documentation with Doxygen" VERBATIM)
endif(DOXYGEN_FOUND)

# Set the build rpath so that we find the shared libraries.
if (BUILD_SHARED_LIBS AND UNIX)
  include (RpathUtils)
  set (CMAKE_BUILD_RPATH ${CMAKE_PREFIX_PATH})
  appendToEach(CMAKE_BUILD_RPATH "/lib")
  set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
  # cmake less than 3.8 doesn't support build_rpath, so use install rpath.
  if (CMAKE_VERSION VERSION_LESS "3.8")
    set (CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
    foreach (_dir ${CMAKE_BUILD_RPATH})
      list (APPEND CMAKE_INSTALL_RPATH ${_dir})
    endforeach (_dir)
    list (APPEND CMAKE_INSTALL_RPATH ${CMAKE_CURRENT_BINARY_DIR})
  endif ()
endif ()

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}/proto
  )

# Put the protobuf stuff first, since the version has to match between
# the library, generated code, and the include files.
include_directories(BEFORE ${PROTOBUF_INCLUDE_DIR})

include_directories(SYSTEM
  ${ASIO_INCLUDE_DIR}
  ${RAPIDXML_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR})

add_subdirectory(common)
add_subdirectory(fs)
add_subdirectory(reader)
add_subdirectory(rpc)
add_subdirectory(proto)
add_subdirectory(connection)
add_subdirectory(bindings)
add_subdirectory(test)

set(LIBHDFSPP_ALL_OBJECTS
    $<TARGET_OBJECTS:bindings_c>
    $<TARGET_OBJECTS:fs>
    $<TARGET_OBJECTS:rpc>
    $<TARGET_OBJECTS:reader>
    $<TARGET_OBJECTS:proto>
    $<TARGET_OBJECTS:connection>
    $<TARGET_OBJECTS:common>)

if (BUILD_SHARED_LIBS)
  add_library(hdfspp SHARED ${LIBHDFSPP_ALL_OBJECTS})
  set_target_properties(hdfspp PROPERTIES
                        SOVERSION ${HDFSPP_LIB_VERSION}
                        VERSION ${HDFSPP_VERSION})
else ()
  add_library(hdfspp STATIC ${LIBHDFSPP_ALL_OBJECTS})
    set_property(TARGET hdfspp
      PROPERTY INTERFACE_LINK_LIBRARIES
        sasl2 ssl crypto krb5 protobuf uriparser dl ${CMAKE_THREAD_LIBS_INIT}
    )
endif ()

target_link_libraries(hdfspp
  protobuf
  uriparser
  ssl
  crypto
  ${LIB_DL}
  ${SASL_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT}
)

get_filename_component(ABS_DIR ../include/hdfs REALPATH)

install(DIRECTORY ../include/hdfspp DESTINATION include)
install(DIRECTORY ${ABS_DIR} DESTINATION include)
install(TARGETS hdfspp
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib)

add_custom_target(
    InstallToBuildDirectory
    COMMAND "${CMAKE_MAKE_PROGRAM}" install DESTDIR=${PROJECT_BINARY_DIR}/output
)
set(LIBHDFSPP_DIR ${PROJECT_BINARY_DIR}/output)
