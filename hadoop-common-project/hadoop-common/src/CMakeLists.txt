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

cmake_minimum_required(VERSION 2.6 FATAL_ERROR)

# Default to release builds
set(CMAKE_BUILD_TYPE, Release)

include(JNIFlags.cmake NO_POLICY_SCOPE)

# Compile a library with both shared and static variants
function(add_dual_library LIBNAME)
    add_library(${LIBNAME} SHARED ${ARGN})
    add_library(${LIBNAME}_static STATIC ${ARGN})
    set_target_properties(${LIBNAME}_static PROPERTIES OUTPUT_NAME ${LIBNAME})
endfunction(add_dual_library)

# Link both a static and a dynamic target against some libraries
function(target_link_dual_libraries LIBNAME)
    target_link_libraries(${LIBNAME} ${ARGN})
    target_link_libraries(${LIBNAME}_static ${ARGN})
endfunction(target_link_dual_libraries)

function(output_directory TGT DIR)
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
endfunction(output_directory TGT DIR)

function(dual_output_directory TGT DIR)
    output_directory(${TGT} "${DIR}")
    output_directory(${TGT}_static "${DIR}")
endfunction(dual_output_directory TGT DIR)

if (NOT GENERATED_JAVAH)
    # Must identify where the generated headers have been placed
    MESSAGE(FATAL_ERROR "You must set the cmake variable GENERATED_JAVAH")
endif (NOT GENERATED_JAVAH)
find_package(JNI REQUIRED)
find_package(ZLIB REQUIRED)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -O2")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_REENTRANT -D_FILE_OFFSET_BITS=64")
set(D main/native/src/org/apache/hadoop)

GET_FILENAME_COMPONENT(HADOOP_ZLIB_LIBRARY ${ZLIB_LIBRARIES} NAME)

INCLUDE(CheckFunctionExists)
INCLUDE(CheckCSourceCompiles)
CHECK_FUNCTION_EXISTS(sync_file_range HAVE_SYNC_FILE_RANGE)
CHECK_FUNCTION_EXISTS(posix_fadvise HAVE_POSIX_FADVISE)

find_library(SNAPPY_LIBRARY 
    NAMES snappy
    PATHS ${CUSTOM_SNAPPY_PREFIX} ${CUSTOM_SNAPPY_PREFIX}/lib
          ${CUSTOM_SNAPPY_PREFIX}/lib64 ${CUSTOM_SNAPPY_LIB})
find_path(SNAPPY_INCLUDE_DIR 
    NAMES snappy.h
    PATHS ${CUSTOM_SNAPPY_PREFIX} ${CUSTOM_SNAPPY_PREFIX}/include
          ${CUSTOM_SNAPPY_INCLUDE})
if (SNAPPY_LIBRARY AND SNAPPY_INCLUDE_DIR)
    GET_FILENAME_COMPONENT(HADOOP_SNAPPY_LIBRARY ${SNAPPY_LIBRARY} NAME)
    set(SNAPPY_SOURCE_FILES
        "${D}/io/compress/snappy/SnappyCompressor.c"
        "${D}/io/compress/snappy/SnappyDecompressor.c")
else (SNAPPY_LIBRARY AND SNAPPY_INCLUDE_DIR)
    set(SNAPPY_INCLUDE_DIR "")
    set(SNAPPY_SOURCE_FILES "")
    IF(REQUIRE_SNAPPY)
        MESSAGE(FATAL_ERROR "Required snappy library could not be found.  SNAPPY_LIBRARY=${SNAPPY_LIBRARY}, SNAPPY_INCLUDE_DIR=${SNAPPY_INCLUDE_DIR}, CUSTOM_SNAPPY_INCLUDE_DIR=${CUSTOM_SNAPPY_INCLUDE_DIR}, CUSTOM_SNAPPY_PREFIX=${CUSTOM_SNAPPY_PREFIX}, CUSTOM_SNAPPY_INCLUDE=${CUSTOM_SNAPPY_INCLUDE}")
    ENDIF(REQUIRE_SNAPPY)
endif (SNAPPY_LIBRARY AND SNAPPY_INCLUDE_DIR)

include_directories(
    ${GENERATED_JAVAH}
    main/native/src
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}
    ${JNI_INCLUDE_DIRS}
    ${ZLIB_INCLUDE_DIRS}
    ${SNAPPY_INCLUDE_DIR}
)
CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

add_dual_library(hadoop
    ${D}/io/compress/lz4/Lz4Compressor.c
    ${D}/io/compress/lz4/Lz4Decompressor.c
    ${D}/io/compress/lz4/lz4.c
    ${SNAPPY_SOURCE_FILES}
    ${D}/io/compress/zlib/ZlibCompressor.c
    ${D}/io/compress/zlib/ZlibDecompressor.c
    ${D}/io/nativeio/NativeIO.c
    ${D}/io/nativeio/errno_enum.c
    ${D}/io/nativeio/file_descriptor.c
    ${D}/security/JniBasedUnixGroupsMapping.c
    ${D}/security/JniBasedUnixGroupsNetgroupMapping.c
    ${D}/security/getGroup.c
    ${D}/util/NativeCrc32.c
    ${D}/util/bulk_crc32.c
)
target_link_dual_libraries(hadoop
    dl
    ${JAVA_JVM_LIBRARY}
)
SET(LIBHADOOP_VERSION "1.0.0")
SET_TARGET_PROPERTIES(hadoop PROPERTIES
    SOVERSION ${LIBHADOOP_VERSION})
dual_output_directory(hadoop target/usr/local/lib)
