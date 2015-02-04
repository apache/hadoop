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

#
# This macro alters the behavior of find_package and find_library.
# It does this by setting the CMAKE_FIND_LIBRARY_SUFFIXES global variable. 
# You should save that variable before calling this function and restore it
# after you have accomplished your goal.
#
# The behavior is altered in two ways:
# 1. We always find shared libraries, never static;
# 2. We find shared libraries with the given version number.
#
# On Windows this function is a no-op.  Windows does not encode
# version number information information into library path names.
#
macro(set_find_shared_library_version LVERS)
    IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
        # Mac OS uses .dylib
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".${LVERS}.dylib")
    ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
        # FreeBSD has always .so installed.
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
    ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
        # Windows doesn't support finding shared libraries by version.
    ELSE()
        # Most UNIX variants use .so
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".so.${LVERS}")
    ENDIF()
endmacro(set_find_shared_library_version LVERS)

#
# Alter the behavior of find_package and find_library so that we find only
# shared libraries without any version suffix.  You should save
# CMAKE_FIND_LIBRARY_SUFFIXES before calling this function and restore it
# afterwards.
#
macro(set_find_shared_library_without_version)
    IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
        # Mac OS uses .dylib
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".dylib")
    ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
        # No effect
    ELSE()
        # Most UNIX variants use .so
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
    ENDIF()
endmacro(set_find_shared_library_without_version)

if (NOT GENERATED_JAVAH)
    # Must identify where the generated headers have been placed
    MESSAGE(FATAL_ERROR "You must set the cmake variable GENERATED_JAVAH")
endif (NOT GENERATED_JAVAH)
find_package(JNI REQUIRED)

SET(STORED_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
set_find_shared_library_version("1")
find_package(ZLIB REQUIRED)
SET(CMAKE_FIND_LIBRARY_SUFFIXES ${STORED_CMAKE_FIND_LIBRARY_SUFFIXES})

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -O2")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_REENTRANT -D_GNU_SOURCE")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(D main/native/src/org/apache/hadoop)
set(T main/native/src/test/org/apache/hadoop)

GET_FILENAME_COMPONENT(HADOOP_ZLIB_LIBRARY ${ZLIB_LIBRARIES} NAME)

SET(STORED_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
set_find_shared_library_version("1")
find_package(BZip2 QUIET)
if (BZIP2_INCLUDE_DIR AND BZIP2_LIBRARIES)
    GET_FILENAME_COMPONENT(HADOOP_BZIP2_LIBRARY ${BZIP2_LIBRARIES} NAME)
    set(BZIP2_SOURCE_FILES
          "${D}/io/compress/bzip2/Bzip2Compressor.c"
          "${D}/io/compress/bzip2/Bzip2Decompressor.c")
else (BZIP2_INCLUDE_DIR AND BZIP2_LIBRARIES)
    set(BZIP2_SOURCE_FILES "")
    set(BZIP2_INCLUDE_DIR "")
    IF(REQUIRE_BZIP2)
        MESSAGE(FATAL_ERROR "Required bzip2 library and/or header files could not be found.")
    ENDIF(REQUIRE_BZIP2)
endif (BZIP2_INCLUDE_DIR AND BZIP2_LIBRARIES)
SET(CMAKE_FIND_LIBRARY_SUFFIXES ${STORED_CMAKE_FIND_LIBRARY_SUFFIXES})

INCLUDE(CheckFunctionExists)
INCLUDE(CheckCSourceCompiles)
INCLUDE(CheckLibraryExists)
CHECK_FUNCTION_EXISTS(sync_file_range HAVE_SYNC_FILE_RANGE)
CHECK_FUNCTION_EXISTS(posix_fadvise HAVE_POSIX_FADVISE)
CHECK_LIBRARY_EXISTS(dl dlopen "" NEED_LINK_DL)

SET(STORED_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
set_find_shared_library_version("1")
find_library(SNAPPY_LIBRARY 
    NAMES snappy
    PATHS ${CUSTOM_SNAPPY_PREFIX} ${CUSTOM_SNAPPY_PREFIX}/lib
          ${CUSTOM_SNAPPY_PREFIX}/lib64 ${CUSTOM_SNAPPY_LIB})
SET(CMAKE_FIND_LIBRARY_SUFFIXES ${STORED_CMAKE_FIND_LIBRARY_SUFFIXES})
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

# Find the no-suffix version of libcrypto.
# See HADOOP-11216 for details.
SET(STORED_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
set_find_shared_library_without_version()
SET(OPENSSL_NAME "crypto")
IF(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    SET(OPENSSL_NAME "eay32")
ENDIF()
MESSAGE("CUSTOM_OPENSSL_PREFIX = ${CUSTOM_OPENSSL_PREFIX}")
find_library(OPENSSL_LIBRARY
    NAMES ${OPENSSL_NAME}
    PATHS ${CUSTOM_OPENSSL_PREFIX} ${CUSTOM_OPENSSL_PREFIX}/lib
          ${CUSTOM_OPENSSL_PREFIX}/lib64 ${CUSTOM_OPENSSL_LIB} NO_DEFAULT_PATH)
find_library(OPENSSL_LIBRARY NAMES ${OPENSSL_NAME})
find_path(OPENSSL_INCLUDE_DIR 
    NAMES openssl/evp.h
    PATHS ${CUSTOM_OPENSSL_PREFIX} ${CUSTOM_OPENSSL_PREFIX}/include
          ${CUSTOM_OPENSSL_INCLUDE} NO_DEFAULT_PATH)
find_path(OPENSSL_INCLUDE_DIR NAMES openssl/evp.h)
SET(CMAKE_FIND_LIBRARY_SUFFIXES ${STORED_CMAKE_FIND_LIBRARY_SUFFIXES})
SET(USABLE_OPENSSL 0)
if (OPENSSL_LIBRARY AND OPENSSL_INCLUDE_DIR)
    GET_FILENAME_COMPONENT(HADOOP_OPENSSL_LIBRARY ${OPENSSL_LIBRARY} NAME)
    INCLUDE(CheckCSourceCompiles)
    SET(OLD_CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES})
    SET(CMAKE_REQUIRED_INCLUDES ${OPENSSL_INCLUDE_DIR})
    CHECK_C_SOURCE_COMPILES("#include \"${OPENSSL_INCLUDE_DIR}/openssl/evp.h\"\nint main(int argc, char **argv) { return !EVP_aes_256_ctr; }" HAS_NEW_ENOUGH_OPENSSL)
    SET(CMAKE_REQUIRED_INCLUDES ${OLD_CMAKE_REQUIRED_INCLUDES})
    if(NOT HAS_NEW_ENOUGH_OPENSSL)
        MESSAGE("The OpenSSL library installed at ${OPENSSL_LIBRARY} is too old.  You need a version at least new enough to have EVP_aes_256_ctr.")
    else(NOT HAS_NEW_ENOUGH_OPENSSL)
        SET(USABLE_OPENSSL 1)
    endif(NOT HAS_NEW_ENOUGH_OPENSSL)
endif (OPENSSL_LIBRARY AND OPENSSL_INCLUDE_DIR)
if (USABLE_OPENSSL)
    SET(OPENSSL_SOURCE_FILES
        "${D}/crypto/OpensslCipher.c"
        "${D}/crypto/random/OpensslSecureRandom.c")
else (USABLE_OPENSSL)
    MESSAGE("Cannot find a usable OpenSSL library.  OPENSSL_LIBRARY=${OPENSSL_LIBRARY}, OPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}, CUSTOM_OPENSSL_LIB=${CUSTOM_OPENSSL_LIB}, CUSTOM_OPENSSL_PREFIX=${CUSTOM_OPENSSL_PREFIX}, CUSTOM_OPENSSL_INCLUDE=${CUSTOM_OPENSSL_INCLUDE}")
    IF(REQUIRE_OPENSSL)
        MESSAGE(FATAL_ERROR "Terminating build because require.openssl was specified.")
    ENDIF(REQUIRE_OPENSSL)
    SET(OPENSSL_LIBRARY "")
    SET(OPENSSL_INCLUDE_DIR "")
    SET(OPENSSL_SOURCE_FILES "")
endif (USABLE_OPENSSL)

include_directories(
    ${GENERATED_JAVAH}
    main/native/src
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}
    ${JNI_INCLUDE_DIRS}
    ${ZLIB_INCLUDE_DIRS}
    ${BZIP2_INCLUDE_DIR}
    ${SNAPPY_INCLUDE_DIR}
    ${OPENSSL_INCLUDE_DIR}
    ${D}/util
)
CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

add_executable(test_bulk_crc32
    ${D}/util/bulk_crc32.c
    ${T}/util/test_bulk_crc32.c
)

SET(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
add_dual_library(hadoop
    main/native/src/exception.c
    ${D}/io/compress/lz4/Lz4Compressor.c
    ${D}/io/compress/lz4/Lz4Decompressor.c
    ${D}/io/compress/lz4/lz4.c
    ${D}/io/compress/lz4/lz4hc.c
    ${SNAPPY_SOURCE_FILES}
    ${OPENSSL_SOURCE_FILES}
    ${D}/io/compress/zlib/ZlibCompressor.c
    ${D}/io/compress/zlib/ZlibDecompressor.c
    ${BZIP2_SOURCE_FILES}
    ${D}/io/nativeio/NativeIO.c
    ${D}/io/nativeio/errno_enum.c
    ${D}/io/nativeio/file_descriptor.c
    ${D}/io/nativeio/SharedFileDescriptorFactory.c
    ${D}/net/unix/DomainSocket.c
    ${D}/net/unix/DomainSocketWatcher.c
    ${D}/security/JniBasedUnixGroupsMapping.c
    ${D}/security/JniBasedUnixGroupsNetgroupMapping.c
    ${D}/security/hadoop_group_info.c
    ${D}/security/hadoop_user_info.c
    ${D}/util/NativeCodeLoader.c
    ${D}/util/NativeCrc32.c
    ${D}/util/bulk_crc32.c
)
if (NEED_LINK_DL)
   set(LIB_DL dl)
endif (NEED_LINK_DL)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    #
    # By embedding '$ORIGIN' into the RPATH of libhadoop.so,
    # dlopen will look in the directory containing libhadoop.so.
    # However, $ORIGIN is not supported by all operating systems.
    #
    set(RPATH "\$ORIGIN/")
    if (EXTRA_LIBHADOOP_RPATH)
        set(RPATH "${RPATH}:${EXTRA_LIBHADOOP_RPATH}/")
    endif(EXTRA_LIBHADOOP_RPATH)
    SET_TARGET_PROPERTIES(hadoop 
        PROPERTIES INSTALL_RPATH "${RPATH}")
ENDIF()

target_link_dual_libraries(hadoop
    ${LIB_DL}
    ${JAVA_JVM_LIBRARY}
)
SET(LIBHADOOP_VERSION "1.0.0")
SET_TARGET_PROPERTIES(hadoop PROPERTIES
    SOVERSION ${LIBHADOOP_VERSION})
dual_output_directory(hadoop target/usr/local/lib)
