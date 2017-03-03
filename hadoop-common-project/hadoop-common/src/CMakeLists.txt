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

#
# CMake configuration.
#

cmake_minimum_required(VERSION 2.6 FATAL_ERROR)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/..)
include(HadoopCommon)

# Source and test locations.
set(SRC main/native/src/org/apache/hadoop)
set(TST main/native/src/test/org/apache/hadoop)

#
# Main configuration.
#

# The caller must specify where the generated headers have been placed.
if(NOT GENERATED_JAVAH)
    message(FATAL_ERROR "You must set the CMake variable GENERATED_JAVAH")
endif()

# Configure JNI.
include(HadoopJNI)

# Require zlib.
set(STORED_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
hadoop_set_find_shared_library_version("1")
find_package(ZLIB REQUIRED)
set(CMAKE_FIND_LIBRARY_SUFFIXES ${STORED_CMAKE_FIND_LIBRARY_SUFFIXES})
get_filename_component(HADOOP_ZLIB_LIBRARY ${ZLIB_LIBRARIES} NAME)

# Look for bzip2.
set(STORED_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
hadoop_set_find_shared_library_version("1")
find_package(BZip2 QUIET)
if(BZIP2_INCLUDE_DIR AND BZIP2_LIBRARIES)
    get_filename_component(HADOOP_BZIP2_LIBRARY ${BZIP2_LIBRARIES} NAME)
    set(BZIP2_SOURCE_FILES
          "${SRC}/io/compress/bzip2/Bzip2Compressor.c"
          "${SRC}/io/compress/bzip2/Bzip2Decompressor.c")
    set(REQUIRE_BZIP2 ${REQUIRE_BZIP2}) # Stop warning about unused variable.
else()
    set(BZIP2_SOURCE_FILES "")
    set(BZIP2_INCLUDE_DIR "")
    if(REQUIRE_BZIP2)
        message(FATAL_ERROR "Required bzip2 library and/or header files could not be found.")
    endif()
endif()
set(CMAKE_FIND_LIBRARY_SUFFIXES ${STORED_CMAKE_FIND_LIBRARY_SUFFIXES})

# Require snappy.
set(STORED_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
hadoop_set_find_shared_library_version("1")
find_library(SNAPPY_LIBRARY
    NAMES snappy
    PATHS ${CUSTOM_SNAPPY_PREFIX} ${CUSTOM_SNAPPY_PREFIX}/lib
          ${CUSTOM_SNAPPY_PREFIX}/lib64 ${CUSTOM_SNAPPY_LIB})
set(CMAKE_FIND_LIBRARY_SUFFIXES ${STORED_CMAKE_FIND_LIBRARY_SUFFIXES})
find_path(SNAPPY_INCLUDE_DIR
    NAMES snappy.h
    PATHS ${CUSTOM_SNAPPY_PREFIX} ${CUSTOM_SNAPPY_PREFIX}/include
          ${CUSTOM_SNAPPY_INCLUDE})
if(SNAPPY_LIBRARY AND SNAPPY_INCLUDE_DIR)
    get_filename_component(HADOOP_SNAPPY_LIBRARY ${SNAPPY_LIBRARY} NAME)
    set(SNAPPY_SOURCE_FILES
        "${SRC}/io/compress/snappy/SnappyCompressor.c"
        "${SRC}/io/compress/snappy/SnappyDecompressor.c")
    set(REQUIRE_SNAPPY ${REQUIRE_SNAPPY}) # Stop warning about unused variable.
    message(STATUS "Found Snappy: ${SNAPPY_LIBRARY}")
else()
    set(SNAPPY_INCLUDE_DIR "")
    set(SNAPPY_SOURCE_FILES "")
    if(REQUIRE_SNAPPY)
        message(FATAL_ERROR "Required snappy library could not be found.  SNAPPY_LIBRARY=${SNAPPY_LIBRARY}, SNAPPY_INCLUDE_DIR=${SNAPPY_INCLUDE_DIR}, CUSTOM_SNAPPY_INCLUDE_DIR=${CUSTOM_SNAPPY_INCLUDE_DIR}, CUSTOM_SNAPPY_PREFIX=${CUSTOM_SNAPPY_PREFIX}, CUSTOM_SNAPPY_INCLUDE=${CUSTOM_SNAPPY_INCLUDE}")
    endif()
endif()

# Require zstandard
SET(STORED_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
hadoop_set_find_shared_library_version("1")
find_library(ZSTD_LIBRARY
    NAMES zstd
    PATHS ${CUSTOM_ZSTD_PREFIX} ${CUSTOM_ZSTD_PREFIX}/lib
          ${CUSTOM_ZSTD_PREFIX}/lib64 ${CUSTOM_ZSTD_LIB})
SET(CMAKE_FIND_LIBRARY_SUFFIXES ${STORED_CMAKE_FIND_LIBRARY_SUFFIXES})
find_path(ZSTD_INCLUDE_DIR
    NAMES zstd.h
    PATHS ${CUSTOM_ZSTD_PREFIX} ${CUSTOM_ZSTD_PREFIX}/include
          ${CUSTOM_ZSTD_INCLUDE})
if (ZSTD_LIBRARY AND ZSTD_INCLUDE_DIR)
    GET_FILENAME_COMPONENT(HADOOP_ZSTD_LIBRARY ${ZSTD_LIBRARY} NAME)
    set(ZSTD_SOURCE_FILES
        "${SRC}/io/compress/zstd/ZStandardCompressor.c"
        "${SRC}/io/compress/zstd/ZStandardDecompressor.c")
    set(REQUIRE_ZSTD ${REQUIRE_ZSTD}) # Stop warning about unused variable.
        message(STATUS "Found ZStandard: ${ZSTD_LIBRARY}")
else ()
    set(ZSTD_INCLUDE_DIR "")
    set(ZSTD_SOURCE_FILES "")
    IF(REQUIRE_ZSTD)
        MESSAGE(FATAL_ERROR "Required zstandard library could not be found.  ZSTD_LIBRARY=${ZSTD_LIBRARY}, ZSTD_INCLUDE_DIR=${ZSTD_INCLUDE_DIR}, CUSTOM_ZSTD_INCLUDE_DIR=${CUSTOM_ZSTD_INCLUDE_DIR}, CUSTOM_ZSTD_PREFIX=${CUSTOM_ZSTD_PREFIX}, CUSTOM_ZSTD_INCLUDE=${CUSTOM_ZSTD_INCLUDE}")
    ENDIF(REQUIRE_ZSTD)
endif ()

set(STORED_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
hadoop_set_find_shared_library_version("2")
find_library(ISAL_LIBRARY
    NAMES isal
     PATHS ${CUSTOM_ISAL_PREFIX} ${CUSTOM_ISAL_PREFIX}/lib
              ${CUSTOM_ISAL_PREFIX}/lib64 ${CUSTOM_ISAL_LIB} /usr/lib)
set(CMAKE_FIND_LIBRARY_SUFFIXES ${STORED_CMAKE_FIND_LIBRARY_SUFFIXES})
if (ISAL_LIBRARY)
    GET_FILENAME_COMPONENT(HADOOP_ISAL_LIBRARY ${ISAL_LIBRARY} NAME)
    set(ISAL_INCLUDE_DIR ${SRC}/io/erasurecode)
    set(ISAL_SOURCE_FILES
        ${SRC}/io/erasurecode/isal_load.c
        ${SRC}/io/erasurecode/erasure_code.c
        ${SRC}/io/erasurecode/gf_util.c
        ${SRC}/io/erasurecode/dump.c
        ${SRC}/io/erasurecode/erasure_coder.c
        ${SRC}/io/erasurecode/jni_erasure_code_native.c
        ${SRC}/io/erasurecode/jni_common.c
        ${SRC}/io/erasurecode/jni_rs_encoder.c
        ${SRC}/io/erasurecode/jni_rs_decoder.c
        ${SRC}/io/erasurecode/jni_xor_encoder.c
        ${SRC}/io/erasurecode/jni_xor_decoder.c)

        add_executable(erasure_code_test
        ${SRC}/io/erasurecode/isal_load.c
        ${SRC}/io/erasurecode/erasure_code.c
        ${SRC}/io/erasurecode/gf_util.c
        ${SRC}/io/erasurecode/dump.c
        ${SRC}/io/erasurecode/erasure_coder.c
        ${TST}/io/erasurecode/erasure_code_test.c
        )
        target_link_libraries(erasure_code_test ${CMAKE_DL_LIBS})
else (ISAL_LIBRARY)
    IF(REQUIRE_ISAL)
        MESSAGE(FATAL_ERROR "Required ISA-L library could not be found.  ISAL_LIBRARY=${ISAL_LIBRARY}, CUSTOM_ISAL_PREFIX=${CUSTOM_ISAL_PREFIX}")
    ENDIF(REQUIRE_ISAL)
endif (ISAL_LIBRARY)

# Build hardware CRC32 acceleration, if supported on the platform.
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^i.86$" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64")
  set(BULK_CRC_ARCH_SOURCE_FIlE "${SRC}/util/bulk_crc32_x86.c")
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
  set(BULK_CRC_ARCH_SOURCE_FIlE "${SRC}/util/bulk_crc32_aarch64.c")
else()
  message("No HW CRC acceleration for ${CMAKE_SYSTEM_PROCESSOR}, falling back to SW")
endif()

# Find the no-suffix version of libcrypto/openssl. See HADOOP-11216 for details.
set(STORED_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
hadoop_set_find_shared_library_without_version()
set(OPENSSL_NAME "crypto")
if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    SET(OPENSSL_NAME "eay32")
endif()
message("CUSTOM_OPENSSL_PREFIX = ${CUSTOM_OPENSSL_PREFIX}")
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
set(CMAKE_FIND_LIBRARY_SUFFIXES ${STORED_CMAKE_FIND_LIBRARY_SUFFIXES})
set(USABLE_OPENSSL 0)
if(OPENSSL_LIBRARY AND OPENSSL_INCLUDE_DIR)
    include(CheckCSourceCompiles)
    set(OLD_CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES})
    set(CMAKE_REQUIRED_INCLUDES ${OPENSSL_INCLUDE_DIR})
    check_c_source_compiles("#include \"${OPENSSL_INCLUDE_DIR}/openssl/evp.h\"\nint main(int argc, char **argv) { return !EVP_aes_256_ctr; }" HAS_NEW_ENOUGH_OPENSSL)
    set(CMAKE_REQUIRED_INCLUDES ${OLD_CMAKE_REQUIRED_INCLUDES})
    if(NOT HAS_NEW_ENOUGH_OPENSSL)
        message("The OpenSSL library installed at ${OPENSSL_LIBRARY} is too old.  You need a version at least new enough to have EVP_aes_256_ctr.")
    else()
        SET(USABLE_OPENSSL 1)
    endif()
endif()
if(USABLE_OPENSSL)
    get_filename_component(HADOOP_OPENSSL_LIBRARY ${OPENSSL_LIBRARY} NAME)
    set(OPENSSL_SOURCE_FILES
        "${SRC}/crypto/OpensslCipher.c"
        "${SRC}/crypto/random/OpensslSecureRandom.c")
    set(REQUIRE_OPENSSL ${REQUIRE_OPENSSL}) # Stop warning about unused variable.
else()
    message("Cannot find a usable OpenSSL library. OPENSSL_LIBRARY=${OPENSSL_LIBRARY}, OPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}, CUSTOM_OPENSSL_LIB=${CUSTOM_OPENSSL_LIB}, CUSTOM_OPENSSL_PREFIX=${CUSTOM_OPENSSL_PREFIX}, CUSTOM_OPENSSL_INCLUDE=${CUSTOM_OPENSSL_INCLUDE}")
    if(REQUIRE_OPENSSL)
        message(FATAL_ERROR "Terminating build because require.openssl was specified.")
    endif()
    set(OPENSSL_LIBRARY "")
    set(OPENSSL_INCLUDE_DIR "")
    set(OPENSSL_SOURCE_FILES "")
endif()

# Check for platform-specific functions and libraries.
include(CheckFunctionExists)
include(CheckLibraryExists)
check_function_exists(sync_file_range HAVE_SYNC_FILE_RANGE)
check_function_exists(posix_fadvise HAVE_POSIX_FADVISE)
check_library_exists(dl dlopen "" NEED_LINK_DL)

# Configure the build.
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
    ${ISAL_INCLUDE_DIR}
    ${ZSTD_INCLUDE_DIR}
    ${OPENSSL_INCLUDE_DIR}
    ${SRC}/util
)
configure_file(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
hadoop_add_dual_library(hadoop
    main/native/src/exception.c
    ${SRC}/io/compress/lz4/Lz4Compressor.c
    ${SRC}/io/compress/lz4/Lz4Decompressor.c
    ${SRC}/io/compress/lz4/lz4.c
    ${SRC}/io/compress/lz4/lz4hc.c
    ${ISAL_SOURCE_FILES}
    ${SNAPPY_SOURCE_FILES}
    ${ZSTD_SOURCE_FILES}
    ${OPENSSL_SOURCE_FILES}
    ${SRC}/io/compress/zlib/ZlibCompressor.c
    ${SRC}/io/compress/zlib/ZlibDecompressor.c
    ${BZIP2_SOURCE_FILES}
    ${SRC}/io/nativeio/NativeIO.c
    ${SRC}/io/nativeio/errno_enum.c
    ${SRC}/io/nativeio/file_descriptor.c
    ${SRC}/io/nativeio/SharedFileDescriptorFactory.c
    ${SRC}/net/unix/DomainSocket.c
    ${SRC}/net/unix/DomainSocketWatcher.c
    ${SRC}/security/JniBasedUnixGroupsMapping.c
    ${SRC}/security/JniBasedUnixGroupsNetgroupMapping.c
    ${SRC}/security/hadoop_group_info.c
    ${SRC}/security/hadoop_user_info.c
    ${SRC}/util/NativeCodeLoader.c
    ${SRC}/util/NativeCrc32.c
    ${SRC}/util/bulk_crc32.c
    ${BULK_CRC_ARCH_SOURCE_FIlE}
)
if(NEED_LINK_DL)
   set(LIB_DL dl)
endif()

hadoop_target_link_dual_libraries(hadoop ${LIB_DL} ${JAVA_JVM_LIBRARY})
set(LIBHADOOP_VERSION "1.0.0")
set_target_properties(hadoop PROPERTIES SOVERSION ${LIBHADOOP_VERSION})
hadoop_dual_output_directory(hadoop target/usr/local/lib)

# By embedding '$ORIGIN' into the RPATH of libhadoop.so, dlopen will look in
# the directory containing libhadoop.so. However, $ORIGIN is not supported by
# all operating systems.
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux|SunOS")
    set(RPATH "\$ORIGIN/")
    if(EXTRA_LIBHADOOP_RPATH)
        set(RPATH "${RPATH}:${EXTRA_LIBHADOOP_RPATH}/")
    endif()
    set_target_properties(hadoop PROPERTIES INSTALL_RPATH "${RPATH}")
endif()

# Build the CRC32 test executable.
add_executable(test_bulk_crc32
    ${SRC}/util/bulk_crc32.c
    ${BULK_CRC_ARCH_SOURCE_FIlE}
    ${TST}/util/test_bulk_crc32.c
)
