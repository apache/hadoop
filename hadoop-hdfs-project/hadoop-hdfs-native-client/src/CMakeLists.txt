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

cmake_minimum_required(VERSION 3.1 FATAL_ERROR)

enable_testing()

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/../../../hadoop-common-project/hadoop-common)
include(HadoopCommon)

# Check to see if our compiler and linker support the __thread attribute.
# On Linux and some other operating systems, this is a more efficient
# alternative to POSIX thread local storage.
include(CheckCSourceCompiles)
check_c_source_compiles("int main(void) { static __thread int i = 0; return 0; }" HAVE_BETTER_TLS)

# Check to see if we have Intel SSE intrinsics.
check_c_source_compiles("#include <emmintrin.h>\nint main(void) { __m128d sum0 = _mm_set_pd(0.0,0.0); return 0; }" HAVE_INTEL_SSE_INTRINSICS)

set(_FUSE_DFS_VERSION 0.1.0)
configure_file(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

# Check if we need to link dl library to get dlopen.
# dlopen on Linux is in separate library but on FreeBSD its in libc
include(CheckLibraryExists)
check_library_exists(dl dlopen "" NEED_LINK_DL)

if(WIN32)
    # Set the optimizer level.
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /O2")
    # Set warning level 4.
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /W4")
    # Skip "unreferenced formal parameter".
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /wd4100")
    # Skip "conditional expression is constant".
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /wd4127")
    # Skip deprecated POSIX function warnings.
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_CRT_NONSTDC_NO_DEPRECATE")
    # Skip CRT non-secure function warnings.  If we can convert usage of
    # strerror, getenv and ctime to their secure CRT equivalents, then we can
    # re-enable the CRT non-secure function warnings.
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_CRT_SECURE_NO_WARNINGS")
    # Omit unneeded headers.
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DWIN32_LEAN_AND_MEAN")
    set(OS_DIR ${CMAKE_SOURCE_DIR}/main/native/libhdfs/os/windows)
    set(OUT_DIR target/bin)
else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fvisibility=hidden")
    set(OS_DIR ${CMAKE_SOURCE_DIR}/main/native/libhdfs/os/posix)
    set(OUT_DIR target/usr/local/lib)
endif()

# Configure JNI.
include(HadoopJNI)

function(build_libhdfs_test NAME LIBRARY)
    set(FILES)
    foreach(FIL ${ARGN})
        if (IS_ABSOLUTE ${FIL})
            list(APPEND FILES ${FIL})
        else()
            list(APPEND FILES ${CMAKE_SOURCE_DIR}/main/native/libhdfs-tests/${FIL})
        endif()
    endforeach()
    add_executable("${NAME}_${LIBRARY}" ${FILES})
endfunction()

function(add_libhdfs_test NAME LIBRARY)
    add_test("test_${NAME}_${LIBRARY}" "${NAME}_${LIBRARY}")
endfunction()

function(link_libhdfs_test NAME LIBRARY)
target_link_libraries("${NAME}_${LIBRARY}" ${LIBRARY} ${ARGN})
endfunction()


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

add_subdirectory(main/native/libhdfs)
add_subdirectory(main/native/libhdfs-tests)

# Temporary fix to disable Libhdfs++ build on older systems that do not support thread_local
include(CheckCXXSourceCompiles)
unset (THREAD_LOCAL_SUPPORTED CACHE)
set (CMAKE_REQUIRED_DEFINITIONS "-std=c++11")
set (CMAKE_REQUIRED_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
check_cxx_source_compiles(
    "#include <thread>
    int main(void) {
      thread_local int s;
      return 0;
    }"
    THREAD_LOCAL_SUPPORTED)
if (THREAD_LOCAL_SUPPORTED)
    add_subdirectory(main/native/libhdfspp)
else()
    message(WARNING
    "WARNING: Libhdfs++ library was not built because the required feature thread_local storage \
    is not supported by your compiler. Known compilers that support this feature: GCC 4.8+, Visual Studio 2015+, \
    Clang (community version 3.3+), Clang (version for Xcode 8+ and iOS 9+).")
endif (THREAD_LOCAL_SUPPORTED)

if(REQUIRE_LIBWEBHDFS)
    add_subdirectory(contrib/libwebhdfs)
endif()

# Find Linux FUSE
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(FUSE fuse)
    if(FUSE_FOUND)
        add_subdirectory(main/native/fuse-dfs)
    else()
        message(STATUS "Failed to find Linux FUSE libraries or include files.  Will not build FUSE client.")
        if(REQUIRE_FUSE)
            message(FATAL_ERROR "Required component fuse_dfs could not be built.")
        endif()
    endif(FUSE_FOUND)
else()
    message(STATUS "Non-Linux system detected.  Will not build FUSE client.")
    if(REQUIRE_FUSE)
        message(FATAL_ERROR "Required component fuse_dfs could not be built.")
    endif()
endif()
