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

add_subdirectory(main/native/libhdfs)
add_subdirectory(main/native/libhdfs-tests)
add_subdirectory(main/native/libhdfspp)

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
