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

include(../../../hadoop-common-project/hadoop-common/src/JNIFlags.cmake NO_POLICY_SCOPE)

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

# Flatten a list into a string.
function(FLATTEN_LIST INPUT SEPARATOR OUTPUT)
  string (REPLACE ";" "${SEPARATOR}" _TMPS "${INPUT}")
  set (${OUTPUT} "${_TMPS}" PARENT_SCOPE)
endfunction()

# Check to see if our compiler and linker support the __thread attribute.
# On Linux and some other operating systems, this is a more efficient 
# alternative to POSIX thread local storage. 
INCLUDE(CheckCSourceCompiles)
CHECK_C_SOURCE_COMPILES("int main(void) { static __thread int i = 0; return 0; }" HAVE_BETTER_TLS)

# Check to see if we have Intel SSE intrinsics.
CHECK_C_SOURCE_COMPILES("#include <emmintrin.h>\nint main(void) { __m128d sum0 = _mm_set_pd(0.0,0.0); return 0; }" HAVE_INTEL_SSE_INTRINSICS)

# Check if we need to link dl library to get dlopen.
# dlopen on Linux is in separate library but on FreeBSD its in libc
INCLUDE(CheckLibraryExists)
CHECK_LIBRARY_EXISTS(dl dlopen "" NEED_LINK_DL)

find_package(JNI REQUIRED)
if (NOT GENERATED_JAVAH)
    # Must identify where the generated headers have been placed
    MESSAGE(FATAL_ERROR "You must set the CMake variable GENERATED_JAVAH")
endif (NOT GENERATED_JAVAH)

if (WIN32)
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

    set(OS_DIR main/native/libhdfs/os/windows)
    set(OUT_DIR target/bin)
else (WIN32)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -O2")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_REENTRANT -D_GNU_SOURCE")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
    set(OS_DIR main/native/libhdfs/os/posix)
    set(OS_LINK_LIBRARIES pthread)
    set(OUT_DIR target/usr/local/lib)
endif (WIN32)

include_directories(
    ${GENERATED_JAVAH}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
    ${JNI_INCLUDE_DIRS}
    main/native
    main/native/libhdfs
    ${OS_DIR}
)

set(_FUSE_DFS_VERSION 0.1.0)
CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

add_dual_library(hdfs
    main/native/libhdfs/exception.c
    main/native/libhdfs/jni_helper.c
    main/native/libhdfs/hdfs.c
    main/native/libhdfs/common/htable.c
    ${OS_DIR}/mutexes.c
    ${OS_DIR}/thread_local_storage.c
)
if (NEED_LINK_DL)
   set(LIB_DL dl)
endif(NEED_LINK_DL)

target_link_dual_libraries(hdfs
    ${JAVA_JVM_LIBRARY}
    ${LIB_DL}
    ${OS_LINK_LIBRARIES}
)

dual_output_directory(hdfs ${OUT_DIR})
set(LIBHDFS_VERSION "0.0.0")
set_target_properties(hdfs PROPERTIES
    SOVERSION ${LIBHDFS_VERSION})

add_executable(test_libhdfs_ops
    main/native/libhdfs/test/test_libhdfs_ops.c
)
target_link_libraries(test_libhdfs_ops
    hdfs
    ${JAVA_JVM_LIBRARY}
)

add_executable(test_libhdfs_read
    main/native/libhdfs/test/test_libhdfs_read.c
)
target_link_libraries(test_libhdfs_read
    hdfs
    ${JAVA_JVM_LIBRARY}
)

add_executable(test_libhdfs_write
    main/native/libhdfs/test/test_libhdfs_write.c
)
target_link_libraries(test_libhdfs_write
    hdfs
    ${JAVA_JVM_LIBRARY}
)

add_library(native_mini_dfs
    main/native/libhdfs/native_mini_dfs.c
)
target_link_libraries(native_mini_dfs
    hdfs
)

add_executable(test_native_mini_dfs
    main/native/libhdfs/test_native_mini_dfs.c
)
target_link_libraries(test_native_mini_dfs
    native_mini_dfs
)

add_executable(test_libhdfs_threaded
    main/native/libhdfs/expect.c
    main/native/libhdfs/test_libhdfs_threaded.c
    ${OS_DIR}/thread.c
)
target_link_libraries(test_libhdfs_threaded
    hdfs
    native_mini_dfs
    ${OS_LINK_LIBRARIES}
)

add_executable(test_libhdfs_zerocopy
    main/native/libhdfs/expect.c
    main/native/libhdfs/test/test_libhdfs_zerocopy.c
)
target_link_libraries(test_libhdfs_zerocopy
    hdfs
    native_mini_dfs
    ${OS_LINK_LIBRARIES}
)

# Skip vecsum on Windows.  This could be made to work in the future by
# introducing an abstraction layer over the sys/mman.h functions.
if (NOT WIN32)
    add_executable(test_libhdfs_vecsum
        main/native/libhdfs/test/vecsum.c
    )
    target_link_libraries(test_libhdfs_vecsum
        hdfs
        pthread
        rt
    )
endif(NOT WIN32)

IF(REQUIRE_LIBWEBHDFS)
    add_subdirectory(contrib/libwebhdfs)
ENDIF(REQUIRE_LIBWEBHDFS)
add_subdirectory(main/native/fuse-dfs)
