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

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/../../../hadoop-common-project/hadoop-common)
include(HadoopCommon)

#
# Main configuration
#

# The caller must specify where the generated headers have been placed.
if(NOT GENERATED_JAVAH)
    message(FATAL_ERROR "You must set the CMake variable GENERATED_JAVAH")
endif()

# Check to see if our compiler and linker support the __thread attribute.
# On Linux and some other operating systems, this is a more efficient
# alternative to POSIX thread local storage.
include(CheckCSourceCompiles)
check_c_source_compiles("int main(void) { static __thread int i = 0; return 0; }" HAVE_BETTER_TLS)

# Check to see if we have Intel SSE intrinsics.
check_c_source_compileS("#include <emmintrin.h>\nint main(void) { __m128d sum0 = _mm_set_pd(0.0,0.0); return 0; }" HAVE_INTEL_SSE_INTRINSICS)

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
    set(OS_DIR main/native/libhdfs/os/windows)
    set(OUT_DIR target/bin)
else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fvisibility=hidden")
    set(OS_DIR main/native/libhdfs/os/posix)
    set(OUT_DIR target/usr/local/lib)
endif()

# Configure JNI.
include(HadoopJNI)

add_definitions(-DLIBHDFS_DLL_EXPORT)

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
configure_file(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

hadoop_add_dual_library(hdfs
    main/native/libhdfs/exception.c
    main/native/libhdfs/jni_helper.c
    main/native/libhdfs/hdfs.c
    main/native/libhdfs/common/htable.c
    ${OS_DIR}/mutexes.c
    ${OS_DIR}/thread_local_storage.c
)
if(NEED_LINK_DL)
   set(LIB_DL dl)
endif()

hadoop_target_link_dual_libraries(hdfs
    ${JAVA_JVM_LIBRARY}
    ${LIB_DL}
    ${OS_LINK_LIBRARIES}
)

hadoop_dual_output_directory(hdfs ${OUT_DIR})
set(LIBHDFS_VERSION "0.0.0")
set_target_properties(hdfs PROPERTIES
    SOVERSION ${LIBHDFS_VERSION})

add_executable(test_libhdfs_ops
    main/native/libhdfs/test/test_libhdfs_ops.c
)
target_link_libraries(test_libhdfs_ops
    hdfs_static
    ${JAVA_JVM_LIBRARY}
)

add_executable(test_libhdfs_read
    main/native/libhdfs/test/test_libhdfs_read.c
)
target_link_libraries(test_libhdfs_read
    hdfs_static
    ${JAVA_JVM_LIBRARY}
)

add_executable(test_libhdfs_write
    main/native/libhdfs/test/test_libhdfs_write.c
)
target_link_libraries(test_libhdfs_write
    hdfs_static
    ${JAVA_JVM_LIBRARY}
)

add_library(native_mini_dfs
    main/native/libhdfs/native_mini_dfs.c
    main/native/libhdfs/common/htable.c
    main/native/libhdfs/exception.c
    main/native/libhdfs/jni_helper.c
    ${OS_DIR}/mutexes.c
    ${OS_DIR}/thread_local_storage.c
)
target_link_libraries(native_mini_dfs
    ${JAVA_JVM_LIBRARY}
    ${OS_LINK_LIBRARIES}
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
    hdfs_static
    native_mini_dfs
    ${OS_LINK_LIBRARIES}
)

add_executable(test_libhdfs_zerocopy
    main/native/libhdfs/expect.c
    main/native/libhdfs/test/test_libhdfs_zerocopy.c
)
target_link_libraries(test_libhdfs_zerocopy
    hdfs_static
    native_mini_dfs
    ${OS_LINK_LIBRARIES}
)

add_executable(test_htable
    main/native/libhdfs/common/htable.c
    main/native/libhdfs/test/test_htable.c
)
target_link_libraries(test_htable
    ${OS_LINK_LIBRARIES}
)

# Skip vecsum on Windows.  This could be made to work in the future by
# introducing an abstraction layer over the sys/mman.h functions.
if(NOT WIN32)
    add_executable(test_libhdfs_vecsum main/native/libhdfs/test/vecsum.c)
    if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
        target_link_libraries(test_libhdfs_vecsum
            hdfs
            pthread)
    else()
        target_link_libraries(test_libhdfs_vecsum
            hdfs
            pthread
            rt)
    endif()
endif()

if(REQUIRE_LIBWEBHDFS)
    add_subdirectory(contrib/libwebhdfs)
endif()
add_subdirectory(main/native/fuse-dfs)
