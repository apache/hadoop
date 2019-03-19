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

add_definitions(-DLIBHDFS_DLL_EXPORT)

include_directories(
    include
    ${GENERATED_JAVAH}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
    ${JNI_INCLUDE_DIRS}
    main/native
    main/native/libhdfs
    ${OS_DIR}
)

hadoop_add_dual_library(hdfs
    exception.c
    jni_helper.c
    hdfs.c
    jclasses.c
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

build_libhdfs_test(test_libhdfs_ops hdfs_static test_libhdfs_ops.c)
link_libhdfs_test(test_libhdfs_ops hdfs_static native_mini_dfs ${JAVA_JVM_LIBRARY})
add_libhdfs_test(test_libhdfs_ops hdfs_static)

build_libhdfs_test(test_libhdfs_threaded hdfs_static expect.c test_libhdfs_threaded.c ${OS_DIR}/thread.c)
link_libhdfs_test(test_libhdfs_threaded hdfs_static native_mini_dfs)
add_libhdfs_test(test_libhdfs_threaded hdfs_static)

if (NOT WIN32)
build_libhdfs_test(test_libhdfs_zerocopy hdfs_static expect.c test_libhdfs_zerocopy.c)
link_libhdfs_test(test_libhdfs_zerocopy hdfs_static native_mini_dfs ${OS_LINK_LIBRARIES})
add_libhdfs_test(test_libhdfs_zerocopy hdfs_static)
endif()

# Skip vecsum on Windows.  This could be made to work in the future by
# introducing an abstraction layer over the sys/mman.h functions.
if(NOT WIN32)
    build_libhdfs_test(test_libhdfs_vecsum hdfs vecsum.c)
    set(THREADS_PREFER_PTHREAD_FLAG ON)
    find_package(Threads REQUIRED)
    if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
        link_libhdfs_test(test_libhdfs_vecsum hdfs ${CMAKE_THREAD_LIBS_INIT})
    else()
        link_libhdfs_test(test_libhdfs_vecsum hdfs ${CMAKE_THREAD_LIBS_INIT} rt)
    endif()
endif()
