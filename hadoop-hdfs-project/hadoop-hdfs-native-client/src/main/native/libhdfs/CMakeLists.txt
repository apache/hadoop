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

hadoop_add_dual_library(hdfs
    exception.c
    jni_helper.c
    hdfs.c
    common/htable.c
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
    test/test_libhdfs_ops.c
)
target_link_libraries(test_libhdfs_ops
    hdfs_static
    ${JAVA_JVM_LIBRARY}
)

add_executable(test_libhdfs_read
    test/test_libhdfs_read.c
)
target_link_libraries(test_libhdfs_read
    hdfs_static
    ${JAVA_JVM_LIBRARY}
)

add_executable(test_libhdfs_write
    test/test_libhdfs_write.c
)
target_link_libraries(test_libhdfs_write
    hdfs_static
    ${JAVA_JVM_LIBRARY}
)

add_library(native_mini_dfs
    native_mini_dfs.c
    common/htable.c
    exception.c
    jni_helper.c
    ${OS_DIR}/mutexes.c
    ${OS_DIR}/thread_local_storage.c
)
target_link_libraries(native_mini_dfs
    ${JAVA_JVM_LIBRARY}
    ${LIB_DL}
    ${OS_LINK_LIBRARIES}
)

add_executable(test_native_mini_dfs
    test_native_mini_dfs.c
)
target_link_libraries(test_native_mini_dfs
    native_mini_dfs
)

add_executable(test_libhdfs_threaded
    expect.c
    test_libhdfs_threaded.c
    ${OS_DIR}/thread.c
)
target_link_libraries(test_libhdfs_threaded
    hdfs_static
    native_mini_dfs
    ${OS_LINK_LIBRARIES}
)

add_executable(test_libhdfs_zerocopy
    expect.c
    test/test_libhdfs_zerocopy.c
)
target_link_libraries(test_libhdfs_zerocopy
    hdfs_static
    native_mini_dfs
    ${OS_LINK_LIBRARIES}
)

add_executable(test_htable
    common/htable.c
    test/test_htable.c
)
target_link_libraries(test_htable
    ${OS_LINK_LIBRARIES}
)

# Skip vecsum on Windows.  This could be made to work in the future by
# introducing an abstraction layer over the sys/mman.h functions.
if(NOT WIN32)
    add_executable(test_libhdfs_vecsum test/vecsum.c)
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
