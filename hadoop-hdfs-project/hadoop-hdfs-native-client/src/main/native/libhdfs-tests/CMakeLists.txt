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

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/../libhdfs/include
    ${GENERATED_JAVAH}
    ${CMAKE_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../libhdfs
    ${JNI_INCLUDE_DIRS}
    ${OS_DIR}
)

add_library(native_mini_dfs
    native_mini_dfs.c
    ../libhdfs/exception.c
    ../libhdfs/jclasses.c
    ../libhdfs/jni_helper.c
    ${OS_DIR}/mutexes.c
    ${OS_DIR}/thread_local_storage.c
)

add_executable(test_native_mini_dfs test_native_mini_dfs.c)
target_link_libraries(test_native_mini_dfs native_mini_dfs ${JAVA_JVM_LIBRARY})
add_test(test_test_native_mini_dfs test_native_mini_dfs)
