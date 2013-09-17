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

find_package(CURL REQUIRED)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
"${CMAKE_SOURCE_DIR}/contrib/libwebhdfs/resources/")

find_package(Jansson REQUIRED)
include_directories(${JANSSON_INCLUDE_DIR})

add_dual_library(webhdfs
    src/hdfs_web.c
    src/hdfs_http_client.c
    src/hdfs_http_query.c
    src/hdfs_json_parser.c
    ../../main/native/libhdfs/exception.c
    ../../main/native/libhdfs/jni_helper.c
)
target_link_dual_libraries(webhdfs
    ${JAVA_JVM_LIBRARY}
    ${CURL_LIBRARY}
    ${JANSSON_LIBRARY}
    pthread
)
dual_output_directory(webhdfs target)
set(LIBWEBHDFS_VERSION "0.0.0")
set_target_properties(webhdfs PROPERTIES
    SOVERSION ${LIBWEBHDFS_VERSION})

add_executable(test_libwebhdfs_ops
    src/test_libwebhdfs_ops.c
)
target_link_libraries(test_libwebhdfs_ops
    webhdfs
    native_mini_dfs
)

add_executable(test_libwebhdfs_read
    src/test_libwebhdfs_read.c
)
target_link_libraries(test_libwebhdfs_read
    webhdfs
)

add_executable(test_libwebhdfs_write
    src/test_libwebhdfs_write.c
)
target_link_libraries(test_libwebhdfs_write
    webhdfs
)

add_executable(test_libwebhdfs_threaded
    src/test_libwebhdfs_threaded.c
)
target_link_libraries(test_libwebhdfs_threaded
    webhdfs
    native_mini_dfs
    pthread
)
