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

set(CMAKE_SKIP_RPATH TRUE)

# Flatten a list into a string.
function(flatten_list INPUT SEPARATOR OUTPUT)
  string (REPLACE ";" "${SEPARATOR}" _TMPS "${INPUT}")
  set (${OUTPUT} "${_TMPS}" PARENT_SCOPE)
endfunction()

flatten_list("${FUSE_CFLAGS}" " " FUSE_CFLAGS)
flatten_list("${FUSE_LDFLAGS}" " " FUSE_LDFLAGS)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${FUSE_CFLAGS}")
set(CMAKE_LD_FLAGS "${CMAKE_LD_FLAGS} ${FUSE_LDFLAGS}")
message(STATUS "Building Linux FUSE client.")

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
    ${JNI_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/main/native
    ${CMAKE_SOURCE_DIR}/main/native/libhdfs
    ${CMAKE_SOURCE_DIR}/main/native/libhdfs/include
    ${OS_DIR}
    ${FUSE_INCLUDE_DIRS})

add_executable(fuse_dfs
    fuse_dfs.c
    fuse_options.c
    fuse_connect.c
    fuse_impls_access.c
    fuse_impls_chmod.c
    fuse_impls_chown.c
    fuse_impls_create.c
    fuse_impls_flush.c
    fuse_impls_getattr.c
    fuse_impls_mkdir.c
    fuse_impls_mknod.c
    fuse_impls_open.c
    fuse_impls_read.c
    fuse_impls_readdir.c
    fuse_impls_release.c
    fuse_impls_rename.c
    fuse_impls_rmdir.c
    fuse_impls_statfs.c
    fuse_impls_symlink.c
    fuse_impls_truncate.c
    fuse_impls_unlink.c
    fuse_impls_utimens.c
    fuse_impls_write.c
    fuse_init.c
    fuse_stat_struct.c
    fuse_trash.c
    fuse_users.c
)
target_link_libraries(fuse_dfs
    ${FUSE_LIBRARIES}
    ${JAVA_JVM_LIBRARY}
    hdfs
    m
    pthread
    rt
)
add_executable(test_fuse_dfs
    test/test_fuse_dfs.c
    test/fuse_workload.c
    util/posix_util.c
)
target_link_libraries(test_fuse_dfs
    ${FUSE_LIBRARIES}
    native_mini_dfs
    ${JAVA_JVM_LIBRARY}
    pthread
)
