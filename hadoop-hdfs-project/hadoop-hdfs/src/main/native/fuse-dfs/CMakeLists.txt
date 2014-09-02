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

set(CMAKE_SKIP_RPATH TRUE)

# Find Linux FUSE
IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(FUSE fuse)
    IF(FUSE_FOUND)
        FLATTEN_LIST("${FUSE_CFLAGS}" " " FUSE_CFLAGS)
        FLATTEN_LIST("${FUSE_LDFLAGS}" " " FUSE_LDFLAGS)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${FUSE_CFLAGS}")
        set(CMAKE_LD_FLAGS "${CMAKE_LD_FLAGS} ${FUSE_LDFLAGS}")
        MESSAGE(STATUS "Building Linux FUSE client.")
        include_directories(${FUSE_INCLUDE_DIRS})
    ELSE(FUSE_FOUND)
        MESSAGE(STATUS "Failed to find Linux FUSE libraries or include files.  Will not build FUSE client.")
    ENDIF(FUSE_FOUND)
ELSE (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    MESSAGE(STATUS "Non-Linux system detected.  Will not build FUSE client.")
ENDIF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")

IF(FUSE_FOUND)
    add_library(posix_util
        ../util/posix_util.c
    )

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
    )
    target_link_libraries(test_fuse_dfs
        ${FUSE_LIBRARIES}
        native_mini_dfs
        posix_util
        pthread
    )
ELSE(FUSE_FOUND)
    IF(REQUIRE_FUSE)
        MESSAGE(FATAL_ERROR "Required component fuse_dfs could not be built.")
    ENDIF(REQUIRE_FUSE)
ENDIF(FUSE_FOUND)
