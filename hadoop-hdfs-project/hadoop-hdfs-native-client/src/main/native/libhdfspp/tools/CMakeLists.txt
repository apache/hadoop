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

find_package(Boost 1.72 COMPONENTS program_options REQUIRED)

# Default LIBHDFSPP_DIR to the default install location.  You can override
#    it by add -DLIBHDFSPP_DIR=... to your cmake invocation
set(LIBHDFSPP_DIR CACHE STRING ${CMAKE_INSTALL_PREFIX})

include_directories( ${LIBHDFSPP_DIR}/include )
link_directories( ${LIBHDFSPP_DIR}/lib )

add_library(tools_common_obj OBJECT tools_common.cc)
add_library(tools_common $<TARGET_OBJECTS:tools_common_obj>)

add_subdirectory(internal)

add_library(hdfs_tool_obj OBJECT hdfs-tool.cc)
target_include_directories(hdfs_tool_obj PRIVATE ../tools)

add_subdirectory(hdfs-cat)
add_subdirectory(hdfs-chgrp)
add_subdirectory(hdfs-chown)
add_subdirectory(hdfs-chmod)
add_subdirectory(hdfs-find)
add_subdirectory(hdfs-mkdir)
add_subdirectory(hdfs-rm)
add_subdirectory(hdfs-ls)
add_subdirectory(hdfs-stat)
add_subdirectory(hdfs-count)
add_subdirectory(hdfs-df)
add_subdirectory(hdfs-du)
add_subdirectory(hdfs-get)
add_subdirectory(hdfs-copy-to-local)
add_subdirectory(hdfs-move-to-local)
add_subdirectory(hdfs-setrep)
add_subdirectory(hdfs-allow-snapshot)
add_subdirectory(hdfs-disallow-snapshot)
add_subdirectory(hdfs-create-snapshot)
add_subdirectory(hdfs-rename-snapshot)
add_subdirectory(hdfs-delete-snapshot)
add_subdirectory(hdfs-tail)
