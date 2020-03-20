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

cmake_minimum_required(VERSION 2.8.12)
cmake_policy(SET CMP0022 NEW) # support interface_link_libraries
if (POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW) # suppress warning about mac rpath
endif ()

project (HDFSPP_TOOLS)

include(DecideSasl)

find_package(HDFSPP REQUIRED)
find_package(Threads REQUIRED)

include(HdfsppCompilerOptions)

function (define_tool name)
  add_executable(${name} ${ARGN} tools_common.cc)
  target_link_libraries(${name} hdfspp)
  install(TARGETS ${name} DESTINATION bin)
endfunction ()

define_tool(hdfs_cat hdfs_cat.cc)
define_tool(hdfs_chgrp hdfs_chgrp.cc)
define_tool(hdfs_chown hdfs_chown.cc)
define_tool(hdfs_chmod hdfs_chmod.cc)
define_tool(hdfs_find hdfs_find.cc)
define_tool(hdfs_mkdir hdfs_mkdir.cc)
define_tool(hdfs_rm hdfs_rm.cc)
define_tool(hdfs_ls hdfs_ls.cc)
define_tool(hdfs_stat hdfs_stat.cc)
define_tool(hdfs_count hdfs_count.cc)
define_tool(hdfs_df hdfs_df.cc)
define_tool(hdfs_du hdfs_du.cc)
define_tool(hdfs_get hdfs_get.cc)
define_tool(hdfs_copyToLocal hdfs_copyToLocal.cc)
define_tool(hdfs_moveToLocal hdfs_moveToLocal.cc)
define_tool(hdfs_setrep hdfs_setrep.cc)
define_tool(hdfs_allowSnapshot hdfs_allowSnapshot.cc)
define_tool(hdfs_disallowSnapshot hdfs_disallowSnapshot.cc)
define_tool(hdfs_createSnapshot hdfs_createSnapshot.cc)
define_tool(hdfs_renameSnapshot hdfs_renameSnapshot.cc)
define_tool(hdfs_deleteSnapshot hdfs_deleteSnapshot.cc)
define_tool(hdfs_tail hdfs_tail.cc)
