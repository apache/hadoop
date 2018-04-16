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

# Default LIBHDFSPP_DIR to the default install location.  You can override
#    it by add -DLIBHDFSPP_DIR=... to your cmake invocation
set(LIBHDFSPP_DIR CACHE STRING ${CMAKE_INSTALL_PREFIX})

include_directories( ${LIBHDFSPP_DIR}/include )
link_directories( ${LIBHDFSPP_DIR}/lib )

add_library(tools_common_obj OBJECT tools_common.cc)
add_library(tools_common $<TARGET_OBJECTS:tools_common_obj>)

add_executable(hdfs_cat hdfs_cat.cc)
target_link_libraries(hdfs_cat tools_common hdfspp_static)

add_executable(hdfs_chgrp hdfs_chgrp.cc)
target_link_libraries(hdfs_chgrp tools_common hdfspp_static)

add_executable(hdfs_chown hdfs_chown.cc)
target_link_libraries(hdfs_chown tools_common hdfspp_static)

add_executable(hdfs_chmod hdfs_chmod.cc)
target_link_libraries(hdfs_chmod tools_common hdfspp_static)

add_executable(hdfs_find hdfs_find.cc)
target_link_libraries(hdfs_find tools_common hdfspp_static)

add_executable(hdfs_mkdir hdfs_mkdir.cc)
target_link_libraries(hdfs_mkdir tools_common hdfspp_static)

add_executable(hdfs_rm hdfs_rm.cc)
target_link_libraries(hdfs_rm tools_common hdfspp_static)

add_executable(hdfs_ls hdfs_ls.cc)
target_link_libraries(hdfs_ls tools_common hdfspp_static)

add_executable(hdfs_stat hdfs_stat.cc)
target_link_libraries(hdfs_stat tools_common hdfspp_static)

add_executable(hdfs_count hdfs_count.cc)
target_link_libraries(hdfs_count tools_common hdfspp_static)

add_executable(hdfs_df hdfs_df.cc)
target_link_libraries(hdfs_df tools_common hdfspp_static)

add_executable(hdfs_du hdfs_du.cc)
target_link_libraries(hdfs_du tools_common hdfspp_static)

add_executable(hdfs_get hdfs_get.cc)
target_link_libraries(hdfs_get tools_common hdfspp_static)

add_executable(hdfs_copyToLocal hdfs_copyToLocal.cc)
target_link_libraries(hdfs_copyToLocal tools_common hdfspp_static)

add_executable(hdfs_moveToLocal hdfs_moveToLocal.cc)
target_link_libraries(hdfs_moveToLocal tools_common hdfspp_static)

add_executable(hdfs_setrep hdfs_setrep.cc)
target_link_libraries(hdfs_setrep tools_common hdfspp_static)

add_executable(hdfs_allowSnapshot hdfs_allowSnapshot.cc)
target_link_libraries(hdfs_allowSnapshot tools_common hdfspp_static)

add_executable(hdfs_disallowSnapshot hdfs_disallowSnapshot.cc)
target_link_libraries(hdfs_disallowSnapshot tools_common hdfspp_static)

add_executable(hdfs_createSnapshot hdfs_createSnapshot.cc)
target_link_libraries(hdfs_createSnapshot tools_common hdfspp_static)

add_executable(hdfs_renameSnapshot hdfs_renameSnapshot.cc)
target_link_libraries(hdfs_renameSnapshot tools_common hdfspp_static)

add_executable(hdfs_deleteSnapshot hdfs_deleteSnapshot.cc)
target_link_libraries(hdfs_deleteSnapshot tools_common hdfspp_static)

add_executable(hdfs_tail hdfs_tail.cc)
target_link_libraries(hdfs_tail tools_common hdfspp_static)