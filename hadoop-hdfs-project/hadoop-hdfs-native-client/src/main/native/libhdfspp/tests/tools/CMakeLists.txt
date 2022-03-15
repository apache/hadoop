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

add_executable(hdfs_tool_tests
                hdfs-allow-snapshot-mock.cc
                hdfs-disallow-snapshot-mock.cc
                hdfs-rename-snapshot-mock.cc
                hdfs-delete-snapshot-mock.cc
                hdfs-create-snapshot-mock.cc
                hdfs-cat-mock.cc
                hdfs-chown-mock.cc
                hdfs-chmod-mock.cc
                hdfs-chgrp-mock.cc
                hdfs-tool-test-fixtures.cc
                hdfs-tool-tests.cc
                hdfs-df-mock.cc
                hdfs-du-mock.cc
                hdfs-copy-to-local-mock.cc
                hdfs-move-to-local-mock.cc
                hdfs-count-mock.cc
                hdfs-mkdir-mock.cc
                hdfs-rm-mock.cc
                hdfs-get-mock.cc
                main.cc)
target_include_directories(hdfs_tool_tests PRIVATE
                            ../tools
                            ../../tools
                            ../../tools/hdfs-df
                            ../../tools/hdfs-du
                            ../../tools/hdfs-allow-snapshot
                            ../../tools/hdfs-disallow-snapshot
                            ../../tools/hdfs-delete-snapshot
                            ../../tools/hdfs-create-snapshot
                            ../../tools/hdfs-rename-snapshot
                            ../../tools/hdfs-chown
                            ../../tools/hdfs-chgrp
                            ../../tools/hdfs-chmod
                            ../../tools/hdfs-copy-to-local
                            ../../tools/hdfs-move-to-local
                            ../../tools/hdfs-count
                            ../../tools/hdfs-mkdir
                            ../../tools/hdfs-rm
                            ../../tools/hdfs-get
                            ../../tools/hdfs-cat)
target_link_libraries(hdfs_tool_tests PRIVATE
                       gmock_main
                       hdfs_df_lib
                       hdfs_du_lib
                       hdfs_allowSnapshot_lib
                       hdfs_disallowSnapshot_lib
                       hdfs_deleteSnapshot_lib
                       hdfs_createSnapshot_lib
                       hdfs_renameSnapshot_lib
                       hdfs_chown_lib
                       hdfs_chgrp_lib
                       hdfs_chmod_lib
                       hdfs_copyToLocal_lib
                       hdfs_moveToLocal_lib
                       hdfs_count_lib
                       hdfs_mkdir_lib
                       hdfs_rm_lib
                       hdfs_get_lib
                       hdfs_cat_lib)
add_test(hdfs_tool_tests hdfs_tool_tests)
