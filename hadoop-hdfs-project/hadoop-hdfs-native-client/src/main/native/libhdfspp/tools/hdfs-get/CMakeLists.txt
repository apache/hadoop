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

add_library(hdfs_get_lib STATIC $<TARGET_OBJECTS:hdfs_tool_obj> hdfs-get.cc)
target_include_directories(hdfs_get_lib PRIVATE ../../tools hdfs-get ${Boost_INCLUDE_DIRS})
target_link_libraries(hdfs_get_lib PRIVATE Boost::boost Boost::program_options tools_common hdfspp_static hdfs_copyToLocal_lib)

add_executable(hdfs_get main.cc)
target_include_directories(hdfs_get PRIVATE ../../tools)
target_link_libraries(hdfs_get PRIVATE hdfs_get_lib)

install(TARGETS hdfs_get RUNTIME DESTINATION bin)
