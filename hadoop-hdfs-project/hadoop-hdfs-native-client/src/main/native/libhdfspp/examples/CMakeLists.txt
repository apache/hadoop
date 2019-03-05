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

project (HDFSPP_EXAMPLES C CXX)

include(DecideSasl)

find_package(ASIO REQUIRED)
find_package(HDFSPP REQUIRED)
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

include(HdfsppCompilerOptions)

include_directories(
  ${HDFSPP_INCLUDE_DIR}
  ${ASIO_INCLUDE_DIR}
  )

add_subdirectory(c)
add_subdirectory(cc)

install(TARGETS)
