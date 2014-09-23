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

CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

SET(gmock_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/gmock/include)
SET(gmock_SOURCES_DIR ${CMAKE_SOURCE_DIR}/gmock)

AUTO_SOURCES(files "*.cc" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND gmock_SOURCES ${files})

AUTO_SOURCES(files "*.h" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND gmock_SOURCES ${files})

SET(gmock_SOURCES ${gmock_SOURCES} PARENT_SCOPE)
SET(gmock_INCLUDE_DIR ${gmock_INCLUDE_DIR} PARENT_SCOPE)
SET(gmock_SOURCES_DIR ${gmock_SOURCES_DIR} PARENT_SCOPE)
