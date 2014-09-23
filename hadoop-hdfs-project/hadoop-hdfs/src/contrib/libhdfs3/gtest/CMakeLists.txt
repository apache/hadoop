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

SET(gtest_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/gtest ${CMAKE_SOURCE_DIR}/gtest/include)
SET(gtest_SOURCES_DIR ${CMAKE_SOURCE_DIR}/gtest)

AUTO_SOURCES(files "*.cc" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND gtest_SOURCES ${files})

AUTO_SOURCES(files "*.h" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND gtest_SOURCES ${files})

SET(gtest_SOURCES ${gtest_SOURCES} PARENT_SCOPE)
SET(gtest_INCLUDE_DIR ${gtest_INCLUDE_DIR} PARENT_SCOPE)
SET(gtest_SOURCES_DIR ${gtest_SOURCES_DIR} PARENT_SCOPE)
