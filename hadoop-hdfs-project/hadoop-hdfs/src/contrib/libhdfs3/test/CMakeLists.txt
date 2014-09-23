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

SET(TEST_WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/data/)
ADD_DEFINITIONS(-DGTEST_LANG_CXX11=0 -Dprivate=public -Dprotected=public -DDATA_DIR="${TEST_WORKING_DIR}/")

ADD_SUBDIRECTORY(function)
ADD_SUBDIRECTORY(unit)
ADD_SUBDIRECTORY(secure)

IF(TEST_RUNNER)
    SEPARATE_ARGUMENTS(TEST_RUNNER_LIST UNIX_COMMAND ${TEST_RUNNER})
ENDIF(TEST_RUNNER)

ADD_CUSTOM_TARGET(unittest
	COMMAND ${TEST_RUNNER_LIST} ${CMAKE_CURRENT_BINARY_DIR}/unit/unit
	DEPENDS unit
	WORKING_DIRECTORY ${TEST_WORKING_DIR}
	COMMENT "Run Unit Test..."
)

ADD_CUSTOM_TARGET(functiontest
	COMMAND ${TEST_RUNNER_LIST} ${CMAKE_CURRENT_BINARY_DIR}/function/function
	DEPENDS function
	WORKING_DIRECTORY ${TEST_WORKING_DIR}
	COMMENT "Run Function Test..."
)

ADD_CUSTOM_TARGET(securetest
	COMMAND ${TEST_RUNNER_LIST} ${CMAKE_CURRENT_BINARY_DIR}/secure/secure
	DEPENDS secure
	WORKING_DIRECTORY ${TEST_WORKING_DIR}
	COMMENT "Run Security Function Test..."
)

ADD_CUSTOM_TARGET(test 
    COMMAND ${CMAKE_MAKE_PROGRAM} unittest || true
    COMMAND ${CMAKE_MAKE_PROGRAM} functiontest || true
    COMMENT "Run All Test..."
)

SET(unit_SOURCES ${unit_SOURCES} PARENT_SCOPE)
SET(function_SOURCES ${function_SOURCES} PARENT_SCOPE)
SET(secure_SOURCES ${secure_SOURCES} PARENT_SCOPE)
