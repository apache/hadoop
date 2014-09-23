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

# Check prereqs
FIND_PROGRAM(GCOV_PATH gcov)

IF(CMAKE_COMPILER_IS_GNUCXX)
    FIND_PROGRAM(LCOV_PATH lcov)
ENDIF(CMAKE_COMPILER_IS_GNUCXX)

FIND_PROGRAM(GENHTML_PATH genhtml)

IF(NOT GCOV_PATH)
    MESSAGE(FATAL_ERROR "gcov not found! Aborting...")
ENDIF(NOT GCOV_PATH)

IF(NOT CMAKE_BUILD_TYPE STREQUAL Debug)
    MESSAGE(WARNING "Code coverage results with an optimised (non-Debug) build may be misleading")
ENDIF(NOT CMAKE_BUILD_TYPE STREQUAL Debug)

#Setup compiler options
ADD_DEFINITIONS(-fprofile-arcs -ftest-coverage)

IF(CMAKE_COMPILER_IS_GNUCXX)
    LINK_LIBRARIES(gcov)
ELSEIF(CMAKE_COMPILER_IS_CLANG)
    LINK_LIBRARIES(profile_rt)
ENDIF(CMAKE_COMPILER_IS_GNUCXX)

IF((NOT LCOV_PATH) AND CMAKE_COMPILER_IS_GNUCXX)
    MESSAGE(FATAL_ERROR "lcov not found! Aborting...")
ENDIF((NOT LCOV_PATH) AND CMAKE_COMPILER_IS_GNUCXX)

IF(NOT GENHTML_PATH)
    MESSAGE(FATAL_ERROR "genhtml not found! Aborting...")
ENDIF(NOT GENHTML_PATH)

#Setup target
ADD_CUSTOM_TARGET(ShowCoverage
    #Capturing lcov counters and generating report
    COMMAND ${LCOV_PATH} --directory . --capture --output-file CodeCoverage.info
    COMMAND ${LCOV_PATH} --remove CodeCoverage.info '${CMAKE_CURRENT_BINARY_DIR}/*' 'test/*' 'mock/*' '/usr/*' '/opt/*' '*ext/rhel5_x86_64*' '*ext/osx*' --output-file CodeCoverage.info.cleaned
    COMMAND ${GENHTML_PATH} -o CodeCoverageReport CodeCoverage.info.cleaned
)


ADD_CUSTOM_TARGET(ShowAllCoverage
    #Capturing lcov counters and generating report
    COMMAND ${LCOV_PATH} -a CodeCoverage.info.cleaned -a CodeCoverage.info.cleaned_withoutHA -o AllCodeCoverage.info
    COMMAND sed -e 's|/.*/src|${CMAKE_SOURCE_DIR}/src|' -ig AllCodeCoverage.info
    COMMAND ${GENHTML_PATH} -o AllCodeCoverageReport AllCodeCoverage.info
)

ADD_CUSTOM_TARGET(ResetCoverage
    #Cleanup lcov
    COMMAND ${LCOV_PATH} --directory . --zerocounters
)
	
