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
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

AUTO_SOURCES(unit_SOURCES "*.cc" "RECURSE" ${CMAKE_CURRENT_SOURCE_DIR})

INCLUDE_DIRECTORIES(${gmock_INCLUDE_DIR} ${gtest_INCLUDE_DIR} ${libhdfs3_ROOT_SOURCES_DIR})

IF(NEED_BOOST)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
ENDIF(NEED_BOOST)

INCLUDE_DIRECTORIES(${libhdfs3_ROOT_SOURCES_DIR})
INCLUDE_DIRECTORIES(${libhdfs3_COMMON_SOURCES_DIR})
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})
INCLUDE_DIRECTORIES(${libhdfs3_PLATFORM_HEADER_DIR})
INCLUDE_DIRECTORIES(${LIBXML2_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${KERBEROS_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${GSASL_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/mock)

ADD_DEFINITIONS(-DMOCK)

#PROTOBUF_GENERATE_CPP(libhdfs3_PROTO_SOURCES libhdfs3_PROTO_HEADERS ${libhdfs3_PROTO_FILES})

SET(libhdfs3_SOURCES ${libhdfs3_SOURCES} ${libhdfs3_MOCK_SOURCES})

ADD_EXECUTABLE(unit EXCLUDE_FROM_ALL
    ${gtest_SOURCES}
    ${gmock_SOURCES}
    ${libhdfs3_SOURCES} 
    ${libhdfs3_PROTO_SOURCES} 
    ${libhdfs3_PROTO_HEADERS}
    ${unit_SOURCES}
)

TARGET_LINK_LIBRARIES(unit pthread)

IF(NEED_BOOST)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
    SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L${Boost_LIBRARY_DIRS}")
    TARGET_LINK_LIBRARIES(unit boost_thread)
    TARGET_LINK_LIBRARIES(unit boost_chrono)
    TARGET_LINK_LIBRARIES(unit boost_system)
    TARGET_LINK_LIBRARIES(unit boost_atomic)
    TARGET_LINK_LIBRARIES(unit boost_iostreams)
ENDIF(NEED_BOOST)

IF(NEED_GCCEH)
	TARGET_LINK_LIBRARIES(unit gcc_eh)
ENDIF(NEED_GCCEH)

IF(OS_LINUX)
	TARGET_LINK_LIBRARIES(unit uuid)
ENDIF(OS_LINUX)

TARGET_LINK_LIBRARIES(unit ${PROTOBUF_LIBRARIES})
TARGET_LINK_LIBRARIES(unit ${LIBXML2_LIBRARIES})
TARGET_LINK_LIBRARIES(unit ${KERBEROS_LIBRARIES})
TARGET_LINK_LIBRARIES(unit ${GSASL_LIBRARIES})

SET(unit_SOURCES ${unit_SOURCES} PARENT_SCOPE)

