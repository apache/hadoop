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

SET(libhdfs3_VERSION_MAJOR 2)
SET(libhdfs3_VERSION_MINOR 2)
SET(libhdfs3_VERSION_PATCH 28)
SET(libhdfs3_VERSION_STRING "${libhdfs3_VERSION_MAJOR}.${libhdfs3_VERSION_MINOR}.${libhdfs3_VERSION_PATCH}")
SET(libhdfs3_VERSION_API 1)
SET(libhdfs3_ROOT_SOURCES_DIR ${CMAKE_SOURCE_DIR}/src)
SET(libhdfs3_COMMON_SOURCES_DIR ${libhdfs3_ROOT_SOURCES_DIR}/common)

IF(ENABLE_DEBUG)
	SET(libhdfs3_VERSION_STRING "${libhdfs3_VERSION_STRING}d")
ENDIF(ENABLE_DEBUG)

CONFIGURE_FILE(platform.h.in platform.h)
CONFIGURE_FILE(doxyfile.in doxyfile)

AUTO_SOURCES(files "*.cc" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND LIBHDFS3_SOURCES ${files})

AUTO_SOURCES(files "*.c" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND LIBHDFS3_SOURCES ${files})

AUTO_SOURCES(files "*.h" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND LIBHDFS3_SOURCES ${files})

AUTO_SOURCES(libhdfs3_PROTO_FILES "proto/*.proto" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
SET(libhdfs3_PROTO_FILES ${libhdfs3_PROTO_FILES} PARENT_SCOPE)

INCLUDE(GenerateProtobufs.cmake)
INCLUDE_DIRECTORIES("${CMAKE_BINARY_DIR}")

SET(HEADER 
    client/BlockLocation.h
    client/DirectoryIterator.h
    client/FileStatus.h
    client/FileSystem.h
    client/FileSystemStats.h
    client/InputStream.h
    client/Permission.h
    common/Config.h
    common/SharedPtr.h
    common/Status.h
)

ADD_LIBRARY(libhdfs3-static STATIC ${LIBHDFS3_SOURCES} ${LIBHDFS3_PROTO_SOURCES} ${LIBHDFS3_PROTO_HEADERS})
ADD_LIBRARY(libhdfs3-shared SHARED ${LIBHDFS3_SOURCES} ${LIBHDFS3_PROTO_SOURCES} ${LIBHDFS3_PROTO_HEADERS})

ADD_CUSTOM_COMMAND(
	TARGET libhdfs3-shared libhdfs3-static
	PRE_BUILD
	COMMAND echo ${libhdfs3_VERSION_STRING} > version
	WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

TARGET_LINK_LIBRARIES(libhdfs3-static pthread)
TARGET_LINK_LIBRARIES(libhdfs3-shared pthread)
       
IF(NEED_BOOST)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
    TARGET_LINK_LIBRARIES(libhdfs3-static boost_thread)
    TARGET_LINK_LIBRARIES(libhdfs3-static boost_chrono)
    TARGET_LINK_LIBRARIES(libhdfs3-static boost_system)
    TARGET_LINK_LIBRARIES(libhdfs3-static boost_atomic)
    TARGET_LINK_LIBRARIES(libhdfs3-static boost_iostreams)
    TARGET_LINK_LIBRARIES(libhdfs3-shared boost_thread)
    TARGET_LINK_LIBRARIES(libhdfs3-shared boost_chrono)
    TARGET_LINK_LIBRARIES(libhdfs3-shared boost_system)
    TARGET_LINK_LIBRARIES(libhdfs3-shared boost_atomic)
    TARGET_LINK_LIBRARIES(libhdfs3-shared boost_iostreams)
ENDIF(NEED_BOOST)

IF(NEED_GCCEH)
	TARGET_LINK_LIBRARIES(libhdfs3-static gcc_eh)
	TARGET_LINK_LIBRARIES(libhdfs3-shared gcc_eh)
ENDIF(NEED_GCCEH)

IF(OS_LINUX)
    TARGET_LINK_LIBRARIES(libhdfs3-static uuid)
	TARGET_LINK_LIBRARIES(libhdfs3-shared uuid)
ENDIF(OS_LINUX)

INCLUDE_DIRECTORIES(${libhdfs3_ROOT_SOURCES_DIR})
INCLUDE_DIRECTORIES(${libhdfs3_COMMON_SOURCES_DIR})
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${LIBXML2_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${KERBEROS_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${GSASL_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/mock)

TARGET_LINK_LIBRARIES(libhdfs3-static ${PROTOBUF_LIBRARIES})
TARGET_LINK_LIBRARIES(libhdfs3-static ${LIBXML2_LIBRARIES})
TARGET_LINK_LIBRARIES(libhdfs3-static ${KERBEROS_LIBRARIES})
TARGET_LINK_LIBRARIES(libhdfs3-static ${GSASL_LIBRARIES})

TARGET_LINK_LIBRARIES(libhdfs3-shared ${PROTOBUF_LIBRARIES})
TARGET_LINK_LIBRARIES(libhdfs3-shared ${LIBXML2_LIBRARIES})
TARGET_LINK_LIBRARIES(libhdfs3-shared ${KERBEROS_LIBRARIES})
TARGET_LINK_LIBRARIES(libhdfs3-shared ${GSASL_LIBRARIES})

SET_TARGET_PROPERTIES(libhdfs3-static PROPERTIES OUTPUT_NAME "hdfs3")
SET_TARGET_PROPERTIES(libhdfs3-shared PROPERTIES OUTPUT_NAME "hdfs3")

IF(NEED_BOOST)
	SET_TARGET_PROPERTIES(libhdfs3-shared libhdfs3-static PROPERTIES LINK_FLAGS "-L${Boost_LIBRARY_DIRS}")
ENDIF(NEED_BOOST)

SET_TARGET_PROPERTIES(libhdfs3-shared PROPERTIES 
    VERSION ${libhdfs3_VERSION_MAJOR}.${libhdfs3_VERSION_MINOR}.${libhdfs3_VERSION_PATCH} 
    SOVERSION ${libhdfs3_VERSION_API})

INSTALL(TARGETS libhdfs3-static libhdfs3-shared
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
INSTALL(FILES ${HEADER} DESTINATION include/hdfs)

SET(LIBHDFS3_SOURCES ${LIBHDFS3_SOURCES} PARENT_SCOPE)
SET(libhdfs3_PLATFORM_HEADER_DIR ${CMAKE_CURRENT_BINARY_DIR} PARENT_SCOPE)
SET(libhdfs3_ROOT_SOURCES_DIR ${libhdfs3_ROOT_SOURCES_DIR} PARENT_SCOPE)
SET(libhdfs3_COMMON_SOURCES_DIR ${libhdfs3_COMMON_SOURCES_DIR} PARENT_SCOPE)
