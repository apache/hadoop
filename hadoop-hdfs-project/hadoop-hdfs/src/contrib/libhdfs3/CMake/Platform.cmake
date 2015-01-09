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

IF(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    SET(OS_LINUX true CACHE INTERNAL "Linux operating system")
ELSEIF(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    SET(OS_MACOSX true CACHE INTERNAL "Mac Darwin operating system")
ELSEIF(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    SET(OS_WINDOWS true CACHE INTERNAL "Windows operation system")
ELSE(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    MESSAGE(FATAL_ERROR "Unsupported OS: \"${CMAKE_SYSTEM_NAME}\"")
ENDIF(CMAKE_SYSTEM_NAME STREQUAL "Linux")

IF(CMAKE_COMPILER_IS_GNUCXX)
    EXECUTE_PROCESS(COMMAND ${CMAKE_CXX_COMPILER} --version  OUTPUT_VARIABLE COMPILER_OUTPUT)
    
    STRING(REGEX MATCH "[^0-9]*([0-9]\\.[0-9]\\.[0-9])" GCC_COMPILER_VERSION ${COMPILER_OUTPUT})
    STRING(REGEX MATCHALL "[0-9]" GCC_COMPILER_VERSION ${GCC_COMPILER_VERSION})
    
    LIST(LENGTH GCC_COMPILER_VERSION GCC_COMPILER_VERSION_LEN)
    IF (${GCC_COMPILER_VERSION_LEN} LESS 3)
        MESSAGE(FATAL_ERROR "Cannot get gcc version from \"${COMPILER_OUTPUT}\"")
    ENDIF(${GCC_COMPILER_VERSION_LEN} LESS 3)
    
    LIST(GET GCC_COMPILER_VERSION 0 GCC_COMPILER_VERSION_MAJOR)
    LIST(GET GCC_COMPILER_VERSION 1 GCC_COMPILER_VERSION_MINOR)
    LIST(GET GCC_COMPILER_VERSION 2 GCC_COMPILER_VERSION_PATCH)
    
    SET(GCC_COMPILER_VERSION_MAJOR ${GCC_COMPILER_VERSION_MAJOR} CACHE INTERNAL "gcc major version")
    SET(GCC_COMPILER_VERSION_MINOR ${GCC_COMPILER_VERSION_MINOR} CACHE INTERNAL "gcc minor version")
    SET(GCC_COMPILER_VERSION_PATCH ${GCC_COMPILER_VERSION_PATCH} CACHE INTERNAL "gcc patch version")
    
    MESSAGE(STATUS "checking compiler: GCC (${GCC_COMPILER_VERSION_MAJOR}.${GCC_COMPILER_VERSION_MINOR}.${GCC_COMPILER_VERSION_PATCH})")
ELSEIF(MSVC)
    IF (NOT(${MSVC_VERSION} EQUAL 1600))
        MESSAGE(FATAL_ERROR "Only supports Visual Studio 2010. Unsupported MSVC_VERSION: ${MSVC_VERSION}")
    ENDIF(NOT(${MSVC_VERSION} EQUAL 1600))
    MESSAGE(STATUS "checking compiler: MSVC version ${MSVC_VERSION}")
ELSE(CMAKE_COMPILER_IS_GNUCXX)
    EXECUTE_PROCESS(COMMAND ${CMAKE_C_COMPILER} --version  OUTPUT_VARIABLE COMPILER_OUTPUT)
    IF(COMPILER_OUTPUT MATCHES "clang")
        SET(CMAKE_COMPILER_IS_CLANG true CACHE INTERNAL "using clang as compiler")
        MESSAGE(STATUS "checking compiler: CLANG")
    ELSE(COMPILER_OUTPUT MATCHES "clang")
        MESSAGE(FATAL_ERROR "Unsupported compiler: \"${CMAKE_CXX_COMPILER}\"")
    ENDIF(COMPILER_OUTPUT MATCHES "clang")
ENDIF(CMAKE_COMPILER_IS_GNUCXX)
