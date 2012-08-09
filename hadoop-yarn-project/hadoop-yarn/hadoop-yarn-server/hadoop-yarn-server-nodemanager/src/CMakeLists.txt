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

cmake_minimum_required(VERSION 2.6 FATAL_ERROR)

set(CMAKE_BUILD_TYPE, Release)

if (JVM_ARCH_DATA_MODEL EQUAL 32)
    # force 32-bit code generation on amd64/x86_64, ppc64, sparc64
    if (CMAKE_COMPILER_IS_GNUCC AND CMAKE_SYSTEM_PROCESSOR MATCHES ".*64")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m32")
        set(CMAKE_LD_FLAGS "${CMAKE_LD_FLAGS} -m32")
    endif ()
    if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64")
        set(CMAKE_SYSTEM_PROCESSOR "i686")
    endif ()
endif (JVM_ARCH_DATA_MODEL EQUAL 32)

function(output_directory TGT DIR)
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
endfunction(output_directory TGT DIR)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -O2 -D_GNU_SOURCE")
# note: can't enable -D_LARGEFILE: see MAPREDUCE-4258
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_REENTRANT")

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
    main/native/container-executor
    main/native/container-executor/impl
)
CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

add_library(container
    main/native/container-executor/impl/configuration.c
    main/native/container-executor/impl/container-executor.c
)

add_executable(container-executor
    main/native/container-executor/impl/main.c
)
target_link_libraries(container-executor
    container
)
output_directory(container-executor target/usr/local/bin)

add_executable(test-container-executor
    main/native/container-executor/test/test-container-executor.c
)
target_link_libraries(test-container-executor
    container
)
output_directory(test-container-executor target/usr/local/bin)
