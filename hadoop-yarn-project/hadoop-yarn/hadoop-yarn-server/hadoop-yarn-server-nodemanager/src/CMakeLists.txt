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

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/../../../../../hadoop-common-project/hadoop-common)
include(HadoopCommon)

# Note: can't use -D_FILE_OFFSET_BITS=64, see MAPREDUCE-4258
string(REPLACE "-D_FILE_OFFSET_BITS=64" "" CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
string(REPLACE "-D_FILE_OFFSET_BITS=64" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

include(CheckFunctionExists)
check_function_exists(fcloseall HAVE_FCLOSEALL)

function(output_directory TGT DIR)
    set_target_properties(${TGT} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    set_target_properties(${TGT} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    set_target_properties(${TGT} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
endfunction()

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
    main/native/container-executor
    main/native/container-executor/impl
)
configure_file(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

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
