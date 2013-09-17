#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
 
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

cmake_minimum_required(VERSION 2.6 FATAL_ERROR)
find_package(OpenSSL REQUIRED)

set(CMAKE_BUILD_TYPE, Release)

set(PIPES_FLAGS "-g -Wall -O2 -D_REENTRANT -D_GNU_SOURCE")
set(PIPES_FLAGS "${PIPES_FLAGS} -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${PIPES_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${PIPES_FLAGS}")

include(../../../hadoop-common-project/hadoop-common/src/JNIFlags.cmake NO_POLICY_SCOPE)

function(output_directory TGT DIR)
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
   SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
endfunction(output_directory TGT DIR)

include_directories(
    main/native/utils/api
    main/native/pipes/api
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${OPENSSL_INCLUDE_DIR}
)

# Example programs
add_executable(wordcount-simple main/native/examples/impl/wordcount-simple.cc)
target_link_libraries(wordcount-simple hadooppipes hadooputils)
output_directory(wordcount-simple examples)

add_executable(wordcount-part main/native/examples/impl/wordcount-part.cc)
target_link_libraries(wordcount-part hadooppipes hadooputils)
output_directory(wordcount-part examples)

add_executable(wordcount-nopipe main/native/examples/impl/wordcount-nopipe.cc)
target_link_libraries(wordcount-nopipe hadooppipes hadooputils)
output_directory(wordcount-nopipe examples)

add_executable(pipes-sort main/native/examples/impl/sort.cc)
target_link_libraries(pipes-sort hadooppipes hadooputils)
output_directory(pipes-sort examples)

add_library(hadooputils STATIC
    main/native/utils/impl/StringUtils.cc
    main/native/utils/impl/SerialUtils.cc
)

add_library(hadooppipes STATIC
    main/native/pipes/impl/HadoopPipes.cc
)
target_link_libraries(hadooppipes
    ${OPENSSL_LIBRARIES}
    pthread
)
