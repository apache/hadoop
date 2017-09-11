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

cmake_minimum_required(VERSION 3.1 FATAL_ERROR)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/../../../hadoop-common-project/hadoop-common)
include(HadoopCommon)

find_package(OpenSSL REQUIRED)

include_directories(
    main/native/utils/api
    main/native/pipes/api
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${OPENSSL_INCLUDE_DIR}
)

# Example programs
add_executable(wordcount-simple main/native/examples/impl/wordcount-simple.cc)
target_link_libraries(wordcount-simple hadooppipes hadooputils)
hadoop_output_directory(wordcount-simple examples)

add_executable(wordcount-part main/native/examples/impl/wordcount-part.cc)
target_link_libraries(wordcount-part hadooppipes hadooputils)
hadoop_output_directory(wordcount-part examples)

add_executable(wordcount-nopipe main/native/examples/impl/wordcount-nopipe.cc)
target_link_libraries(wordcount-nopipe hadooppipes hadooputils)
hadoop_output_directory(wordcount-nopipe examples)

add_executable(pipes-sort main/native/examples/impl/sort.cc)
target_link_libraries(pipes-sort hadooppipes hadooputils)
hadoop_output_directory(pipes-sort examples)

add_library(hadooputils STATIC
    main/native/utils/impl/StringUtils.cc
    main/native/utils/impl/SerialUtils.cc
)

add_library(hadooppipes STATIC
    main/native/pipes/impl/HadoopPipes.cc
)

include(CheckLibraryExists)
check_library_exists(dl dlopen "" NEED_LINK_DL)

if(NEED_LINK_DL)
    set(LIB_DL "dl")
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "SunOS")
    exec_program("uname" ARGS "-r" OUTPUT_VARIABLE OS_VERSION)
    if(OS_VERSION VERSION_LESS "5.12")
        set(LIB_NET "socket" "nsl")
    endif()
endif()

target_link_libraries(hadooppipes
    ${OPENSSL_LIBRARIES}
    ${LIB_DL}
    ${LIB_NET}
)
