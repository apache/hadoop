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

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -O2")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall -O2")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_REENTRANT -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_REENTRANT -D_FILE_OFFSET_BITS=64")

if (JVM_ARCH_DATA_MODEL EQUAL 32)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m32")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -m32")
    set(CMAKE_LD_FLAGS "${CMAKE_LD_FLAGS} -m32")
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

include_directories(
    main/native/utils/api
    main/native/pipes/api
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${OPENSSL_INCLUDE_DIR}
)

# Example programs
add_executable(wordcount-simple main/native/examples/impl/wordcount-simple.cc)
target_link_libraries(wordcount-simple hadooppipes hadooputils)

add_executable(wordcount-part main/native/examples/impl/wordcount-part.cc)
target_link_libraries(wordcount-part hadooppipes hadooputils)

add_executable(wordcount-nopipe main/native/examples/impl/wordcount-nopipe.cc)
target_link_libraries(wordcount-nopipe hadooppipes hadooputils)

add_executable(pipes-sort main/native/examples/impl/sort.cc)
target_link_libraries(pipes-sort hadooppipes hadooputils)

install(TARGETS wordcount-simple wordcount-part wordcount-nopipe pipes-sort 
    RUNTIME DESTINATION bin
)

add_library(hadooputils STATIC
    main/native/utils/impl/StringUtils.cc
    main/native/utils/impl/SerialUtils.cc
)

install(FILES
    main/native/utils/api/hadoop/SerialUtils.hh
    main/native/utils/api/hadoop/StringUtils.hh
    DESTINATION api/hadoop
    COMPONENT headers
)
install(TARGETS hadooputils DESTINATION lib)

add_library(hadooppipes STATIC
    main/native/pipes/impl/HadoopPipes.cc
)
target_link_libraries(hadooppipes
    ${JAVA_JVM_LIBRARY}
    ${OPENSSL_LIBRARIES}
    pthread
)

install(FILES
    main/native/pipes/api/hadoop/Pipes.hh
    main/native/pipes/api/hadoop/TemplateFactory.hh
    DESTINATION api/hadoop
    COMPONENT headers
)
install(TARGETS hadooppipes DESTINATION lib)
