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

cmake_minimum_required(VERSION 3.1 FATAL_ERROR)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/../../../../../hadoop-common-project/hadoop-common)
include(HadoopCommon)

# Set gtest path
set(GTEST_SRC_DIR ${CMAKE_SOURCE_DIR}/../../../../../hadoop-common-project/hadoop-common/src/main/native/gtest)

# determine if container-executor.conf.dir is an absolute
# path in case the OS we're compiling on doesn't have
# a hook in get_executable. We'll use this define
# later in the code to potentially throw a compile error
string(REGEX MATCH . HCD_ONE "${HADOOP_CONF_DIR}")
string(COMPARE EQUAL ${HCD_ONE} / HADOOP_CONF_DIR_IS_ABS)

set (CMAKE_C_STANDARD 99)

include(CheckIncludeFiles)
check_include_files("sys/types.h;sys/sysctl.h" HAVE_SYS_SYSCTL_H)

include(CheckFunctionExists)
check_function_exists(canonicalize_file_name HAVE_CANONICALIZE_FILE_NAME)
check_function_exists(fcloseall HAVE_FCLOSEALL)
check_function_exists(fchmodat HAVE_FCHMODAT)
check_function_exists(fdopendir HAVE_FDOPENDIR)
check_function_exists(fstatat HAVE_FSTATAT)
check_function_exists(openat HAVE_OPENAT)
check_function_exists(unlinkat HAVE_UNLINKAT)

include(CheckSymbolExists)
check_symbol_exists(sysctl "sys/types.h;sys/sysctl.h" HAVE_SYSCTL)

if(APPLE)
  include_directories( /System/Library/Frameworks )
  find_library(COCOA_LIBRARY Cocoa)
  mark_as_advanced(COCOA_LIBRARY)
  set(EXTRA_LIBS ${COCOA_LIBRARY})
endif(APPLE)

include(CheckCCompilerFlag)

# Building setuid = attempt to enable stack protection.
# assumption here is that the C compiler and the C++
# compiler match.  need both so that gtest gets same
# stack treatment that the real c-e does
IF(CMAKE_C_COMPILER_ID STREQUAL "GNU")
    CHECK_C_COMPILER_FLAG("-fstack-check" STACKRESULT)
    IF(STACKRESULT)
      SET (CMAKE_C_FLAGS "-fstack-check ${CMAKE_C_FLAGS}")
      SET (CMAKE_CXX_FLAGS "-fstack-check ${CMAKE_CXX_FLAGS}")
    ENDIF()
ELSEIF(CMAKE_C_COMPILER_ID STREQUAL "Clang" OR
       CMAKE_C_COMPILER_ID STREQUAL "AppleClang")

  # clang is a bit difficult here:
  # - some versions don't support the flag
  # - some versions support the flag, despite not having
  #   the library that is actually required (!)
  # Notably, Xcode is a problem here.
  # In the end, this is needlessly complex. :(

  SET(PRE_SANITIZE ${CMAKE_REQUIRED_FLAGS})
  SET(CMAKE_REQUIRED_FLAGS "-fsanitize=safe-stack ${CMAKE_REQUIRED_FLAGS}")
  CHECK_C_COMPILER_FLAG("" STACKRESULT)
  SET(CMAKE_REQUIRED_FLAGS ${PRE_SANITIZE})
  IF(STACKRESULT)
     SET(CMAKE_C_FLAGS "-fsanitize=safe-stack ${CMAKE_C_FLAGS}")
     SET(CMAKE_CXX_FLAGS "-fsanitize=safe-stack ${CMAKE_CXX_FLAGS}")
  ENDIF()
ELSEIF(CMAKE_C_COMPILER_ID STREQUAL "SunPro")

  # this appears to only be supported on SPARC, for some reason

  CHECK_C_COMPILER_FLAG("-xcheck=stkovf" STACKRESULT)
  IF(STACKRESULT)
    SET (CMAKE_C_FLAGS "-xcheck=stkovf ${CMAKE_C_FLAGS}")
    SET (CMAKE_CXX_FLAGS "-xcheck=stkovf ${CMAKE_CXX_FLAGS}")
  ENDIF()
ENDIF()

IF(NOT STACKRESULT)
   MESSAGE(WARNING "Stack Clash security protection is not suported.")
ENDIF()

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
    ${GTEST_SRC_DIR}/include
    main/native/container-executor
    main/native/container-executor/impl
)
# add gtest as system library to suppress gcc warnings
include_directories(SYSTEM ${GTEST_SRC_DIR}/include)

configure_file(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

add_library(gtest ${GTEST_SRC_DIR}/gtest-all.cc)
set_target_properties(gtest PROPERTIES COMPILE_FLAGS "-w")

add_library(container
    main/native/container-executor/impl/util.c
    main/native/container-executor/impl/configuration.c
    main/native/container-executor/impl/container-executor.c
    main/native/container-executor/impl/get_executable.c
    main/native/container-executor/impl/utils/string-utils.c
    main/native/container-executor/impl/utils/path-utils.c
    main/native/container-executor/impl/modules/cgroups/cgroups-operations.c
    main/native/container-executor/impl/modules/common/module-configs.c
    main/native/container-executor/impl/modules/gpu/gpu-module.c
)

add_executable(container-executor
    main/native/container-executor/impl/main.c
)

target_link_libraries(container-executor
    container
)

output_directory(container-executor target/usr/local/bin)

# Test cases
add_executable(test-container-executor
    main/native/container-executor/test/test-container-executor.c
)
target_link_libraries(test-container-executor
    container ${EXTRA_LIBS}
)

output_directory(test-container-executor target/usr/local/bin)

# unit tests for container executor
add_executable(cetest
        main/native/container-executor/impl/util.c
        main/native/container-executor/test/test_configuration.cc
        main/native/container-executor/test/test_main.cc
        main/native/container-executor/test/utils/test-string-utils.cc
        main/native/container-executor/test/utils/test-path-utils.cc
        main/native/container-executor/test/modules/cgroups/test-cgroups-module.cc
        main/native/container-executor/test/modules/gpu/test-gpu-module.cc
        main/native/container-executor/test/test_util.cc)
target_link_libraries(cetest gtest container)
output_directory(cetest test)
