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

set(HADOOP_COMMON_PATH ${CMAKE_SOURCE_DIR}/../../../../../hadoop-common-project/hadoop-common)
list(APPEND CMAKE_MODULE_PATH ${HADOOP_COMMON_PATH})
include(HadoopCommon)

# Set gtest path
set(GTEST_SRC_DIR ${CMAKE_SOURCE_DIR}/../../../../../hadoop-common-project/hadoop-common/src/main/native/gtest)

set(HADOOP_COMMON_SEC_PATH ${HADOOP_COMMON_PATH}/src/main/native/src/org/apache/hadoop/security)

set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

# determine if container-executor.conf.dir is an absolute
# path in case the OS we're compiling on doesn't have
# a hook in get_executable. We'll use this define
# later in the code to potentially throw a compile error
string(REGEX MATCH . HCD_ONE "${HADOOP_CONF_DIR}")
string(COMPARE EQUAL ${HCD_ONE} / HADOOP_CONF_DIR_IS_ABS)

set (CMAKE_C_STANDARD 99)
set (CMAKE_CXX_STANDARD 11)

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
    main/native/oom-listener/impl
    ${HADOOP_COMMON_SEC_PATH}
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
    main/native/container-executor/impl/utils/file-utils.c
    main/native/container-executor/impl/utils/string-utils.c
    main/native/container-executor/impl/utils/path-utils.c
    main/native/container-executor/impl/modules/cgroups/cgroups-operations.c
    main/native/container-executor/impl/modules/common/module-configs.c
    main/native/container-executor/impl/modules/gpu/gpu-module.c
    main/native/container-executor/impl/modules/fpga/fpga-module.c
    main/native/container-executor/impl/modules/devices/devices-module.c
    main/native/container-executor/impl/utils/docker-util.c
    main/native/container-executor/impl/utils/mount-utils.c
    main/native/container-executor/impl/utils/cJSON/cJSON.c
    main/native/container-executor/impl/runc/runc.c
    main/native/container-executor/impl/runc/runc_base_ctx.c
    main/native/container-executor/impl/runc/runc_launch_cmd.c
    main/native/container-executor/impl/runc/runc_reap.c
    main/native/container-executor/impl/runc/runc_write_config.c
    ${HADOOP_COMMON_SEC_PATH}/hadoop_user_info.c
    ${HADOOP_COMMON_SEC_PATH}/hadoop_group_info.c
)

add_executable(container-executor
    main/native/container-executor/impl/main.c
)

# By embedding '$ORIGIN' into the RPATH of container-executor, dlopen will look in
# the directory containing container-executor. However, $ORIGIN is not supported by
# all operating systems.
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux|SunOS")
    set(RPATH "\$ORIGIN/")
    if(EXTRA_LIBHADOOP_RPATH)
        set(RPATH "${RPATH}:${EXTRA_LIBHADOOP_RPATH}/")
    endif()
    message("RPATH SET AS ${RPATH}.")
    set_target_properties(container-executor PROPERTIES INSTALL_RPATH "${RPATH}")
endif()

target_link_libraries(container-executor
    container
    crypto
)

output_directory(container-executor target/usr/local/bin)

# Test cases
add_executable(test-container-executor
    main/native/container-executor/test/test-container-executor.c
)

# By embedding '$ORIGIN' into the RPATH of test-container-executor, dlopen will look in
# the directory containing test-container-executor. However, $ORIGIN is not supported by
# all operating systems.
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux|SunOS")
    set(RPATH "\$ORIGIN/")
    if(EXTRA_LIBHADOOP_RPATH)
        set(RPATH "${RPATH}:${EXTRA_LIBHADOOP_RPATH}/")
    endif()
    message("RPATH SET AS ${RPATH}.")
    set_target_properties(test-container-executor PROPERTIES INSTALL_RPATH "${RPATH}")
endif()

target_link_libraries(test-container-executor
    container
    ${EXTRA_LIBS}
    crypto
)

output_directory(test-container-executor target/usr/local/bin)

# unit tests for container executor
add_executable(cetest
        main/native/container-executor/impl/get_executable.c
        main/native/container-executor/impl/util.c
        main/native/container-executor/test/test_configuration.cc
        main/native/container-executor/test/test_main.cc
        main/native/container-executor/test/utils/test-string-utils.cc
        main/native/container-executor/test/utils/test-path-utils.cc
        main/native/container-executor/test/modules/cgroups/test-cgroups-module.cc
        main/native/container-executor/test/modules/gpu/test-gpu-module.cc
        main/native/container-executor/test/modules/fpga/test-fpga-module.cc
        main/native/container-executor/test/modules/devices/test-devices-module.cc
        main/native/container-executor/test/test_util.cc
        main/native/container-executor/test/utils/test_docker_util.cc
        main/native/container-executor/test/utils/test_runc_util.cc
)
target_link_libraries(cetest
        gtest
        container
        crypto
)

output_directory(cetest test)

# CGroup OOM listener
add_executable(oom-listener
        main/native/oom-listener/impl/oom_listener.c
        main/native/oom-listener/impl/oom_listener.h
        main/native/oom-listener/impl/oom_listener_main.c
)
output_directory(oom-listener target/usr/local/bin)

# CGroup OOM listener test with GTest
add_executable(test-oom-listener
        main/native/oom-listener/impl/oom_listener.c
        main/native/oom-listener/impl/oom_listener.h
        main/native/oom-listener/test/oom_listener_test_main.cc
)
if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
    # macOS doesn't have librt
    target_link_libraries(test-oom-listener gtest)
else()
    target_link_libraries(test-oom-listener gtest rt)
endif()
output_directory(test-oom-listener test)
