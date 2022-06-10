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

if(WIN32)
    add_executable(x_platform_utils_test $<TARGET_OBJECTS:x_platform_obj> utils_common_test.cc utils_test_main.cc utils_win_test.cc)
    add_executable(x_platform_syscall_test $<TARGET_OBJECTS:x_platform_obj> syscall_common_test.cc utils_test_main.cc syscall_win_test.cc)
else(WIN32)
    add_executable(x_platform_utils_test $<TARGET_OBJECTS:x_platform_obj> utils_common_test.cc utils_test_main.cc utils_nix_test.cc)
    add_executable(x_platform_syscall_test $<TARGET_OBJECTS:x_platform_obj> syscall_common_test.cc utils_test_main.cc syscall_nix_test.cc)
endif(WIN32)

target_include_directories(x_platform_utils_test PRIVATE ${LIBHDFSPP_LIB_DIR})
target_link_libraries(x_platform_utils_test gmock_main)
add_test(x_platform_utils_test x_platform_utils_test)

target_include_directories(x_platform_syscall_test PRIVATE ${LIBHDFSPP_LIB_DIR})
target_link_libraries(x_platform_syscall_test gmock_main)
add_test(x_platform_syscall_test x_platform_syscall_test)

add_executable(x_platform_types_test types_test.cc)
target_include_directories(x_platform_types_test PRIVATE ${LIBHDFSPP_LIB_DIR})
target_link_libraries(x_platform_types_test gtest_main)
add_test(x_platform_types_test x_platform_types_test)

add_library(x_platform_dirent_test_obj OBJECT $<TARGET_OBJECTS:x_platform_obj> dirent_test.cc)
add_executable(x_platform_dirent_test $<TARGET_OBJECTS:x_platform_dirent_test_obj> $<TARGET_OBJECTS:x_platform_obj>)
target_include_directories(x_platform_dirent_test PRIVATE ${LIBHDFSPP_LIB_DIR})
target_link_libraries(x_platform_dirent_test PRIVATE gtest_main)
add_test(x_platform_dirent_test x_platform_dirent_test)

add_executable(x_platform_dirent_c_test $<TARGET_OBJECTS:x_platform_dirent_test_obj> $<TARGET_OBJECTS:x_platform_obj> $<TARGET_OBJECTS:x_platform_obj_c_api> c-api/dirent_test.cc)
target_compile_definitions(x_platform_dirent_c_test PRIVATE USE_X_PLATFORM_DIRENT)
target_include_directories(x_platform_dirent_c_test PRIVATE ${LIBHDFSPP_LIB_DIR} ../)
target_link_libraries(x_platform_dirent_c_test PRIVATE gtest_main)
add_test(x_platform_dirent_c_test x_platform_dirent_c_test)
