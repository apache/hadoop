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
    add_executable(x_platform_utils_test $<TARGET_OBJECTS:x_platform_utils_obj> utils_common_test.cc utils_test_main.cc utils_win_test.cc)
else(WIN32)
    add_executable(x_platform_utils_test $<TARGET_OBJECTS:x_platform_utils_obj> utils_common_test.cc utils_test_main.cc utils_nix_test.cc)
endif(WIN32)
target_include_directories(x_platform_utils_test PRIVATE ${LIBHDFSPP_LIB_DIR})
target_link_libraries(x_platform_utils_test gmock_main)
add_test(x_platform_utils_test x_platform_utils_test)
