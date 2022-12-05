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

if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  set(SYSCALL_SRC syscall_windows.cc)
else()
  set(SYSCALL_SRC syscall_linux.cc)
endif()

add_library(x_platform_obj OBJECT ${SYSCALL_SRC} utils.cc dirent.cc)
add_library(x_platform_obj_c_api OBJECT $<TARGET_OBJECTS:x_platform_obj> c-api/syscall.cc c-api/dirent.cc)
target_compile_definitions(x_platform_obj_c_api PRIVATE USE_X_PLATFORM_DIRENT)
