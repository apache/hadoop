# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set (CMAKE_REQUIRED_DEFINITIONS "-std=c++11")

# Disable optimizations if compiling debug
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0")

if(UNIX)
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -std=c++11 -g -fPIC -fno-strict-aliasing")
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -fPIC -fno-strict-aliasing")
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    add_definitions(-DASIO_HAS_STD_ADDRESSOF -DASIO_HAS_STD_ARRAY
    -DASIO_HAS_STD_ATOMIC -DASIO_HAS_CSTDINT -DASIO_HAS_STD_SHARED_PTR
    -DASIO_HAS_STD_TYPE_TRAITS -DASIO_HAS_VARIADIC_TEMPLATES
    -DASIO_HAS_STD_FUNCTION -DASIO_HAS_STD_CHRONO -DASIO_HAS_STD_SYSTEM_ERROR)
endif ()

# Mac OS 10.7 and later deprecates most of the methods in OpenSSL.
# Add -Wno-deprecated-declarations to avoid the warnings.
if(APPLE)
  set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} -stdlib=libc++ -Wno-deprecated-declarations\
       -Wno-unused-local-typedef -Wno-unused-parameter -Wno-nested-anon-types")
endif()

add_definitions(-DASIO_STANDALONE -DASIO_CPP11_DATE_TIME)

