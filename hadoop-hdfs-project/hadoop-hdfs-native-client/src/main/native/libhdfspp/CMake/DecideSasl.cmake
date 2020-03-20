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

# Allow either Cyrus or GSASL for a SASL implementation
if (HDFSPP_SASL_IMPL STREQUAL "CYRUS")
  find_package(CyrusSASL REQUIRED)
  set (SASL_LIBRARIES sasl2)
  set (CMAKE_USING_CYRUS_SASL 1)
  add_definitions(-DUSE_SASL -DUSE_CYRUS_SASL)
elseif (HDFSPP_SASL_IMPL STREQUAL "GSASL")
  find_package(GSasl REQUIRED)
  set (SASL_LIBRARIES gsasl)
  set (CMAKE_USING_GSASL 1)
  add_definitions(-DUSE_SASL -DUSE_GSASL)
else ()
  message(STATUS "Compiling with NO SASL SUPPORT")
  set (SASL_LIBRARIES)
endif ()
