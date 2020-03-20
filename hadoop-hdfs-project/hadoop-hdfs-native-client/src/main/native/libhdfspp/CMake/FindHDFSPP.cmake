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

include(${CMAKE_CURRENT_LIST_DIR}/FindPackageExtension.cmake)

if (NOT HDFSPP_FOUND)

  findPackageExtension("hdfspp/hdfspp.h" "hdfspp" true)
  if (HDFSPP_FOUND)
    find_package(CyrusSASL)
    find_package(OpenSSL)
    find_package(Protobuf)
    find_package(URIparser)
    find_package(Threads)
    set_property(TARGET hdfspp
      PROPERTY INTERFACE_LINK_LIBRARIES
        sasl2 ssl crypto protobuf uriparser dl ${CMAKE_THREAD_LIBS_INIT}
    )
  endif ()
endif ()
