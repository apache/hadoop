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


#uripaser lib likes to use always_inline and gcc complains
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-attributes")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-attributes")

add_library(uriparser2_obj OBJECT uriparser2/uriparser2.c uriparser2/uriparser/UriParse.c uriparser2/uriparser/UriParseBase.c 
    uriparser2/uriparser/UriCommon.c uriparser2/uriparser/UriIp4Base.c uriparser2/uriparser/UriIp4.c)
add_library(uriparser2 $<TARGET_OBJECTS:uriparser2_obj>)
