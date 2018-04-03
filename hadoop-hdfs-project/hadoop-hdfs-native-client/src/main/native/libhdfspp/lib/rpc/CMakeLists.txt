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

list(APPEND rpc_object_items rpc_connection_impl.cc rpc_engine.cc namenode_tracker.cc request.cc sasl_protocol.cc sasl_engine.cc)
if (CMAKE_USING_CYRUS_SASL)
  list(APPEND rpc_object_items cyrus_sasl_engine.cc)
endif (CMAKE_USING_CYRUS_SASL)
if (CMAKE_USING_GSASL)
 list(APPEND rpc_object_items gsasl_engine.cc)
endif (CMAKE_USING_GSASL)

add_library(rpc_obj OBJECT ${rpc_object_items})


add_dependencies(rpc_obj proto)
add_library(rpc $<TARGET_OBJECTS:rpc_obj>)
