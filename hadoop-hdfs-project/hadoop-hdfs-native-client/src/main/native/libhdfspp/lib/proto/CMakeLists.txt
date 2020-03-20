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

include(CMakeParseArguments)
include(ProtobufGenerate)

# Paths to find the imported files
set(PROTOBUF_IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS
  ${CMAKE_CURRENT_SOURCE_DIR}/datatransfer.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/ClientDatanodeProtocol.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/ClientNamenodeProtocol.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/acl.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/datatransfer.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/encryption.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/erasurecoding.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/hdfs.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/inotify.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/xattr.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/ReconfigurationProtocol.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/IpcConnectionContext.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/ProtobufRpcEngine.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/RpcHeader.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/Security.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/HAServiceProtocol.proto
)

add_executable(protoc-gen-hrpc protoc_gen_hrpc.cc)
target_link_libraries(protoc-gen-hrpc protoc protobuf)

function(GEN_HRPC SRCS)
  if(NOT ARGN)
    message(SEND_ERROR "Error: GEN_HRPC() called without any proto files")
    return()
  endif()

  if(DEFINED PROTOBUF_IMPORT_DIRS)
    foreach(DIR ${PROTOBUF_IMPORT_DIRS})
      get_filename_component(ABS_PATH ${DIR} ABSOLUTE)
      list(FIND _protobuf_include_path ${ABS_PATH} _contains_already)
      if(${_contains_already} EQUAL -1)
          list(APPEND _protobuf_include_path -I ${ABS_PATH})
      endif()
    endforeach()
  endif()

  set(${SRCS})

  foreach(FIL ${ARGN})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    get_filename_component(FIL_WE ${FIL} NAME_WE)

    list(APPEND ${SRCS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.hrpc.inl")

    add_custom_command(
      OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.hrpc.inl"
      COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
      ARGS --plugin=protoc-gen-hrpc=${CMAKE_CURRENT_BINARY_DIR}/protoc-gen-hrpc --hrpc_out=${CMAKE_CURRENT_BINARY_DIR} ${_protobuf_include_path} ${ABS_FIL}
      DEPENDS ${ABS_FIL} ${PROTOBUF_PROTOC_EXECUTABLE} protoc-gen-hrpc
      COMMENT "Running HRPC protocol buffer compiler on ${FIL}"
      VERBATIM )
  endforeach()

  set_source_files_properties(${${SRCS}} PROPERTIES GENERATED TRUE)
  set(${SRCS} ${${SRCS}} PARENT_SCOPE)
endfunction()

gen_hrpc(HRPC_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/ClientNamenodeProtocol.proto
)

add_library(proto OBJECT ${PROTO_SRCS} ${PROTO_HDRS} ${HRPC_SRCS})
add_dependencies(proto protobuf)
