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

set(PROTOBUF_IMPORT_DIRS ${PROTO_HDFS_DIR} ${PROTO_HADOOP_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS
  ${PROTO_HDFS_DIR}/datatransfer.proto
  ${PROTO_HDFS_DIR}/ClientDatanodeProtocol.proto
  ${PROTO_HDFS_DIR}/ClientNamenodeProtocol.proto
  ${PROTO_HDFS_DIR}/acl.proto
  ${PROTO_HDFS_DIR}/datatransfer.proto
  ${PROTO_HDFS_DIR}/encryption.proto
  ${PROTO_HDFS_DIR}/erasurecoding.proto
  ${PROTO_HDFS_DIR}/hdfs.proto
  ${PROTO_HDFS_DIR}/inotify.proto
  ${PROTO_HDFS_DIR}/xattr.proto
  ${PROTO_HADOOP_DIR}/IpcConnectionContext.proto
  ${PROTO_HADOOP_DIR}/ProtobufRpcEngine.proto
  ${PROTO_HADOOP_DIR}/RpcHeader.proto
  ${PROTO_HADOOP_DIR}/Security.proto
)

add_executable(protoc-gen-hrpc protoc_gen_hrpc.cc)
target_link_libraries(protoc-gen-hrpc ${PROTOBUF_PROTOC_LIBRARY} ${PROTOBUF_LIBRARY})

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
  ${PROTO_HDFS_DIR}/ClientNamenodeProtocol.proto
)

add_library(proto_obj OBJECT ${PROTO_SRCS} ${PROTO_HDRS} ${HRPC_SRCS})
if(HADOOP_BUILD)
  add_dependencies(proto_obj copy_hadoop_files)
endif(HADOOP_BUILD)
add_library(proto $<TARGET_OBJECTS:proto_obj>)
