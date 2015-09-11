set(CLIENT_PROTO_DIR ${CMAKE_CURRENT_LIST_DIR}/../../../../proto)
set(COMMON_PROTO_DIR ${CMAKE_CURRENT_LIST_DIR}/../../../../../../../../hadoop-common-project/hadoop-common/src/main/proto)
set(PROTOBUF_IMPORT_DIRS ${CLIENT_PROTO_DIR} ${COMMON_PROTO_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS
  ${CLIENT_PROTO_DIR}/datatransfer.proto
  ${CLIENT_PROTO_DIR}/ClientDatanodeProtocol.proto
  ${CLIENT_PROTO_DIR}/ClientNamenodeProtocol.proto
  ${CLIENT_PROTO_DIR}/acl.proto
  ${CLIENT_PROTO_DIR}/datatransfer.proto
  ${CLIENT_PROTO_DIR}/encryption.proto
  ${CLIENT_PROTO_DIR}/hdfs.proto
  ${CLIENT_PROTO_DIR}/inotify.proto
  ${CLIENT_PROTO_DIR}/xattr.proto
  ${COMMON_PROTO_DIR}/IpcConnectionContext.proto
  ${COMMON_PROTO_DIR}/ProtobufRpcEngine.proto
  ${COMMON_PROTO_DIR}/RpcHeader.proto
  ${COMMON_PROTO_DIR}/Security.proto
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
  ${CLIENT_PROTO_DIR}/ClientNamenodeProtocol.proto
)

add_library(proto ${PROTO_SRCS} ${PROTO_HDRS} ${HRPC_SRCS})
