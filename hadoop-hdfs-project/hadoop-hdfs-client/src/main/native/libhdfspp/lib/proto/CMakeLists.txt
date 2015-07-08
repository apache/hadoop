set(CLIENT_PROTO_DIR ${CMAKE_SOURCE_DIR}/../proto)
set(COMMON_PROTO_DIR ${CMAKE_SOURCE_DIR}/../../../../../hadoop-common-project/hadoop-common/src/main/proto)
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

add_library(proto ${PROTO_SRCS} ${PROTO_HDRS})
