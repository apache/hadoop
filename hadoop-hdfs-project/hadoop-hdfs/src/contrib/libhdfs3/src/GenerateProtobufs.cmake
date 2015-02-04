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

MESSAGE(STATUS "Processing hadoop protobuf definitions.")

function(COPY_IF_CHANGED TARGET)
    file(MAKE_DIRECTORY "${TARGET}")
    foreach(PB_PATH ${ARGN})
        get_filename_component(PB_FILENAME "${PB_PATH}" NAME)
        configure_file("${PB_PATH}" "${TARGET}/${PB_FILENAME}" COPY_ONLY)
    endforeach()
endfunction(COPY_IF_CHANGED TARGET)

COPY_IF_CHANGED("${CMAKE_BINARY_DIR}/common_pb"
    ${HADOOP_TOP_DIR}/hadoop-common-project/hadoop-common/src/main/proto/GetUserMappingsProtocol.proto
    ${HADOOP_TOP_DIR}/hadoop-common-project/hadoop-common/src/main/proto/HAServiceProtocol.proto
    ${HADOOP_TOP_DIR}/hadoop-common-project/hadoop-common/src/main/proto/IpcConnectionContext.proto
    ${HADOOP_TOP_DIR}/hadoop-common-project/hadoop-common/src/main/proto/ProtobufRpcEngine.proto
    ${HADOOP_TOP_DIR}/hadoop-common-project/hadoop-common/src/main/proto/ProtocolInfo.proto
    ${HADOOP_TOP_DIR}/hadoop-common-project/hadoop-common/src/main/proto/RefreshAuthorizationPolicyProtocol.proto
    ${HADOOP_TOP_DIR}/hadoop-common-project/hadoop-common/src/main/proto/RefreshCallQueueProtocol.proto
    ${HADOOP_TOP_DIR}/hadoop-common-project/hadoop-common/src/main/proto/RefreshUserMappingsProtocol.proto
    ${HADOOP_TOP_DIR}/hadoop-common-project/hadoop-common/src/main/proto/RpcHeader.proto
    ${HADOOP_TOP_DIR}/hadoop-common-project/hadoop-common/src/main/proto/Security.proto
    ${HADOOP_TOP_DIR}/hadoop-common-project/hadoop-common/src/main/proto/ZKFCProtocol.proto
)

COPY_IF_CHANGED("${CMAKE_BINARY_DIR}/hdfs_pb"
    #${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/DatanodeProtocol.proto
    ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/HAZKInfo.proto
    ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/ClientDatanodeProtocol.proto
    ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/acl.proto
    ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/fsimage.proto
    ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/hdfs.proto
    ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/datatransfer.proto
    ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/InterDatanodeProtocol.proto
    #${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/QJournalProtocol.proto
    ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/JournalProtocol.proto
    ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/NamenodeProtocol.proto
    ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/ClientNamenodeProtocol.proto
    ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/xattr.proto
    ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/encryption.proto
    ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/inotify.proto
)

AUTO_SOURCES(PB_SOURCES "*.proto" "RECURSE" "${CMAKE_BINARY_DIR}")
MESSAGE("PB_SOURCES = ${PB_SOURCES}")

PROTOBUF_GENERATE_CPP(LIBHDFS3_PROTO_SOURCES LIBHDFS3_PROTO_HEADERS "${PB_SOURCES}")
set(${LIBHDFS3_PROTO_SOURCES} ${LIBHDFS3_PROTO_HEADERS} PARENT_SCOPE)
MESSAGE("LIBHDFS3_PROTO_SOURCES = ${LIBHDFS3_PROTO_SOURCES}")
