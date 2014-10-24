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

get_filename_component(R "${PROJECT_SOURCE_DIR}/../../../../../" REALPATH)

COPY_IF_CHANGED("${CMAKE_BINARY_DIR}/common_pb"
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/GetUserMappingsProtocol.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/HAServiceProtocol.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/IpcConnectionContext.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/ProtobufRpcEngine.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/ProtocolInfo.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/RefreshAuthorizationPolicyProtocol.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/RefreshCallQueueProtocol.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/RefreshUserMappingsProtocol.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/RpcHeader.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/Security.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/ZKFCProtocol.proto
)

COPY_IF_CHANGED("${CMAKE_BINARY_DIR}/hdfs_pb"
    #${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/DatanodeProtocol.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/HAZKInfo.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/ClientDatanodeProtocol.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/acl.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/fsimage.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/hdfs.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/datatransfer.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/InterDatanodeProtocol.proto
    #${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/QJournalProtocol.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/JournalProtocol.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/NamenodeProtocol.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/ClientNamenodeProtocol.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/xattr.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/encryption.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/inotify.proto
)

AUTO_SOURCES(PB_SOURCES "*.proto" "RECURSE" "${CMAKE_BINARY_DIR}")
MESSAGE("PB_SOURCES = ${PB_SOURCES}")

PROTOBUF_GENERATE_CPP(LIBHDFS3_PROTO_SOURCES LIBHDFS3_PROTO_HEADERS "${PB_SOURCES}")
set(${LIBHDFS3_PROTO_SOURCES} ${LIBHDFS3_PROTO_HEADERS} PARENT_SCOPE)
MESSAGE("LIBHDFS3_PROTO_SOURCES = ${LIBHDFS3_PROTO_SOURCES}")
