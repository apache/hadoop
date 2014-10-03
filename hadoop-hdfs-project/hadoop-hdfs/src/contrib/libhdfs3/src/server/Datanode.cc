/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ClientDatanodeProtocol.pb.h"
#include "Datanode.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "RpcHelper.h"

#include <string>

#define DATANODE_VERSION 1
#define DATANODE_PROTOCOL "org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol"
#define BLOCK_TOKEN_KIND "HDFS_BLOCK_TOKEN"

using namespace google::protobuf;
using namespace hadoop::common;
using namespace hadoop::hdfs;

namespace hdfs {
namespace internal {

DatanodeImpl::DatanodeImpl(const std::string &host, uint32_t port,
                           const SessionConfig &c, const RpcAuth &a) :
    auth(a), client(RpcClient::getClient()), conf(c), protocol(
        DATANODE_VERSION, DATANODE_PROTOCOL, BLOCK_TOKEN_KIND),
        server(host, port) {
    server.setTokenService("");
}

void DatanodeImpl::invoke(const RpcCall & call, bool reuse) {
    RpcChannel & channel = client.getChannel(auth, protocol, server, conf);

    try {
        channel.invoke(call);
    } catch (const HdfsFailoverException & e) {
        //Datanode do not have HA configuration.
        channel.close(true);
        rethrow_if_nested(e);
        assert(false && "HdfsFailoverException should be always a "
               "wrapper of other exception");
    } catch (...) {
        channel.close(true);
        throw;
    }

    channel.close(!reuse);
}

int64_t DatanodeImpl::getReplicaVisibleLength(const ExtendedBlock &b) {
    try {
        GetReplicaVisibleLengthRequestProto request;
        GetReplicaVisibleLengthResponseProto response;
        Build(b, request.mutable_block());
        invoke(RpcCall(true, "getReplicaVisibleLength",
                       &request, &response), false);
        return response.length();
    } catch (const HdfsRpcServerException & e) {
        UnWrapper<ReplicaNotFoundException, HdfsIOException> unwraper(e);
        unwraper.unwrap(__FILE__, __LINE__);
    }
}

void DatanodeImpl::getBlockLocalPathInfo(const ExtendedBlock &block,
        const Token &token, BlockLocalPathInfo &info) {
    try {
        ExtendedBlock eb;
        GetBlockLocalPathInfoRequestProto request;
        GetBlockLocalPathInfoResponseProto response;
        Build(block, request.mutable_block());
        Build(token, request.mutable_token());
        invoke(RpcCall(true, "getBlockLocalPathInfo", &request, &response), true);
        Convert(eb, response.block());
        info.setBlock(eb);
        info.setLocalBlockPath(response.localpath().c_str());
        info.setLocalMetaPath(response.localmetapath().c_str());
    } catch (const HdfsRpcServerException &e) {
        UnWrapper<ReplicaNotFoundException, HdfsIOException> unwraper(e);
        unwraper.unwrap(__FILE__, __LINE__);
    }
}

}
}
