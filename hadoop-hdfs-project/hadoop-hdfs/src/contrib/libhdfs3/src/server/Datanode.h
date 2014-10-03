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

#ifndef _HDFS_LIBHDFS3_SERVER_DATANODE_H_
#define _HDFS_LIBHDFS3_SERVER_DATANODE_H_

#include "BlockLocalPathInfo.h"
#include "client/Token.h"
#include "ExtendedBlock.h"
#include "rpc/RpcAuth.h"
#include "rpc/RpcCall.h"
#include "rpc/RpcClient.h"
#include "rpc/RpcConfig.h"
#include "rpc/RpcProtocolInfo.h"
#include "rpc/RpcServerInfo.h"
#include "SessionConfig.h"

#include <stdint.h>

namespace hdfs {
namespace internal {

class Datanode {
public:
    virtual ~Datanode() {
    }

    /**
     * Return the visible length of a replica.
     * @param b The block which visible length is to be returned.
     * @return the visible length of the block.
     * @throw ReplicaNotFoundException
     * @throw HdfsIOException
     */
    //Idempotent
    virtual int64_t getReplicaVisibleLength(const ExtendedBlock &b)
    /*throw (ReplicaNotFoundException, HdfsIOException)*/ = 0;

    /**
     * Retrieves the path names of the block file and metadata file stored on the
     * local file system.
     *
     * In order for this method to work, one of the following should be satisfied:
     * <ul>
     * <li>
     * The client user must be configured at the datanode to be able to use this
     * method.</li>
     * <li>
     * When security is enabled, kerberos authentication must be used to connect
     * to the datanode.</li>
     * </ul>
     *
     * @param block The specified block on the local datanode
     * @param token The block access token.
     * @param info Output the BlockLocalPathInfo of block.
     * @throw HdfsIOException
     */
    //Idempotent
    virtual void getBlockLocalPathInfo(const ExtendedBlock &block,
                                       const Token & token, BlockLocalPathInfo &info)
    /*throw (HdfsIOException)*/ = 0;
};

class DatanodeImpl: public Datanode {
public:
    DatanodeImpl(const std::string & host, uint32_t port, const SessionConfig & c,
                 const RpcAuth & a);

    virtual int64_t getReplicaVisibleLength(const ExtendedBlock &b);

    virtual void getBlockLocalPathInfo(const ExtendedBlock &block,
                                       const Token & token, BlockLocalPathInfo &info);

private:
    void invoke(const RpcCall & call, bool reuse);

private:
    RpcAuth auth;
    RpcClient &client;
    RpcConfig conf;
    RpcProtocolInfo protocol;
    RpcServerInfo server;
};

}
}

#endif /* _HDFS_LIBHDFS3_SERVER_DATANODE_H_ */
