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

#ifndef _HDFS_LIBHDFS3_RPC_RPCCHANNELKEY_H_
#define _HDFS_LIBHDFS3_RPC_RPCCHANNELKEY_H_

#include "Hash.h"
#include "RpcAuth.h"
#include "RpcConfig.h"
#include "RpcProtocolInfo.h"
#include "RpcServerInfo.h"
#include "SharedPtr.h"
#include "client/Token.h"

namespace hdfs {
namespace internal {

class RpcChannelKey {
public:
    RpcChannelKey(const RpcAuth &a, const RpcProtocolInfo &p,
                  const RpcServerInfo &s, const RpcConfig &c);

public:
    size_t hash_value() const;

    const RpcAuth &getAuth() const {
        return auth;
    }

    const RpcConfig &getConf() const {
        return conf;
    }

    const RpcProtocolInfo &getProtocol() const {
        return protocol;
    }

    const RpcServerInfo &getServer() const {
        return server;
    }

    bool operator ==(const RpcChannelKey &other) const {
        return this->auth == other.auth && this->protocol == other.protocol
               && this->server == other.server && this->conf == other.conf
               && ((token == NULL && other.token == NULL)
                   || (token && other.token && *token == *other.token));
    }

    const Token &getToken() const {
        assert(token != NULL);
        return *token;
    }

    bool hasToken() {
        return token != NULL;
    }

private:
    const RpcAuth auth;
    const RpcConfig conf;
    const RpcProtocolInfo protocol;
    const RpcServerInfo server;
    hdfs::internal::shared_ptr<Token> token;
};

}
}

HDFS_HASH_DEFINE(::hdfs::internal::RpcChannelKey);

#endif /* _HDFS_LIBHDFS3_RPC_RPCCHANNELKEY_H_ */
