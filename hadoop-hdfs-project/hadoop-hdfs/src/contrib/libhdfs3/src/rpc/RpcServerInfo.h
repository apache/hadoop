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

#ifndef _HDFS_LIBHDFS3_RPC_RPCSERVERINFO_H_
#define _HDFS_LIBHDFS3_RPC_RPCSERVERINFO_H_

#include "Hash.h"

#include <string>
#include <sstream>

namespace hdfs {
namespace internal {

class RpcServerInfo {
public:
    RpcServerInfo(const std::string &tokenService, const std::string &h,
                  const std::string &p) :
        host(h), port(p), tokenService(tokenService) {
    }

    RpcServerInfo(const std::string &h, uint32_t p) :
        host(h) {
        std::stringstream ss;
        ss << p;
        port = ss.str();
    }

    size_t hash_value() const;

    bool operator ==(const RpcServerInfo &other) const {
        return this->host == other.host && this->port == other.port &&
            tokenService == other.tokenService;
    }

    const std::string &getTokenService() const {
        return tokenService;
    }

    const std::string &getHost() const {
        return host;
    }

    const std::string &getPort() const {
        return port;
    }

    void setTokenService(const std::string &tokenService) {
        this->tokenService = tokenService;
    }

private:
    std::string host;
    std::string port;
    std::string tokenService;
};

}
}

HDFS_HASH_DEFINE(::hdfs::internal::RpcServerInfo);

#endif /* _HDFS_LIBHDFS3_RPC_RPCSERVERINFO_H_ */
