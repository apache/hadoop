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

#ifndef _HDFS_LIBHDFS3_RPC_RPCPROTOCOLINFO_H_
#define _HDFS_LIBHDFS3_RPC_RPCPROTOCOLINFO_H_

#include "Hash.h"

#include <string>

namespace hdfs {
namespace internal {

class RpcProtocolInfo {
public:
    RpcProtocolInfo(int v, const std::string &p,
            const std::string &tokenKind) :
        version(v), protocol(p), tokenKind(tokenKind) {
    }

    size_t hash_value() const;

    bool operator ==(const RpcProtocolInfo &other) const {
        return version == other.version && protocol == other.protocol &&
          tokenKind == other.tokenKind;
    }

    const std::string &getProtocol() const {
        return protocol;
    }

    void setProtocol(const std::string &protocol) {
        this->protocol = protocol;
    }

    int getVersion() const {
        return version;
    }

    void setVersion(int version) {
        this->version = version;
    }

    const std::string getTokenKind() const {
        return tokenKind;
    }

    void setTokenKind(const std::string &tokenKind) {
        this->tokenKind = tokenKind;
    }

private:
    int version;
    std::string protocol;
    std::string tokenKind;
};

}
}

HDFS_HASH_DEFINE(::hdfs::internal::RpcProtocolInfo);

#endif /* _HDFS_LIBHDFS3_RPC_RPCPROTOCOLINFO_H_ */
