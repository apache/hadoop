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

#include "RpcChannelKey.h"

#include <vector>

namespace hdfs {
namespace internal {

RpcChannelKey::RpcChannelKey(const RpcAuth &a, const RpcProtocolInfo &p,
                             const RpcServerInfo &s, const RpcConfig &c) :
    auth(a), conf(c), protocol(p), server(s) {
    const Token *temp = auth.getUser().selectToken(protocol.getTokenKind(),
                         server.getTokenService());

    if (temp) {
        token = shared_ptr<Token> (new Token(*temp));
    }
}

size_t RpcChannelKey::hash_value() const {
    size_t tokenHash = token ? token->hash_value() : 0;
    size_t values[] = { auth.hash_value(), protocol.hash_value(),
                        server.hash_value(), conf.hash_value(), tokenHash
                      };
    return CombineHasher(values, sizeof(values) / sizeof(values[0]));
}

}
}
