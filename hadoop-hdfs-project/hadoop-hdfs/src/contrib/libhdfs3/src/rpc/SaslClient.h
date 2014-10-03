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

#ifndef _HDFS_LIBHDFS3_RPC_SASLCLIENT_H_
#define _HDFS_LIBHDFS3_RPC_SASLCLIENT_H_

#include "RpcAuth.h"
#include "RpcHeader.pb.h"
#include "client/Token.h"
#include "network/Socket.h"

#include <gsasl.h>
#include <string>

#define SWITCH_TO_SIMPLE_AUTH -88

namespace hdfs {
namespace internal {

using hadoop::common::RpcSaslProto_SaslAuth;

class SaslClient {
public:
    SaslClient(const RpcSaslProto_SaslAuth &auth, const Token &token,
               const std::string &principal);

    ~SaslClient();

    std::string evaluateChallenge(const std::string &challenge);

    bool isComplete();

private:
    void initKerberos(const RpcSaslProto_SaslAuth &auth,
                      const std::string &principal);
    void initDigestMd5(const RpcSaslProto_SaslAuth &auth, const Token &token);

private:
    Gsasl *ctx;
    Gsasl_session *session;
    bool complete;
};

}
}

#endif /* _HDFS_LIBHDFS3_RPC_SASLCLIENT_H_ */
