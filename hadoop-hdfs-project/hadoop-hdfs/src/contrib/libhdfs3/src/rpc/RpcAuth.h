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

#ifndef _HDFS_LIBHDFS3_RPC_RPCAUTH_H_
#define _HDFS_LIBHDFS3_RPC_RPCAUTH_H_

#include "client/UserInfo.h"
#include "Hash.h"

#include <string>

namespace hdfs {
namespace internal {

enum AuthMethod {
    SIMPLE = 80, KERBEROS = 81, //"GSSAPI"
    TOKEN = 82, //"DIGEST-MD5"
    UNKNOWN = 255
};

enum AuthProtocol {
    NONE = 0, SASL = -33
};

class RpcAuth {
public:
    RpcAuth() :
        method(SIMPLE) {
    }

    explicit RpcAuth(AuthMethod mech) :
        method(mech) {
    }

    RpcAuth(const UserInfo &ui, AuthMethod mech) :
        method(mech), user(ui) {
    }

    AuthProtocol getProtocol() const {
        return method == SIMPLE ? AuthProtocol::NONE : AuthProtocol::SASL;
    }

    const UserInfo &getUser() const {
        return user;
    }

    UserInfo &getUser() {
        return user;
    }

    void setUser(const UserInfo &user) {
        this->user = user;
    }

    AuthMethod getMethod() const {
        return method;
    }

    size_t hash_value() const;

    bool operator ==(const RpcAuth &other) const {
        return method == other.method && user == other.user;
    }

public:
    static AuthMethod ParseMethod(const std::string &str);

private:
    AuthMethod method;
    UserInfo user;
};

}
}

HDFS_HASH_DEFINE(::hdfs::internal::RpcAuth);

#endif /* _HDFS_LIBHDFS3_RPC_RPCAUTH_H_ */
