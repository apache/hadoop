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

#ifndef LIB_FS_AUTHINFO_H
#define LIB_FS_AUTHINFO_H

#include <optional.hpp>

namespace hdfs {

class Token {
public:
  std::string identifier;
  std::string password;
};

class AuthInfo {
public:
    enum AuthMethod {
        kSimple,
        kKerberos,
        kToken,
        kUnknownAuth,
        kAuthFailed
    };

    AuthInfo() :
        method(kSimple) {
    }

    explicit AuthInfo(AuthMethod mech) :
        method(mech) {
    }

    bool useSASL() {
        return method != kSimple;
    }

    const std::string & getUser() const {
        return user;
    }

    void setUser(const std::string & user) {
        this->user = user;
    }

    AuthMethod getMethod() const {
        return method;
    }

    void setMethod(AuthMethod method) {
        this->method = method;
    }

    const std::experimental::optional<Token> & getToken() const {
        return token;
    }

    void setToken(const Token & token) {
        this->token = token;
    }

    void clearToken() {
        this->token = std::experimental::nullopt;
    }

private:
    AuthMethod method;
    std::string user;
    std::experimental::optional<Token> token;
};

}

#endif /* RPCAUTHINFO_H */
