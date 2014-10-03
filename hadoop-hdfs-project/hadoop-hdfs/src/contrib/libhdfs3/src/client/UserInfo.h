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
#ifndef _HDFS_LIBHDFS3_CLIENT_USERINFO_H_
#define _HDFS_LIBHDFS3_CLIENT_USERINFO_H_

#include "Hash.h"
#include "KerberosName.h"
#include "Logger.h"
#include "Token.h"

#include <map>
#include <string>

namespace hdfs {
namespace internal {

class UserInfo {
public:
    UserInfo() {
    }

    explicit UserInfo(const std::string &u) :
        effectiveUser(u) {
    }

    const std::string &getRealUser() const {
        return realUser;
    }

    void setRealUser(const std::string &user) {
        this->realUser = user;
    }

    const std::string &getEffectiveUser() const {
        return effectiveUser.getName();
    }

    void setEffectiveUser(const std::string &effectiveUser) {
        this->effectiveUser = KerberosName(effectiveUser);
    }

    std::string getPrincipal() const {
        return effectiveUser.getPrincipal();
    }

    bool operator ==(const UserInfo &other) const {
        return realUser == other.realUser
               && effectiveUser == other.effectiveUser;
    }

    void addToken(const Token &token) {
        tokens[std::make_pair(token.getKind(), token.getService())] = token;
    }

    const Token * selectToken(const std::string &kind,
                              const std::string &service) const {
        std::map<std::pair<std::string, std::string>,
            Token>::const_iterator it;
        it = tokens.find(std::make_pair(kind, service));
        if (it == tokens.end()) {
            return NULL;
        }
        return &it->second;
    }

    size_t hash_value() const;

public:
    static UserInfo LocalUser();

private:
    KerberosName effectiveUser;
    std::map<std::pair<std::string, std::string>, Token> tokens;
    std::string realUser;
};

}
}

HDFS_HASH_DEFINE(::hdfs::internal::UserInfo);

#endif /* _HDFS_LIBHDFS3_CLIENT_USERINFO_H_ */
