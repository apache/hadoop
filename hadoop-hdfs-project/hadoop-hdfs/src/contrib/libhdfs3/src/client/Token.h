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

#ifndef _HDFS_LIBHDFS3_CLIENT_TOKEN_H_
#define _HDFS_LIBHDFS3_CLIENT_TOKEN_H_

#include <string>

namespace hdfs {
namespace internal {

class Token {
public:
    std::string getIdentifier() const {
        return identifier;
    }

    void setIdentifier(const std::string &identifier) {
        this->identifier = identifier;
    }

    std::string getKind() const {
        return kind;
    }

    void setKind(const std::string &kind) {
        this->kind = kind;
    }

    std::string getPassword() const {
        return password;
    }

    void setPassword(const std::string &password) {
        this->password = password;
    }

    std::string getService() const {
        return service;
    }

    void setService(const std::string &service) {
        this->service = service;
    }

    bool operator ==(const Token &other) const {
        return identifier == other.identifier && password == other.password
               && kind == other.kind && service == other.service;
    }

    std::string toString() const;

    void fromString(const std::string &str);

    size_t hash_value() const;

private:
    std::string identifier;
    std::string password;
    std::string kind;
    std::string service;
};

}
}

#endif
