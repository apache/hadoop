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

#ifndef _HDFS_LIBHDFS3_CLIENT_FILESYSTEMKEY_H_
#define _HDFS_LIBHDFS3_CLIENT_FILESYSTEMKEY_H_

#include "Hash.h"
#include "UserInfo.h"

#include <string>

namespace hdfs {
namespace internal {

class FileSystemKey {
public:
    FileSystemKey(const std::string &uri, const char *user);

    FileSystemKey(const std::string &auth, const std::string &host,
                  const std::string &port, const std::string &scheme,
                  const std::string &user, size_t u)
        : authority(auth), host(host), port(port), scheme(scheme), user(user) {
    }

    bool operator==(const FileSystemKey &other) const {
        return scheme == other.scheme && authority == other.authority;
    }

    size_t hash_value() const {
        size_t values[] = {StringHasher(scheme), StringHasher(authority)};
        return CombineHasher(values, sizeof(values) / sizeof(values[0]));
    }

    const std::string &getHost() const {
        return host;
    }

    void setHost(const std::string &host) {
        this->host = host;
    }

    const std::string &getPort() const {
        return port;
    }

    void setPort(const std::string &port) {
        this->port = port;
    }

    const std::string &getScheme() const {
        return scheme;
    }

    void setScheme(const std::string &scheme) {
        this->scheme = scheme;
    }

    const UserInfo &getUser() const {
        return user;
    }

    void setUser(const UserInfo &user) {
        this->user = user;
    }

    void addToken(const Token &token) {
        user.addToken(token);
    }

private:
    std::string authority;
    std::string host;
    std::string port;
    std::string scheme;
    UserInfo user;
};
}
}

HDFS_HASH_DEFINE(hdfs::internal::FileSystemKey);

#endif /* _HDFS_LIBHDFS3_CLIENT_FILESYSTEMKEY_H_ */
