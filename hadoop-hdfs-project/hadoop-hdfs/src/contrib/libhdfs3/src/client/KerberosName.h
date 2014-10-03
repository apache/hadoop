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
#ifndef _HDFS_LIBHDFS3_CLIENT_KERBEROSNAME_H_
#define _HDFS_LIBHDFS3_CLIENT_KERBEROSNAME_H_

#include <string>
#include <sstream>

#include "Hash.h"

namespace hdfs {
namespace internal {

class KerberosName {
public:
    KerberosName();
    KerberosName(const std::string &principal);

    std::string getPrincipal() const {
        std::stringstream ss;
        ss << name;

        if (!host.empty()) {
            ss << "/" << host;
        }

        if (!realm.empty()) {
            ss << '@' << realm;
        }

        return ss.str();
    }

    const std::string &getHost() const {
        return host;
    }

    void setHost(const std::string &host) {
        this->host = host;
    }

    const std::string &getName() const {
        return name;
    }

    void setName(const std::string &name) {
        this->name = name;
    }

    const std::string &getRealm() const {
        return realm;
    }

    void setRealm(const std::string &realm) {
        this->realm = realm;
    }

    size_t hash_value() const;

    bool operator ==(const KerberosName &other) const {
        return name == other.name && host == other.host && realm == other.realm;
    }

private:
    void parse(const std::string &principal);

private:
    std::string name;
    std::string host;
    std::string realm;
};

}
}

HDFS_HASH_DEFINE(::hdfs::internal::KerberosName);

#endif /* _HDFS_LIBHDFS3_CLIENT_KERBEROSNAME_H_ */
