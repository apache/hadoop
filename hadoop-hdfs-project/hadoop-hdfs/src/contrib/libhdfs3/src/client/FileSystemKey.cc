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

#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileSystemKey.h"

#include <algorithm>
#include <libxml/uri.h>
#include <sstream>

namespace hdfs {
namespace internal {

FileSystemKey::FileSystemKey(const std::string &uri, const char *u) {
    xmlURIPtr uriobj;
    std::stringstream ss;
    uriobj = xmlParseURI(uri.c_str());

    try {
        if (!uriobj || uriobj->server == NULL || 0 == strlen(uriobj->server)) {
            THROW(InvalidParameter,
                  "Invalid input: uri: %s is not a valid URI type.",
                  uri.c_str());
        }

        host = uriobj->server;

        if (NULL == uriobj->scheme || 0 == strlen(uriobj->scheme)) {
            scheme = "hdfs";
        } else {
            scheme = uriobj->scheme;
        }

        if (strcasecmp(scheme.c_str(), "hdfs")) {
            THROW(InvalidParameter,
                  "Invalid input: uri is not a valid URI type.");
        }

        if (u && strlen(u) > 0) {
            user = UserInfo(u);
        } else if (NULL == uriobj->user || 0 == strlen(uriobj->user)) {
            user = UserInfo::LocalUser();
        } else {
            user = UserInfo(uriobj->user);
        }

        ss << user.getEffectiveUser();

        if (uriobj->port == 0) {
            ss << "@" << uriobj->server;
        } else {
            std::stringstream s;
            s << uriobj->port;
            port = s.str();
            ss << "@" << uriobj->server << ":" << uriobj->port;
        }

        authority = ss.str();
    } catch (...) {
        if (uriobj) {
            xmlFreeURI(uriobj);
        }

        throw;
    }

    xmlFreeURI(uriobj);
    std::transform(authority.begin(), authority.end(), authority.begin(),
                   tolower);
    std::transform(scheme.begin(), scheme.end(), scheme.begin(), tolower);
}
}
}
