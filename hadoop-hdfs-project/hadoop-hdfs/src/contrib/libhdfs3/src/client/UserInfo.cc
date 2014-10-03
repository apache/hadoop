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

#include "UserInfo.h"

#include "Exception.h"
#include "ExceptionInternal.h"

#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

namespace hdfs {
namespace internal {

UserInfo UserInfo::LocalUser() {
    UserInfo retval;
    uid_t uid, euid;
    int bufsize;
    struct passwd pwd, epwd, *result = NULL;
    euid = geteuid();
    uid = getuid();

    if ((bufsize = sysconf(_SC_GETPW_R_SIZE_MAX)) == -1) {
        THROW(InvalidParameter,
              "Invalid input: \"sysconf\" function failed to get the "
              "configure with key \"_SC_GETPW_R_SIZE_MAX\".");
    }

    std::vector<char> buffer(bufsize);

    if (getpwuid_r(euid, &epwd, &buffer[0], bufsize, &result) != 0 || !result) {
        THROW(InvalidParameter,
              "Invalid input: effective user name cannot be found with UID %u.",
              euid);
    }

    retval.setEffectiveUser(epwd.pw_name);

    if (getpwuid_r(uid, &pwd, &buffer[0], bufsize, &result) != 0 || !result) {
        THROW(InvalidParameter,
              "Invalid input: real user name cannot be found with UID %u.",
              uid);
    }

    retval.setRealUser(pwd.pw_name);
    return retval;
}

size_t UserInfo::hash_value() const {
    size_t values[] = { StringHasher(realUser), effectiveUser.hash_value() };
    return CombineHasher(values, sizeof(values) / sizeof(values[0]));
}

}
}
