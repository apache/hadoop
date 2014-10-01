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

#ifndef _HDFS_LIBHDFS3_COMMON_STRINGUTIL_H_
#define _HDFS_LIBHDFS3_COMMON_STRINGUTIL_H_

#include <string.h>
#include <string>
#include <vector>
#include <cctype>

namespace hdfs {
namespace internal {

static inline std::vector<std::string> StringSplit(const std::string &str,
        const char *sep) {
    char *token, *lasts = NULL;
    std::string s = str;
    std::vector<std::string> retval;
    token = strtok_r(&s[0], sep, &lasts);

    while (token) {
        retval.push_back(token);
        token = strtok_r(NULL, sep, &lasts);
    }

    return retval;
}

static inline  std::string StringTrim(const std::string &str) {
    int start = 0, end = str.length();

    for (; start < static_cast<int>(str.length()); ++start) {
        if (!std::isspace(str[start])) {
            break;
        }
    }

    for (; end > 0; --end) {
        if (!std::isspace(str[end - 1])) {
            break;
        }
    }

    return str.substr(start, end - start);
}

}
}
#endif /* _HDFS_LIBHDFS3_COMMON_STRINGUTIL_H_ */
