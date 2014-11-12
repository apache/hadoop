/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"){} you may not use this file except in compliance
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

#include "Atoi.h"
#include "Status.h"

#include <limits>
#include <sstream>
#include <stdint.h>
#include <stdlib.h>
#include <string>

using hdfs::Status;
using std::ostringstream;

Status StrToInt32(const char *str, int32_t *ret) {
    long retval;
    char *end = NULL;
    errno = 0;
    retval = strtol(str, &end, 0);

    if (EINVAL == errno || 0 != *end) {
        ostringstream oss;
        oss << "Invalid int32_t type: " << str;
        return Status(EINVAL, oss.str());
    }
    if (ERANGE == errno || retval > std::numeric_limits<int32_t>::max() ||
        retval < std::numeric_limits<int32_t>::min()) {
        ostringstream oss;
        oss << "Underflow/Overflow in int32_t type: " << str;
        return Status(EINVAL, oss.str());
    }
    *ret = retval;
    return Status::OK();
}

Status StrToInt64(const char *str, int64_t *ret) {
    long long retval;
    char *end = NULL;
    errno = 0;
    retval = strtoll(str, &end, 0);

    if (EINVAL == errno || 0 != *end) {
        ostringstream oss;
        oss << "Invalid int64_t type: " << str;
        return Status(EINVAL, oss.str());
    }
    if (ERANGE == errno || retval > std::numeric_limits<int64_t>::max() ||
        retval < std::numeric_limits<int64_t>::min()) {
        ostringstream oss;
        oss << "Underflow/Overflow in int64_t type: " << str;
        return Status(EINVAL, oss.str());
    }
    *ret = retval;
    return Status::OK();
}

Status StrToBool(const char *str, bool *ret) {
    bool retval = false;

    if (!strcasecmp(str, "true") || !strcmp(str, "1")) {
        retval = true;
    } else if (!strcasecmp(str, "false") || !strcmp(str, "0")) {
        retval = false;
    } else {
        ostringstream oss;
        oss << "Invalid bool type: " << str;
        return Status(EINVAL, oss.str());
    }
    *ret = retval;
    return Status::OK();
}

Status StrToDouble(const char *str, double *ret) {
    double retval;
    char *end = NULL;
    errno = 0;
    retval = strtod(str, &end);

    if (EINVAL == errno || 0 != *end) {
        ostringstream oss;
        oss << "Invalid double type: " << str;
        return Status(EINVAL, oss.str());
    }
    if (ERANGE == errno || retval > std::numeric_limits<double>::max() ||
        retval < std::numeric_limits<double>::min()) {
        ostringstream oss;
        oss << "Underflow/Overflow in double type: " << str;
        return Status(EINVAL, oss.str());
    }
    *ret = retval;
    return Status::OK();
}
