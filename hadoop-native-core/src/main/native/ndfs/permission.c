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

#include "common/hadoop_err.h"
#include "ndfs/permission.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct hadoop_err *parse_permission(const char *str, uint32_t *perm)
{
    if (strspn(str, " 01234567") != strlen(str)) {
        // TODO: support permission strings as in PermissionParser.java (see
        // HADOOP-10981)
        return hadoop_lerr_alloc(ENOTSUP, "parse_permission(%s): "
            "can't parse non-octal permissions (yet)", str);
    }
    errno = 0;
    *perm = strtol(str, NULL, 8);
    if (errno) {
        int ret = errno;
        return hadoop_lerr_alloc(EINVAL, "parse_permission(%s): "
                "failed to parse this octal string: %s",
                str, terror(ret));
    }
    return NULL;
}

// vim: ts=4:sw=4:tw=79:et
