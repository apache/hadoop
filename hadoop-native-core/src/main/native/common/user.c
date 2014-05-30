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

#include "common/user.h"

#include <errno.h>
#include <pwd.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#define GETPWUID_R_BUFLEN_MIN 4096
#define GETPWUID_R_BUFLEN_MAX (8 * 1024 * 1024)

/**
 * Get the user name associated with a given numeric user ID.
 *
 * The POSIX APIs to do this require a little more code than you might expect.
 * We have to supply a buffer to the getpwuid_r call.  But how big should it
 * be?  We can call sysconf to get a "suggested size," but this may or may not
 * be big enough for our particular user.  Also, on some platforms, sysconf
 * returns -1 for this.  So we have a loop which keeps doubling the size of the
 * buffer.  As a sanity check against buggy libraries, we give up when we reach
 * some ridiculous buffer size.
 *
 * @param uid           UID to look up
 * @param out           (out param) on success, the user name.
 *
 * @return              0 on success; error code on failure.
 */
static int uid_to_string(uid_t uid, char **out)
{
    int ret;
    struct passwd pwd, *result;
    char *buf = NULL, *nbuf;
    size_t buflen, nbuflen;

    buflen = sysconf(_SC_GETPW_R_SIZE_MAX);
    if (buflen < GETPWUID_R_BUFLEN_MIN) {
        buflen = GETPWUID_R_BUFLEN_MIN;
    }
    while (1) {
        nbuf = realloc(buf, buflen);
        if (!nbuf) {
            ret = ENOMEM;
            goto done;
        }
        buf = nbuf;
        ret = getpwuid_r(uid, &pwd, buf, buflen, &result);
        if (ret == 0)
            break;
        if (ret != ERANGE) {
            fprintf(stderr, "geteuid_string: getpwuid_r(%lld) failed "
                    "with error %d\n", (long long)uid, ret);
            goto done;
        }
        nbuflen = buflen *2;
        if (nbuflen > GETPWUID_R_BUFLEN_MAX) {
            nbuflen = GETPWUID_R_BUFLEN_MAX;
        }
        if (buflen == nbuflen) {
            fprintf(stderr, "geteuid_string: getpwuid_r(%lld) still gets "
                    "ERANGE with buflen %zd\n", (long long)uid, buflen);
            ret = ERANGE;
            goto done;
        }
        buflen = nbuflen;
    }
    if (!result) {
        ret = ENOENT;
        fprintf(stderr, "geteuid_string: getpwuid_r(%lld): no name "
                "found for current effective user id.\n", (long long)uid);
        goto done;
    }
    *out = strdup(result->pw_name);
    if (!*out) {
        ret = ENOMEM;
        goto done;
    }

done:
    free(buf);
    return ret;
}

int geteuid_string(char **out)
{
    return uid_to_string(geteuid(), out);
}

// vim: ts=4:sw=4:tw=79:et
