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
#include "common/string.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void hex_buf_print(FILE *fp, const void *buf, int32_t buf_len,
                   const char *fmt, ...)
{
    int32_t i, j = 0;
    va_list ap;
    const uint8_t *b = buf;

    va_start(ap, fmt);
    vfprintf(fp, fmt, ap);
    va_end(ap);
    for (i = 0; i < buf_len; i++) {
        const char *suffix = " ";

        if (++j == 8) {
            suffix = "\n";
            j = 0;
        }
        fprintf(fp, "%02x%s", b[i], suffix);
    }
}

int strdupto(char **dst, const char *src)
{
    char *ndst;
    size_t src_len;

    if (!src) {
        free(*dst);
        *dst = NULL;
        return 0;
    }
    src_len = strlen(src);
    ndst = realloc(*dst, src_len + 1);
    if (!ndst) {
        return ENOMEM;
    }
    strcpy(ndst, src);
    *dst = ndst;
    return 0;
}

struct hadoop_err *vdynprintf(char **out, const char *fmt, va_list ap)
{
    char *str;

    if (vasprintf(&str, fmt, ap) < 0) {
        return hadoop_lerr_alloc(ENOMEM, "vdynprintf: OOM");
    }
    if (*out) {
        free(*out);
    }
    *out = str;
    return NULL;
}

struct hadoop_err *dynprintf(char **out, const char *fmt, ...)
{
    struct hadoop_err *err;
    va_list ap;

    va_start(ap, fmt);
    err = vdynprintf(out, fmt, ap);
    va_end(ap);
    return err;
}

// vim: ts=4:sw=4:tw=79:et
