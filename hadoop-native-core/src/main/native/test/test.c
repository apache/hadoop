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

#include "test/test.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

void fail(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    exit(1);
}

static int vexpect_decode(const char *expected, const char *text, int ty,
        const char *str)
{
    switch (ty) {
    case TEST_ERROR_EQ:
        if (strcmp(expected, str) != 0) {
            fprintf(stderr, "error: expected '%s', but got '%s', which "
                   "is not equal. %s\n", expected, str, text);
            return -1;
        }
        break;
    case TEST_ERROR_GE:
        if (strcmp(expected, str) > 0) {
            fprintf(stderr, "error: expected '%s', but got '%s'. "
                   "Expected something greater or equal.  %s\n",
                   expected, str, text);
            return -1;
        }
        break;
    case TEST_ERROR_GT:
        if (strcmp(expected, str) >= 0) {
            fprintf(stderr, "error: expected '%s', but got '%s'. "
                   "Expected something greater.  %s\n",
                   expected, str, text);
            return -1;
        }
        break;
    case TEST_ERROR_LE:
        if (strcmp(expected, str) < 0) {
            fprintf(stderr, "error: expected '%s', but got '%s'. "
                   "Expected something less or equal.  %s\n",
                   expected, str, text);
            return -1;
        }
        break;
    case TEST_ERROR_LT:
        if (strcmp(expected, str) <= 0) {
            fprintf(stderr, "error: expected '%s', but got '%s'. "
                   "Expected something less.  %s\n",
                   expected, str, text);
            return -1;
        }
        break;
    case TEST_ERROR_NE:
        if (strcmp(expected, str) == 0) {
            fprintf(stderr, "error: did not expect '%s': '%s'\n",
                   expected, text);
            return -1;
        }
        break;
    default:
        fprintf(stderr, "runtime error: invalid type %d passed in: '%s'\n",
                ty, text);
        return -1;
    }
    return 0;
}

int vexpect(const char *expected, const char *text, int ty,
        const char *fmt, va_list ap)
{
    char *str = NULL;
    int ret;

    if (vasprintf(&str, fmt, ap) < 0) { // TODO: portability
        fprintf(stderr, "error: vasprintf failed: %s\n", text);
        return -1;
    }
    ret = vexpect_decode(expected, text, ty, str);
    free(str);
    return ret;
}

int expect(const char *expected, const char *text, int ty,
        const char *fmt, ...)
{
    int ret;
    va_list ap;

    va_start(ap, fmt);
    ret = vexpect(expected, text, ty, fmt, ap);
    va_end(ap);
    return ret;
}

void *xcalloc(size_t len)
{
    void *v = calloc(1, len);
    if (!v) {
        abort();
    }
    return v;
}

// vim: ts=4:sw=4:tw=79:et
