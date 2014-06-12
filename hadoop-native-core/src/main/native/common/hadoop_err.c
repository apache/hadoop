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

#include "hadoop_err.h"

#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <uv.h>

struct hadoop_err {
    int code;
    char *msg;
    int malloced;
};

#define SUFFIX ": "
#define SUFFIX_LEN (sizeof(SUFFIX) - 1)

#define RUNTIME_EXCEPTION_ERROR_CODE EFAULT

#define RUNTIME_EXCEPTION_CLASS \
    "org.apache.hadoop.native.HadoopCore.RuntimeException"

static const struct hadoop_err HADOOP_OOM_ERR = {
    RUNTIME_EXCEPTION_ERROR_CODE,
    RUNTIME_EXCEPTION_CLASS": Failed to allcoate memory for an error message.",
    0
};

static const struct hadoop_err HADOOP_VSNPRINTF_ERR = {
    RUNTIME_EXCEPTION_ERROR_CODE,
    RUNTIME_EXCEPTION_CLASS": vsnprintf error while preparing an error "
        "message.",
    0
};

static const char *errno_to_class(int code)
{
    switch (code) {
    case RUNTIME_EXCEPTION_ERROR_CODE:
        return RUNTIME_EXCEPTION_CLASS;
    case EINVAL:
        return "org.apache.hadoop.native.HadoopCore.InvalidRequestException";
    case ENOMEM:
        return "org.apache.hadoop.native.HadoopCore.OutOfMemoryException";
    default:
        return "org.apache.hadoop.native.HadoopCore.IOException";
    }
}

static const char *uv_code_to_class(int code)
{
    switch (code) {
    case UV_EOF:
        return "org.apache.hadoop.native.HadoopCore.EOFException";
    case UV_EINVAL:
        return "org.apache.hadoop.native.HadoopCore.InvalidRequestException";
    case UV_ECONNREFUSED:
        return "org.apache.hadoop.native.HadoopCore.ConnectionRefusedException";
    case UV_ENOMEM:
        return "org.apache.hadoop.native.HadoopCore.OutOfMemoryException";
    default:
        return "org.apache.hadoop.native.HadoopCore.IOException";
    }
}

struct hadoop_err *hadoop_lerr_alloc(int code, const char *fmt, ...)
{
    struct hadoop_err *err = NULL, *ierr = NULL;
    va_list ap, ap2;
    const char *class;
    char test_buf[1];
    int fmt_len, class_len, msg_len;

    err = calloc(1, sizeof(*err));
    if (!err) {
        ierr = (struct hadoop_err*)&HADOOP_OOM_ERR;
        goto done;
    }
    err->code = code;
    err->malloced = 1;
    va_start(ap, fmt);
    va_copy(ap2, ap);
    fmt_len = vsnprintf(test_buf, sizeof(test_buf), fmt, ap);
    if (fmt_len < 0) {
        ierr = (struct hadoop_err*)&HADOOP_VSNPRINTF_ERR;
        goto done;
    }
    class = errno_to_class(code);
    class_len = strlen(class);
    msg_len = class_len + SUFFIX_LEN + fmt_len + 1;
    err->msg = malloc(msg_len);
    if (!err->msg) {
        ierr = (struct hadoop_err*)&HADOOP_OOM_ERR;
        goto done;
    }
    strcpy(err->msg, class);
    strcpy(err->msg + class_len, SUFFIX);
    vsprintf(err->msg + class_len + SUFFIX_LEN, fmt, ap2);
    va_end(ap2);
    va_end(ap);

done:
    if (ierr) {
        hadoop_err_free(err);
        return ierr;
    }
    return err;
}

struct hadoop_err *hadoop_uverr_alloc(int code, const char *fmt, ...)
{
    struct hadoop_err *err = NULL, *ierr = NULL;
    va_list ap, ap2;
    const char *class;
    const char *umsg;
    char test_buf[1];
    int fmt_len, umsg_len, class_len, msg_len;

    err = calloc(1, sizeof(*err));
    if (!err) {
        ierr = (struct hadoop_err*)&HADOOP_OOM_ERR;
        goto done;
    }
    err->code = code;
    umsg = uv_strerror(code);
    umsg_len = strlen(umsg);
    err->malloced = 1;
    va_start(ap, fmt);
    va_copy(ap2, ap);
    fmt_len = vsnprintf(test_buf, sizeof(test_buf), fmt, ap);
    if (fmt_len < 0) {
        ierr = (struct hadoop_err*)&HADOOP_VSNPRINTF_ERR;
        goto done;
    }
    class = uv_code_to_class(err->code);
    class_len = strlen(class);
    msg_len = class_len + SUFFIX_LEN + umsg_len + SUFFIX_LEN + fmt_len + 1;
    err->msg = malloc(msg_len);
    if (!err->msg) {
        ierr = (struct hadoop_err*)&HADOOP_OOM_ERR;
        goto done;
    }
    strcpy(err->msg, class);
    strcpy(err->msg + class_len, SUFFIX);
    strcpy(err->msg + class_len + SUFFIX_LEN, umsg);
    strcpy(err->msg + class_len + SUFFIX_LEN + umsg_len, SUFFIX);
    vsprintf(err->msg + class_len + SUFFIX_LEN + umsg_len + SUFFIX_LEN,
        fmt, ap2);
    va_end(ap2);
    va_end(ap);

done:
    if (ierr) {
        hadoop_err_free(err);
        return ierr;
    }
    return err;
}

void hadoop_err_free(struct hadoop_err *err)
{
    if (!err)
        return;
    if (!err->malloced)
        return;
    free(err->msg);
    free(err);
}

int hadoop_err_code(const struct hadoop_err *err)
{
    return err->code;
}

const char *hadoop_err_msg(const struct hadoop_err *err)
{
    return err->msg;
}

struct hadoop_err *hadoop_err_prepend(struct hadoop_err *err,
        int code, const char *fmt, ...)
{
    struct hadoop_err *nerr = NULL;
    va_list ap;
    char *nmsg = NULL, *prepend_str = NULL;

    va_start(ap, fmt);
    if (vasprintf(&prepend_str, fmt, ap) < 0) {
        prepend_str = NULL;
        va_end(ap);
        return err;
    }
    va_end(ap);
    if (asprintf(&nmsg, "%s: %s", prepend_str, err->msg) < 0) {
        free(prepend_str);
        return (struct hadoop_err*)err;
    }
    free(prepend_str);
    nerr = calloc(1, sizeof(*nerr));
    if (!nerr) {
        free(nmsg);
        return err;
    }
    nerr->code = code ? code : err->code;
    hadoop_err_free(err);
    nerr->malloced = 1;
    nerr->msg = nmsg;
    return nerr;
}

struct hadoop_err *hadoop_err_copy(const struct hadoop_err *err)
{
    struct hadoop_err *nerr;

    if (!err->malloced) {
        return (struct hadoop_err*)err;
    }
    nerr = malloc(sizeof(*nerr));
    if (!nerr) {
        return (struct hadoop_err*)&HADOOP_OOM_ERR;
    }
    nerr->code = err->code;
    nerr->msg = strdup(err->msg);
    nerr->malloced = 1;
    if (!nerr->msg) {
        free(nerr);
        return (struct hadoop_err*)&HADOOP_OOM_ERR;
    }
    return nerr;
}

const char* terror(int errnum)
{
    if ((errnum < 0) || (errnum >= sys_nerr)) {
        return "unknown error.";
    }
    return sys_errlist[errnum];
}

// vim: ts=4:sw=4:tw=79:et
