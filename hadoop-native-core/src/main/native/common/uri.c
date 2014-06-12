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
#include "common/uri.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static struct hadoop_err *uri_err_to_hadoop_err(int err)
{
    switch (err) {
    case URI_SUCCESS:
        return NULL;
    case URI_ERROR_SYNTAX:
        return hadoop_lerr_alloc(EINVAL, "invalid URI format");
    case URI_ERROR_NULL:
        return hadoop_lerr_alloc(EINVAL, "unexpected NULL pointer "
                                 "passed as parameter to uriparse");
    case URI_ERROR_MALLOC:
        return hadoop_lerr_alloc(ENOMEM, "out of memory");
    case URI_ERROR_OUTPUT_TOO_LARGE:
        return hadoop_lerr_alloc(ENAMETOOLONG, "data too big for uriparse "
                                 "buffer");
    case URI_ERROR_NOT_IMPLEMENTED:
        return hadoop_lerr_alloc(ENOTSUP, "uriparse function not "
                                 "implemented.");
    case URI_ERROR_ADDBASE_REL_BASE:
        return hadoop_lerr_alloc(EINVAL, "given add base is not absolute");
    case URI_ERROR_REMOVEBASE_REL_BASE:
        return hadoop_lerr_alloc(EINVAL, "given remove base is not absolute");
    case URI_ERROR_REMOVEBASE_REL_SOURCE:
        return hadoop_lerr_alloc(EINVAL, "given remove source is not "
                                 "absolute");
    case URI_ERROR_RANGE_INVALID:
        return hadoop_lerr_alloc(ERANGE, "invalid range in uriparse.");
    default:
        return hadoop_lerr_alloc(EIO, "unknown uri error.");
    }
}

struct hadoop_err *uri_parse_abs(const char *str, UriParserStateA *state,
            UriUriA *uri, const char *def_scheme)
{
    int ret;
    struct hadoop_err *err = NULL;
    size_t str_len;
    const char *effective_str = NULL;
    char *malloced_str = NULL, *nmalloced_str;

    // If the URI doesn't end with a slash, append one.
    // This is necessary to get AddBaseUri to act like we expect when using
    // this absolute URI as a base.
    state->uri = NULL;
    str_len = strlen(str);
    if ((str_len == 0) || (str[str_len - 1] != '/')) {
        if (asprintf(&malloced_str, "%s/", str) < 0) {
            err = hadoop_lerr_alloc(ENOMEM, "uri_parse_abs: OOM");
            malloced_str = NULL;
            goto done;
        }
        effective_str = malloced_str;
    } else {
        effective_str = str;
    }
    state->uri = uri;
    ret = uriParseUriA(state, effective_str);
    if (ret) {
        state->uri = NULL;
        err = hadoop_err_prepend(uri_err_to_hadoop_err(ret),
            0, "uri_parse: failed to parse '%s' as URI",
            effective_str);
        goto done;
    }
    if (uri->scheme.first == NULL) {
        // If the URI doesn't have a scheme, prepend the default one to the
        // string, and re-parse.  This is necessary because AddBaseUri refuses
        // to rebase URIs on absolute URIs without a scheme.
        if (asprintf(&nmalloced_str, "%s://%s", def_scheme,
                     effective_str) < 0) {
            err = hadoop_lerr_alloc(ENOMEM, "uri_parse_abs: OOM");
            goto done;
        }
        free(malloced_str);
        malloced_str = nmalloced_str;
        effective_str = malloced_str;
        uriFreeUriMembersA(uri);
        state->uri = uri;
        ret = uriParseUriA(state, effective_str);
        if (ret) {
            state->uri = NULL;
            err = hadoop_err_prepend(uri_err_to_hadoop_err(ret),
                0, "uri_parse: failed to parse '%s' as URI",
                effective_str);
            goto done;
        }
    }
    err = NULL;
done:
    if (err) {
        if (state->uri) {
            uriFreeUriMembersA(state->uri);
            state->uri = NULL;
        }
    }
    return err;
}

struct hadoop_err *uri_parse(const char *str, UriParserStateA *state,
            UriUriA *uri, UriUriA *base_uri)
{
    int ret;
    struct hadoop_err *err = NULL;
    UriUriA first_uri;

    state->uri = &first_uri;
    ret = uriParseUriA(state, str);
    if (ret) {
        state->uri = NULL;
        err = hadoop_err_prepend(uri_err_to_hadoop_err(ret),
            0, "uri_parse: failed to parse '%s' as a URI", str);
        goto done;
    }
//    fprintf(stderr, "str=%s, base_path=%s, base_uri->absolutePath=%d\n",
//            str, base_path, base_uri.absolutePath);
//        fprintf(stderr, "uriAddBaseUriA base_path=%s, str=%s, ret %d\n", base_path, str, ret); 
    ret = uriAddBaseUriA(uri, &first_uri, base_uri);
    if (ret) {
        err = hadoop_err_prepend(uri_err_to_hadoop_err(ret),
            0, "uri_parse: failed to add base URI");
        goto done;
    }
    uriFreeUriMembersA(&first_uri);
    state->uri = uri;
    ret = uriNormalizeSyntaxA(uri);
    if (ret) {
        err = hadoop_err_prepend(uri_err_to_hadoop_err(ret),
            0, "uri_parse: failed to normalize URI");
        goto done;
    }
done:
    if (err) {
        if (state->uri) {
            uriFreeUriMembersA(uri);
            state->uri = NULL;
        }
    }
    return err;
}

static struct hadoop_err *text_range_to_str(struct UriTextRangeStructA *text,
                                            char **out, const char *def)
{
    struct hadoop_err *err = NULL;
    char *str = NULL;
    const char *c;
    size_t len = 0;

    if (!text->first) {
        str = strdup(def);
        if (!str) {
            err = hadoop_lerr_alloc(ENOMEM, "text_range_to_str: out of memory "
                "trying to allocate a %zd-byte default string.",
                strlen(def) + 1);
        }
        goto done;
    }
    for (c = text->first; c != text->afterLast; c++) {
        ++len;
    }
    str = malloc(len + 1);
    if (!str) {
        err = hadoop_lerr_alloc(ENOMEM, "text_range_to_str: out of memory "
            "trying to allocate a %zd-byte string.", len + 1);
        goto done;
    }
    memcpy(str, text->first, len);
    str[len] = '\0';
    err = NULL;

done:
    if (err) {
        free(str);
        return err;
    }
    *out = str;
    return NULL;
}

struct hadoop_err *uri_get_scheme(UriUriA *uri, char **out)
{
    struct hadoop_err *err;
    char *scheme = NULL;

    err = text_range_to_str(&uri->scheme, &scheme, "");
    if (err)
        return err;
    *out = scheme;
    return NULL;
}

struct hadoop_err *uri_get_user_info(UriUriA *uri, char **user_info)
{
    return text_range_to_str(&uri->userInfo, user_info, "");
}

struct hadoop_err *uri_get_authority(UriUriA *uri, char **authority)
{
    return text_range_to_str(&uri->hostText, authority, "");
}

struct hadoop_err *uri_get_port(UriUriA *uri, uint16_t *out)
{
    struct hadoop_err *err;
    char *port_str = NULL;
    int port;

    err = text_range_to_str(&uri->portText, &port_str, "");
    if (err)
        return err;
    port = atoi(port_str);
    free(port_str);
    if (port < 0 || port > 0xffff) {
        return hadoop_lerr_alloc(EINVAL, "uri_get_port: invalid "
                                 "port number %d\n", port);
    }
    *out = port;
    return NULL;
}

struct hadoop_err *uri_get_path(UriUriA *uri, char **out)
{
    struct UriPathSegmentStructA *cur;
    size_t i = 0, path_len = 0;
    char *path = NULL;
    int absolute = 0;

    if (uri->absolutePath) {
        absolute = 1;
    } else if (uri->pathHead && uri->scheme.first) {
        // Hadoop treats all URIs with a path as absolute, if they have a
        // non-empty path.
        // So hdfs://mynamenode/ maps to the root path, for example.  But as a
        // special case, hdfs://mynamenode (no terminating slash) maps to "."
        absolute = 1;
    }
    // The URI parser library splits paths up into lots of PathSegment
    // structures-- one per path segment.  We need to reconstruct the full
    // path.  The first step is figuring out the upper bound on the path
    // length.
    for (cur = uri->pathHead; cur; cur = cur->next) {
        const char *c;
        path_len++; // +1 for the leading slash.
        for (c = cur->text.first; c != cur->text.afterLast; c++) {
            path_len++;
        }
    }
    path = malloc(path_len + 1); // +1 for the NULL terminator
    if (!path) {
        return hadoop_lerr_alloc(ENOMEM, "uri_get_path: OOM copying "
                                 "%zd byte path.", path_len);
    }
    // The next step is copying over the path.
    for (cur = uri->pathHead; cur; cur = cur->next) {
        const char *c;
        size_t copy_len = 0;
        if ((i != 0) || absolute) {
            path[i++] = '/';
        }
        for (c = cur->text.first; c != cur->text.afterLast; c++) {
            copy_len++;
        }
        memcpy(path + i, cur->text.first, copy_len);
        i += copy_len;
    }
    path[i] = '\0';
    *out = path;
    return NULL;
}

// vim: ts=4:sw=4:tw=79:et
