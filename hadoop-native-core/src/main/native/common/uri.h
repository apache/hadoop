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

#ifndef HADOOP_CORE_COMMON_URI
#define HADOOP_CORE_COMMON_URI

#include <inttypes.h>
#include <uriparser/Uri.h>

struct hadoop_uri {
    /** The text we parsed to create this URI. */
    char *text;

    /** URI structure. */
    UriUriA uri;

    /** URI Scheme or NULL. */
    char *scheme;

    /** User info or NULL. */
    char *user_info;

    /** Authority or NULL. */
    char *auth;

    /** URI Port. */
    uint16_t port;

    /** URI path. */
    char *path;
};

#define H_URI_APPEND_SLASH              0x01
#define H_URI_PARSE_SCHEME              0x02
#define H_URI_PARSE_USER_INFO           0x04
#define H_URI_PARSE_AUTH                0x08
#define H_URI_PARSE_PORT                0x10
#define H_URI_PARSE_PATH                0x20
#define H_URI_PARSE_ALL \
        (H_URI_PARSE_SCHEME | \
        H_URI_PARSE_USER_INFO | \
        H_URI_PARSE_AUTH | \
        H_URI_PARSE_PORT | \
        H_URI_PARSE_PATH)

/**
 * Parse a Hadoop URI.
 *
 * @param text          The text to parse.
 * @param base          If non-NULL, the URI to use as a base if the URI we're
 *                          parsing is relative.
 * @param out           (out param) The URI.
 * @param flags         Parse flags.
 */
struct hadoop_err *hadoop_uri_parse(const char *text,
                struct hadoop_uri *base, struct hadoop_uri **out, int flags);

/**
 * Dynamically allocate a string representing the hadoop_uri.
 *
 * @param uri           The URI.
 * @param out           (out param)
 */
struct hadoop_err *hadoop_uri_to_str(const struct hadoop_uri *uri, char **out);

/**
 * Free a Hadoop URI.
 *
 * @param uri           The URI to free.
 */
void hadoop_uri_free(struct hadoop_uri *uri);

/**
 * Print a URI to a file.
 *
 * @param fp            The FILE*
 * @param prefix        Prefix string to print out first.
 * @param uri           The URI to print.
 */
void hadoop_uri_print(FILE *fp, const char *prefix,
                      const struct hadoop_uri *uri);

#endif

// vim: ts=4:sw=4:tw=79:et
