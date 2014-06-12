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

#include <uriparser/Uri.h>

/**
 * Parse an absolute URI.
 *
 * @param str           The string to parse.  If there is not a slash at the
 *                          end, one will be added.
 * @param state         (inout) The URI parser state to use.
 *                          On success, state->uri will be set to a non-NULL
 *                          value.
 * @param uri           (out param) The URI object to fill.
 * @param def_scheme    The default scheme to add if there is no scheme.
 *
 * @return              NULL on success; the URI parsing problem otherwise.
 */
struct hadoop_err *uri_parse_abs(const char *str, UriParserStateA *state,
            UriUriA *uri, const char *def_scheme);

/**
 * Parse a relative or absolute URI.
 *
 * @param str           The string to parse.
 * @param state         (inout) The URI parser state to use.
 *                          On success, state->uri will be set to a non-NULL
 *                          value.
 * @param uri           (out param) The URI object to fill.
 *
 * @return              NULL on success; the URI parsing problem otherwise.
 */
struct hadoop_err *uri_parse(const char *str, UriParserStateA *state,
            UriUriA *uri, UriUriA *base_uri);

/**
 * Get the scheme of a URI.
 *
 * We disallow schemes with non-ASCII characters.
 *
 * @param uri           The Uri object.
 * @param scheme        (out param) the scheme.
 *
 * @return              NULL on success; the URI parsing problem otherwise.
 */
struct hadoop_err *uri_get_scheme(UriUriA *uri, char **scheme);

/**
 * Get the user_info of a URI.
 *
 * @param uri           The Uri object.
 * @param user_info     (out param) the user_info.
 *
 * @return              NULL on success; the URI parsing problem otherwise.
 */
struct hadoop_err *uri_get_user_info(UriUriA *uri, char **user_info);

/**
 * Get the authority of a URI.
 *
 * @param uri           The Uri object.
 * @param authority     (out param) the authority.
 *
 * @return              NULL on success; the URI parsing problem otherwise.
 */
struct hadoop_err *uri_get_authority(UriUriA *uri, char **authority);

/**
 * Get the port of a URI.
 *
 * @param uri           The Uri object.
 * @param port          (out param) the port, or 0 if there was no port.
 *
 * @return              NULL on success; the URI parsing problem otherwise.
 */
struct hadoop_err *uri_get_port(UriUriA *uri, uint16_t *port);

/**
 * Get the path of a URI.
 *
 * @param uri       The Uri object.
 * @param path      (out param) the path.
 *
 * @return          NULL on success; the URI parsing problem otherwise.
 */
struct hadoop_err *uri_get_path(UriUriA *uri, char **path);

#endif

// vim: ts=4:sw=4:tw=79:et
