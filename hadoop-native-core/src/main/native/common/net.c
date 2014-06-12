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
#include "common/net.h"

#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <uv.h>

static const char * const NET_IPV4_NAME_ERROR = "(uv_ip4_name error)";

const char *net_ipv4_name(struct sockaddr_in *src, char *dst, size_t size)
{
  if (uv_ip4_name(src, dst, size) < 0) {
    return NET_IPV4_NAME_ERROR;
  }
  return dst;
}

const char *net_ipv4_name_and_port(struct sockaddr_in *src,
                                   char *dst, size_t size)
{
    size_t len;

    if (net_ipv4_name(src, dst, size) == NET_IPV4_NAME_ERROR)
        return NET_IPV4_NAME_ERROR;
    len = strlen(dst);
    snprintf(dst + len, size - len + 1, ":%d", 
             htons(src->sin_port));
    return dst;
}

struct hadoop_err *get_first_ipv4_addr(const char *hostname, uint32_t *out)
{
    struct hadoop_err *err = NULL;
    uint32_t addr = 0;
    int ret;
    struct addrinfo hints, *list, *info;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags |= AI_CANONNAME;
    ret = getaddrinfo(hostname, NULL, &hints, &list);
    if (ret) {
        if (ret == EAI_SYSTEM) {
            ret = errno;
            err = hadoop_lerr_alloc(ret, "getaddrinfo(%s): %s",
                                    hostname, terror(ret));
        } else {
            // TODO: gai_strerror is not thread-safe on Windows, need
            // workaround
            err = hadoop_lerr_alloc(ENOENT, "getaddrinfo(%s): %s",
                                    hostname, gai_strerror(ret));
        }
        list = NULL;
        goto done;
    }
    for (info = list; info; info = info->ai_next) {
        if (info->ai_family != AF_INET)
            continue;
        addr = ((struct sockaddr_in*)list->ai_addr)->sin_addr.s_addr;
        err = NULL;
        goto done;
    }
    err = hadoop_lerr_alloc(ENOENT, "getaddrinfo(%s): no IPv4 addresses "
                            "found for hostname.", hostname);
done:
    freeaddrinfo(list);
    *out = ntohl(addr);
    return err;
}

// vim: ts=4:sw=4:tw=79:et
