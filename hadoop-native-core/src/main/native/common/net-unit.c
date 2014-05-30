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

#include "common/net.h"
#include "common/test.h"

#include <stdlib.h>
#include <uv.h>
#include <stdio.h>
#include <string.h>

int main(void)
{
    struct sockaddr_in so;
    char remote_str[64];

    memset(&so,0,sizeof(so));
    so.sin_addr.s_addr = inet_addr("127.0.0.1");

    EXPECT_STR_EQ("127.0.0.1",net_ipv4_name(&so, remote_str, sizeof(remote_str)));
    EXPECT_STR_EQ("(uv_ip4_name error)",net_ipv4_name(&so, remote_str, 0));
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
