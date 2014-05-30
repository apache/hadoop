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
#include "rpc/client_id.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void hrpc_client_id_generate_rand(struct hrpc_client_id *id)
{
    int i;

    for (i = 0; i <= HRPC_CLIENT_ID_LEN - 3; i += 3) {
        long int r = lrand48();
        id->buf[i + 0] = r & 0xff;
        id->buf[i + 1] = (r >> 8) & 0xff;
        id->buf[i + 2] = (r >> 16) & 0xff;
    }
    id->buf[HRPC_CLIENT_ID_LEN - 2] = lrand48() & 0xff;
    id->buf[HRPC_CLIENT_ID_LEN - 1] = lrand48() & 0xff;
}

struct hadoop_err *hrpc_client_id_from_bytes(void *bytes, int len,
                                             struct hrpc_client_id *out)
{
    if (len != HRPC_CLIENT_ID_LEN) {
        return hadoop_lerr_alloc(EINVAL, "hrpc_client_id_from_bytes: "
                "invalid client id length of %d (expected %d)",
                len, HRPC_CLIENT_ID_LEN);
    }
    memcpy(out->buf, bytes, len);
    return NULL;
}

const char *hrpc_client_id_to_str(const struct hrpc_client_id *id,
                           char *str, size_t str_len)
{
    snprintf(str, str_len, 
             "%02x%02x%02x%02x-%02x%02x%02x%02x-%02x%02x%02x%02x-%02x%02x%02x%02x",
             id->buf[0], id->buf[1], id->buf[2], id->buf[3],
             id->buf[4], id->buf[5], id->buf[6], id->buf[7],
             id->buf[8], id->buf[9], id->buf[10], id->buf[11],
             id->buf[12], id->buf[13], id->buf[14], id->buf[15]);
    return str;
}

int hrpc_client_id_compare(const struct hrpc_client_id *a,
                            const struct hrpc_client_id *b)
{
    return memcmp(a->buf, b->buf, HRPC_CLIENT_ID_LEN);
}

// vim: ts=4:sw=4:tw=79:et
