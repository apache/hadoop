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

#include "rpc/call.h"
#include "rpc/proxy.h"

#include <stdlib.h>
#include <string.h>
#include <uv.h>

void hrpc_call_deliver_err(struct hrpc_call *call, struct hadoop_err *err)
{
    hrpc_raw_cb_t cb = call->cb;
    void *cb_data = call->cb_data;
    free(call->payload.base);
    memset(call, 0, sizeof(*call));
    __sync_fetch_and_or(&call->active, 0);
    cb(NULL, err, cb_data);
}

void hrpc_call_deliver_resp(struct hrpc_call *call, struct hrpc_response *resp)
{
    hrpc_raw_cb_t cb = call->cb;
    void *cb_data = call->cb_data;
    free(call->payload.base);
    memset(call, 0, sizeof(*call));
    __sync_fetch_and_or(&call->active, 0);
    cb(resp, NULL, cb_data);
}

int hrpc_call_activate(struct hrpc_call *call)
{
    return !!__sync_bool_compare_and_swap(&call->active, 0, 1);
}

void hrpc_call_deactivate(struct hrpc_call *call)
{
    __sync_fetch_and_and(&call->active, 0);
}

int hrpc_call_is_active(const struct hrpc_call *call)
{
    return !!__sync_fetch_and_or((int*)&call->active, 0);
}

// vim: ts=4:sw=4:tw=79:et
