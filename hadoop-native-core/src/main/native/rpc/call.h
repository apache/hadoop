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

#ifndef HADOOP_CORE_RPC_CALL_H
#define HADOOP_CORE_RPC_CALL_H

#include "common/queue.h"

#include <netinet/in.h> // for struct sockaddr_in
#include <stdint.h> // for int32_t
#include <uv.h> // for uv_buf_t

struct hadoop_err;
struct hrpc_response;

/**
 * The Hadoop call.
 *
 * Note: this is an internal header which users of the RPC layer don't need to
 * include.
 */

typedef void (*hrpc_raw_cb_t)(struct hrpc_response *,
    struct hadoop_err *, void *);

/**
 * A Hadoop RPC call.
 */
struct hrpc_call {
    /**
     * Pointers used to put this call into an implicit linked list.
     */
    STAILQ_ENTRY(hrpc_call) entry;

    /**
     * Remote address we're sending to.
     */
    struct sockaddr_in *remote;

    /**
     * The callback to make from the reactor thread when this call completes or
     * hits an error.
     */
    hrpc_raw_cb_t cb;

    /**
     * The data to pass to the callback.
     */
    void *cb_data;

    /**
     * Malloc'ed payload to send.
     */
    uv_buf_t payload;

    /**
     * String describing the protocol this call is using.
     */
    const char *protocol;

    /**
     * String describing the username this call is using. 
     */
    const char *username;

    /**
     * Nonzero if the call is currently active.  Must be set using atomic
     * operations.
     */
    int active;
};

STAILQ_HEAD(hrpc_calls, hrpc_call);

/**
 * Activate this call.
 *
 * @param call          The call.
 *
 * @return              1 if the call was activated; 0 if it was already
 *                          active.
 */
int hrpc_call_activate(struct hrpc_call *call);

/**
 * Deactivate this call.
 *
 * @param call          The call.
 */
void hrpc_call_deactivate(struct hrpc_call *call);

/**
 * Determine if the call is active.
 *
 * @param call          The call.
 *
 * @return              1 if the call is active; 0 if it is not.
 */
int hrpc_call_is_active(const struct hrpc_call *call);

/**
 * Deliver an error message.  Will free call->payload and zero all fields.
 *
 * @param call          The call.
 * @param resp          The error to pass to the callback.  The callback
 *                        is responsible for freeing this error.
 */
void hrpc_call_deliver_err(struct hrpc_call *call, struct hadoop_err *err);

/**
 * Deliver a response.  Will free call->payload and zero all fields.
 *
 * @param call          The call.
 * @param resp          Malloc'ed data to pass to the callback.  The callback
 *                        is responsible for freeing this data.
 */
void hrpc_call_deliver_resp(struct hrpc_call *call,
                            struct hrpc_response *resp);

#endif

// vim: ts=4:sw=4:tw=79:et
