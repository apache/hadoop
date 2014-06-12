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

#ifndef HADOOP_CORE_RPC_PROXY_H
#define HADOOP_CORE_RPC_PROXY_H

#include "rpc/call.h"

#include <stdint.h> /* for uint8_t */
#include <uv.h> /* for uv_buf_t */

struct hadoop_err;
struct hrpc_messenger;

struct hrpc_response {
    uint8_t *pb_base;
    int pb_len;
    void *base;
};

struct hrpc_sync_ctx {
    uv_sem_t sem;
    struct hadoop_err *err;
    struct hrpc_response resp;
};

typedef size_t (*hrpc_pack_cb_t)(const void *, uint8_t *);

#define RPC_PROXY_USERDATA_MAX 64

struct hrpc_proxy {
    /**
     * The messenger that this proxy is associated with.
     */
    struct hrpc_messenger *msgr;

    /**
     * String describing the protocol this proxy speaks. 
     */
    const char *protocol;

    /**
     * String describing the username this proxy uses. 
     */
    const char *username;

    /**
     * The current call.
     */
    struct hrpc_call call;

    /**
     * A memory area which can be used by the current call.
     *
     * This will be null if userdata_len is 0.
     */
    uint8_t userdata[RPC_PROXY_USERDATA_MAX];
};

/**
 * Initialize a Hadoop proxy.
 *
 * @param proxy     The Hadoop proxy to initialize.
 * @param msgr      The messenger to associate the proxy with.
 *                      This messenger must not be de-allocated while the proxy
 *                      still exists.
 * @param protocol  The protocol to use for this proxy.
 *                      This string must remain valid for the lifetime of the
 *                      proxy.
 * @param remote    The remote to contact.  Will be copied.
 * @param username  The username to use.
 *                      This string must remain valid for the lifetime of the
 *                      proxy.
 */
void hrpc_proxy_init(struct hrpc_proxy *proxy,
            struct hrpc_messenger *msgr,
            const struct sockaddr_in *remote,
            const char *protocol, const char *username);

/**
 * Mark the proxy as active.
 *
 * @param proxy                 The proxy
 *
 * @return                      NULL on success.  If the proxy is already
 *                              active, an error will be returned. 
 */
struct hadoop_err *hrpc_proxy_activate(struct hrpc_proxy *proxy);

/**
 * Mark the proxy as inactive.
 *
 * This function should not be called after hrpc_proxy_start, since a proxy
 * that has been started will mark itself as inactive when appropriate.
 *
 * @param proxy                 The proxy.
 */
void hrpc_proxy_deactivate(struct hrpc_proxy *proxy);

/**
 * Allocate some data in the proxy's userdata area.
 *
 * This will overwrite anything previously allocated in the proxy's userdata
 * area.  It is not necessary to free this memory later; it will be freed when
 * the proxy is freed.
 *
 * @param proxy                 The proxy
 *
 * @return                      NULL on OOM; a pointer to the userdata
 *                              otherwise.
 */
void *hrpc_proxy_alloc_userdata(struct hrpc_proxy *proxy, size_t size);

/**
 * Allocate a sync context from a proxy via hrpc_proxy_alloc_userdata.
 *
 * @param proxy                 The proxy
 *
 * @return                      NULL on OOM; the sync context otherwise.
 */
struct hrpc_sync_ctx *hrpc_proxy_alloc_sync_ctx(struct hrpc_proxy *proxy);

/**
 * Release the memory associated with a sync context.  This doesn't free the
 * context object itself.
 *
 * @param proxy                 The sync context.
 */
void hrpc_release_sync_ctx(struct hrpc_sync_ctx *ctx);

/**
 * A callback which synchronous RPCs can use.
 */
void hrpc_proxy_sync_cb(struct hrpc_response *resp, struct hadoop_err *err,
                        void *cb_data);

/**
 * Start an outgoing RPC from the proxy.
 *
 * This method will return after queuing up the RPC to be sent.
 *
 * Note: after the proxy has been started, you may __not__ de-allocate the
 * proxy until the callback has happened.
 *
 * @param proxy                 The Hadoop proxy to use.  A single proxy can
 *                                  only make one call at once.
 * @param method                The method we're calling.
 * @param payload               The protobuf message we're sending.
 * @param payload_packed_len    Length of payload when serialized.
 * @param payload_pack_cb       Function used to pack the payload.
 * @param cb                    Callback invoked when the message is done.
 * @param cb_data               Data provided along with cb.
 */
void hrpc_proxy_start(struct hrpc_proxy *proxy,
        const char *method, const void *payload, int payload_packed_len,
        hrpc_pack_cb_t payload_pack_cb,
        hrpc_raw_cb_t cb, void *cb_data);

#endif

// vim: ts=4:sw=4:tw=79:et
