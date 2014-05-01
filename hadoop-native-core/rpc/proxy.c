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
#include "protobuf/ProtobufRpcEngine.pb-c.h.s"
#include "rpc/call.h"
#include "rpc/messenger.h"
#include "rpc/proxy.h"
#include "rpc/varint.h"

#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#define proxy_log_warn(proxy, fmt, ...) \
    fprintf(stderr, "WARN: proxy 0x%p: " fmt, proxy, __VA_ARGS__)
#define proxy_log_info(proxy, fmt, ...) \
    fprintf(stderr, "INFO: proxy 0x%p: " fmt, proxy, __VA_ARGS__)
#define proxy_log_debug(proxy, fmt, ...) \
    fprintf(stderr, "DEBUG: proxy 0x%p: " fmt, proxy, __VA_ARGS__)

/**
 * The maximum length that we'll allocate to hold a request to the server.
 * This number includes the RequestHeader, but not the RpcRequestHeader.
 */
#define MAX_SEND_LEN (63 * 1024 * 1024)

struct hrpc_proxy {
    /**
     * The messenger that this proxy is associated with.
     */
    struct hrpc_messenger *msgr;

    /**
     * Dynamically allocated string describing the protocol this proxy speaks. 
     */
    char *protocol;

    /**
     * The current call.
     */
    struct hrpc_call call;

    /**
     * A memory area which can be used by the current call.
     *
     * This will be null if userdata_len is 0.
     */
    uint8_t *userdata;

    /**
     * Length of userdata.
     */
    size_t userdata_len;
};

struct hrpc_proxy_builder {
    struct hrpc_proxy *proxy;
};

static const char OOM_ERROR[] = "OOM";

struct hrpc_proxy_builder *hrpc_proxy_builder_alloc(
            struct hrpc_messenger *msgr)
{
    struct hrpc_proxy_builder *bld;

    bld = calloc(1, sizeof(struct hrpc_proxy_builder));
    if (!bld)
        return NULL;
    bld->proxy = calloc(1, sizeof(struct hrpc_proxy));
    if (!bld->proxy) {
        free(bld);
        return NULL;
    }
    bld->proxy->msgr = msgr;
    bld->proxy->call.remote.sin_addr.s_addr = INADDR_ANY;

    return bld;
}

void hrpc_proxy_builder_free(struct hrpc_proxy_builder *bld)
{
    if (!bld)
        return;
    free(bld->proxy);
    free(bld);
}

void hrpc_proxy_builder_set_protocol(struct hrpc_proxy_builder *bld,
                                     const char *protocol)
{
    struct hrpc_proxy *proxy = bld->proxy;

    if (proxy->protocol) {
        if (proxy->protocol != OOM_ERROR) {
            free(proxy->protocol);
        }
        proxy->protocol = NULL;
    }
    proxy->protocol = strdup(protocol);
    if (!proxy->protocol) {
        proxy->protocol = (char*)OOM_ERROR;
    }
}

void hrpc_proxy_builder_set_remote(struct hrpc_proxy_builder *bld,
                                     const struct sockaddr_in *remote)
{
    bld->proxy->call.remote = *remote;
}

struct hadoop_err *hrpc_proxy_create(struct hrpc_proxy_builder *bld,
                            struct hrpc_proxy **out)
{
    struct hrpc_proxy *proxy;
    
    proxy = bld->proxy;
    free(bld);
    //fprintf(stderr, "proxy = %p, proxy->protocol = %s, proxy->call.cb = %p\n", proxy, proxy->protocol, proxy->call.cb);
    if (proxy->call.remote.sin_addr.s_addr == INADDR_ANY) {
        hrpc_proxy_free(proxy);
        return hadoop_lerr_alloc(EINVAL, "hrpc_proxy_create: you must specify "
                                 "a remote.");
    }
    if (!proxy->protocol) {
        hrpc_proxy_free(proxy);
        return hadoop_lerr_alloc(EINVAL, "hrpc_proxy_create: can't create "
                                 "a proxy without a protocol argument.");
    } else if (proxy->protocol == OOM_ERROR) {
        // There was an OOM error during hrpc_proxy_builder_set_protocol.
        hrpc_proxy_free(proxy);
        return hadoop_lerr_alloc(ENOMEM, "hrpc_proxy_create: OOM error.");
    }
    *out = proxy;
    return NULL;
}

void hrpc_proxy_free(struct hrpc_proxy *proxy)
{
    if (!proxy)
        return;
    if (hrpc_call_is_active(&proxy->call)) {
        proxy_log_warn(proxy, "%s", "hrpc_proxy_free: attempt to free a proxy "
                       "which is currently active!\n");
        return;
    }
    if (proxy->protocol != OOM_ERROR) {
        free(proxy->protocol);
    }
    free(proxy->userdata);
    free(proxy);
}

struct hadoop_err *hrpc_proxy_activate(struct hrpc_proxy *proxy)
{
    struct hadoop_err *err;

    if (!hrpc_call_activate(&proxy->call)) {
        err = hadoop_lerr_alloc(EINVAL, "tried to start a call on a "
                    "proxy which was still in use by another call.");
        proxy_log_warn(proxy, "hrpc_proxy_activate: %s",
                       hadoop_err_msg(err));
        return err;
    }
    return NULL;
}

void hrpc_proxy_deactivate(struct hrpc_proxy *proxy)
{
    hrpc_call_deactivate(&proxy->call);
}

void *hrpc_proxy_alloc_userdata(struct hrpc_proxy *proxy, size_t size)
{
    if (size > proxy->userdata_len) {
        uint8_t *new_userdata = realloc(proxy->userdata, size);
        if (!new_userdata) {
            return NULL;
        }
        proxy->userdata = new_userdata;
        proxy->userdata_len = size;
    }
    return proxy->userdata;
}

struct hrpc_sync_ctx *hrpc_proxy_alloc_sync_ctx(struct hrpc_proxy *proxy)
{
    struct hrpc_sync_ctx *ctx = 
        hrpc_proxy_alloc_userdata(proxy, sizeof(struct hrpc_proxy));
    if (!ctx) {
        return NULL;
    }
    if (uv_sem_init(&ctx->sem, 0)) {
        return NULL;
    }
    memset(&ctx, 0, sizeof(ctx));
    return ctx;
}

void hrpc_free_sync_ctx(struct hrpc_sync_ctx *ctx)
{
    free(ctx->resp.base);
    uv_sem_destroy(&ctx->sem);
}

void hrpc_proxy_sync_cb(struct hrpc_response *resp, struct hadoop_err *err,
                        void *cb_data)
{
    struct hrpc_sync_ctx *ctx = cb_data;
    ctx->resp = *resp;
    ctx->err = err;
    uv_sem_post(&ctx->sem);
}

void hrpc_proxy_start(struct hrpc_proxy *proxy,
        const char *method, const void *payload, int payload_packed_len,
        hrpc_pack_cb_t payload_pack_cb,
        hrpc_raw_cb_t cb, void *cb_data)
{
    RequestHeaderProto req_header = REQUEST_HEADER_PROTO__INIT;
    uint64_t buf_len;
    int32_t req_header_len, off = 0;
    uint8_t *buf;
    struct hrpc_call *call = &proxy->call;

    call->cb = cb;
    call->cb_data = cb_data;
    call->protocol = strdup(proxy->protocol);
    if (!call->protocol) {
        hrpc_call_deliver_err(call, hadoop_lerr_alloc(ENOMEM,
                "hrpc_proxy_start_internal: out of memory"));
        return;
    }

    req_header.methodname = (char*)method;
    req_header.declaringclassprotocolname = proxy->protocol;
    req_header.clientprotocolversion = 1;
    req_header_len = request_header_proto__get_packed_size(&req_header);
    buf_len = varint32_size(req_header_len);
    buf_len += req_header_len;
    buf_len += varint32_size(payload_packed_len);
    buf_len += payload_packed_len;
    if (buf_len >= MAX_SEND_LEN) {
        hrpc_call_deliver_err(call,
            hadoop_lerr_alloc(EINVAL, "hrpc_proxy_setup_header: the "
                "request length is too long at %"PRId64 " bytes.  The "
                "maximum we will send is %d bytes.", buf_len, MAX_SEND_LEN));
        return;
    }
    buf = malloc((size_t)buf_len);
    if (!buf) {
        hrpc_call_deliver_err(call, 
            hadoop_lerr_alloc(ENOMEM, "hrpc_proxy_setup_header: "
                "failed to allocate a buffer of length %"PRId64" bytes.",
                buf_len));
        return;
    }
    varint32_encode(req_header_len, buf, buf_len, &off);
    request_header_proto__pack(&req_header, buf + off);
    off += req_header_len;
    varint32_encode(payload_packed_len, buf, buf_len, &off);
    payload_pack_cb(payload, buf + off);

    call->payload = uv_buf_init((char*)buf, buf_len);
    hrpc_messenger_start_outbound(proxy->msgr, &proxy->call);
}

// vim: ts=4:sw=4:tw=79:et
