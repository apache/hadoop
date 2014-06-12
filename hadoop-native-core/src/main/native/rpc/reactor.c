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
#include "common/queue.h"
#include "common/tree.h"
#include "rpc/call.h"
#include "rpc/messenger.h"
#include "rpc/reactor.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#define reactor_log_warn(reactor, fmt, ...) \
    fprintf(stderr, "WARN: reactor %p: " fmt, reactor, __VA_ARGS__)
#define reactor_log_info(msgr, fmt, ...) \
    fprintf(stderr, "INFO: reactor %p: " fmt, reactor, __VA_ARGS__)
#define reactor_log_debug(msgr, fmt, ...) \
    fprintf(stderr, "DEBUG: reactor %p: " fmt, reactor, __VA_ARGS__)

RB_GENERATE(hrpc_conns, hrpc_conn, entry, hrpc_conn_compare);

static void reactor_thread_run(void *arg)
{
    struct hrpc_reactor *reactor = arg;
    struct hrpc_conn *conn, *conn_tmp;

    reactor_log_debug(reactor, "%s", "reactor thread starting.\n");
    uv_run(&reactor->loop, UV_RUN_DEFAULT);
    reactor_log_debug(reactor, "%s", "reactor thread terminating.\n");
    RB_FOREACH_SAFE(conn, hrpc_conns, &reactor->conns, conn_tmp) {
        hrpc_conn_destroy(conn, hadoop_lerr_alloc(ESHUTDOWN, 
            "hrpc_reactor_start_outbound: the reactor is being shut down."));
    }
}

/**
 * Find an idle connection with a given address in the idle connection map.
 *
 * @param reactor       The reactor.
 * @param remote        The remote address to find.
 */
static struct hrpc_conn *reuse_idle_conn(struct hrpc_reactor *reactor,
            const struct sockaddr_in *remote, const struct hrpc_call *call)
{
    struct hrpc_conn *conn;
    struct hrpc_conn exemplar;

    memset(&exemplar, 0, sizeof(exemplar));
    exemplar.remote = *remote;
    exemplar.protocol = (char*)call->protocol;
    conn = RB_NFIND(hrpc_conns, &reactor->conns, &exemplar);
    if (!conn)
        return NULL;
    if (hrpc_conn_usable(conn, remote, call->protocol, call->username)) {
        if (conn->writer.state == HRPC_CONN_WRITE_IDLE) {
            RB_REMOVE(hrpc_conns, &reactor->conns, conn);
            return conn;
        }
    }
    return NULL;
}

static void reactor_begin_shutdown(struct hrpc_reactor *reactor,
                             struct hrpc_calls *pending_calls)
{
    struct hrpc_call *call;

    reactor_log_debug(reactor, "%s", "reactor_begin_shutdown\n");
    STAILQ_FOREACH(call, pending_calls, entry) {
        hrpc_call_deliver_err(call, hadoop_lerr_alloc(ESHUTDOWN, 
            "hrpc_reactor_start_outbound: the reactor is being shut down."));
    }
    // Note: other callbacks may still run after the libuv loop has been
    // stopped.  But we won't block for I/O after this point.
    uv_stop(&reactor->loop);
}

static void reactor_async_start_outbound(struct hrpc_reactor *reactor,
                                         struct hrpc_call *call)
{
    char remote_str[64] = { 0 };
    struct hrpc_conn *conn;
    struct hadoop_err *err;

    conn = reuse_idle_conn(reactor, &call->remote, call);
    if (conn) {
        reactor_log_debug(reactor, "reactor_async_start_outbound(remote=%s) "
                          "assigning to connection %p\n",
                net_ipv4_name(&call->remote, remote_str, sizeof(remote_str)),
                conn);
        hrpc_conn_start_outbound(conn, call);
    } else {
        err = hrpc_conn_create_outbound(reactor, call, &conn);
        if (err) {
            reactor_log_warn(reactor, "reactor_async_start_outbound("
                "remote=%s) got error %s\n",
                net_ipv4_name(&call->remote, remote_str, sizeof(remote_str)),
                hadoop_err_msg(err));
            hrpc_call_deliver_err(call, err);
            return;
        }
        reactor_log_debug(reactor, "reactor_async_start_outbound("
                "remote=%s) created new connection %p\n",
                net_ipv4_name(&call->remote, remote_str, sizeof(remote_str)),
                conn);
    }
    // Add or re-add the connection to the reactor's tree.
    RB_INSERT(hrpc_conns, &reactor->conns, conn);
}

static void reactor_async_cb(uv_async_t *handle)
{
    struct hrpc_reactor *reactor = handle->data;
    int shutdown;
    struct hrpc_calls pending_calls = STAILQ_HEAD_INITIALIZER(pending_calls);
    struct hrpc_call *call;

    uv_mutex_lock(&reactor->inbox.lock);
    shutdown = reactor->inbox.shutdown;
    STAILQ_SWAP(&reactor->inbox.pending_calls, &pending_calls,
                hrpc_call);
    uv_mutex_unlock(&reactor->inbox.lock);

    if (shutdown) {
        reactor_begin_shutdown(reactor, &pending_calls);
        return;
    }
    STAILQ_FOREACH(call, &pending_calls, entry) {
        reactor_async_start_outbound(reactor, call);
    }
}

void reactor_remove_conn(struct hrpc_reactor *reactor, struct hrpc_conn *conn)
{
    struct hrpc_conn *removed;

    removed = RB_REMOVE(hrpc_conns, &reactor->conns, conn);
    if (!removed) {
        reactor_log_warn(reactor, "reactor_remove_conn("
            "conn=%p): no such connection found.\n", conn);
    }
}

struct hadoop_err *hrpc_reactor_create(struct hrpc_reactor **out)
{
    struct hrpc_reactor *reactor = NULL;
    struct hadoop_err *err = NULL;
    int res;

    reactor = calloc(1, sizeof(struct hrpc_reactor));
    if (!reactor) {
        err = hadoop_lerr_alloc(ENOMEM, "hrpc_reactor_create: OOM while allocating "
                                "reactor structure.");
        goto error_free_reactor;
    }
    if (uv_mutex_init(&reactor->inbox.lock) < 0) {
        err = hadoop_lerr_alloc(ENOMEM, "hrpc_reactor_create: failed to init "
                                "mutex.");
        goto error_free_reactor;
    }
    RB_INIT(&reactor->conns);
    STAILQ_INIT(&reactor->inbox.pending_calls);
    if (uv_loop_init(&reactor->loop)) {
        err = hadoop_lerr_alloc(ENOMEM, "hrpc_reactor_create: uv_loop_init "
                                "failed.");
        goto error_free_mutex;
    }
    res = uv_async_init(&reactor->loop, &reactor->inbox.notifier,
                      reactor_async_cb);
    if (res) {
        err = hadoop_uverr_alloc(res, "hrpc_reactor_create: "
                                 "uv_async_init failed.");
        goto error_close_loop;
    }
    reactor->inbox.notifier.data = reactor;
    res = uv_thread_create(&reactor->thread, reactor_thread_run, reactor);
    if (res) {
        err = hadoop_lerr_alloc(ENOMEM, "hrpc_reactor_create: "
                                "uv_thread_create failed.");
        goto error_free_async;
    }
    *out = reactor;
    return NULL;

error_free_async:
    uv_close((uv_handle_t*)&reactor->inbox.notifier, NULL);
error_close_loop:
    uv_loop_close(&reactor->loop);
error_free_mutex:
    uv_mutex_destroy(&reactor->inbox.lock);
error_free_reactor:
    free(reactor);
    return err;
}

void hrpc_reactor_shutdown(struct hrpc_reactor *reactor)
{
    reactor_log_debug(reactor, "%s", "hrpc_reactor_shutdown\n");
    uv_mutex_lock(&reactor->inbox.lock);
    reactor->inbox.shutdown = 1;
    uv_mutex_unlock(&reactor->inbox.lock);
    uv_async_send(&reactor->inbox.notifier);
    uv_thread_join(&reactor->thread);
}

void hrpc_reactor_free(struct hrpc_reactor *reactor)
{
    reactor_log_debug(reactor, "%s", "hrpc_reactor_free\n");
    uv_loop_close(&reactor->loop);
    uv_mutex_destroy(&reactor->inbox.lock);
    free(reactor);
}

void hrpc_reactor_start_outbound(struct hrpc_reactor *reactor,
                                 struct hrpc_call *call)
{
    int shutdown = 0;

    uv_mutex_lock(&reactor->inbox.lock);
    shutdown = reactor->inbox.shutdown;
    if (!shutdown) {
        STAILQ_INSERT_TAIL(&reactor->inbox.pending_calls, call, entry);
    }
    uv_mutex_unlock(&reactor->inbox.lock);
    if (shutdown) {
        hrpc_call_deliver_err(call, hadoop_lerr_alloc(ESHUTDOWN, 
            "hrpc_reactor_start_outbound: can't start call because the "
            "reactor has been shut down."));
    } else {
        uv_async_send(&reactor->inbox.notifier);
    }
}

// vim: ts=4:sw=4:tw=79:et
