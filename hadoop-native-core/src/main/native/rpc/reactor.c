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
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#define reactor_log_warn(reactor, fmt, ...) \
    fprintf(stderr, "WARN: reactor %s: " fmt, \
            reactor->name, __VA_ARGS__)
#define reactor_log_info(msgr, fmt, ...) \
    fprintf(stderr, "INFO: reactor %s: " fmt, \
            reactor->name, __VA_ARGS__)
#define reactor_log_debug(msgr, fmt, ...) \
    fprintf(stderr, "DEBUG: reactor %s: " fmt, \
            reactor->name, __VA_ARGS__)

RB_GENERATE(hrpc_conns, hrpc_conn, entry, hrpc_conn_compare);

/**
 * Historically, UNIX has delivered a SIGPIPE to threads that write to a socket
 * which has been disconnected by the other end (the "broken pipe" error.)
 * We don't want this signal.  We would rather handle this as part of the
 * normal control flow.
 *
 * It would be easy to ignore SIGPIPE for the whole process by setting a
 * different signal handler (such as SIG_IGN).  But this could have unforseen
 * effects on the process running our library.
 *
 * Some operating systems provide variants of the sendto() and send() functions
 * (or optional arguments to those functions) that suppress SIGPIPE only for
 * that invocation of the function.  But not all OSes provide this, and not all
 * versions of libuv use those arguments anyway.
 *
 * So what can we do?  Well, since each reactor runs in its own special thread
 * which is not shared with the rest of the application, we simply set the
 * "signal mask" so that SIGPIPE is ignored for the reactor thread.  Since
 * SIGPIPE is delivered only to these specific threads, this fixes the issue.
 */
static void block_sigpipe(struct hrpc_reactor *reactor)
{
    sigset_t sig_ign;
    int ret;

    if (sigemptyset(&sig_ign) || sigaddset(&sig_ign, SIGPIPE)) {
        reactor_log_warn(reactor, "%s",
            "suppress_sigpipe: failed to set up signal set for SIGPIPE.\n");
        return;
    }
    ret = pthread_sigmask(SIG_BLOCK, &sig_ign, NULL);
    if (ret) {
        reactor_log_warn(reactor, "suppress_sigpipe: pthread_sigmask "
                         "failed with error %d\n", ret);
        return;
    }
}

static void reactor_thread_run(void *arg)
{
    struct hrpc_reactor *reactor = arg;

    reactor_log_debug(reactor, "%s", "reactor thread starting.\n");
    block_sigpipe(reactor);
    uv_run(&reactor->loop, UV_RUN_DEFAULT);
    reactor_log_debug(reactor, "%s", "reactor thread terminating.\n");
    if (uv_loop_close(&reactor->loop)) {
        reactor_log_warn(reactor, "%s",
                "uv_loop_close: failed to close the loop.");
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
    exemplar.username = (char*)call->username;
    conn = RB_NFIND(hrpc_conns, &reactor->conns, &exemplar);
    if (!conn) {
        return NULL;
    }
    if (hrpc_conn_usable(conn, remote, call->protocol, call->username)) {
        if (conn->writer.state == HRPC_CONN_WRITE_IDLE) {
            RB_REMOVE(hrpc_conns, &reactor->conns, conn);
            return conn;
        }
    }
    return NULL;
}

void reactor_finish_shutdown(uv_handle_t *handle)
{
    struct hrpc_reactor *reactor = handle->data;

    // Note: other callbacks may still run after the libuv loop has been
    // stopped.  But we won't block for I/O after this point.
    uv_stop(&reactor->loop);
}

static void reactor_begin_shutdown(struct hrpc_reactor *reactor,
                             struct hrpc_calls *pending_calls)
{
    struct hrpc_conn *conn, *conn_tmp;
    struct hrpc_call *call;

    reactor_log_info(reactor, "%s", "reactor_begin_shutdown\n");
    STAILQ_FOREACH(call, pending_calls, entry) {
        hrpc_call_deliver_err(call, hadoop_lerr_alloc(ESHUTDOWN,
            "the reactor is being shut down."));
    }
    RB_FOREACH_SAFE(conn, hrpc_conns, &reactor->conns, conn_tmp) {
        hrpc_conn_destroy(conn, hadoop_lerr_alloc(ESHUTDOWN,
            "the reactor is being shut down."));
    }
    uv_close((uv_handle_t*)&reactor->inbox.notifier, NULL);
}

static void reactor_async_start_outbound(struct hrpc_reactor *reactor,
                                         struct hrpc_call *call)
{
    struct hrpc_conn *conn;
    struct hadoop_err *err;

    conn = reuse_idle_conn(reactor, call->remote, call);
    if (conn) {
        hrpc_conn_restart_outbound(conn, call);
        reactor_log_debug(reactor, "start_outbound: reused "
                       "connection %s\n", conn->name);
    } else {
        char remote_str[64], conn_name[128];

        net_ipv4_name(call->remote, remote_str, sizeof(remote_str));
        snprintf(conn_name, sizeof(conn_name), "%s-%s-%"PRId64,
                 reactor->name, remote_str, reactor->conns_created++);
        err = hrpc_conn_create_outbound(reactor, call, conn_name, &conn);
        if (err) {
            reactor_log_warn(reactor, "reactor_async_start_outbound("
                "remote=%s) got error %s\n",
                net_ipv4_name(call->remote, remote_str, sizeof(remote_str)),
                hadoop_err_msg(err));
            hrpc_call_deliver_err(call, err);
            return;
        }
        reactor_log_debug(reactor, "start_outbound: created new "
                       "connection %s\n", conn->name);
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
    } else {
        STAILQ_FOREACH(call, &pending_calls, entry) {
            reactor_async_start_outbound(reactor, call);
        }
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

void reactor_insert_conn(struct hrpc_reactor *reactor, struct hrpc_conn *conn)
{
    RB_INSERT(hrpc_conns, &reactor->conns, conn);
}

struct hadoop_err *hrpc_reactor_create(struct hrpc_reactor **out,
                                       const char *name)
{
    struct hrpc_reactor *reactor = NULL;
    struct hadoop_err *err = NULL;
    int res;

    reactor = calloc(1, sizeof(struct hrpc_reactor) + strlen(name) + 1);
    if (!reactor) {
        err = hadoop_lerr_alloc(ENOMEM, "hrpc_reactor_create: OOM while allocating "
                                "reactor structure.");
        goto error_free_reactor;
    }
    strcpy(reactor->name, name);
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
