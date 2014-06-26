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

#ifndef HADOOP_CORE_RPC_REACTOR_H
#define HADOOP_CORE_RPC_REACTOR_H

#include "common/tree.h"
#include "rpc/call.h" // for hrpc_call
#include "rpc/conn.h" // for hrpc_conn_compare

#include <stdint.h>
#include <uv.h>

/**
 * The Hadoop reactor thread header.
 *
 * Note: this is an internal header which users of the RPC layer don't need to
 * include.
 */

RB_HEAD(hrpc_conns, hrpc_conn);
RB_PROTOTYPE(hrpc_conns, hrpc_conn, entry, hrpc_conn_compare);

struct hrpc_reactor_inbox {
    /**
     * Lock which protects the inbox.
     */
    uv_mutex_t lock;

    /**
     * Non-zero if the reactor should shut down.
     */
    int shutdown;

    /**
     * Calls which we have been asked to make.
     */
    struct hrpc_calls pending_calls;

    /**
     * Used to trigger the inbox callback on the reactor thread.
     *
     * You do not need the inbox lock to send an async signal.
     */
    uv_async_t notifier;
};

/**
 * A Hadoop RPC reactor thread.
 *
 * Each reactor thread uses libuv to send and receive on multiple TCP sockets
 * asynchronously.
 *
 * With the exception of the inbox, everything in this structure must be
 * accessed ONLY from the reactor thread.  Nothing is safe to read or write
 * from another thread except the inbox.
 */
struct hrpc_reactor {
    /**
     * Number of connections we've created.
     */
    uint64_t conns_created;

    /**
     * The inbox for incoming work for this reactor thread.
     */
    struct hrpc_reactor_inbox inbox;

    /**
     * A red-black tree of connections.  This makes it possible to find a
     * connection to a given IP address quickly.
     *
     * We may have multiple connections for the same IP:port combination.
     */
    struct hrpc_conns conns;

    /**
     * The libuv loop.
     */
    uv_loop_t loop;

    /**
     * The libuv timer.  Used to expire connections after a timeout has
     * elapsed.
     */
    uv_timer_t timer;

    /**
     * The reactor thread.  All reactor callbacks are made from this context.
     */
    uv_thread_t thread;

    /**
     * Name of the reactor.
     */
    char name[0];
};

/**
 * Remove a connection from the reactor.
 *
 * @param reactor       The reactor.
 * @param conn          The connection.
 */
void reactor_remove_conn(struct hrpc_reactor *reactor, struct hrpc_conn *conn);

/**
 * Insert a connection to the reactor.
 *
 * @param reactor       The reactor.
 * @param conn          The connection.
 */
void reactor_insert_conn(struct hrpc_reactor *reactor, struct hrpc_conn *conn);

/**
 * Create the reactor thread.
 *
 * @param out           (out param) on success, the new reactor thread.
 * @param name          The reactor name to use.
 */
struct hadoop_err *hrpc_reactor_create(struct hrpc_reactor **out,
                                       const char *reactor_name);

/**
 * Shut down the reactor thread and wait for it to terminate.
 *
 * All pending calls will get timeout errors.
 */
void hrpc_reactor_shutdown(struct hrpc_reactor *reactor);

/**
 * Free the reactor.
 */
void hrpc_reactor_free(struct hrpc_reactor *reactor);

/**
 * Start an outbound transfer.
 *
 * @param reactor       The reactor.
 * @param conn          The connection.  This connection must be either new, or
 * All pending calls will get timeout errors.
 */
void hrpc_reactor_start_outbound(struct hrpc_reactor *reactor,
                                 struct hrpc_call *call);

#endif

// vim: ts=4:sw=4:tw=79:et
