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
#include "rpc/conn.h"
#include "rpc/messenger.h"
#include "rpc/reactor.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <uv.h>

#define msgr_log_warn(msgr, fmt, ...) \
    fprintf(stderr, "WARN: msgr %p: " fmt, msgr, __VA_ARGS__)
#define msgr_log_info(msgr, fmt, ...) \
    fprintf(stderr, "INFO: msgr %p: " fmt, msgr, __VA_ARGS__)
#define msgr_log_debug(msgr, fmt, ...) \
    fprintf(stderr, "DEBUG: msgr %p: " fmt, msgr, __VA_ARGS__)

struct hrpc_messenger_builder {
};

/**
 * The Hadoop Messenger.
 *
 * The messenger owns all the reactor threads, and is the main entry point into
 * the RPC system.
 */
struct hrpc_messenger {
    /**
     * The reactor thread which makes the actual network calls.
     *
     * TODO: support multiple reactor threads.
     */
    struct hrpc_reactor *reactor;
};

struct hrpc_messenger_builder *hrpc_messenger_builder_alloc(void)
{
    struct hrpc_messenger_builder *bld;

    bld = calloc(1, sizeof(struct hrpc_messenger_builder));
    if (!bld)
        return NULL;
    return bld;
}

void hrpc_messenger_builder_free(struct hrpc_messenger_builder *bld)
{
    if (!bld)
        return;
    free(bld);
}

struct hadoop_err *hrpc_messenger_create(
        struct hrpc_messenger_builder *bld, struct hrpc_messenger **out)
{
    struct hrpc_messenger *msgr = NULL;
    struct hadoop_err *err = NULL;

    free(bld);
    msgr = calloc(1, sizeof(struct hrpc_messenger));
    if (!msgr) {
        err = hadoop_lerr_alloc(ENOMEM, "hrpc_messenger_create: OOM");
        goto error;
    }
    err = hrpc_reactor_create(&msgr->reactor);
    if (err) {
        goto error_free_msgr;
    }
    msgr_log_info(msgr, "created messenger %p\n", msgr);
    *out = msgr;
    return NULL;

error_free_msgr:
    free(msgr);
error:
    return err;
}

void hrpc_messenger_start_outbound(struct hrpc_messenger *msgr,
                                   struct hrpc_call *call)
{
    hrpc_reactor_start_outbound(msgr->reactor, call);
}

void hrpc_messenger_shutdown(struct hrpc_messenger *msgr)
{
    msgr_log_debug(msgr, "%s", "hrpc_messenger_shutdown\n");
    hrpc_reactor_shutdown(msgr->reactor);
}

void hrpc_messenger_free(struct hrpc_messenger *msgr)
{
    msgr_log_debug(msgr, "%s", "hrpc_messenger_free\n");
    hrpc_reactor_free(msgr->reactor);
    free(msgr);
}

// vim: ts=4:sw=4:tw=79:et
