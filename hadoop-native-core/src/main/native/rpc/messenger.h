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

#ifndef HADOOP_CORE_RPC_MSGR_H
#define HADOOP_CORE_RPC_MSGR_H

struct hrpc_call;
struct hrpc_messenger;
struct hrpc_messenger_builder;

#include <stdint.h> // for int32_t

/**
 * Allocate a Hadoop messenger builder.
 *
 * @return              A Hadoop messenger builder, or NULL on OOM.
 */
struct hrpc_messenger_builder *hrpc_messenger_builder_alloc(void);

/**
 * Set the timeout for the Hadoop messenger.
 *
 * @param bld           The messenger builder.
 * @param timeout_ms    The timeout in milliseconds
 */
void hrpc_messenger_builder_set_timeout_ms(struct hrpc_messenger_builder *bld,
                                           int32_t timeout_ms);

/**
 * Free a Hadoop messenger builder.
 *
 * @param bld           The Hadoop proxy builder to free.
 */
void hrpc_messenger_builder_free(struct hrpc_messenger_builder *bld);

/**
 * Create a Hadoop messenger.
 *
 * @param bld           The Hadoop messenger builder to use.
 *                        You must still free the builder even after calling
 *                        this function.
 * @param out       (out param) On success, the Hadoop messenger.
 *
 * @return          On success, NULL.  On error, the error.
 */
struct hadoop_err *hrpc_messenger_create(struct hrpc_messenger_builder *bld,
                                         struct hrpc_messenger **out);

/**
 * Start an outbound call.
 *
 * @param msgr          The messenger.
 * @param call          The call.
 */
void hrpc_messenger_start_outbound(struct hrpc_messenger *msgr,
                                   struct hrpc_call *call);

/**
 * Shut down the messenger.
 *
 * After this function is called, the messenger will stop accepting new calls.
 * We will deliver ESHUTDOWN to existing calls.
 *
 * @param msgr          The messenger.
 */
void hrpc_messenger_shutdown(struct hrpc_messenger *msgr);

/**
 * De-allocate the memory and other resources associated with this messenger.
 *
 * After this function is called, the messenger pointer will be invalid.
 *
 * @param msgr          The messenger.
 */
void hrpc_messenger_free(struct hrpc_messenger *msgr);

#endif

// vim: ts=4:sw=4:tw=79:et
