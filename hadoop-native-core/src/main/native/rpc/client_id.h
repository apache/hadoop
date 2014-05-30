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

#ifndef HADOOP_CORE_RPC_CLIENT_ID
#define HADOOP_CORE_RPC_CLIENT_ID

#include <stddef.h> // for size_t
#include <stdint.h> // for uint8_t

#define HRPC_CLIENT_ID_LEN 16
#define HRPC_CLIENT_ID_STR_LEN 36

struct hrpc_client_id {
    uint8_t buf[HRPC_CLIENT_ID_LEN];
};

void hrpc_client_id_generate_rand(struct hrpc_client_id *id);

struct hadoop_err *hrpc_client_id_from_bytes(void *bytes, int len,
                                             struct hrpc_client_id *out);

const char *hrpc_client_id_to_str(const struct hrpc_client_id *id,
                           char *str, size_t str_len);

int hrpc_client_id_compare(const struct hrpc_client_id *a,
                            const struct hrpc_client_id *b);

#endif

// vim: ts=4:sw=4:tw=79:et
