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
#include "common/test.h"
#include "protobuf/ClientNamenodeProtocol.call.h"
#include "rpc/messenger.h"
#include "rpc/proxy.h"

#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <uv.h>

struct options {
    struct sockaddr_in remote;
};

static void options_from_env(struct options *opts)
{
    const char *ip_str;
    const char *port_str;
    int res, port;

    ip_str = getenv("HDFS_IP");
    if (!ip_str) {
        fprintf(stderr, "You must set an ip via the HDFS_IP "
                "environment variable.\n");
        exit(EXIT_FAILURE);
    }
    port_str = getenv("HDFS_PORT");
    if (!port_str) {
        fprintf(stderr, "You must set a port via the HDFS_PORT "
                "environment variable.\n");
        exit(EXIT_FAILURE);
    }
    port = atoi(port_str);
    res = uv_ip4_addr(ip_str, port, &opts->remote);
    if (res) {
        fprintf(stderr, "Invalid IP and port %s and %d: error %s\n",
                ip_str, port, uv_strerror(res));
        exit(EXIT_FAILURE);
    }
}

void set_replication_cb(SetReplicationResponseProto *resp,
                        struct hadoop_err *err, void *cb_data)
{
    uv_sem_t *sem = cb_data;

    if (err) {
        fprintf(stderr, "set_replication_cb: got an error.  %s\n",
                hadoop_err_msg(err));
        goto done;
    }
    fprintf(stderr, "set_replication_cb: resp->result = %d\n",
            !!resp->result);

done:
    sem_post(sem);
    if (err) {
        hadoop_err_free(err);
    }
    if (resp) {
        set_replication_response_proto__free_unpacked(resp, NULL);
    }
}

int main(void)
{
    struct hrpc_messenger_builder *msgr_bld;
    struct hrpc_proxy_builder *proxy_bld;
    struct hrpc_proxy *proxy;
    struct hrpc_messenger *msgr;
    struct options opts;
    uv_sem_t sem;

    memset(&opts, 0, sizeof(opts));
    options_from_env(&opts);
    msgr_bld = hrpc_messenger_builder_alloc();
    EXPECT_NONNULL(msgr_bld);
    EXPECT_NO_HADOOP_ERR(hrpc_messenger_create(msgr_bld, &msgr));
    proxy_bld = hrpc_proxy_builder_alloc(msgr);
    EXPECT_NONNULL(proxy_bld);
    hrpc_proxy_builder_set_remote(proxy_bld, &opts.remote);
    hrpc_proxy_builder_set_protocol(proxy_bld,
                "org.apache.hadoop.hdfs.protocol.ClientProtocol");
    EXPECT_NO_HADOOP_ERR(hrpc_proxy_create(proxy_bld, &proxy));
    EXPECT_INT_ZERO(uv_sem_init(&sem, 0));
    {
        SetReplicationRequestProto req = SET_REPLICATION_REQUEST_PROTO__INIT;
        req.src = "/foo2";
        req.replication = 2;
        cnn_async_set_replication(proxy, &req, set_replication_cb, &sem);
    }
    sem_wait(&sem);

    hrpc_proxy_free(proxy);
    hrpc_messenger_shutdown(msgr);
    hrpc_messenger_free(msgr);
    uv_sem_destroy(&sem);

    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
