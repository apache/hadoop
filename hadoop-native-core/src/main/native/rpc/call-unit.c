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

 #include "rpc/call.h"
 #include "common/test.h"
 #include "common/hadoop_err.h"
 #include "rpc/proxy.h"

 #include <stdlib.h>
 #include <stdio.h>
 #include <uv.h>
 #include <string.h>
 #include <stdint.h>

int err_test(struct hrpc_response *resp, struct hadoop_err *err)
{
    EXPECT_STR_EQ("org.apache.hadoop.native.HadoopCore.InvalidRequestException: "
                        "test hadoop_lerr_alloc", hadoop_err_msg(err));
    EXPECT_NULL(resp);
    hadoop_err_free(err);
    return 0;
}

void hrpc_call_deliver_err_test_cb(struct hrpc_response *resp,
                            struct hadoop_err *err, void *cb_data)
{
    uv_sem_t *sem = cb_data;
    if (err_test(resp, err))
    {
        abort();
    }
    uv_sem_post(sem);
}

int hrpc_call_deliver_err_test(struct hrpc_call *call, struct hadoop_err *err)
{
    hrpc_call_deliver_err(call, err);
    return 0;
}

int resp_test(struct hrpc_response *resp, struct hadoop_err *err)
{
    EXPECT_NONNULL(resp);
    EXPECT_NULL(err);
    free(resp->base);
    return 0;
}

void hrpc_call_deliver_resp_test_cb(struct hrpc_response *resp,
                            struct hadoop_err *err, void *cb_data)
{
    uv_sem_t *sem = cb_data;
    if (resp_test(resp, err))
    {
        abort();
    }
    uv_sem_post(sem);
}

int hrpc_call_deliver_resp_test(struct hrpc_call *call, struct hrpc_response *resp)
{
     hrpc_call_deliver_resp(call, resp);
     return 0;
}

int hrpc_call_test(struct hrpc_call *call)
{
     // Test hrpc_call_activate
    EXPECT_INT_EQ(1, hrpc_call_activate(call));
    EXPECT_INT_EQ(1, hrpc_call_is_active(call));

    // Test hrpc_call_activate if it was already active
    EXPECT_INT_EQ(0, hrpc_call_activate(call));
    EXPECT_INT_EQ(1, hrpc_call_is_active(call));

    //Test hrpc_call_deactivate
    hrpc_call_deactivate(call);
    EXPECT_INT_EQ(0, hrpc_call_is_active(call));

    return 0;
}

int main(void)
{
    struct hrpc_call call;
    struct hadoop_err *err;
    struct hrpc_response resp;
    uv_buf_t payload;
    uv_sem_t sem1, sem2;

    EXPECT_INT_ZERO(uv_sem_init(&sem1, 0));
    EXPECT_INT_ZERO(uv_sem_init(&sem2, 0));

    payload.base = strdup("testbuff");
    payload.len = strlen(payload.base);

    call.remote->sin_addr.s_addr = inet_addr("127.0.0.1");
    call.protocol = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
    call.username = "root";
    call.payload = payload;
    call.cb = hrpc_call_deliver_err_test_cb;
    call.cb_data = &sem1;
    call.active = 0;
    err =  hadoop_lerr_alloc(EINVAL, "test hadoop_lerr_alloc");

    //Test hrpc call
    EXPECT_INT_ZERO(hrpc_call_test(&call));

    //Test hrpc call deliver error
    EXPECT_INT_ZERO(hrpc_call_deliver_err_test(&call, err));
    uv_sem_wait(&sem1);

    call.cb_data = &sem2;
    call.cb = hrpc_call_deliver_resp_test_cb;

    resp.pb_base = (uint8_t*)(payload.len);
    resp.pb_len = (int32_t)(payload.len);
    resp.base = strdup("testbuff");

    //Test hrpc call deliver response
    EXPECT_INT_ZERO(hrpc_call_deliver_resp_test(&call, &resp));
    uv_sem_wait(&sem2);

    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
