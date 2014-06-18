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

#include "rpc/client_id.h"
#include "common/test.h"
#include "common/hadoop_err.h"

#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <uv.h>

#define HRPC_CLIENT_ID_LEN 16

int test_hrpc_client_id(void)
{
    struct hrpc_client_id id, id1;
    char str[36];
    char bytes[16] = { 0 };
    struct hadoop_err *err;

    memset(&id, 0, sizeof(id));
    memset(&id1, 0, sizeof(id1));

    hrpc_client_id_generate_rand(&id);
    hrpc_client_id_generate_rand(&id1);

    EXPECT_INT_ZERO(!hrpc_client_id_compare(&id, &id1));
    EXPECT_INT_ZERO(hrpc_client_id_compare(&id, &id));

    EXPECT_NO_HADOOP_ERR(hrpc_client_id_from_bytes(&bytes, HRPC_CLIENT_ID_LEN, &id));
    EXPECT_STR_EQ("00000000-00000000-00000000-00000000"
                    ,hrpc_client_id_to_str(&id, str, sizeof(str)));

    err = hrpc_client_id_from_bytes(&bytes, 6, &id);
    EXPECT_STR_EQ("org.apache.hadoop.native.HadoopCore.InvalidRequestException: "
                    "hrpc_client_id_from_bytes: "
                    "invalid client id length of 6 (expected 16)",
                    hadoop_err_msg(err));
    hadoop_err_free(err);
    return 0;
}

int main(void) {
    EXPECT_INT_ZERO(test_hrpc_client_id());
    return EXIT_SUCCESS;
}
  // vim: ts=4:sw=4:tw=79:et