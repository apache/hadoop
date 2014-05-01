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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <uv.h>

int main(void)
{
    struct hadoop_err *err;

    err = hadoop_lerr_alloc(EINVAL, "foo bar baz %d", 101);
    EXPECT_STR_EQ("org.apache.hadoop.native.HadoopCore.InvalidRequestException: "
              "foo bar baz 101", hadoop_err_msg(err));
    EXPECT_INT_EQ(EINVAL, hadoop_err_code(err));
    hadoop_err_free(err);

    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
