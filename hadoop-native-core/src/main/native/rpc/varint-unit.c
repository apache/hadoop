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

#include "rpc/varint.h"
#include "test/test.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static int varint32_round_trip_test(int val)
{
    int size;
    int32_t oval = 0;
    int32_t off = 0;
    uint8_t buf[16] = { 0 };
    const size_t buf_len = sizeof(buf)/sizeof(buf[0]);

    size = varint32_size(val);
    EXPECT_INT_ZERO(varint32_encode(val, buf, buf_len, &off));
    EXPECT_INT_EQ(size, off);
    off = 0;
    EXPECT_INT_ZERO(varint32_decode(&oval, buf, buf_len, &off));
    EXPECT_INT_EQ(size, off);
    EXPECT_INT_EQ(val, oval);
    return 0;
}

static int be32_round_trip_test(int val)
{
    int32_t oval;
    uint8_t buf[sizeof(int32_t)] = { 0 };

    be32_encode(val, buf);
    oval = be32_decode(buf);
    EXPECT_INT_EQ(val, oval);
    return 0;
}

static int round_trip_test(int var)
{
    EXPECT_INT_ZERO(varint32_round_trip_test(var));
    EXPECT_INT_ZERO(be32_round_trip_test(var));
    return 0;
}

int main()
{
    round_trip_test(0);
    round_trip_test(123);
    round_trip_test(6578);
    round_trip_test(0x7fffffff);
    round_trip_test(-15);
    round_trip_test(-1);
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
