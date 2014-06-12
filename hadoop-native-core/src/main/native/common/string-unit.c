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

#include "common/string.h"
#include "test/test.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int test_strdupto(void)
{
    char *dst = NULL;
    EXPECT_INT_ZERO(strdupto(&dst, "FOO"));
    EXPECT_INT_ZERO(strcmp(dst, "FOO"));
    EXPECT_INT_ZERO(strdupto(&dst, NULL));
    EXPECT_NULL(dst);
    EXPECT_INT_ZERO(strdupto(&dst, "BAR"));
    EXPECT_INT_ZERO(strcmp(dst, "BAR"));
    EXPECT_INT_ZERO(strdupto(&dst, "BAZ"));
    EXPECT_INT_ZERO(strcmp(dst, "BAZ"));
    EXPECT_INT_ZERO(strdupto(&dst, NULL));
    EXPECT_NULL(dst);
    return 0;
}

int main(void)
{
    EXPECT_INT_ZERO(test_strdupto());

    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
