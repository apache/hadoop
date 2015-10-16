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

#include "expect.h"
#include "hdfs/hdfs.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int expectFileStats(hdfsFile file,
      uint64_t expectedTotalBytesRead,
      uint64_t expectedTotalLocalBytesRead,
      uint64_t expectedTotalShortCircuitBytesRead,
      uint64_t expectedTotalZeroCopyBytesRead)
{
    struct hdfsReadStatistics *stats = NULL;
    EXPECT_ZERO(hdfsFileGetReadStatistics(file, &stats));
    fprintf(stderr, "expectFileStats(expectedTotalBytesRead=%"PRId64", "
            "expectedTotalLocalBytesRead=%"PRId64", "
            "expectedTotalShortCircuitBytesRead=%"PRId64", "
            "expectedTotalZeroCopyBytesRead=%"PRId64", "
            "totalBytesRead=%"PRId64", "
            "totalLocalBytesRead=%"PRId64", "
            "totalShortCircuitBytesRead=%"PRId64", "
            "totalZeroCopyBytesRead=%"PRId64")\n",
            expectedTotalBytesRead,
            expectedTotalLocalBytesRead,
            expectedTotalShortCircuitBytesRead,
            expectedTotalZeroCopyBytesRead,
            stats->totalBytesRead,
            stats->totalLocalBytesRead,
            stats->totalShortCircuitBytesRead,
            stats->totalZeroCopyBytesRead);
    if (expectedTotalBytesRead != UINT64_MAX) {
        EXPECT_UINT64_EQ(expectedTotalBytesRead, stats->totalBytesRead);
    }
    if (expectedTotalLocalBytesRead != UINT64_MAX) {
        EXPECT_UINT64_EQ(expectedTotalLocalBytesRead,
                      stats->totalLocalBytesRead);
    }
    if (expectedTotalShortCircuitBytesRead != UINT64_MAX) {
        EXPECT_UINT64_EQ(expectedTotalShortCircuitBytesRead,
                      stats->totalShortCircuitBytesRead);
    }
    if (expectedTotalZeroCopyBytesRead != UINT64_MAX) {
        EXPECT_UINT64_EQ(expectedTotalZeroCopyBytesRead,
                      stats->totalZeroCopyBytesRead);
    }
    hdfsFileFreeReadStatistics(stats);
    return 0;
}
