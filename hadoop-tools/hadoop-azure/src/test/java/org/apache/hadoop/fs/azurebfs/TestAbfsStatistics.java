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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;

/**
 * Unit tests for Abfs common counters.
 */
public class TestAbfsStatistics extends AbstractAbfsIntegrationTest {

  private static final int LARGE_OPS = 100;

  public TestAbfsStatistics() throws Exception {
  }

  /**
   * Tests for op_get_delegation_token and error_ignore counter values.
   */
  @Test
  public void testInitializeStats() throws IOException {
    describe("Testing the counter values after Abfs is initialised");

    AbfsCounters instrumentation =
        new AbfsCountersImpl(getFileSystem().getUri());

    //Testing summation of the counter values.
    for (int i = 0; i < LARGE_OPS; i++) {
      instrumentation.incrementCounter(AbfsStatistic.CALL_GET_DELEGATION_TOKEN, 1);
      instrumentation.incrementCounter(AbfsStatistic.ERROR_IGNORED, 1);
    }

    Map<String, Long> metricMap = instrumentation.toMap();

    assertAbfsStatistics(AbfsStatistic.CALL_GET_DELEGATION_TOKEN, LARGE_OPS,
        metricMap);
    assertAbfsStatistics(AbfsStatistic.ERROR_IGNORED, LARGE_OPS, metricMap);

  }
}
