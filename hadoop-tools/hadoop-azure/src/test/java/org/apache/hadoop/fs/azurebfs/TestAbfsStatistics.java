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

public class TestAbfsStatistics extends AbstractAbfsIntegrationTest {

  private static final int LARGE_OPS = 100;

  public TestAbfsStatistics() throws Exception {
  }

  /**
   * Tests for op_get_delegation_token and error_ignore counter values.
   */
  @Test
  public void testInitialiseStats() throws IOException {
    describe("Testing the counter values after Abfs is initialised");

    AbfsInstrumentation instrumentation =
        new AbfsInstrumentation(getFileSystem().getUri());
    Map<String, Long> metricMap = instrumentation.toMap();

    System.out.println(metricMap);

    //Testing the statistics values at initial stage.
    assertEquals("Mismatch in op_get_delegation_token", 0,
        (long) metricMap.get("op_get_delegation_token"));
    assertEquals("Mismatch in error_ignored", 0,
        (long) metricMap.get("error_ignored"));

    //Testing summation of the counter values.
    for (int i = 0; i < LARGE_OPS; i++) {
      instrumentation.incrementStat(AbfsStatistic.CALL_GET_DELEGATION_TOKEN, 1);
      instrumentation.incrementStat(AbfsStatistic.ERROR_IGNORED, 1);
    }

    metricMap = instrumentation.toMap();

    assertEquals("Mismatch in op_get_delegation_token", LARGE_OPS,
        (long) metricMap.get("op_get_delegation_token"));
    assertEquals("Mismatch in error_ignored", LARGE_OPS,
        (long) metricMap.get("error_ignored"));

  }
}
