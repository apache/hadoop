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
package org.apache.hadoop.util;

import org.junit.Test;
import java.math.BigInteger;
import static org.junit.Assert.assertTrue;

public class TestCpuTimeTracker {
  @Test
  public void test() throws InterruptedException {
    CpuTimeTracker tracker = new CpuTimeTracker(10);
    tracker.updateElapsedJiffies(
        BigInteger.valueOf(100),
        System.currentTimeMillis());
    float val1 = tracker.getCpuTrackerUsagePercent();
    assertTrue(
        "Not invalid CPU usage",
        val1 == -1.0);
    Thread.sleep(1000);
    tracker.updateElapsedJiffies(
        BigInteger.valueOf(200),
        System.currentTimeMillis());
    float val2 = tracker.getCpuTrackerUsagePercent();
    assertTrue(
        "Not positive CPU usage",
        val2 > 0);
    Thread.sleep(1000);
    tracker.updateElapsedJiffies(
        BigInteger.valueOf(0),
        System.currentTimeMillis());
    float val3 = tracker.getCpuTrackerUsagePercent();
    assertTrue(
        "Not positive CPU usage",
        val3 == 0.0);
  }
}
