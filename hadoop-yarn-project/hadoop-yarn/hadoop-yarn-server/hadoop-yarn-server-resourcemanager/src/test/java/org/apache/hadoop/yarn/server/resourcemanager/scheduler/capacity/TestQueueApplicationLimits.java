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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestQueueApplicationLimits {

  @Test
  public void testBaseMaximumApplications() {
    assertEquals(10000,
        QueueApplicationLimits.baseMaximumApplications(-1, 10000));
    assertEquals(10000,
        QueueApplicationLimits.baseMaximumApplications(0, 10000));
    assertEquals(200,
        QueueApplicationLimits.baseMaximumApplications(200, 10000));
    assertEquals(10000,
        QueueApplicationLimits.baseMaximumApplications(20000, 10000));
  }

  @Test
  public void testBaseAppliesUnscaledOnlyWithGlobalLimitAndNoAbsolute() {
    assertTrue(QueueApplicationLimits.usesBaseMaximumApplications(200, false));
    assertFalse(QueueApplicationLimits.usesBaseMaximumApplications(200, true));
    assertFalse(QueueApplicationLimits.usesBaseMaximumApplications(0, false));
  }

  @Test
  public void testScaleRoundsDown() {
    assertEquals(3333,
        QueueApplicationLimits.scaleByAbsoluteCapacity(10000, 1f / 3));
    assertEquals(0, QueueApplicationLimits.scaleByAbsoluteCapacity(10000, 0f));
  }

  @Test
  public void testMaximumApplicationsPerUser() {
    // No user limit factor: the queue limit
    assertEquals(100, QueueApplicationLimits.maximumApplicationsPerUser(100,
        50f, -1f, true));
    // 100 x 50% x 3 = 150, capped at the queue limit for a leaf queue only
    assertEquals(100, QueueApplicationLimits.maximumApplicationsPerUser(100,
        50f, 3f, true));
    assertEquals(150, QueueApplicationLimits.maximumApplicationsPerUser(100,
        50f, 3f, false));
    assertEquals(25, QueueApplicationLimits.maximumApplicationsPerUser(100,
        25f, 1f, true));
  }
}
