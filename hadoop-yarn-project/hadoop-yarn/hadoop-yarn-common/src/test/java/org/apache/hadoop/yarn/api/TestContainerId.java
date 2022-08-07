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


package org.apache.hadoop.yarn.api;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestContainerId {

  @Test
  void testContainerId() {
    ContainerId c1 = newContainerId(1, 1, 10l, 1);
    ContainerId c2 = newContainerId(1, 1, 10l, 2);
    ContainerId c3 = newContainerId(1, 1, 10l, 1);
    ContainerId c4 = newContainerId(1, 3, 10l, 1);
    ContainerId c5 = newContainerId(1, 3, 8l, 1);

    assertEquals(c1, c3);
    assertNotEquals(c1, c2);
    assertNotEquals(c1, c4);
    assertNotEquals(c1, c5);

    assertTrue(c1.compareTo(c3) == 0);
    assertTrue(c1.compareTo(c2) < 0);
    assertTrue(c1.compareTo(c4) < 0);
    assertTrue(c1.compareTo(c5) > 0);

    assertTrue(c1.hashCode() == c3.hashCode());
    assertFalse(c1.hashCode() == c2.hashCode());
    assertFalse(c1.hashCode() == c4.hashCode());
    assertFalse(c1.hashCode() == c5.hashCode());

    long ts = System.currentTimeMillis();
    ContainerId c6 = newContainerId(36473, 4365472, ts, 25645811);
    assertEquals("container_10_0001_01_000001", c1.toString());
    assertEquals(25645811, 0xffffffffffL & c6.getContainerId());
    assertEquals(0, c6.getContainerId() >> 40);
    assertEquals("container_" + ts + "_36473_4365472_25645811",
        c6.toString());

    ContainerId c7 = newContainerId(36473, 4365472, ts, 4298334883325L);
    assertEquals(999799999997L, 0xffffffffffL & c7.getContainerId());
    assertEquals(3, c7.getContainerId() >> 40);
    assertEquals(
        "container_e03_" + ts + "_36473_4365472_999799999997",
        c7.toString());

    ContainerId c8 = newContainerId(36473, 4365472, ts, 844424930131965L);
    assertEquals(1099511627773L, 0xffffffffffL & c8.getContainerId());
    assertEquals(767, c8.getContainerId() >> 40);
    assertEquals(
        "container_e767_" + ts + "_36473_4365472_1099511627773",
        c8.toString());
  }

  public static ContainerId newContainerId(int appId, int appAttemptId,
      long timestamp, long containerId) {
    ApplicationId applicationId = ApplicationId.newInstance(timestamp, appId);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, appAttemptId);
    return ContainerId.newContainerId(applicationAttemptId, containerId);
  }
}
