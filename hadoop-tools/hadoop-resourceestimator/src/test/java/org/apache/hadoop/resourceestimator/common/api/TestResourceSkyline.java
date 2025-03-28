/*
 *
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
 *
 */

package org.apache.hadoop.resourceestimator.common.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.TreeMap;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test {@link ResourceSkyline} class.
 */
public class TestResourceSkyline {
  /**
   * Testing variables.
   */
  private ResourceSkyline resourceSkyline;

  private Resource resource1;
  private Resource resource2;
  private TreeMap<Long, Resource> resourceOverTime;
  private RLESparseResourceAllocation skylineList;

  @BeforeEach
  public final void setup() {
    resourceOverTime = new TreeMap<>();
    skylineList = new RLESparseResourceAllocation(resourceOverTime,
        new DefaultResourceCalculator());
    resource1 = Resource.newInstance(1024 * 100, 100);
    resource2 = Resource.newInstance(1024 * 200, 200);
  }

  @Test public final void testGetJobId() {
    assertNull(resourceSkyline);
    ReservationInterval riAdd = new ReservationInterval(0, 10);
    skylineList.addInterval(riAdd, resource1);
    riAdd = new ReservationInterval(10, 20);
    skylineList.addInterval(riAdd, resource1);
    resourceSkyline =
        new ResourceSkyline("1", 1024.5, 0, 20, resource1, skylineList);
    assertEquals("1", resourceSkyline.getJobId());
  }

  @Test public final void testGetJobSubmissionTime() {
    assertNull(resourceSkyline);
    ReservationInterval riAdd = new ReservationInterval(0, 10);
    skylineList.addInterval(riAdd, resource1);
    riAdd = new ReservationInterval(10, 20);
    skylineList.addInterval(riAdd, resource1);
    resourceSkyline =
        new ResourceSkyline("1", 1024.5, 0, 20, resource1, skylineList);
    assertEquals(0, resourceSkyline.getJobSubmissionTime());
  }

  @Test public final void testGetJobFinishTime() {
    assertNull(resourceSkyline);
    ReservationInterval riAdd = new ReservationInterval(0, 10);
    skylineList.addInterval(riAdd, resource1);
    riAdd = new ReservationInterval(10, 20);
    skylineList.addInterval(riAdd, resource1);
    resourceSkyline =
        new ResourceSkyline("1", 1024.5, 0, 20, resource1, skylineList);
    assertEquals(20, resourceSkyline.getJobFinishTime());
  }

  @Test public final void testGetKthResource() {
    assertNull(resourceSkyline);
    ReservationInterval riAdd = new ReservationInterval(10, 20);
    skylineList.addInterval(riAdd, resource1);
    riAdd = new ReservationInterval(20, 30);
    skylineList.addInterval(riAdd, resource2);
    resourceSkyline =
        new ResourceSkyline("1", 1024.5, 0, 20, resource1, skylineList);
    final RLESparseResourceAllocation skylineList2 =
        resourceSkyline.getSkylineList();
    for (int i = 10; i < 20; i++) {
      assertEquals(resource1.getMemorySize(),
          skylineList2.getCapacityAtTime(i).getMemorySize());
      assertEquals(resource1.getVirtualCores(),
          skylineList2.getCapacityAtTime(i).getVirtualCores());
    }
    for (int i = 20; i < 30; i++) {
      assertEquals(resource2.getMemorySize(),
          skylineList2.getCapacityAtTime(i).getMemorySize());
      assertEquals(resource2.getVirtualCores(),
          skylineList2.getCapacityAtTime(i).getVirtualCores());
    }
    // test if resourceSkyline automatically extends the skyline with
    // zero-resource at both ends
    assertEquals(0, skylineList2.getCapacityAtTime(9).getMemorySize());
    assertEquals(0, skylineList2.getCapacityAtTime(9).getVirtualCores());
    assertEquals(0, skylineList2.getCapacityAtTime(30).getMemorySize());
    assertEquals(0, skylineList2.getCapacityAtTime(30).getVirtualCores());
  }

  @AfterEach
  public final void cleanUp() {
    resourceSkyline = null;
    resource1 = null;
    resource2 = null;
    resourceOverTime.clear();
    resourceOverTime = null;
    skylineList = null;
  }
}
