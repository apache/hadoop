/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *  
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestInMemoryReservationAllocation {

  private String user = "yarn";
  private String planName = "test-reservation";
  private ResourceCalculator resCalc;
  private Resource minAlloc;

  private Random rand = new Random();

  @Before
  public void setUp() {
    resCalc = new DefaultResourceCalculator();
    minAlloc = Resource.newInstance(1, 1);
  }

  @After
  public void tearDown() {
    user = null;
    planName = null;
    resCalc = null;
    minAlloc = null;
  }

  @Test
  public void testBlocks() {
    ReservationId reservationID =
        ReservationId.newInstance(rand.nextLong(), rand.nextLong());
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            start, start + alloc.length + 1,
            alloc.length);
    Map<ReservationInterval, Resource> allocations =
        generateAllocation(start, alloc, false, false);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length + 1, allocations, resCalc, minAlloc);
    doAssertions(rAllocation, reservationID, rDef, allocations, start, alloc);
    Assert.assertFalse(rAllocation.containsGangs());
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(Resource.newInstance(1024 * (alloc[i]), (alloc[i])),
          rAllocation.getResourcesAtTime(start + i));
    }
  }

  @Test
  public void testSteps() {
    ReservationId reservationID =
        ReservationId.newInstance(rand.nextLong(), rand.nextLong());
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            start, start + alloc.length + 1,
            alloc.length);
    Map<ReservationInterval, Resource> allocations =
        generateAllocation(start, alloc, true, false);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length + 1, allocations, resCalc, minAlloc);
    doAssertions(rAllocation, reservationID, rDef, allocations, start, alloc);
    Assert.assertFalse(rAllocation.containsGangs());
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(
          Resource.newInstance(1024 * (alloc[i] + i), (alloc[i] + i)),
          rAllocation.getResourcesAtTime(start + i));
    }
  }

  @Test
  public void testSkyline() {
    ReservationId reservationID =
        ReservationId.newInstance(rand.nextLong(), rand.nextLong());
    int[] alloc = { 0, 5, 10, 10, 5, 0 };
    int start = 100;
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            start, start + alloc.length + 1,
            alloc.length);
    Map<ReservationInterval, Resource> allocations =
        generateAllocation(start, alloc, true, false);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length + 1, allocations, resCalc, minAlloc);
    doAssertions(rAllocation, reservationID, rDef, allocations, start, alloc);
    Assert.assertFalse(rAllocation.containsGangs());
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(
          Resource.newInstance(1024 * (alloc[i] + i), (alloc[i] + i)),
          rAllocation.getResourcesAtTime(start + i));
    }
  }

  @Test
  public void testZeroAlloaction() {
    ReservationId reservationID =
        ReservationId.newInstance(rand.nextLong(), rand.nextLong());
    int[] alloc = {};
    long start = 0;
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            start, start + alloc.length + 1,
            alloc.length);
    Map<ReservationInterval, Resource> allocations =
        new HashMap<ReservationInterval, Resource>();
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length + 1, allocations, resCalc, minAlloc);
    doAssertions(rAllocation, reservationID, rDef, allocations, (int) start,
        alloc);
    Assert.assertFalse(rAllocation.containsGangs());
  }

  @Test
  public void testGangAlloaction() {
    ReservationId reservationID =
        ReservationId.newInstance(rand.nextLong(), rand.nextLong());
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            start, start + alloc.length + 1,
            alloc.length);
    boolean isGang = true;
    Map<ReservationInterval, Resource> allocations =
        generateAllocation(start, alloc, false, isGang);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length + 1, allocations, resCalc, minAlloc,
            isGang);
    doAssertions(rAllocation, reservationID, rDef, allocations, start, alloc);
    Assert.assertTrue(rAllocation.containsGangs());
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(Resource.newInstance(1024 * (alloc[i]), (alloc[i])),
          rAllocation.getResourcesAtTime(start + i));
    }
  }

  private void doAssertions(ReservationAllocation rAllocation,
      ReservationId reservationID, ReservationDefinition rDef,
      Map<ReservationInterval, Resource> allocations, int start,
      int[] alloc) {
    Assert.assertEquals(reservationID, rAllocation.getReservationId());
    Assert.assertEquals(rDef, rAllocation.getReservationDefinition());
    Assert.assertEquals(allocations, rAllocation.getAllocationRequests());
    Assert.assertEquals(user, rAllocation.getUser());
    Assert.assertEquals(planName, rAllocation.getPlanName());
    Assert.assertEquals(start, rAllocation.getStartTime());
    Assert.assertEquals(start + alloc.length + 1, rAllocation.getEndTime());
  }

  private Map<ReservationInterval, Resource> generateAllocation(
      int startTime, int[] alloc, boolean isStep, boolean isGang) {
    Map<ReservationInterval, Resource> req =
        new HashMap<ReservationInterval, Resource>();
    int numContainers = 0;
    for (int i = 0; i < alloc.length; i++) {
      if (isStep) {
        numContainers = alloc[i] + i;
      } else {
        numContainers = alloc[i];
      }
      ReservationRequest rr =
          ReservationRequest.newInstance(Resource.newInstance(1024, 1),
              (numContainers));
      if (isGang) {
        rr.setConcurrency(numContainers);
      }
      req.put(new ReservationInterval(startTime + i, startTime + i + 1),
          ReservationSystemUtil.toResource(rr));
    }
    return req;
  }

}
