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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRLESparseResourceAllocation {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestRLESparseResourceAllocation.class);

  @Test
  public void testMergeAdd() throws PlanningException {

    TreeMap<Long, Resource> a = new TreeMap<>();
    TreeMap<Long, Resource> b = new TreeMap<>();

    setupArrays(a, b);

    RLESparseResourceAllocation rleA =
        new RLESparseResourceAllocation(a, new DefaultResourceCalculator());
    RLESparseResourceAllocation rleB =
        new RLESparseResourceAllocation(b, new DefaultResourceCalculator());

    RLESparseResourceAllocation out =
        RLESparseResourceAllocation.merge(new DefaultResourceCalculator(),
            Resource.newInstance(100 * 128 * 1024, 100 * 32), rleA, rleB,
            RLEOperator.add, 18, 45);

    System.out.println(out);

    long[] time = { 18, 20, 22, 30, 33, 40, 43, 45 };
    int[] alloc = { 10, 15, 20, 25, 30, 40, 30 };

    validate(out, time, alloc);
  }

  @Test
  public void testMergeMin() throws PlanningException {

    TreeMap<Long, Resource> a = new TreeMap<>();
    TreeMap<Long, Resource> b = new TreeMap<>();

    setupArrays(a, b);

    RLESparseResourceAllocation rleA =
        new RLESparseResourceAllocation(a, new DefaultResourceCalculator());
    RLESparseResourceAllocation rleB =
        new RLESparseResourceAllocation(b, new DefaultResourceCalculator());

    RLESparseResourceAllocation out =
        RLESparseResourceAllocation.merge(new DefaultResourceCalculator(),
            Resource.newInstance(100 * 128 * 1024, 100 * 32), rleA, rleB,
            RLEOperator.min, 0, 60);

    System.out.println(out);

    long[] time = { 10, 22, 33, 40, 43, 50, 60 };
    int[] alloc = { 5, 10, 15, 20, 10, 0 };

    validate(out, time, alloc);

  }

  @Test
  public void testMergeMax() throws PlanningException {

    TreeMap<Long, Resource> a = new TreeMap<>();
    TreeMap<Long, Resource> b = new TreeMap<>();

    setupArrays(a, b);

    RLESparseResourceAllocation rleA =
        new RLESparseResourceAllocation(a, new DefaultResourceCalculator());
    RLESparseResourceAllocation rleB =
        new RLESparseResourceAllocation(b, new DefaultResourceCalculator());

    RLESparseResourceAllocation out =
        RLESparseResourceAllocation.merge(new DefaultResourceCalculator(),
            Resource.newInstance(100 * 128 * 1024, 100 * 32), rleA, rleB,
            RLEOperator.max, 0, 60);

    System.out.println(out);

    long[] time = { 10, 20, 30, 40, 50, 60 };
    int[] alloc = { 5, 10, 15, 20, 10 };

    validate(out, time, alloc);

  }

  @Test
  public void testMergeSubtract() throws PlanningException {

    TreeMap<Long, Resource> a = new TreeMap<>();
    TreeMap<Long, Resource> b = new TreeMap<>();

    setupArrays(a, b);

    RLESparseResourceAllocation rleA =
        new RLESparseResourceAllocation(a, new DefaultResourceCalculator());
    RLESparseResourceAllocation rleB =
        new RLESparseResourceAllocation(b, new DefaultResourceCalculator());

    RLESparseResourceAllocation out =
        RLESparseResourceAllocation.merge(new DefaultResourceCalculator(),
            Resource.newInstance(100 * 128 * 1024, 100 * 32), rleA, rleB,
            RLEOperator.subtract, 0, 60);

    System.out.println(out);

    long[] time = { 10, 11, 20, 22, 30, 33, 43, 50, 60 };
    int[] alloc = { 5, 0, 5, 0, 5, 0, 10, -10 };

    validate(out, time, alloc);

  }

  @Test
  public void testMergesubtractTestNonNegative() throws PlanningException {

    // starting with default array example
    TreeMap<Long, Resource> a = new TreeMap<>();
    TreeMap<Long, Resource> b = new TreeMap<>();

    setupArrays(a, b);

    RLESparseResourceAllocation rleA =
        new RLESparseResourceAllocation(a, new DefaultResourceCalculator());
    RLESparseResourceAllocation rleB =
        new RLESparseResourceAllocation(b, new DefaultResourceCalculator());

    try {
      RLESparseResourceAllocation out =
          RLESparseResourceAllocation.merge(new DefaultResourceCalculator(),
              Resource.newInstance(100 * 128 * 1024, 100 * 32), rleA, rleB,
              RLEOperator.subtractTestNonNegative, 0, 60);
      fail();
    } catch (PlanningException pe) {
      // Expected!
    }

    // NOTE a is empty!! so the subtraction is implicitly considered negative
    // and the test should fail

    a = new TreeMap<>();
    b = new TreeMap<>();
    b.put(11L, Resource.newInstance(5, 6));

    rleA = new RLESparseResourceAllocation(a, new DefaultResourceCalculator());
    rleB = new RLESparseResourceAllocation(b, new DefaultResourceCalculator());

    try {
      RLESparseResourceAllocation out =
          RLESparseResourceAllocation.merge(new DefaultResourceCalculator(),
              Resource.newInstance(100 * 128 * 1024, 100 * 32), rleA, rleB,
              RLEOperator.subtractTestNonNegative, 0, 60);
      fail();
    } catch (PlanningException pe) {
      // Expected!
    }

    // Testing that the subtractTestNonNegative detects problems even if only one
    // of the resource dimensions is "<0"
    a.put(10L, Resource.newInstance(10, 5));
    b.put(11L, Resource.newInstance(5, 6));

    rleA = new RLESparseResourceAllocation(a, new DefaultResourceCalculator());
    rleB = new RLESparseResourceAllocation(b, new DefaultResourceCalculator());

    try {
      RLESparseResourceAllocation out =
          RLESparseResourceAllocation.merge(new DefaultResourceCalculator(),
              Resource.newInstance(100 * 128 * 1024, 100 * 32), rleA, rleB,
              RLEOperator.subtractTestNonNegative, 0, 60);
      fail();
    } catch (PlanningException pe) {
      // Expected!
    }

    // try with reverse setting
    a.put(10L, Resource.newInstance(5, 10));
    b.put(11L, Resource.newInstance(6, 5));

    rleA = new RLESparseResourceAllocation(a, new DefaultResourceCalculator());
    rleB = new RLESparseResourceAllocation(b, new DefaultResourceCalculator());

    try {
      RLESparseResourceAllocation out =
          RLESparseResourceAllocation.merge(new DefaultResourceCalculator(),
              Resource.newInstance(100 * 128 * 1024, 100 * 32), rleA, rleB,
              RLEOperator.subtractTestNonNegative, 0, 60);
      fail();
    } catch (PlanningException pe) {
      // Expected!
    }

    // trying a case that should work
    a.put(10L, Resource.newInstance(10, 6));
    b.put(11L, Resource.newInstance(5, 6));

    rleA = new RLESparseResourceAllocation(a, new DefaultResourceCalculator());
    rleB = new RLESparseResourceAllocation(b, new DefaultResourceCalculator());

    RLESparseResourceAllocation out =
        RLESparseResourceAllocation.merge(new DefaultResourceCalculator(),
            Resource.newInstance(100 * 128 * 1024, 100 * 32), rleA, rleB,
            RLEOperator.subtractTestNonNegative, 0, 60);

  }

  @Test
  @Ignore
  public void testMergeSpeed() throws PlanningException {

    for (int j = 0; j < 100; j++) {
      TreeMap<Long, Resource> a = new TreeMap<>();
      TreeMap<Long, Resource> b = new TreeMap<>();
      Random rand = new Random();
      long startA = 0;
      long startB = 0;

      for (int i = 0; i < 1000 + rand.nextInt(9000); i++) {
        startA += rand.nextInt(100);
        startB += rand.nextInt(100);
        a.put(startA,
            Resource.newInstance(rand.nextInt(10240), rand.nextInt(10)));
        b.put(startB,
            Resource.newInstance(rand.nextInt(10240), rand.nextInt(10)));
      }

      RLESparseResourceAllocation rleA =
          new RLESparseResourceAllocation(a, new DefaultResourceCalculator());
      RLESparseResourceAllocation rleB =
          new RLESparseResourceAllocation(b, new DefaultResourceCalculator());

      long start = System.currentTimeMillis();
      RLESparseResourceAllocation out =
          RLESparseResourceAllocation.merge(new DefaultResourceCalculator(),
              Resource.newInstance(100 * 128 * 1024, 100 * 32), rleA, rleB,
              RLEOperator.add, Long.MIN_VALUE, Long.MAX_VALUE);
      long end = System.currentTimeMillis();

      System.out.println(" Took: " + (end - start) + "ms ");
    }

  }

  @Test
  public void testRangeOverlapping() {
    ResourceCalculator resCalc = new DefaultResourceCalculator();

    RLESparseResourceAllocation r =
        new RLESparseResourceAllocation(resCalc);
    int[] alloc = {10, 10, 10, 10, 10, 10};
    int start = 100;
    Set<Entry<ReservationInterval, Resource>> inputs =
        generateAllocation(start, alloc, false).entrySet();
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      r.addInterval(ip.getKey(), ip.getValue());
    }
    long s = r.getEarliestStartTime();
    long d = r.getLatestNonNullTime();

    // tries to trigger "out-of-range" bug
    r =  r.getRangeOverlapping(s, d);
    r = r.getRangeOverlapping(s-1, d-1);
    r = r.getRangeOverlapping(s+1, d+1);
  }

  @Test
  public void testBlocks() {
    ResourceCalculator resCalc = new DefaultResourceCalculator();

    RLESparseResourceAllocation rleSparseVector =
        new RLESparseResourceAllocation(resCalc);
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    Set<Entry<ReservationInterval, Resource>> inputs =
        generateAllocation(start, alloc, false).entrySet();
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.addInterval(ip.getKey(), ip.getValue());
    }
    LOG.info(rleSparseVector.toString());
    Assert.assertFalse(rleSparseVector.isEmpty());
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(99));
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(start + alloc.length + 1));
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(Resource.newInstance(1024 * (alloc[i]), (alloc[i])),
          rleSparseVector.getCapacityAtTime(start + i));
    }
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(start + alloc.length + 2));
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.removeInterval(ip.getKey(), ip.getValue());
    }
    LOG.info(rleSparseVector.toString());
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(Resource.newInstance(0, 0),
          rleSparseVector.getCapacityAtTime(start + i));
    }
    Assert.assertTrue(rleSparseVector.isEmpty());
  }

  @Test
  public void testPartialRemoval() {
    ResourceCalculator resCalc = new DefaultResourceCalculator();

    RLESparseResourceAllocation rleSparseVector =
        new RLESparseResourceAllocation(resCalc);

    ReservationInterval riAdd = new ReservationInterval(10, 20);
    Resource rr = Resource.newInstance(1024 * 100, 100);

    ReservationInterval riAdd2 = new ReservationInterval(20, 30);

    Resource rr2 = Resource.newInstance(1024 * 200, 200);

    ReservationInterval riRemove = new ReservationInterval(12, 25);
    // same if we use this
    // ReservationRequest rrRemove =
    // ReservationRequest.newInstance(Resource.newInstance(1024, 1), 100, 1,6);
    LOG.info(rleSparseVector.toString());

    rleSparseVector.addInterval(riAdd, rr);
    rleSparseVector.addInterval(riAdd2, rr2);
    LOG.info(rleSparseVector.toString());

    rleSparseVector.removeInterval(riRemove, rr);
    LOG.info(rleSparseVector.toString());

    // Current bug prevents this to pass. The RLESparseResourceAllocation
    // does not handle removal of "partial"
    // allocations correctly.
    Assert.assertEquals(102400, rleSparseVector.getCapacityAtTime(10)
        .getMemorySize());
    Assert.assertEquals(0, rleSparseVector.getCapacityAtTime(13).getMemorySize());
    Assert.assertEquals(0, rleSparseVector.getCapacityAtTime(19).getMemorySize());
    Assert.assertEquals(102400, rleSparseVector.getCapacityAtTime(21)
        .getMemorySize());
    Assert.assertEquals(2 * 102400, rleSparseVector.getCapacityAtTime(26)
        .getMemorySize());

    ReservationInterval riRemove2 = new ReservationInterval(9, 13);
    rleSparseVector.removeInterval(riRemove2, rr);
    LOG.info(rleSparseVector.toString());

    Assert.assertEquals(0, rleSparseVector.getCapacityAtTime(11).getMemorySize());
    Assert.assertEquals(-102400, rleSparseVector.getCapacityAtTime(9)
        .getMemorySize());
    Assert.assertEquals(0, rleSparseVector.getCapacityAtTime(13).getMemorySize());
    Assert.assertEquals(102400, rleSparseVector.getCapacityAtTime(20)
        .getMemorySize());

  }

  @Test
  public void testSteps() {
    ResourceCalculator resCalc = new DefaultResourceCalculator();

    RLESparseResourceAllocation rleSparseVector =
        new RLESparseResourceAllocation(resCalc);
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    Set<Entry<ReservationInterval, Resource>> inputs =
        generateAllocation(start, alloc, true).entrySet();
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.addInterval(ip.getKey(), ip.getValue());
    }
    LOG.info(rleSparseVector.toString());
    Assert.assertFalse(rleSparseVector.isEmpty());
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(99));
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(start + alloc.length + 1));
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(
          Resource.newInstance(1024 * (alloc[i] + i), (alloc[i] + i)),
          rleSparseVector.getCapacityAtTime(start + i));
    }
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(start + alloc.length + 2));
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.removeInterval(ip.getKey(), ip.getValue());
    }
    LOG.info(rleSparseVector.toString());
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(Resource.newInstance(0, 0),
          rleSparseVector.getCapacityAtTime(start + i));
    }
    Assert.assertTrue(rleSparseVector.isEmpty());
  }

  @Test
  public void testSkyline() {
    ResourceCalculator resCalc = new DefaultResourceCalculator();

    RLESparseResourceAllocation rleSparseVector =
        new RLESparseResourceAllocation(resCalc);
    int[] alloc = { 0, 5, 10, 10, 5, 0 };
    int start = 100;
    Set<Entry<ReservationInterval, Resource>> inputs =
        generateAllocation(start, alloc, true).entrySet();
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.addInterval(ip.getKey(), ip.getValue());
    }
    LOG.info(rleSparseVector.toString());
    Assert.assertFalse(rleSparseVector.isEmpty());
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(99));
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(start + alloc.length + 1));
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(
          Resource.newInstance(1024 * (alloc[i] + i), (alloc[i] + i)),
          rleSparseVector.getCapacityAtTime(start + i));
    }
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(start + alloc.length + 2));
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.removeInterval(ip.getKey(), ip.getValue());
    }
    LOG.info(rleSparseVector.toString());
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(Resource.newInstance(0, 0),
          rleSparseVector.getCapacityAtTime(start + i));
    }
    Assert.assertTrue(rleSparseVector.isEmpty());
  }

  @Test
  public void testZeroAllocation() {
    ResourceCalculator resCalc = new DefaultResourceCalculator();
    RLESparseResourceAllocation rleSparseVector =
        new RLESparseResourceAllocation(resCalc);
    rleSparseVector.addInterval(new ReservationInterval(0, Long.MAX_VALUE),
        Resource.newInstance(0, 0));
    LOG.info(rleSparseVector.toString());
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(new Random().nextLong()));
    Assert.assertTrue(rleSparseVector.isEmpty());
  }

  @Test
  public void testToIntervalMap() {
    ResourceCalculator resCalc = new DefaultResourceCalculator();
    RLESparseResourceAllocation rleSparseVector =
        new RLESparseResourceAllocation(resCalc);
    Map<ReservationInterval, Resource> mapAllocations;

    // Check empty
    mapAllocations = rleSparseVector.toIntervalMap();
    Assert.assertTrue(mapAllocations.isEmpty());

    // Check full
    int[] alloc = { 0, 5, 10, 10, 5, 0, 5, 0 };
    int start = 100;
    Set<Entry<ReservationInterval, Resource>> inputs =
        generateAllocation(start, alloc, false).entrySet();
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.addInterval(ip.getKey(), ip.getValue());
    }
    mapAllocations = rleSparseVector.toIntervalMap();
    Assert.assertTrue(mapAllocations.size() == 5);
    for (Entry<ReservationInterval, Resource> entry : mapAllocations.entrySet()) {
      ReservationInterval interval = entry.getKey();
      Resource resource = entry.getValue();
      if (interval.getStartTime() == 101L) {
        Assert.assertTrue(interval.getEndTime() == 102L);
        Assert.assertEquals(resource, Resource.newInstance(5 * 1024, 5));
      } else if (interval.getStartTime() == 102L) {
        Assert.assertTrue(interval.getEndTime() == 104L);
        Assert.assertEquals(resource, Resource.newInstance(10 * 1024, 10));
      } else if (interval.getStartTime() == 104L) {
        Assert.assertTrue(interval.getEndTime() == 105L);
        Assert.assertEquals(resource, Resource.newInstance(5 * 1024, 5));
      } else if (interval.getStartTime() == 105L) {
        Assert.assertTrue(interval.getEndTime() == 106L);
        Assert.assertEquals(resource, Resource.newInstance(0 * 1024, 0));
      } else if (interval.getStartTime() == 106L) {
        Assert.assertTrue(interval.getEndTime() == 107L);
        Assert.assertEquals(resource, Resource.newInstance(5 * 1024, 5));
      } else {
        Assert.fail();
      }
    }
  }

  private void setupArrays(TreeMap<Long, Resource> a, TreeMap<Long, Resource> b) {
    a.put(10L, Resource.newInstance(5, 5));
    a.put(20L, Resource.newInstance(10, 10));
    a.put(30L, Resource.newInstance(15, 15));
    a.put(40L, Resource.newInstance(20, 20));
    a.put(50L, Resource.newInstance(0, 0));

    b.put(11L, Resource.newInstance(5, 5));
    b.put(22L, Resource.newInstance(10, 10));
    b.put(33L, Resource.newInstance(15, 15));
    b.put(40L, Resource.newInstance(20, 20));
    b.put(42L, Resource.newInstance(20, 20));
    b.put(43L, Resource.newInstance(10, 10));
  }

  private void validate(RLESparseResourceAllocation out, long[] time,
      int[] alloc) {
    int i = 0;
    for (Entry<Long, Resource> res : out.getCumulative().entrySet()) {
      assertEquals(time[i], ((long) res.getKey()));
      if (i > alloc.length - 1) {
        assertNull(res.getValue());
      } else {
        assertEquals(alloc[i], res.getValue().getVirtualCores());
      }
      i++;
    }
    assertEquals(time.length, i);
  }

  private Map<ReservationInterval, Resource> generateAllocation(int startTime,
      int[] alloc, boolean isStep) {
    Map<ReservationInterval, Resource> req =
        new HashMap<ReservationInterval, Resource>();
    int numContainers = 0;
    for (int i = 0; i < alloc.length; i++) {
      if (isStep) {
        numContainers = alloc[i] + i;
      } else {
        numContainers = alloc[i];
      }
      req.put(new ReservationInterval(startTime + i, startTime + i + 1),
          ReservationSystemUtil.toResource(ReservationRequest.newInstance(
              Resource.newInstance(1024, 1), (numContainers))));
    }
    return req;
  }

}
