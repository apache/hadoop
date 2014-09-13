package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRLESparseResourceAllocation {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestRLESparseResourceAllocation.class);

  @Test
  public void testBlocks() {
    ResourceCalculator resCalc = new DefaultResourceCalculator();
    Resource minAlloc = Resource.newInstance(1, 1);

    RLESparseResourceAllocation rleSparseVector =
        new RLESparseResourceAllocation(resCalc, minAlloc);
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    Set<Entry<ReservationInterval, ReservationRequest>> inputs =
        generateAllocation(start, alloc, false).entrySet();
    for (Entry<ReservationInterval, ReservationRequest> ip : inputs) {
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
    for (Entry<ReservationInterval, ReservationRequest> ip : inputs) {
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
  public void testSteps() {
    ResourceCalculator resCalc = new DefaultResourceCalculator();
    Resource minAlloc = Resource.newInstance(1, 1);

    RLESparseResourceAllocation rleSparseVector =
        new RLESparseResourceAllocation(resCalc, minAlloc);
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    Set<Entry<ReservationInterval, ReservationRequest>> inputs =
        generateAllocation(start, alloc, true).entrySet();
    for (Entry<ReservationInterval, ReservationRequest> ip : inputs) {
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
    for (Entry<ReservationInterval, ReservationRequest> ip : inputs) {
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
    Resource minAlloc = Resource.newInstance(1, 1);

    RLESparseResourceAllocation rleSparseVector =
        new RLESparseResourceAllocation(resCalc, minAlloc);
    int[] alloc = { 0, 5, 10, 10, 5, 0 };
    int start = 100;
    Set<Entry<ReservationInterval, ReservationRequest>> inputs =
        generateAllocation(start, alloc, true).entrySet();
    for (Entry<ReservationInterval, ReservationRequest> ip : inputs) {
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
    for (Entry<ReservationInterval, ReservationRequest> ip : inputs) {
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
  public void testZeroAlloaction() {
    ResourceCalculator resCalc = new DefaultResourceCalculator();
    Resource minAlloc = Resource.newInstance(1, 1);
    RLESparseResourceAllocation rleSparseVector =
        new RLESparseResourceAllocation(resCalc, minAlloc);
    rleSparseVector.addInterval(new ReservationInterval(0, Long.MAX_VALUE),
        ReservationRequest.newInstance(Resource.newInstance(0, 0), (0)));
    LOG.info(rleSparseVector.toString());
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(new Random().nextLong()));
    Assert.assertTrue(rleSparseVector.isEmpty());
  }

  private Map<ReservationInterval, ReservationRequest> generateAllocation(
      int startTime, int[] alloc, boolean isStep) {
    Map<ReservationInterval, ReservationRequest> req =
        new HashMap<ReservationInterval, ReservationRequest>();
    int numContainers = 0;
    for (int i = 0; i < alloc.length; i++) {
      if (isStep) {
        numContainers = alloc[i] + i;
      } else {
        numContainers = alloc[i];
      }
      req.put(new ReservationInterval(startTime + i, startTime + i + 1),

      ReservationRequest.newInstance(Resource.newInstance(1024, 1),
          (numContainers)));
    }
    return req;
  }

}
