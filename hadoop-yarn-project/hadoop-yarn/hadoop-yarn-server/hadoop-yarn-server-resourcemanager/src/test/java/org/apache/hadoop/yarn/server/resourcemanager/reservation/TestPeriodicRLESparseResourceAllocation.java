/******************************************************************************
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
 *****************************************************************************/

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing the class {@link PeriodicRLESparseResourceAllocation}.
 */
@SuppressWarnings("checkstyle:nowhitespaceafter")
public class TestPeriodicRLESparseResourceAllocation {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestPeriodicRLESparseResourceAllocation.class);

  @Test
  public void testPeriodicCapacity() {
    int[] alloc = { 10, 7, 5, 2, 0 };
    long[] timeSteps = { 0L, 5L, 10L, 15L, 19L };
    RLESparseResourceAllocation rleSparseVector = ReservationSystemTestUtil
        .generateRLESparseResourceAllocation(alloc, timeSteps);
    PeriodicRLESparseResourceAllocation periodicVector =
        new PeriodicRLESparseResourceAllocation(rleSparseVector, 20L);
    LOG.info(periodicVector.toString());
    Assert.assertEquals(Resource.newInstance(5, 5),
        periodicVector.getCapacityAtTime(10L));
    Assert.assertEquals(Resource.newInstance(10, 10),
        periodicVector.getCapacityAtTime(20L));
    Assert.assertEquals(Resource.newInstance(7, 7),
        periodicVector.getCapacityAtTime(27L));
    Assert.assertEquals(Resource.newInstance(5, 5),
        periodicVector.getCapacityAtTime(50L));
  }

  @Test
  public void testMaxPeriodicCapacity() {
    int[] alloc = { 2, 5, 7, 10, 3, 4, 6, 8 };
    long[] timeSteps = { 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L };
    RLESparseResourceAllocation rleSparseVector = ReservationSystemTestUtil
        .generateRLESparseResourceAllocation(alloc, timeSteps);
    PeriodicRLESparseResourceAllocation periodicVector =
        new PeriodicRLESparseResourceAllocation(rleSparseVector, 8L);
    LOG.info(periodicVector.toString());
    Assert.assertEquals(periodicVector.getMaximumPeriodicCapacity(0, 1),
        Resource.newInstance(10, 10));
    Assert.assertEquals(periodicVector.getMaximumPeriodicCapacity(8, 2),
        Resource.newInstance(7, 7));
    Assert.assertEquals(periodicVector.getMaximumPeriodicCapacity(16, 3),
        Resource.newInstance(10, 10));
    Assert.assertEquals(periodicVector.getMaximumPeriodicCapacity(17, 4),
        Resource.newInstance(5, 5));
    Assert.assertEquals(periodicVector.getMaximumPeriodicCapacity(32, 5),
        Resource.newInstance(4, 4));
  }

  @Test
  public void testMixPeriodicAndNonPeriodic() throws PlanningException {
    int[] alloc = { 2, 5, 0 };
    long[] timeSteps = { 1L, 2L, 3L };
    RLESparseResourceAllocation tempPeriodic = ReservationSystemTestUtil
        .generateRLESparseResourceAllocation(alloc, timeSteps);
    PeriodicRLESparseResourceAllocation periodic =
        new PeriodicRLESparseResourceAllocation(tempPeriodic, 10L);

    int[] alloc2 = { 10, 10, 0 };
    long[] timeSteps2 = { 12L, 13L, 14L };
    RLESparseResourceAllocation nonPeriodic = ReservationSystemTestUtil
        .generateRLESparseResourceAllocation(alloc2, timeSteps2);

    RLESparseResourceAllocation merged =
        RLESparseResourceAllocation.merge(nonPeriodic.getResourceCalculator(),
            Resource.newInstance(100 * 1024, 100), periodic, nonPeriodic,
            RLESparseResourceAllocation.RLEOperator.add, 2, 25);

    Assert.assertEquals(Resource.newInstance(5, 5),
        merged.getCapacityAtTime(2L));
    Assert.assertEquals(Resource.newInstance(0, 0),
        merged.getCapacityAtTime(3L));
    Assert.assertEquals(Resource.newInstance(2, 2),
        merged.getCapacityAtTime(11L));
    Assert.assertEquals(Resource.newInstance(15, 15),
        merged.getCapacityAtTime(12L));
    Assert.assertEquals(Resource.newInstance(10, 10),
        merged.getCapacityAtTime(13L));
    Assert.assertEquals(Resource.newInstance(0, 0),
        merged.getCapacityAtTime(14L));
    Assert.assertEquals(Resource.newInstance(2, 2),
        merged.getCapacityAtTime(21L));
    Assert.assertEquals(Resource.newInstance(5, 5),
        merged.getCapacityAtTime(22L));
    Assert.assertEquals(Resource.newInstance(0, 0),
        merged.getCapacityAtTime(23L));
  }

  @Test
  public void testSetCapacityInInterval() {
    int[] alloc = { 2, 5, 0 };
    long[] timeSteps = { 1L, 2L, 3L };
    RLESparseResourceAllocation rleSparseVector = ReservationSystemTestUtil
        .generateRLESparseResourceAllocation(alloc, timeSteps);
    PeriodicRLESparseResourceAllocation periodicVector =
        new PeriodicRLESparseResourceAllocation(rleSparseVector, 10L);
    ReservationInterval interval = new ReservationInterval(5L, 10L);
    periodicVector.addInterval(interval, Resource.newInstance(8, 8));
    Assert.assertEquals(Resource.newInstance(8, 8),
        periodicVector.getCapacityAtTime(5L));
    Assert.assertEquals(Resource.newInstance(8, 8),
        periodicVector.getCapacityAtTime(9L));
    Assert.assertEquals(Resource.newInstance(0, 0),
        periodicVector.getCapacityAtTime(10L));
    Assert.assertEquals(Resource.newInstance(0, 0),
        periodicVector.getCapacityAtTime(0L));
    // Assert.assertFalse(periodicVector.addInterval(
    // new ReservationInterval(7L, 12L), Resource.newInstance(8, 8)));
  }

  public void testRemoveInterval() {
    int[] alloc = { 2, 5, 3, 4, 0 };
    long[] timeSteps = { 1L, 3L, 5L, 7L, 9L };
    RLESparseResourceAllocation rleSparseVector = ReservationSystemTestUtil
        .generateRLESparseResourceAllocation(alloc, timeSteps);
    PeriodicRLESparseResourceAllocation periodicVector =
        new PeriodicRLESparseResourceAllocation(rleSparseVector, 10L);
    ReservationInterval interval = new ReservationInterval(3L, 7L);
    Assert.assertTrue(
        periodicVector.removeInterval(interval, Resource.newInstance(3, 3)));
    Assert.assertEquals(Resource.newInstance(2, 2),
        periodicVector.getCapacityAtTime(1L));
    Assert.assertEquals(Resource.newInstance(2, 2),
        periodicVector.getCapacityAtTime(2L));
    Assert.assertEquals(Resource.newInstance(2, 2),
        periodicVector.getCapacityAtTime(3L));
    Assert.assertEquals(Resource.newInstance(2, 2),
        periodicVector.getCapacityAtTime(4L));
    Assert.assertEquals(Resource.newInstance(0, 0),
        periodicVector.getCapacityAtTime(5L));
    Assert.assertEquals(Resource.newInstance(0, 0),
        periodicVector.getCapacityAtTime(6L));
    Assert.assertEquals(Resource.newInstance(4, 4),
        periodicVector.getCapacityAtTime(7L));

    // invalid interval
    Assert.assertFalse(periodicVector.removeInterval(
        new ReservationInterval(7L, 12L), Resource.newInstance(1, 1)));

    // invalid capacity
    Assert.assertFalse(periodicVector.removeInterval(
        new ReservationInterval(2L, 4L), Resource.newInstance(8, 8)));

  }

}
