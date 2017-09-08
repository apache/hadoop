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

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.jcip.annotations.NotThreadSafe;

/**
 * This class is a base test for {@code SharingPolicy} implementors.
 */
@RunWith(value = Parameterized.class)
@NotThreadSafe
@SuppressWarnings("VisibilityModifier")
public abstract class BaseSharingPolicyTest {

  @Parameterized.Parameter(value = 0)
  public long duration;

  @Parameterized.Parameter(value = 1)
  public double height;

  @Parameterized.Parameter(value = 2)
  public int numSubmissions;

  @Parameterized.Parameter(value = 3)
  public String recurrenceExpression;

  @Parameterized.Parameter(value = 4)
  public Class expectedError;

  private long step;
  private long initTime;

  private InMemoryPlan plan;
  private ReservationAgent mAgent;
  private Resource minAlloc;
  private ResourceCalculator res;
  private Resource maxAlloc;

  private int totCont = 1000;

  protected ReservationSchedulerConfiguration conf;

  @Before
  public void setup() {
    // 1 sec step
    step = 1000L;
    initTime = System.currentTimeMillis();

    minAlloc = Resource.newInstance(1024, 1);
    res = new DefaultResourceCalculator();
    maxAlloc = Resource.newInstance(1024 * 8, 8);

    mAgent = mock(ReservationAgent.class);

    QueueMetrics rootQueueMetrics = mock(QueueMetrics.class);
    Resource clusterResource =
        ReservationSystemTestUtil.calculateClusterResource(totCont);

    // invoke implementors initialization of policy
    SharingPolicy policy = getInitializedPolicy();

    RMContext context = ReservationSystemTestUtil.createMockRMContext();

    plan = new InMemoryPlan(rootQueueMetrics, policy, mAgent, clusterResource,
        step, res, minAlloc, maxAlloc, "dedicated", null, true, context);
  }

  public void runTest() throws IOException, PlanningException {

    long period = 1;
    if (recurrenceExpression != null) {
      period = Long.parseLong(recurrenceExpression);
    }

    try {
      RLESparseResourceAllocation rle = generateRLEAlloc(period);

      // Generate the intervalMap (trimming out-of-period entries)
      Map<ReservationInterval, Resource> reservationIntervalResourceMap;
      if (period > 1) {
        rle = new PeriodicRLESparseResourceAllocation(rle, period);
        reservationIntervalResourceMap =
            ReservationSystemTestUtil.toAllocation(rle, 0, period);
      } else {
        reservationIntervalResourceMap = ReservationSystemTestUtil
            .toAllocation(rle, Long.MIN_VALUE, Long.MAX_VALUE);
      }

      ReservationDefinition rDef =
          ReservationSystemTestUtil.createSimpleReservationDefinition(
              initTime % period, initTime % period + duration + 1, duration, 1,
              recurrenceExpression);

      // perform multiple submissions where required
      for (int i = 0; i < numSubmissions; i++) {

        long rstart = rle.getEarliestStartTime();
        long rend = rle.getLatestNonNullTime();

        InMemoryReservationAllocation resAlloc =
            new InMemoryReservationAllocation(
                ReservationSystemTestUtil.getNewReservationId(), rDef, "u1",
                "dedicated", rstart, rend, reservationIntervalResourceMap, res,
                minAlloc);

        assertTrue(plan.toString(), plan.addReservation(resAlloc, false));
      }
      // fail if error was expected
      if (expectedError != null) {
        System.out.println(plan.toString());
        fail();
      }
    } catch (Exception e) {
      if (expectedError == null || !e.getClass().getCanonicalName()
          .equals(expectedError.getCanonicalName())) {
        // fail on unexpected errors
        throw e;
      }
    }
  }

  private RLESparseResourceAllocation generateRLEAlloc(long period) {
    RLESparseResourceAllocation rle =
        new RLESparseResourceAllocation(new DefaultResourceCalculator());

    Resource alloc = Resources.multiply(minAlloc, height * totCont);

    // loop in case the periodicity of the reservation is smaller than LCM
    long rStart = initTime % period;
    long rEnd = initTime % period + duration;


    // handle wrap-around
    if (period > 1 && rEnd > period) {
      long diff = rEnd - period;
      rEnd = period;

      // handle multiple wrap-arounds (e.g., 5h duration on a 2h periodicity)
      if(duration > period) {
        rle.addInterval(new ReservationInterval(0, period),
            Resources.multiply(alloc, duration / period - 1));
        rle.addInterval(new ReservationInterval(0, diff % period), alloc);
      } else {
        rle.addInterval(new ReservationInterval(0, diff), alloc);
      }
    }


    rle.addInterval(new ReservationInterval(rStart, rEnd), alloc);
    return rle;
  }

  public abstract SharingPolicy getInitializedPolicy();
}
