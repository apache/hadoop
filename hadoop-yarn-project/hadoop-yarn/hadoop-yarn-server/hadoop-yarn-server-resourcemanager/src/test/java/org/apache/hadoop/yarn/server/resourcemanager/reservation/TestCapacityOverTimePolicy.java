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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import net.jcip.annotations.NotThreadSafe;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningQuotaException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * This class tests the {@code CapacityOvertimePolicy} sharing policy.
 */
@NotThreadSafe
@SuppressWarnings("VisibilityModifier")
public class TestCapacityOverTimePolicy extends BaseSharingPolicyTest {

  final static long ONEDAY = 86400 * 1000;
  final static long ONEHOUR = 3600 * 1000;
  final static long ONEMINUTE = 60 * 1000;
  final static String TWOHOURPERIOD = "7200000";
  final static String ONEDAYPERIOD = "86400000";

  public void initTestCapacityOverTimePolicy(long pDuration,
      double pHeight, int pNumSubmissions, String pRecurrenceExpression, Class pExpectedError) {
    this.duration = pDuration;
    this.height = pHeight;
    this.numSubmissions = pNumSubmissions;
    this.recurrenceExpression = pRecurrenceExpression;
    this.expectedError = pExpectedError;
    super.setup();
  }

  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {

        // easy fit
        {ONEHOUR, 0.25, 1, null, null },
        {ONEHOUR, 0.25, 1, TWOHOURPERIOD, null },
        {ONEHOUR, 0.25, 1, ONEDAYPERIOD, null },

        // instantaneous high, but fit integral and inst limits
        {ONEMINUTE, 0.74, 1, null, null },
        {ONEMINUTE, 0.74, 1, TWOHOURPERIOD, null },
        {ONEMINUTE, 0.74, 1, ONEDAYPERIOD, null },

        // barely fit
        {ONEHOUR, 0.76, 1, null, PlanningQuotaException.class },
        {ONEHOUR, 0.76, 1, TWOHOURPERIOD, PlanningQuotaException.class },
        {ONEHOUR, 0.76, 1, ONEDAYPERIOD, PlanningQuotaException.class },

        // overcommit with single reservation
        {ONEHOUR, 1.1, 1, null, PlanningQuotaException.class },
        {ONEHOUR, 1.1, 1, TWOHOURPERIOD, PlanningQuotaException.class },
        {ONEHOUR, 1.1, 1, ONEDAYPERIOD, PlanningQuotaException.class },

        // barely fit with multiple reservations (instantaneously, lowering to
        // 1min to fit integral)
        {ONEMINUTE, 0.25, 3, null, null },
        {ONEMINUTE, 0.25, 3, TWOHOURPERIOD, null },
        {ONEMINUTE, 0.25, 3, ONEDAYPERIOD, null },

        // overcommit with multiple reservations (instantaneously)
        {ONEMINUTE, 0.25, 4, null, PlanningQuotaException.class },
        {ONEMINUTE, 0.25, 4, TWOHOURPERIOD, PlanningQuotaException.class },
        {ONEMINUTE, 0.25, 4, ONEDAYPERIOD, PlanningQuotaException.class },

        // (non-periodic) reservation longer than window
        {25 * ONEHOUR, 0.25, 1, null, PlanningQuotaException.class },
        // NOTE: we generally don't accept periodicity < duration but the test
        // generator will "wrap" this correctly
        {25 * ONEHOUR, 0.25, 1, TWOHOURPERIOD, PlanningQuotaException.class },
        {25 * ONEHOUR, 0.25, 1, ONEDAYPERIOD, PlanningQuotaException.class },

        // (non-periodic) reservation longer than window
        {25 * ONEHOUR, 0.05, 5, null, PlanningQuotaException.class },
        // NOTE: we generally don't accept periodicity < duration but the test
        // generator will "wrap" this correctly
        {25 * ONEHOUR, 0.05, 5, TWOHOURPERIOD, PlanningQuotaException.class },
        {25 * ONEHOUR, 0.05, 5, ONEDAYPERIOD, PlanningQuotaException.class },

        // overcommit integral
        {ONEDAY, 0.26, 1, null, PlanningQuotaException.class },
        {2 * ONEHOUR, 0.26, 1, TWOHOURPERIOD, PlanningQuotaException.class },
        {2 * ONEDAY, 0.26, 1, ONEDAYPERIOD, PlanningQuotaException.class },

        // overcommit integral
        {ONEDAY / 2, 0.51, 1, null, PlanningQuotaException.class },
        {2 * ONEHOUR / 2, 0.51, 1, TWOHOURPERIOD,
            PlanningQuotaException.class },
        {2 * ONEDAY / 2, 0.51, 1, ONEDAYPERIOD, PlanningQuotaException.class }

    });
  }

  @Override
  public SharingPolicy getInitializedPolicy() {

    // 24h window
    long timeWindow = 86400000L;

    // 1 sec step
    long step = 1000L;

    // 25% avg cap on capacity
    float avgConstraint = 25;

    // 70% instantaneous cap on capacity
    float instConstraint = 75;

    String reservationQ =
        ReservationSystemTestUtil.getFullReservationQueueName();
    conf = ReservationSystemTestUtil.createConf(reservationQ, timeWindow,
        instConstraint, avgConstraint);
    CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
    policy.init(reservationQ, conf);
    return policy;
  }

  @ParameterizedTest(name = "Duration {0}, height {1}," +
      " numSubmission {2}, periodic {3})")
  @MethodSource("data")
  public void testAllocation(long pDuration, double pHeight, int pNumSubmissions,
      String pRecurrenceExpression, Class pExpectedError)
      throws IOException, PlanningException {
    initTestCapacityOverTimePolicy(pDuration, pHeight, pNumSubmissions,
        pRecurrenceExpression, pExpectedError);
    runTest();
  }

  /**
   * A reservation of the same user that is in progress at the end of the
   * window checked by the policy (the reservation end + 24h) must not
   * make the integral check fail: both reservations below are far within
   * the 25% average and 75% instantaneous limits.
   */
  @Test
  public void testUserAllocationInProgressAtEndOfCheckedWindow()
      throws PlanningException {
    InMemoryPlan plan = createPlan();
    // an arbitrary fixed start time
    long t0 = 1791808200000L;

    // 10% of the cluster for 2h, in progress at t0 + 31h, the end of the
    // window checked for the next reservation (its end + 24h)
    assertTrue(plan.addReservation(
        createReservation(t0 + 30 * ONEHOUR, t0 + 32 * ONEHOUR, 0.1, null),
        false));
    // 10% of the cluster for 1h, ending at t0 + 7h
    assertTrue(plan.addReservation(
        createReservation(t0 + 6 * ONEHOUR, t0 + 7 * ONEHOUR, 0.1, null),
        false));
  }

  /**
   * Same as {@link #testUserAllocationInProgressAtEndOfCheckedWindow()}
   * with daily periodic reservations: three reservations of 5% of the
   * cluster for 1h each, crossing the period boundary, fit in the limits.
   */
  @Test
  public void testPeriodicAllocationCrossingPeriodBoundary()
      throws PlanningException {
    InMemoryPlan plan = createPlan();
    for (int i = 0; i < 3; i++) {
      // 23:30 to 00:30 of each day
      assertTrue(plan.addReservation(createReservation(
          23 * ONEHOUR + 30 * ONEMINUTE, ONEDAY + 30 * ONEMINUTE, 0.05,
          ONEDAYPERIOD), false));
    }
  }

  /**
   * Updating a reservation must not count its old version on top of the
   * user's consumption, which already includes it: the update below keeps
   * the reservation far within the 25% average limit.
   */
  @Test
  public void testUpdateReservationWithinQuota() throws PlanningException {
    InMemoryPlan plan = createPlan();
    // an arbitrary fixed start time
    long t0 = 1791808200000L;
    ReservationId id = ReservationSystemTestUtil.getNewReservationId();

    // 20% of the cluster for 20h, 16.7% on average over the 24h window
    assertTrue(plan.addReservation(
        createReservation(id, t0, t0 + 20 * ONEHOUR, 0.2, null), false));
    // 20% of the cluster for 21h, 17.5% on average
    assertTrue(plan.updateReservation(
        createReservation(id, t0, t0 + 21 * ONEHOUR, 0.2, null)));
    // 30% of the cluster for 21h, 26.25% on average, exceeds the limit
    assertThrows(PlanningQuotaException.class, () -> plan.updateReservation(
        createReservation(id, t0, t0 + 21 * ONEHOUR, 0.3, null)));
  }

  /**
   * Updating a reservation must also be accepted when another reservation
   * of the same user is in progress at the end of the window checked by
   * the policy (the reservation end + 24h): the subtraction merge of the
   * old version inserts an end marker there. Both reservations below are
   * far within the 25% average and 75% instantaneous limits, while an
   * update exceeding the average limit is still rejected.
   */
  @Test
  public void testUpdateReservationWithAllocationInProgressAtCheckedWindowEnd()
      throws PlanningException {
    InMemoryPlan plan = createPlan();
    // an arbitrary fixed start time
    long t0 = 1791808200000L;
    ReservationId id = ReservationSystemTestUtil.getNewReservationId();

    // 10% of the cluster for 10h, in progress at t0 + 32h, the end of the
    // window checked for the reservation being updated (its end + 24h)
    assertTrue(plan.addReservation(
        createReservation(t0 + 30 * ONEHOUR, t0 + 40 * ONEHOUR, 0.1, null),
        false));
    // 10% of the cluster for 7h, 2.9% on average over a 24h window
    assertTrue(plan.addReservation(
        createReservation(id, t0, t0 + 7 * ONEHOUR, 0.1, null), false));
    // 10% of the cluster for 8h, 3.3% on average, within the quota
    assertTrue(plan.updateReservation(
        createReservation(id, t0, t0 + 8 * ONEHOUR, 0.1, null)));
    // 70% of the cluster for 9h, 26.25% on average, exceeds the limit
    assertThrows(PlanningQuotaException.class, () -> plan.updateReservation(
        createReservation(id, t0, t0 + 9 * ONEHOUR, 0.7, null)));
  }

  /**
   * An allocation of the same user starting after the end of the window
   * checked by the policy (the reservation end + 24h) must not affect
   * the validation of an update within that window: the consumption
   * views end at the checked window end, so the update below is
   * accepted with the later reservation in place.
   */
  @Test
  public void testUpdateReservationWithAllocationBeyondTheCheckedWindowBound()
      throws PlanningException {
    InMemoryPlan plan = createPlan();
    // an arbitrary fixed start time
    long t0 = 1791808200000L;
    ReservationId id = ReservationSystemTestUtil.getNewReservationId();

    // 10% of the cluster for 10h, in progress at t0 + 32h, the end of the
    // window checked for the reservation being updated (its end + 24h)
    assertTrue(plan.addReservation(
        createReservation(t0 + 30 * ONEHOUR, t0 + 40 * ONEHOUR, 0.1, null),
        false));
    // 10% of the cluster for 2h, starting beyond t0 + 32h, the end of the
    // window checked for the reservation being updated (its end + 24h)
    assertTrue(plan.addReservation(
        createReservation(t0 + 60 * ONEHOUR, t0 + 62 * ONEHOUR, 0.1, null),
        false));
    // 10% of the cluster for 7h, 2.9% on average over a 24h window
    assertTrue(plan.addReservation(
        createReservation(id, t0, t0 + 7 * ONEHOUR, 0.1, null), false));
    // 10% of the cluster for 8h, 3.3% on average, within the quota
    assertTrue(plan.updateReservation(
        createReservation(id, t0, t0 + 8 * ONEHOUR, 0.1, null)));
  }

  /**
   * A shrinking update must be accepted when the old version of the
   * reservation is still in progress at the end of the checked window
   * (the new end + 24h): the subtraction merge has to clip the old
   * allocation, which extends beyond the checked window, and the updates
   * below stay within the limits.
   */
  @Test
  public void testShrinkingUpdateWithOldAllocationInProgressAtCheckedWindowEnd()
      throws PlanningException {
    InMemoryPlan plan = createPlan();
    // an arbitrary fixed start time
    long t0 = 1791808200000L;
    ReservationId id = ReservationSystemTestUtil.getNewReservationId();

    // 10% of the cluster for 10h, in progress at t0 + 31h and t0 + 33h,
    // the ends of the windows checked for the updates below
    assertTrue(plan.addReservation(
        createReservation(t0 + 30 * ONEHOUR, t0 + 40 * ONEHOUR, 0.1, null),
        false));
    // 10% of the cluster for 40h, 16.7% on average over a 24h window
    assertTrue(plan.addReservation(
        createReservation(id, t0, t0 + 40 * ONEHOUR, 0.1, null), false));
    // shrink to 10% of the cluster for 7h: the old version is still in
    // progress at the end of the checked window, t0 + 31h
    assertTrue(plan.updateReservation(
        createReservation(id, t0, t0 + 7 * ONEHOUR, 0.1, null)));
    // 70% of the cluster for 9h, 26.25% on average, exceeds the limit
    assertThrows(PlanningQuotaException.class, () -> plan.updateReservation(
        createReservation(id, t0, t0 + 9 * ONEHOUR, 0.7, null)));
  }

  /**
   * An update within the quota on its own must still be rejected when the
   * consumption it adds on top of another reservation of the same user
   * exceeds the average limit: 40% of the cluster for 10h plus the
   * concurrent 10% for 10h below give 20.8% on average over a 24h window
   * and are accepted, while 55% for 10h would be 22.9% alone but 27.1%
   * combined, and is rejected.
   */
  @Test
  public void testUpdateReservationRejectedByCombinedConsumption()
      throws PlanningException {
    InMemoryPlan plan = createPlan();
    // an arbitrary fixed start time
    long t0 = 1791808200000L;
    ReservationId id = ReservationSystemTestUtil.getNewReservationId();

    // 10% of the cluster for 10h, 4.2% on average
    assertTrue(plan.addReservation(
        createReservation(id, t0, t0 + 10 * ONEHOUR, 0.1, null), false));
    // 10% of the cluster for 10h, 4.2% on average, within a 24h window of
    // the reservation being updated
    assertTrue(plan.addReservation(
        createReservation(t0 + 14 * ONEHOUR, t0 + 24 * ONEHOUR, 0.1, null),
        false));
    // 40% of the cluster for 10h, 16.7% alone and 20.8% combined with the
    // concurrent reservation, within the limit
    assertTrue(plan.updateReservation(
        createReservation(id, t0, t0 + 10 * ONEHOUR, 0.4, null)));
    // 55% of the cluster for 10h, 22.9% alone but 27.1% combined, exceeds
    // the limit
    assertThrows(PlanningQuotaException.class, () -> plan.updateReservation(
        createReservation(id, t0, t0 + 10 * ONEHOUR, 0.55, null)));
  }

  private InMemoryPlan createPlan() {
    return new InMemoryPlan(mock(QueueMetrics.class), getInitializedPolicy(),
        mock(ReservationAgent.class),
        ReservationSystemTestUtil.calculateClusterResource(1000), 1000L,
        new DefaultResourceCalculator(), Resource.newInstance(1024, 1),
        Resource.newInstance(1024 * 8, 8), "dedicated", null, true,
        ReservationSystemTestUtil.createMockRMContext());
  }

  /**
   * Creates a reservation of user u1 using the given share of a 1000
   * containers cluster over [start, end), folded onto the period (if
   * any), where end - start must not exceed the period.
   */
  private InMemoryReservationAllocation createReservation(long start,
      long end, double share, String recurrenceExpression) {
    return createReservation(ReservationSystemTestUtil.getNewReservationId(),
        start, end, share, recurrenceExpression);
  }

  private InMemoryReservationAllocation createReservation(ReservationId id,
      long start, long end, double share, String recurrenceExpression) {
    ResourceCalculator rc = new DefaultResourceCalculator();
    Resource minAlloc = Resource.newInstance(1024, 1);
    Resource alloc = Resources.multiply(minAlloc, share * 1000);
    Map<ReservationInterval, Resource> allocations = new TreeMap<>();
    long rStart = start;
    long rEnd = end;
    if (recurrenceExpression != null) {
      long period = Long.parseLong(recurrenceExpression);
      rStart = start % period;
      rEnd = rStart + end - start;
      if (rEnd > period) {
        allocations.put(new ReservationInterval(rStart, period), alloc);
        allocations.put(new ReservationInterval(0, rEnd - period), alloc);
        rStart = 0;
        rEnd = period;
      } else {
        allocations.put(new ReservationInterval(rStart, rEnd), alloc);
      }
    } else {
      allocations.put(new ReservationInterval(rStart, rEnd), alloc);
    }
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(start,
            end, end - start, 1, recurrenceExpression);
    return new InMemoryReservationAllocation(id, rDef, "u1", "dedicated",
        rStart, rEnd, allocations, rc, minAlloc);
  }
}
