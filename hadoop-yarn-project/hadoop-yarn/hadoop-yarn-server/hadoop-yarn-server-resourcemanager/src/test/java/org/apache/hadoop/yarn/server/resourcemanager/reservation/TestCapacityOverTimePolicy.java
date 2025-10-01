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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import net.jcip.annotations.NotThreadSafe;

import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningQuotaException;
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

}