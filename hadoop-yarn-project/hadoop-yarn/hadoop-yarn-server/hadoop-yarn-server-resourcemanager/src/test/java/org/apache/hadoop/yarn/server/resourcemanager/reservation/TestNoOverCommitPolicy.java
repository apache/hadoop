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
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ResourceOverCommitException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This clas tests {@code NoOverCommitPolicy} sharing policy.
 */
@RunWith(value = Parameterized.class)
@NotThreadSafe
@SuppressWarnings("VisibilityModifier")
public class TestNoOverCommitPolicy extends BaseSharingPolicyTest {

  final static long ONEHOUR = 3600 * 1000;
  final static String TWOHOURPERIOD = "7200000";

  @Parameterized.Parameters(name = "Duration {0}, height {1}," +
          " submissions {2}, periodic {3})")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {

        // easy fit
        {ONEHOUR, 0.25, 1, null, null },
        {ONEHOUR, 0.25, 1, TWOHOURPERIOD, null },

        // barely fit
        {ONEHOUR, 1, 1, null, null },
        {ONEHOUR, 1, 1, TWOHOURPERIOD, null },

        // overcommit with single reservation
        {ONEHOUR, 1.1, 1, null, ResourceOverCommitException.class },
        {ONEHOUR, 1.1, 1, TWOHOURPERIOD, ResourceOverCommitException.class },

        // barely fit with multiple reservations
        {ONEHOUR, 0.25, 4, null, null },
        {ONEHOUR, 0.25, 4, TWOHOURPERIOD, null },

        // overcommit with multiple reservations
        {ONEHOUR, 0.25, 5, null, ResourceOverCommitException.class },
        {ONEHOUR, 0.25, 5, TWOHOURPERIOD, ResourceOverCommitException.class }

    });
  }

  @Override
  public SharingPolicy getInitializedPolicy() {
    String reservationQ =
        ReservationSystemTestUtil.getFullReservationQueueName();
    conf = new CapacitySchedulerConfiguration();
    SharingPolicy policy = new NoOverCommitPolicy();
    policy.init(reservationQ, conf);
    return policy;
  }

  @Test
  public void testAllocation() throws IOException, PlanningException {
    runTest();
  }

}