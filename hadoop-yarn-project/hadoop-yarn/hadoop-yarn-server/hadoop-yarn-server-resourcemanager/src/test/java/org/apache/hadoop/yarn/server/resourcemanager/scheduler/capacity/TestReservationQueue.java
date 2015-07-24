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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;

public class TestReservationQueue {

  CapacitySchedulerConfiguration csConf;
  CapacitySchedulerContext csContext;
  final static int DEF_MAX_APPS = 10000;
  final static int GB = 1024;
  private final ResourceCalculator resourceCalculator =
      new DefaultResourceCalculator();
  ReservationQueue reservationQueue;

  @Before
  public void setup() throws IOException {
    // setup a context / conf
    csConf = new CapacitySchedulerConfiguration();
    YarnConfiguration conf = new YarnConfiguration();
    csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getMinimumResourceCapability()).thenReturn(
        Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).thenReturn(
        Resources.createResource(16 * GB, 32));
    when(csContext.getClusterResource()).thenReturn(
        Resources.createResource(100 * 16 * GB, 100 * 32));
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    
    RMContext mockRMContext = TestUtils.getMockRMContext();
    when(csContext.getRMContext()).thenReturn(mockRMContext);

    // create a queue
    PlanQueue pq = new PlanQueue(csContext, "root", null, null);
    reservationQueue = new ReservationQueue(csContext, "a", pq);
  }

  private void validateReservationQueue(double capacity) {
    assertTrue(" actual capacity: " + reservationQueue.getCapacity(),
        reservationQueue.getCapacity() - capacity < CSQueueUtils.EPSILON);
    assertEquals(reservationQueue.maxApplications, DEF_MAX_APPS);
    assertEquals(reservationQueue.maxApplicationsPerUser, DEF_MAX_APPS);
  }

  @Test
  public void testAddSubtractCapacity() throws Exception {

    // verify that setting, adding, subtracting capacity works
    reservationQueue.setCapacity(1.0F);
    validateReservationQueue(1);
    reservationQueue.setEntitlement(new QueueEntitlement(0.9f, 1f));
    validateReservationQueue(0.9);
    reservationQueue.setEntitlement(new QueueEntitlement(1f, 1f));
    validateReservationQueue(1);
    reservationQueue.setEntitlement(new QueueEntitlement(0f, 1f));
    validateReservationQueue(0);

    try {
      reservationQueue.setEntitlement(new QueueEntitlement(1.1f, 1f));
      fail();
    } catch (SchedulerDynamicEditException iae) {
      // expected
      validateReservationQueue(1);
    }

    try {
      reservationQueue.setEntitlement(new QueueEntitlement(-0.1f, 1f));
      fail();
    } catch (SchedulerDynamicEditException iae) {
      // expected
      validateReservationQueue(1);
    }

  }
}
