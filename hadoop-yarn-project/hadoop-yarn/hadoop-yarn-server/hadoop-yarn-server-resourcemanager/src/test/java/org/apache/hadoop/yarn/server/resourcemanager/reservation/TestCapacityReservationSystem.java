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

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestCapacityReservationSystem {

  @Test
  public void testInitialize() {
    ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
    CapacityScheduler capScheduler = null;
    try {
      capScheduler = testUtil.mockCapacityScheduler(10);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
    CapacityReservationSystem reservationSystem =
        new CapacityReservationSystem();
    reservationSystem.setRMContext(capScheduler.getRMContext());
    try {
      reservationSystem.reinitialize(capScheduler.getConf(),
          capScheduler.getRMContext());
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }
    String planQName = testUtil.getreservationQueueName();
    Plan plan = reservationSystem.getPlan(planQName);
    Assert.assertNotNull(plan);
    Assert.assertTrue(plan instanceof InMemoryPlan);
    Assert.assertEquals(planQName, plan.getQueueName());
    Assert.assertEquals(8192, plan.getTotalCapacity().getMemory());
    Assert
        .assertTrue(plan.getReservationAgent() instanceof GreedyReservationAgent);
    Assert
        .assertTrue(plan.getSharingPolicy() instanceof CapacityOverTimePolicy);
  }

  @Test
  public void testReinitialize() {
    ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
    CapacityScheduler capScheduler = null;
    try {
      capScheduler = testUtil.mockCapacityScheduler(10);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
    CapacityReservationSystem reservationSystem =
        new CapacityReservationSystem();
    CapacitySchedulerConfiguration conf = capScheduler.getConfiguration();
    RMContext mockContext = capScheduler.getRMContext();
    reservationSystem.setRMContext(mockContext);
    try {
      reservationSystem.reinitialize(capScheduler.getConfiguration(),
          mockContext);
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }
    // Assert queue in original config
    String planQName = testUtil.getreservationQueueName();
    Plan plan = reservationSystem.getPlan(planQName);
    Assert.assertNotNull(plan);
    Assert.assertTrue(plan instanceof InMemoryPlan);
    Assert.assertEquals(planQName, plan.getQueueName());
    Assert.assertEquals(8192, plan.getTotalCapacity().getMemory());
    Assert
        .assertTrue(plan.getReservationAgent() instanceof GreedyReservationAgent);
    Assert
        .assertTrue(plan.getSharingPolicy() instanceof CapacityOverTimePolicy);

    // Dynamically add a plan
    String newQ = "reservation";
    Assert.assertNull(reservationSystem.getPlan(newQ));
    testUtil.updateQueueConfiguration(conf, newQ);
    try {
      capScheduler.reinitialize(conf, mockContext);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
    try {
      reservationSystem.reinitialize(conf, mockContext);
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }
    Plan newPlan = reservationSystem.getPlan(newQ);
    Assert.assertNotNull(newPlan);
    Assert.assertTrue(newPlan instanceof InMemoryPlan);
    Assert.assertEquals(newQ, newPlan.getQueueName());
    Assert.assertEquals(1024, newPlan.getTotalCapacity().getMemory());
    Assert
        .assertTrue(newPlan.getReservationAgent() instanceof GreedyReservationAgent);
    Assert
        .assertTrue(newPlan.getSharingPolicy() instanceof CapacityOverTimePolicy);

  }

}
