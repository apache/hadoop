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
