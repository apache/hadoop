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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import static org.junit.Assert.assertEquals;

import java.util.*;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

public class TestFairOrderingPolicy {

  final static int GB = 1024;

  @Test
  public void testSimpleComparison() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
      new FairOrderingPolicy<MockSchedulableEntity>();
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    assertEquals("Comparator Output", 0,
        policy.getComparator().compare(r1, r2));

    //consumption
    r1.setUsed(Resources.createResource(1, 0));
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r1.getSchedulingResourceUsage());
    Assert.assertTrue(policy.getComparator().compare(r1, r2) > 0);
  }

  @Test
  public void testSizeBasedWeight() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
      new FairOrderingPolicy<MockSchedulableEntity>();
    policy.setSizeBasedWeight(true);
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    //No changes, equal
    assertEquals("Comparator Output", 0,
        policy.getComparator().compare(r1, r2));

    r1.setUsed(Resources.createResource(4 * GB));
    r2.setUsed(Resources.createResource(4 * GB));

    r1.setPending(Resources.createResource(4 * GB));
    r2.setPending(Resources.createResource(4 * GB));

    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r2.getSchedulingResourceUsage());

    //Same, equal
    assertEquals("Comparator Output", 0,
        policy.getComparator().compare(r1, r2));

    r2.setUsed(Resources.createResource(5 * GB));
    r2.setPending(Resources.createResource(5 * GB));

    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r2.getSchedulingResourceUsage());

    //More demand and consumption, but not enough more demand to overcome
    //additional consumption
    Assert.assertTrue(policy.getComparator().compare(r1, r2) < 0);

    //High demand, enough to reverse sbw
    r2.setPending(Resources.createResource(100 * GB));
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r2.getSchedulingResourceUsage());
    Assert.assertTrue(policy.getComparator().compare(r1, r2) > 0);
  }

  @Test
  public void testIterators() {
    OrderingPolicy<MockSchedulableEntity> schedOrder =
     new FairOrderingPolicy<MockSchedulableEntity>();

    MockSchedulableEntity msp1 = new MockSchedulableEntity();
    MockSchedulableEntity msp2 = new MockSchedulableEntity();
    MockSchedulableEntity msp3 = new MockSchedulableEntity();

    msp1.setId("1");
    msp2.setId("2");
    msp3.setId("3");

    msp1.setUsed(Resources.createResource(3));
    msp2.setUsed(Resources.createResource(2));
    msp3.setUsed(Resources.createResource(1));

    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      msp1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      msp2.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      msp2.getSchedulingResourceUsage());

    schedOrder.addSchedulableEntity(msp1);
    schedOrder.addSchedulableEntity(msp2);
    schedOrder.addSchedulableEntity(msp3);


    //Assignment, least to greatest consumption
    checkIds(schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR),
        new String[]{"3", "2", "1"});

    //Preemption, greatest to least
    checkIds(schedOrder.getPreemptionIterator(), new String[]{"1", "2", "3"});

    //Change value without inform, should see no change
    msp2.setUsed(Resources.createResource(6));
    checkIds(schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR),
        new String[]{"3", "2", "1"});
    checkIds(schedOrder.getPreemptionIterator(), new String[]{"1", "2", "3"});

    //Do inform, will reorder
    schedOrder.containerAllocated(msp2, null);
    checkIds(schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR),
        new String[]{"3", "1", "2"});
    checkIds(schedOrder.getPreemptionIterator(), new String[]{"2", "1", "3"});
  }

  @Test
  public void testSizeBasedWeightNotAffectAppActivation() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();

    // Define top-level queues
    String queuePath = CapacitySchedulerConfiguration.ROOT + ".default";
    csConf.set(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class.getCanonicalName());
    csConf.setOrderingPolicy(queuePath,
        CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY);
    csConf.setOrderingPolicyParameter(queuePath,
        FairOrderingPolicy.ENABLE_SIZE_BASED_WEIGHT, "true");
    csConf.setMaximumApplicationMasterResourcePerQueuePercent(queuePath, 0.1f);

    // inject node label manager
    MockRM rm = new MockRM(csConf);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // Get LeafQueue
    LeafQueue lq = (LeafQueue) cs.getQueue("default");
    OrderingPolicy<FiCaSchedulerApp> policy = lq.getOrderingPolicy();
    Assert.assertTrue(policy instanceof FairOrderingPolicy);
    Assert.assertTrue(((FairOrderingPolicy<FiCaSchedulerApp>)policy).getSizeBasedWeight());

    rm.registerNode("h1:1234", 10 * GB);

    // Submit 4 apps
    MockRMAppSubmissionData data3 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    MockRMAppSubmitter.submit(rm, data3);
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    MockRMAppSubmitter.submit(rm, data2);
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    MockRMAppSubmitter.submit(rm, data1);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    MockRMAppSubmitter.submit(rm, data);

    Assert.assertEquals(1, lq.getNumActiveApplications());
    Assert.assertEquals(3, lq.getNumPendingApplications());

    // Try allocate once, #active-apps and #pending-apps should be still correct
    cs.handle(new NodeUpdateSchedulerEvent(
        rm.getRMContext().getRMNodes().get(NodeId.newInstance("h1", 1234))));
    Assert.assertEquals(1, lq.getNumActiveApplications());
    Assert.assertEquals(3, lq.getNumPendingApplications());
  }

  public void checkIds(Iterator<MockSchedulableEntity> si,
      String[] ids) {
    for (int i = 0;i < ids.length;i++) {
      Assert.assertEquals(si.next().getId(),
        ids[i]);
    }
  }

  @Test
  public void testOrderingUsingUsedAndPendingResources() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
        new FairOrderingPolicy<>();
    policy.setSizeBasedWeight(true);
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    r1.setUsed(Resources.createResource(4 * GB));
    r2.setUsed(Resources.createResource(4 * GB));

    r1.setPending(Resources.createResource(4 * GB));
    r2.setPending(Resources.createResource(4 * GB));

    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());

    // Same, equal
    assertEquals("Comparator Output", 0,
        policy.getComparator().compare(r1, r2));

    r1.setUsed(Resources.createResource(4 * GB));
    r2.setUsed(Resources.createResource(8 * GB));

    r1.setPending(Resources.createResource(4 * GB));
    r2.setPending(Resources.createResource(8 * GB));

    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());

    Assert.assertTrue(policy.getComparator().compare(r1, r2) < 0);
  }

  @Test
  public void testOrderingUsingAppSubmitTime() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
        new FairOrderingPolicy<>();
    policy.setSizeBasedWeight(true);
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    // R1, R2 has been started at same time
    assertEquals(r1.getStartTime(), r2.getStartTime());

    // No changes, equal
    assertEquals("Comparator Output", 0,
        policy.getComparator().compare(r1, r2));

    // R2 has been started after R1
    r1.setStartTime(5);
    r2.setStartTime(10);

    Assert.assertTrue(policy.getComparator().compare(r1, r2) < 0);

    // R1 has been started after R2
    r1.setStartTime(10);
    r2.setStartTime(5);

    Assert.assertTrue(policy.getComparator().compare(r1, r2) > 0);
  }

  @Test
  public void testOrderingUsingAppDemand() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
        new FairOrderingPolicy<MockSchedulableEntity>();
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    r1.setUsed(Resources.createResource(0));
    r2.setUsed(Resources.createResource(0));

    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());

    // Same, equal
    assertEquals("Comparator Output", 0,
        policy.getComparator().compare(r1, r2));

    // Compare demands ensures entity without resource demands gets lower
    // priority
    r1.setPending(Resources.createResource(0));
    r2.setPending(Resources.createResource(8 * GB));
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());

    Assert.assertTrue(policy.getComparator().compare(r1, r2) > 0);

    // When both entity has certain demands, then there is no actual comparison
    r1.setPending(Resources.createResource(4 * GB));
    r2.setPending(Resources.createResource(12 * GB));
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());

    assertEquals("Comparator Output", 0,
        policy.getComparator().compare(r1, r2));
  }
}
