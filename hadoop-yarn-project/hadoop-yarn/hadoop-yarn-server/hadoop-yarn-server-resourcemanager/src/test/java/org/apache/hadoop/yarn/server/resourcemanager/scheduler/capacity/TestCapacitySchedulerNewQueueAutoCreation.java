/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assertions;

import java.util.Set;
import java.util.HashSet;

import java.io.IOException;

public class TestCapacitySchedulerNewQueueAutoCreation
    extends TestCapacitySchedulerAutoCreatedQueueBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      org.apache.hadoop.yarn.server.resourcemanager
          .scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.class);
  public static final int GB = 1024;
  private static final int MAX_MEMORY = 1200;
  private MockRM mockRM = null;
  private CapacityScheduler cs;
  private CapacitySchedulerConfiguration csConf;
  private CapacitySchedulerQueueManager autoQueueHandler;
  private AutoCreatedQueueDeletionPolicy policy = new
      AutoCreatedQueueDeletionPolicy();

  public CapacityScheduler getCs() {
    return cs;
  }

  public AutoCreatedQueueDeletionPolicy getPolicy() {
    return policy;
  }

  /*
  Create the following structure:
           root
        /       \
      a          b
    /
  a1
   */
  @BeforeEach
  public void setUp() throws Exception {
    csConf = new CapacitySchedulerConfiguration();
    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    // By default, set 3 queues, a/b, and a.a1
    csConf.setQueues("root", new String[]{"a", "b"});
    csConf.setNonLabeledQueueWeight("root", 1f);
    csConf.setNonLabeledQueueWeight("root.a", 1f);
    csConf.setNonLabeledQueueWeight("root.b", 1f);
    csConf.setQueues("root.a", new String[]{"a1"});
    csConf.setNonLabeledQueueWeight("root.a.a1", 1f);
    csConf.setAutoQueueCreationV2Enabled("root", true);
    csConf.setAutoQueueCreationV2Enabled("root.a", true);
    csConf.setAutoQueueCreationV2Enabled("root.e", true);
    csConf.setAutoQueueCreationV2Enabled(PARENT_QUEUE, true);
    // Test for auto deletion when expired
    csConf.setAutoExpiredDeletionTime(1);
  }

  protected void startScheduler() throws Exception {
    RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(csConf);
    mockRM = new MockRM(csConf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    cs = (CapacityScheduler) mockRM.getResourceScheduler();
    cs.updatePlacementRules();
    // Policy for new auto created queue's auto deletion when expired
    policy.init(cs.getConfiguration(), cs.getRMContext(), cs);
    mockRM.start();
    cs.start();
    autoQueueHandler = cs.getCapacitySchedulerQueueManager();
    mockRM.registerNode("h1:1234", MAX_MEMORY * GB); // label = x
  }

  /*
  Create and validate the following structure:

                          root
     ┌─────┬────────┬─────┴─────┬─────────┐
     a     b      c-auto     e-auto     d-auto
     |                        |
    a1                      e1-auto
   */
  private void createBasicQueueStructureAndValidate() throws Exception {
    // queue's weights are 1
    // root
    // - a (w=1)
    // - b (w=1)
    // - c-auto (w=1)
    // - d-auto (w=1)
    // - e-auto (w=1)
    //   - e1-auto (w=1)
    MockNM nm1 = mockRM.registerNode("h1:1234", 1200 * GB); // label = x

    createQueue("root.c-auto");

    // Check if queue c-auto got created
    CSQueue c = cs.getQueue("root.c-auto");
    Assertions.assertEquals(c.getAbsoluteCapacity(), 1 / 3f, 1e-6);
    Assertions.assertEquals(c.getQueueCapacities().getWeight(), 1e-6, 1f);
    Assertions.assertEquals(400 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assertions.assertEquals(-1, ((LeafQueue)c).getUserLimitFactor(), 1e-6);
    Assertions.assertEquals(1, ((LeafQueue)c).getMaxAMResourcePerQueuePercent(), 1e-6);

    // Now add another queue-d, in the same hierarchy
    createQueue("root.d-auto");

    // Because queue-d has the same weight of other sibling queue, its abs cap
    // become 1/4
    CSQueue d = cs.getQueue("root.d-auto");
    Assertions.assertEquals(d.getAbsoluteCapacity(), 1 / 4f, 1e-6);
    Assertions.assertEquals(d.getQueueCapacities().getWeight(), 1f, 1e-6);
    Assertions.assertEquals(300 * GB,
        d.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Now we check queue c again, it should also become 1/4 capacity
    Assertions.assertEquals(c.getAbsoluteCapacity(), 1e-6, 1 / 4f);
    Assertions.assertEquals(c.getQueueCapacities().getWeight(), 1f, 1e-6);
    Assertions.assertEquals(300 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Now we add a two-level queue, create leaf only
    // Now add another queue a2-auto, under root.a
    createQueue("root.a.a2-auto");

    // root.a has 1/4 abs resource, a2/a1 has the same weight, so a2 has 1/8 abs
    // capacity
    CSQueue a2 = cs.getQueue("root.a.a2-auto");
    Assertions.assertEquals(a2.getAbsoluteCapacity(), 1 / 8f, 1e-6);
    Assertions.assertEquals(a2.getQueueCapacities().getWeight(), 1f, 1e-6);
    Assertions.assertEquals(150 * GB,
        a2.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // try, create leaf + parent, will success
    createQueue("root.e-auto.e1-auto");

    // Now check capacity of e and e1 (under root we have 5 queues, so e1 get
    // 1/5 capacity
    CSQueue e = cs.getQueue("root.e-auto");
    Assertions.assertEquals(e.getAbsoluteCapacity(), 1 / 5f, 1e-6);
    Assertions.assertEquals(e.getQueueCapacities().getWeight(), 1f, 1e-6);
    Assertions.assertEquals(240 * GB,
        e.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Under e, there's only one queue, so e1/e have same capacity
    CSQueue e1 = cs.getQueue("root.e-auto.e1-auto");
    Assertions.assertEquals(e1.getAbsoluteCapacity(), 1 / 5f, 1e-6);
    Assertions.assertEquals(e1.getQueueCapacities().getWeight(), 1f, 1e-6);
    Assertions.assertEquals(240 * GB,
        e1.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
  }

  /*
  Create and validate the structure:
                         root
     ┌─────┬────────┬─────┴───────┐
     a     b      c-auto       d-auto
     |
     a1
   */
  @Test
  void testAutoCreateQueueWithSiblingsUnderRoot() throws Exception {
    startScheduler();

    createQueue("root.c-auto");

    // Check if queue c-auto got created
    CSQueue c = cs.getQueue("root.c-auto");
    Assertions.assertEquals(c.getAbsoluteCapacity(), 1e-6, 1 / 3f);
    Assertions.assertEquals(c.getQueueCapacities().getWeight(), 1e-6, 1f);
    Assertions.assertEquals(400 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Now add another queue-d, in the same hierarchy
    createQueue("root.d-auto");

    // Because queue-d has the same weight of other sibling queue, its abs cap
    // become 1/4
    CSQueue d = cs.getQueue("root.d-auto");
    Assertions.assertEquals(d.getAbsoluteCapacity(), 1e-6, 1 / 4f);
    Assertions.assertEquals(d.getQueueCapacities().getWeight(), 1e-6, 1f);
    Assertions.assertEquals(300 * GB,
        d.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Now we check queue c again, it should also become 1/4 capacity
    Assertions.assertEquals(c.getAbsoluteCapacity(), 1e-6, 1 / 4f);
    Assertions.assertEquals(c.getQueueCapacities().getWeight(), 1e-6, 1f);
    Assertions.assertEquals(300 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
  }

  /*
  Create and validate the structure:
          root
     ┌─────┴─────┐
     b           a
               /  \
              a1  a2-auto
   */
  @Test
  void testAutoCreateQueueStaticParentOneLevel() throws Exception {
    startScheduler();
    // Now we add a two-level queue, create leaf only
    // Now add another queue a2-auto, under root.a
    createQueue("root.a.a2-auto");

    // root.a has 1/2 abs resource, a2/a1 has the same weight, so a2 has 1/4 abs
    // capacity
    CSQueue a2 = cs.getQueue("root.a.a2-auto");
    Assertions.assertEquals(a2.getAbsoluteCapacity(), 1e-6, 1 / 4f);
    Assertions.assertEquals(a2.getQueueCapacities().getWeight(), 1e-6, 1f);
    Assertions.assertEquals(a2.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize(),
        1e-6,
        MAX_MEMORY * (1 / 4f) * GB);

  }

  /*
  Create and validate the structure:
          root
     ┌─────┴─────┐
     b            a
               |    \
             a1    a2-auto
                   |     \
               a3-auto   a4-auto
   */
  @Test
  void testAutoCreateQueueAutoParentTwoLevelsWithSiblings()
      throws Exception {
    startScheduler();
    csConf.setAutoQueueCreationV2Enabled("root.a.a2-auto", true);

    // root.a has 1/2 abs resource -> a1 and a2-auto same weight 1/4
    // -> a3-auto is alone with weight 1/4
    createQueue("root.a.a2-auto.a3-auto");
    CSQueue a3 = cs.getQueue("root.a.a2-auto.a3-auto");
    Assertions.assertEquals(a3.getAbsoluteCapacity(), 1e-6, 1 / 4f);
    Assertions.assertEquals(a3.getQueueCapacities().getWeight(), 1e-6, 1f);
    Assertions.assertEquals(a3.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize(),
        1e-6,
        MAX_MEMORY * (1 / 4f) * GB);

    // root.a has 1/2 abs resource -> a1 and a2-auto same weight 1/4
    // -> a3-auto and a4-auto same weight 1/8
    createQueue("root.a.a2-auto.a4-auto");
    CSQueue a4 = cs.getQueue("root.a.a2-auto.a4-auto");
    Assertions.assertEquals(a3.getAbsoluteCapacity(), 1e-6, 1 / 8f);
    Assertions.assertEquals(a3.getQueueCapacities().getWeight(), 1e-6, 1f);
    Assertions.assertEquals(a4.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize(),
        1e-6,
        MAX_MEMORY * (1 / 8f) * GB);
  }

  @Test
  void testAutoCreateQueueShouldFailWhenNonParentQueue()
      throws Exception {
    Assertions.assertThrows(SchedulerDynamicEditException.class, () -> {
      startScheduler();
      createQueue("root.a.a1.a2-auto");
    });
  }

  @Test
  void testAutoCreateQueueWhenSiblingsNotInWeightMode()
      throws Exception {
    Assertions.assertThrows(SchedulerDynamicEditException.class, () -> {
      startScheduler();
      csConf.setCapacity("root.a", 50f);
      csConf.setCapacity("root.b", 50f);
      csConf.setCapacity("root.a.a1", 100f);
      cs.reinitialize(csConf, mockRM.getRMContext());
      createQueue("root.a.a2-auto");
    });
  }

  @Test()
  public void testAutoCreateMaximumQueueDepth()
      throws Exception {
    startScheduler();
    // By default, max depth is 2, therefore this is an invalid scenario
    Assertions.assertThrows(SchedulerDynamicEditException.class,
        () -> createQueue("root.a.a3-auto.a4-auto.a5-auto"));

    // Set depth 3 for root.a, making it a valid scenario
    csConf.setMaximumAutoCreatedQueueDepth("root.a", 3);
    cs.reinitialize(csConf, mockRM.getRMContext());
    try {
      createQueue("root.a.a3-auto.a4-auto.a5-auto");
    } catch (SchedulerDynamicEditException sde) {
      LOG.error("%s", sde);
      Assertions.fail("Depth is set for root.a, exception should not be thrown");
    }

    // Set global depth to 3
    csConf.setMaximumAutoCreatedQueueDepth(3);
    csConf.unset(CapacitySchedulerConfiguration.getQueuePrefix("root.a")
        + CapacitySchedulerConfiguration.MAXIMUM_QUEUE_DEPTH);
    cs.reinitialize(csConf, mockRM.getRMContext());
    try {
      createQueue("root.a.a6-auto.a7-auto.a8-auto");
    } catch (SchedulerDynamicEditException sde) {
      LOG.error("%s", sde);
      Assertions.fail("Depth is set globally, exception should not be thrown");
    }

    // Set depth on a dynamic queue, which has no effect on auto queue creation validation
    csConf.setMaximumAutoCreatedQueueDepth("root.a.a6-auto.a7-auto.a8-auto", 10);
    Assertions.assertThrows(SchedulerDynamicEditException.class,
        () -> createQueue("root.a.a6-auto.a7-auto.a8-auto.a9-auto.a10-auto.a11-auto"));
  }

  @Test
  void testAutoCreateQueueShouldFailIfNotEnabledForParent()
      throws Exception {
    Assertions.assertThrows(SchedulerDynamicEditException.class, () -> {
      startScheduler();
      csConf.setAutoQueueCreationV2Enabled("root", false);
      cs.reinitialize(csConf, mockRM.getRMContext());
      createQueue("root.c-auto");
    });
  }

  @Test
  void testAutoCreateQueueRefresh() throws Exception {
    startScheduler();

    createBasicQueueStructureAndValidate();

    // Refresh the queue to make sure all queues are still exist.
    // (Basically, dynamic queues should not disappear after refresh).
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Double confirm, after refresh, we should still see root queue has 5
    // children.
    Assertions.assertEquals(5, cs.getQueue("root").getChildQueues().size());
    Assertions.assertNotNull(cs.getQueue("root.c-auto"));
  }

  @Test
  void testConvertDynamicToStaticQueue() throws Exception {
    startScheduler();

    createBasicQueueStructureAndValidate();

    // Now, update root.a's weight to 6
    csConf.setNonLabeledQueueWeight("root.a", 6f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Double confirm, after refresh, we should still see root queue has 5
    // children.
    Assertions.assertEquals(5, cs.getQueue("root").getChildQueues().size());

    // Get queue a
    CSQueue a = cs.getQueue("root.a");

    // a's abs resource should be 6/10, (since a.weight=6, all other 4 peers
    // have weight=1).
    Assertions.assertEquals(a.getAbsoluteCapacity(), 1e-6, 6 / 10f);
    Assertions.assertEquals(720 * GB,
        a.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assertions.assertEquals(a.getQueueCapacities().getWeight(), 1e-6, 6f);

    // Set queue c-auto's weight to 6, and mark c-auto to be static queue
    csConf.setQueues("root", new String[]{"a", "b", "c-auto"});
    csConf.setNonLabeledQueueWeight("root.c-auto", 6f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Get queue c
    CSQueue c = cs.getQueue("root.c-auto");

    // c's abs resource should be 6/15, (since a/c.weight=6, all other 3 peers
    // have weight=1).
    Assertions.assertEquals(c.getAbsoluteCapacity(), 1e-6, 6 / 15f);
    Assertions.assertEquals(480 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assertions.assertEquals(c.getQueueCapacities().getWeight(), 1e-6, 6f);

    // First, create e2-auto queue
    createQueue("root.e-auto.e2-auto");

    // Do change 2nd level queue from dynamic to static
    csConf.setQueues("root", new String[]{"a", "b", "c-auto", "e-auto"});
    csConf.setNonLabeledQueueWeight("root.e-auto", 6f);
    csConf.setQueues("root.e-auto", new String[]{"e1-auto"});
    csConf.setNonLabeledQueueWeight("root.e-auto.e1-auto", 6f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Get queue e1
    CSQueue e1 = cs.getQueue("root.e-auto.e1-auto");

    // e's abs resource should be 6/20 * (6/7),
    // (since a/c/e.weight=6, all other 2 peers
    // have weight=1, and e1's weight is 6, e2's weight is 1).
    float e1NormalizedWeight = (6 / 20f) * (6 / 7f);
    Assertions.assertEquals(e1.getAbsoluteCapacity(), 1e-6, e1NormalizedWeight);
    assertQueueMinResource(e1, MAX_MEMORY * e1NormalizedWeight);
    Assertions.assertEquals(e1.getQueueCapacities().getWeight(), 1e-6, 6f);
  }

  /*
  Create the structure and convert d-auto to static and leave d1-auto as dynamic
                        root
     ┌─────┬─────────────┴──────┐
     a     b                 d-auto
     |                         |
     a1                     d1-auto
   */
  @Test
  void testConvertDynamicParentToStaticParent() throws Exception {
    startScheduler();
    createQueue("root.d-auto.d1-auto");
    csConf.setQueues("root", new String[]{"a", "b", "d-auto"});
    csConf.setNonLabeledQueueWeight("root.a", 6f);
    csConf.setNonLabeledQueueWeight("root.d-auto", 1f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    CSQueue d = cs.getQueue("root.d-auto");

    Assertions.assertEquals(d.getAbsoluteCapacity(), 1e-6, 1 / 8f);
    assertQueueMinResource(d, MAX_MEMORY * (1 / 8f));
    Assertions.assertEquals(d.getQueueCapacities().getWeight(), 1e-6, 1f);

    CSQueue d1 = cs.getQueue("root.d-auto.d1-auto");
    Assertions.assertEquals(d1.getAbsoluteCapacity(), 1e-6, 1 / 8f);
    assertQueueMinResource(d1, MAX_MEMORY * (1 / 8f));
    Assertions.assertEquals(d1.getQueueCapacities().getWeight(), 1e-6, 1f);
  }

  @Test
  void testAutoQueueCreationOnAppSubmission() throws Exception {
    startScheduler();

    submitApp(cs, USER0, USER0, "root.e-auto");

    AbstractCSQueue e = (AbstractCSQueue) cs.getQueue("root.e-auto");
    Assertions.assertNotNull(e);
    Assertions.assertTrue(e.isDynamicQueue());

    AbstractCSQueue user0 = (AbstractCSQueue) cs.getQueue(
        "root.e-auto." + USER0);
    Assertions.assertNotNull(user0);
    Assertions.assertTrue(user0.isDynamicQueue());
  }

  @Test
  void testChildlessParentQueueWhenAutoQueueCreationEnabled()
      throws Exception {
    startScheduler();
    csConf.setQueues("root", new String[]{"a", "b", "empty-auto-parent"});
    csConf.setNonLabeledQueueWeight("root", 1f);
    csConf.setNonLabeledQueueWeight("root.a", 1f);
    csConf.setNonLabeledQueueWeight("root.b", 1f);
    csConf.setQueues("root.a", new String[]{"a1"});
    csConf.setNonLabeledQueueWeight("root.a.a1", 1f);
    csConf.setAutoQueueCreationV2Enabled("root", true);
    csConf.setAutoQueueCreationV2Enabled("root.a", true);
    cs.reinitialize(csConf, mockRM.getRMContext());

    CSQueue empty = cs.getQueue("root.empty-auto-parent");
    Assertions.assertTrue(empty instanceof LeafQueue,
        "empty-auto-parent is not a LeafQueue");
    empty.stopQueue();

    csConf.setQueues("root", new String[]{"a", "b", "empty-auto-parent"});
    csConf.setNonLabeledQueueWeight("root", 1f);
    csConf.setNonLabeledQueueWeight("root.a", 1f);
    csConf.setNonLabeledQueueWeight("root.b", 1f);
    csConf.setQueues("root.a", new String[]{"a1"});
    csConf.setNonLabeledQueueWeight("root.a.a1", 1f);
    csConf.setAutoQueueCreationV2Enabled("root", true);
    csConf.setAutoQueueCreationV2Enabled("root.a", true);
    csConf.setAutoQueueCreationV2Enabled("root.empty-auto-parent", true);
    cs.reinitialize(csConf, mockRM.getRMContext());

    empty = cs.getQueue("root.empty-auto-parent");
    Assertions.assertTrue(empty instanceof ParentQueue,
        "empty-auto-parent is not a ParentQueue");
    Assertions.assertEquals(0,
        empty.getChildQueues().size(), "empty-auto-parent has children");
    Assertions.assertTrue(((ParentQueue)empty).isEligibleForAutoQueueCreation(),
        "empty-auto-parent is not eligible " +
            "for auto queue creation");
  }

  @Test
  void testAutoQueueCreationWithDisabledMappingRules() throws Exception {
    startScheduler();

    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    // Set ApplicationPlacementContext to null in the submitted application
    // in order to imitate a submission with mapping rules turned off
    SchedulerEvent addAppEvent = new AppAddedSchedulerEvent(appId,
        "root.a.a1-auto.a2-auto", USER0, null);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    SchedulerEvent addAttemptEvent = new AppAttemptAddedSchedulerEvent(
        appAttemptId, false);
    cs.handle(addAppEvent);
    cs.handle(addAttemptEvent);

    CSQueue a2Auto = cs.getQueue("root.a.a1-auto.a2-auto");
    Assertions.assertNotNull(a2Auto);
  }

  @Test
  void testAutoCreateQueueUserLimitDisabled() throws Exception {
    startScheduler();
    createBasicQueueStructureAndValidate();

    submitApp(cs, USER0, USER0, "root.e-auto");

    AbstractCSQueue e = (AbstractCSQueue) cs.getQueue("root.e-auto");
    Assertions.assertNotNull(e);
    Assertions.assertTrue(e.isDynamicQueue());

    AbstractCSQueue user0 = (AbstractCSQueue) cs.getQueue(
        "root.e-auto." + USER0);
    Assertions.assertNotNull(user0);
    Assertions.assertTrue(user0.isDynamicQueue());
    Assertions.assertTrue(user0 instanceof LeafQueue);

    LeafQueue user0LeafQueue = (LeafQueue) user0;

    // Assert user limit factor is -1
    Assertions.assertTrue(user0LeafQueue.getUserLimitFactor() == -1);

    // Assert user max applications not limited
    Assertions.assertEquals(user0LeafQueue.getMaxApplicationsPerUser(),
        user0LeafQueue.getMaxApplications());

    // Assert AM Resource
    Assertions.assertEquals(user0LeafQueue.
            getMaxAMResourcePerQueuePercent() * MAX_MEMORY * GB,
        1e-6, user0LeafQueue.getAMResourceLimit().getMemorySize());

    // Assert user limit (no limit) when limit factor is -1
    Assertions.assertEquals(user0LeafQueue.getEffectiveMaxCapacityDown("",
        user0LeafQueue.getMinimumAllocation()).getMemorySize(), 1e-6,
            MAX_MEMORY * GB);
  }

  @Test
  void testAutoQueueCreationMaxAppUpdate() throws Exception {
    startScheduler();

    // When no conf for max apps
    LeafQueue a1 =  (LeafQueue)cs.
        getQueue("root.a.a1");
    Assertions.assertNotNull(a1);
    Assertions.assertEquals(a1.getMaxApplications(), 1, csConf.getMaximumSystemApplications()
            * a1.getAbsoluteCapacity());

    LeafQueue b = (LeafQueue)cs.
        getQueue("root.b");
    Assertions.assertNotNull(b);
    Assertions.assertEquals(b.getMaxApplications(), 1, csConf.getMaximumSystemApplications()
            * b.getAbsoluteCapacity());

    createQueue("root.e");

    // Make sure other children queues
    // max app correct.
    LeafQueue e = (LeafQueue)cs.
        getQueue("root.e");
    Assertions.assertNotNull(e);
    Assertions.assertEquals(e.getMaxApplications(), 1, csConf.getMaximumSystemApplications()
            * e.getAbsoluteCapacity());

    a1 =  (LeafQueue)cs.
        getQueue("root.a.a1");
    Assertions.assertNotNull(a1);
    Assertions.assertEquals(a1.getMaxApplications(), 1, csConf.getMaximumSystemApplications()
            * a1.getAbsoluteCapacity());

    b = (LeafQueue)cs.
        getQueue("root.b");
    Assertions.assertNotNull(b);
    Assertions.assertEquals(b.getMaxApplications(), 1, csConf.getMaximumSystemApplications()
            * b.getAbsoluteCapacity());

    // When update global max app per queue
    csConf.setGlobalMaximumApplicationsPerQueue(1000);
    cs.reinitialize(csConf, mockRM.getRMContext());
    Assertions.assertEquals(1000, b.getMaxApplications());
    Assertions.assertEquals(1000, a1.getMaxApplications());
    Assertions.assertEquals(1000, e.getMaxApplications());

    // when set some queue for max apps
    csConf.setMaximumApplicationsPerQueue("root.e1", 50);
    createQueue("root.e1");
    LeafQueue e1 = (LeafQueue)cs.
        getQueue("root.e1");
    Assertions.assertNotNull(e1);

    cs.reinitialize(csConf, mockRM.getRMContext());
    Assertions.assertEquals(50, e1.getMaxApplications());
  }

  @Test
  void testAutoCreateQueueWithAmbiguousNonFullPathParentName()
      throws Exception {
    Assertions.assertThrows(SchedulerDynamicEditException.class, () -> {
      startScheduler();

      createQueue("root.a.a");
      createQueue("a.a");
    });
  }

  @Test
  void testAutoCreateQueueIfFirstExistingParentQueueIsNotStatic()
      throws Exception {
    startScheduler();

    // create a dynamic ParentQueue
    createQueue("root.a.a-parent-auto.a1-leaf-auto");
    Assertions.assertNotNull(cs.getQueue("root.a.a-parent-auto"));

    // create a new dynamic LeafQueue under the existing ParentQueue
    createQueue("root.a.a-parent-auto.a2-leaf-auto");

    CSQueue a2Leaf = cs.getQueue("a2-leaf-auto");

    // Make sure a2-leaf-auto is under a-parent-auto
    Assertions.assertEquals("root.a.a-parent-auto",
        a2Leaf.getParent().getQueuePath());
  }

  @Test
  void testAutoCreateQueueIfAmbiguousQueueNames() throws Exception {
    startScheduler();

    AbstractCSQueue b = (AbstractCSQueue) cs.getQueue("root.b");
    Assertions.assertFalse(b.isDynamicQueue());

    createQueue("root.a.b.b");

    AbstractCSQueue bAutoParent = (AbstractCSQueue) cs.getQueue("root.a.b");
    Assertions.assertTrue(bAutoParent.isDynamicQueue());
    Assertions.assertTrue(bAutoParent.hasChildQueues());

    AbstractCSQueue bAutoLeafQueue =
        (AbstractCSQueue) cs.getQueue("root.a.b.b");
    Assertions.assertTrue(bAutoLeafQueue.isDynamicQueue());
    Assertions.assertFalse(bAutoLeafQueue.hasChildQueues());
  }

  @Test
  void testAutoCreateQueueMaxQueuesLimit() throws Exception {
    startScheduler();

    csConf.setAutoCreatedQueuesV2MaxChildQueuesLimit("root.e", 5);
    cs.reinitialize(csConf, mockRM.getRMContext());

    for (int i = 0; i < 5; ++i) {
      createQueue("root.e.q_" + i);
    }

    // Check if max queue limit can't be exceeded
    try {
      createQueue("root.e.q_6");
      Assertions.fail("Can't exceed max queue limit.");
    } catch (Exception ex) {
      Assertions.assertTrue(ex
          instanceof SchedulerDynamicEditException);
    }
  }

  @Test
  void testAutoCreatedQueueTemplateConfig() throws Exception {
    startScheduler();
    csConf.set(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        "root.a.*") + "capacity", "6w");
    cs.reinitialize(csConf, mockRM.getRMContext());

    AbstractLeafQueue a2 = createQueue("root.a.a-auto.a2");
    Assertions.assertEquals(6f, a2.getQueueCapacities().getWeight(), 1e-6,
        "weight is not set by template");
    Assertions.assertEquals(-1f,
        a2.getUserLimitFactor(), 1e-6, "user limit factor should be disabled with dynamic queues");
    Assertions.assertEquals(1f,
        a2.getMaxAMResourcePerQueuePercent(), 1e-6, "maximum AM resource percent should be 1 with dynamic queues");

    // Set the user-limit-factor and maximum-am-resource-percent via templates to ensure their
    // modified defaults are indeed overridden
    csConf.set(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        "root.a.*") + "user-limit-factor", "10");
    csConf.set(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        "root.a.*") + "maximum-am-resource-percent", "0.8");

    cs.reinitialize(csConf, mockRM.getRMContext());
    a2 = (LeafQueue) cs.getQueue("root.a.a-auto.a2");
    Assertions.assertEquals(6f, a2.getQueueCapacities().getWeight(), 1e-6,
        "weight is overridden");
    Assertions.assertEquals(10f,
        a2.getUserLimitFactor(), 1e-6, "user limit factor should be modified by templates");
    Assertions.assertEquals(0.8f,
        a2.getMaxAMResourcePerQueuePercent(), 1e-6, "maximum AM resource percent should be modified by templates");


    csConf.setNonLabeledQueueWeight("root.a.a-auto.a2", 4f);
    cs.reinitialize(csConf, mockRM.getRMContext());
    Assertions.assertEquals(4f, a2.getQueueCapacities().getWeight(), 1e-6,
        "weight is not explicitly set");

    csConf.setBoolean(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        "root.a") + CapacitySchedulerConfiguration
        .AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE, false);
    cs.reinitialize(csConf, mockRM.getRMContext());
    AbstractLeafQueue a3 = createQueue("root.a.a3");
    Assertions.assertFalse(a3.isEligibleForAutoDeletion(),
        "auto queue deletion should be turned off on a3");

    // Set the capacity of label TEST
    csConf.set(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        "root.c") + "accessible-node-labels.TEST.capacity", "6w");
    csConf.setQueues("root", new String[]{"a", "b", "c"});
    csConf.setAutoQueueCreationV2Enabled("root.c", true);
    cs.reinitialize(csConf, mockRM.getRMContext());
    AbstractLeafQueue c1 = createQueue("root.c.c1");
    Assertions.assertEquals(6f, c1.getQueueCapacities().getWeight("TEST"), 1e-6,
        "weight is not set for label TEST");
    cs.reinitialize(csConf, mockRM.getRMContext());
    c1 = (AbstractLeafQueue) cs.getQueue("root.c.c1");
    Assertions.assertEquals(6f, c1.getQueueCapacities().getWeight("TEST"), 1e-6,
        "weight is not set for label TEST");
  }

  @Test
  void testAutoCreatedQueueConfigChange() throws Exception {
    startScheduler();
    AbstractLeafQueue a2 = createQueue("root.a.a-auto.a2");
    csConf.setNonLabeledQueueWeight("root.a.a-auto.a2", 4f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    Assertions.assertEquals(4f, a2.getQueueCapacities().getWeight(), 1e-6,
        "weight is not explicitly set");

    a2 = (AbstractLeafQueue) cs.getQueue("root.a.a-auto.a2");
    csConf.setState("root.a.a-auto.a2", QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());
    Assertions.assertEquals(QueueState.STOPPED,
        a2.getState(), "root.a.a-auto.a2 has not been stopped");

    csConf.setState("root.a.a-auto.a2", QueueState.RUNNING);
    cs.reinitialize(csConf, mockRM.getRMContext());
    Assertions.assertEquals(QueueState.RUNNING,
        a2.getState(), "root.a.a-auto.a2 is not running");
  }

  @Test
  void testAutoCreateQueueState() throws Exception {
    startScheduler();

    createQueue("root.e.e1");
    csConf.setState("root.e", QueueState.STOPPED);
    csConf.setState("root.e.e1", QueueState.STOPPED);
    csConf.setState("root.a", QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Make sure the static queue is stopped
    Assertions.assertEquals(cs.getQueue("root.a").getState(),
        QueueState.STOPPED);
    // If not set, default is the queue state of parent
    Assertions.assertEquals(cs.getQueue("root.a.a1").getState(),
        QueueState.STOPPED);

    Assertions.assertEquals(cs.getQueue("root.e").getState(),
        QueueState.STOPPED);
    Assertions.assertEquals(cs.getQueue("root.e.e1").getState(),
        QueueState.STOPPED);

    // Make root.e state to RUNNING
    csConf.setState("root.e", QueueState.RUNNING);
    cs.reinitialize(csConf, mockRM.getRMContext());
    Assertions.assertEquals(cs.getQueue("root.e.e1").getState(),
        QueueState.STOPPED);

    // Make root.e.e1 state to RUNNING
    csConf.setState("root.e.e1", QueueState.RUNNING);
    cs.reinitialize(csConf, mockRM.getRMContext());
    Assertions.assertEquals(cs.getQueue("root.e.e1").getState(),
        QueueState.RUNNING);
  }

  @Test
  void testAutoQueueCreationDepthLimitFromStaticParent()
      throws Exception {
    startScheduler();

    // a is the first existing queue here and it is static, therefore
    // the distance is 2
    createQueue("root.a.a-auto.a1-auto");
    Assertions.assertNotNull(cs.getQueue("root.a.a-auto.a1-auto"));

    try {
      createQueue("root.a.a-auto.a2-auto.a3-auto");
      Assertions.fail("Queue creation should not succeed because the distance " +
          "from the first static parent is above limit");
    } catch (SchedulerDynamicEditException ignored) {

    }

  }

  @Test
  void testCapacitySchedulerAutoQueueDeletion() throws Exception {
    startScheduler();
    csConf.setBoolean(
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    csConf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        AutoCreatedQueueDeletionPolicy.class.getCanonicalName());
    csConf.setAutoExpiredDeletionTime(1);
    cs.reinitialize(csConf, mockRM.getRMContext());

    Set<String> policies = new HashSet<>();
    policies.add(
        AutoCreatedQueueDeletionPolicy.class.getCanonicalName());

    Assertions.assertTrue(
        cs.getSchedulingMonitorManager().
            isSameConfiguredPolicies(policies),
        "No AutoCreatedQueueDeletionPolicy " +
            "is present in running monitors");

    ApplicationAttemptId a2App = submitApp(cs, USER0,
        "a2-auto", "root.a.a1-auto");

    // Wait a2 created successfully.
    GenericTestUtils.waitFor(()-> cs.getQueue(
        "root.a.a1-auto.a2-auto") != null,
        100, 2000);

    AbstractCSQueue a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    Assertions.assertNotNull(a1, "a1 is not present");
    AbstractCSQueue a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    Assertions.assertNotNull(a2, "a2 is not present");
    Assertions.assertTrue(a2.isDynamicQueue(),
        "a2 is not a dynamic queue");

    // Now there are still 1 app in a2 queue.
    Assertions.assertEquals(1, a2.getNumApplications());

    // Wait the time expired.
    long l1 = a2.getLastSubmittedTimestamp();
    GenericTestUtils.waitFor(() -> {
      long duration = (Time.monotonicNow() - l1)/1000;
      return duration > csConf.getAutoExpiredDeletionTime();
    }, 100, 2000);

    // Make sure the queue will not be deleted
    // when expired with remaining apps.
    a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    Assertions.assertNotNull(a2, "a2 is not present");

    // Make app finished.
    AppAttemptRemovedSchedulerEvent event =
        new AppAttemptRemovedSchedulerEvent(a2App,
            RMAppAttemptState.FINISHED, false);
    cs.handle(event);
    AppRemovedSchedulerEvent rEvent = new AppRemovedSchedulerEvent(
        a2App.getApplicationId(), RMAppState.FINISHED);
    cs.handle(rEvent);

    // Now there are no apps in a2 queue.
    Assertions.assertEquals(0, a2.getNumApplications());

    // Wait the a2 deleted.
    GenericTestUtils.waitFor(() -> {
      AbstractCSQueue a2Tmp = (AbstractCSQueue) cs.getQueue(
            "root.a.a1-auto.a2-auto");
      return a2Tmp == null;
    }, 100, 3000);

    a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    Assertions.assertNull(a2, "a2 is not deleted");

    // The parent will not be deleted with child queues
    a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    Assertions.assertNotNull(a1, "a1 is not present");

    // Now the parent queue without child
    // will be deleted for expired.
    // Wait a1 deleted.
    GenericTestUtils.waitFor(() -> {
      AbstractCSQueue a1Tmp = (AbstractCSQueue) cs.getQueue(
          "root.a.a1-auto");
      return a1Tmp == null;
    }, 100, 3000);
    a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    Assertions.assertNull(a1, "a1 is not deleted");
  }

  @Test
  void testCapacitySchedulerAutoQueueDeletionDisabled()
      throws Exception {
    startScheduler();
    // Test for disabled auto deletion
    csConf.setAutoExpiredDeletionEnabled(
        "root.a.a1-auto.a2-auto", false);
    csConf.setBoolean(
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    csConf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        AutoCreatedQueueDeletionPolicy.class.getCanonicalName());
    csConf.setAutoExpiredDeletionTime(1);
    cs.reinitialize(csConf, mockRM.getRMContext());

    Set<String> policies = new HashSet<>();
    policies.add(
        AutoCreatedQueueDeletionPolicy.class.getCanonicalName());

    Assertions.assertTrue(
        cs.getSchedulingMonitorManager().
            isSameConfiguredPolicies(policies),
        "No AutoCreatedQueueDeletionPolicy " +
            "is present in running monitors");

    ApplicationAttemptId a2App = submitApp(cs, USER0,
        "a2-auto", "root.a.a1-auto");

    // Wait a2 created successfully.
    GenericTestUtils.waitFor(()-> cs.getQueue(
        "root.a.a1-auto.a2-auto") != null,
        100, 2000);

    AbstractCSQueue a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    Assertions.assertNotNull(a1, "a1 is not present");
    AbstractCSQueue a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    Assertions.assertNotNull(a2, "a2 is not present");
    Assertions.assertTrue(a2.isDynamicQueue(),
        "a2 is not a dynamic queue");

    // Make app finished.
    AppAttemptRemovedSchedulerEvent event =
        new AppAttemptRemovedSchedulerEvent(a2App,
            RMAppAttemptState.FINISHED, false);
    cs.handle(event);
    AppRemovedSchedulerEvent rEvent = new AppRemovedSchedulerEvent(
        a2App.getApplicationId(), RMAppState.FINISHED);
    cs.handle(rEvent);

    // Now there are no apps in a2 queue.
    Assertions.assertEquals(0, a2.getNumApplications());

    // Wait the time expired.
    long l1 = a2.getLastSubmittedTimestamp();
    GenericTestUtils.waitFor(() -> {
      long duration = (Time.monotonicNow() - l1)/1000;
      return duration > csConf.getAutoExpiredDeletionTime();
    }, 100, 2000);

    // The auto deletion is no enabled for a2-auto
    a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    Assertions.assertNotNull(a1, "a1 is not present");
    a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    Assertions.assertNotNull(a2, "a2 is not present");
    Assertions.assertTrue(a2.isDynamicQueue(),
        "a2 is not a dynamic queue");

    // Enabled now
    // The auto deletion will work.
    csConf.setAutoExpiredDeletionEnabled(
        "root.a.a1-auto.a2-auto", true);
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Wait the a2 deleted.
    GenericTestUtils.waitFor(() -> {
      AbstractCSQueue a2Tmp = (AbstractCSQueue) cs.getQueue(
          "root.a.a1-auto.a2-auto");
      return a2Tmp == null;
    }, 100, 3000);

    a2 = (AbstractCSQueue) cs.
        getQueue("root.a.a1-auto.a2-auto");
    Assertions.assertNull(a2, "a2 is not deleted");
    // The parent will not be deleted with child queues
    a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    Assertions.assertNotNull(a1, "a1 is not present");

    // Now the parent queue without child
    // will be deleted for expired.
    // Wait a1 deleted.
    GenericTestUtils.waitFor(() -> {
      AbstractCSQueue a1Tmp = (AbstractCSQueue) cs.getQueue(
          "root.a.a1-auto");
      return a1Tmp == null;
    }, 100, 3000);
    a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    Assertions.assertNull(a1, "a1 is not deleted");
  }

  @Test
  void testAutoCreateQueueAfterRemoval() throws Exception {
    // queue's weights are 1
    // root
    // - a (w=1)
    // - b (w=1)
    // - c-auto (w=1)
    // - d-auto (w=1)
    // - e-auto (w=1)
    //   - e1-auto (w=1)
    startScheduler();

    createBasicQueueStructureAndValidate();

    // Under e, there's only one queue, so e1/e have same capacity
    CSQueue e1 = cs.getQueue("root.e-auto.e1-auto");
    Assertions.assertEquals(e1.getAbsoluteCapacity(), 1e-6, 1 / 5f);
    Assertions.assertEquals(e1.getQueueCapacities().getWeight(), 1e-6, 1f);
    Assertions.assertEquals(240 * GB,
        e1.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check after removal e1.
    cs.removeQueue(e1);
    CSQueue e = cs.getQueue("root.e-auto");
    Assertions.assertEquals(e.getAbsoluteCapacity(), 1e-6, 1 / 5f);
    Assertions.assertEquals(e.getQueueCapacities().getWeight(), 1e-6, 1f);
    Assertions.assertEquals(240 * GB,
        e.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check after removal e.
    cs.removeQueue(e);
    CSQueue d = cs.getQueue("root.d-auto");
    Assertions.assertEquals(d.getAbsoluteCapacity(), 1e-6, 1 / 4f);
    Assertions.assertEquals(d.getQueueCapacities().getWeight(), 1e-6, 1f);
    Assertions.assertEquals(300 * GB,
        d.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check after removal d.
    cs.removeQueue(d);
    CSQueue c = cs.getQueue("root.c-auto");
    Assertions.assertEquals(c.getAbsoluteCapacity(), 1e-6, 1 / 3f);
    Assertions.assertEquals(c.getQueueCapacities().getWeight(), 1e-6, 1f);
    Assertions.assertEquals(400 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check after removal c.
    cs.removeQueue(c);
    CSQueue b = cs.getQueue("root.b");
    Assertions.assertEquals(b.getAbsoluteCapacity(), 1e-6, 1 / 2f);
    Assertions.assertEquals(b.getQueueCapacities().getWeight(), 1e-6, 1f);
    Assertions.assertEquals(600 * GB,
        b.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check can't remove static queue b.
    try {
      cs.removeQueue(b);
      Assertions.fail("Can't remove static queue b!");
    } catch (Exception ex) {
      Assertions.assertTrue(ex
          instanceof SchedulerDynamicEditException);
    }
    // Check a.
    CSQueue a = cs.getQueue("root.a");
    Assertions.assertEquals(a.getAbsoluteCapacity(), 1e-6, 1 / 2f);
    Assertions.assertEquals(a.getQueueCapacities().getWeight(), 1e-6, 1f);
    Assertions.assertEquals(600 * GB,
        b.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
  }

  @Test
  void testQueueInfoIfAmbiguousQueueNames() throws Exception {
    startScheduler();

    AbstractCSQueue b = (AbstractCSQueue) cs.
        getQueue("root.b");
    Assertions.assertFalse(b.isDynamicQueue());
    Assertions.assertEquals("root.b",
        b.getQueueInfo().getQueuePath());

    createQueue("root.a.b.b");

    AbstractCSQueue bAutoParent = (AbstractCSQueue) cs.
        getQueue("root.a.b");
    Assertions.assertTrue(bAutoParent.isDynamicQueue());
    Assertions.assertTrue(bAutoParent.hasChildQueues());
    Assertions.assertEquals("root.a.b",
        bAutoParent.getQueueInfo().getQueuePath());

    AbstractCSQueue bAutoLeafQueue =
        (AbstractCSQueue) cs.getQueue("root.a.b.b");
    Assertions.assertTrue(bAutoLeafQueue.isDynamicQueue());
    Assertions.assertFalse(bAutoLeafQueue.hasChildQueues());
    Assertions.assertEquals("root.a.b.b",
        bAutoLeafQueue.getQueueInfo().getQueuePath());

    // Make sure all queue name are ambiguous
    Assertions.assertEquals("b",
        b.getQueueInfo().getQueueName());
    Assertions.assertEquals("b",
        bAutoParent.getQueueInfo().getQueueName());
    Assertions.assertEquals("b",
        bAutoLeafQueue.getQueueInfo().getQueueName());
  }

  @Test
  void testRemoveDanglingAutoCreatedQueuesOnReinit() throws Exception {
    startScheduler();

    // Validate static parent deletion
    createQueue("root.a.a-auto");
    AbstractCSQueue aAuto = (AbstractCSQueue) cs.
        getQueue("root.a.a-auto");
    Assertions.assertTrue(aAuto.isDynamicQueue());

    csConf.setState("root.a", QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());
    aAuto = (AbstractCSQueue) cs.
        getQueue("root.a.a-auto");
    Assertions.assertEquals(QueueState.STOPPED, aAuto.getState(), "root.a.a-auto is not in STOPPED state");
    csConf.setQueues("root", new String[]{"b"});
    cs.reinitialize(csConf, mockRM.getRMContext());
    CSQueue aAutoNew = cs.getQueue("root.a.a-auto");
    Assertions.assertNull(aAutoNew);

    submitApp(cs, USER0, "a-auto", "root.a");
    aAutoNew = cs.getQueue("root.a.a-auto");
    Assertions.assertNotNull(aAutoNew);

    // Validate static grandparent deletion
    csConf.setQueues("root", new String[]{"a", "b"});
    csConf.setQueues("root.a", new String[]{"a1"});
    csConf.setAutoQueueCreationV2Enabled("root.a.a1", true);
    cs.reinitialize(csConf, mockRM.getRMContext());

    createQueue("root.a.a1.a1-auto");
    CSQueue a1Auto = cs.getQueue("root.a.a1.a1-auto");
    Assertions.assertNotNull(a1Auto, "a1-auto should exist");

    csConf.setQueues("root", new String[]{"b"});
    cs.reinitialize(csConf, mockRM.getRMContext());
    a1Auto = cs.getQueue("root.a.a1.a1-auto");
    Assertions.assertNull(a1Auto, "a1-auto has no parent and should not exist");

    // Validate dynamic parent deletion
    csConf.setState("root.b", QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());
    csConf.setAutoQueueCreationV2Enabled("root.b", true);
    cs.reinitialize(csConf, mockRM.getRMContext());

    createQueue("root.b.b-auto-parent.b-auto-leaf");
    CSQueue bAutoParent = cs.getQueue("root.b.b-auto-parent");
    Assertions.assertNotNull(bAutoParent, "b-auto-parent should exist");
    ParentQueue b = (ParentQueue) cs.getQueue("root.b");
    b.removeChildQueue(bAutoParent);

    cs.reinitialize(csConf, mockRM.getRMContext());

    bAutoParent = cs.getQueue("root.b.b-auto-parent");
    Assertions.assertNull(bAutoParent, "b-auto-parent should not exist ");
    CSQueue bAutoLeaf = cs.getQueue("root.b.b-auto-parent.b-auto-leaf");
    Assertions.assertNull(bAutoLeaf, "b-auto-leaf should not exist " +
        "when its dynamic parent is removed");
  }

  @Test
  void testParentQueueDynamicChildRemoval() throws Exception {
    startScheduler();

    createQueue("root.a.a-auto");
    createQueue("root.a.a-auto");
    AbstractCSQueue aAuto = (AbstractCSQueue) cs.
        getQueue("root.a.a-auto");
    Assertions.assertTrue(aAuto.isDynamicQueue());
    ParentQueue a = (ParentQueue) cs.
        getQueue("root.a");
    createQueue("root.e.e1-auto");
    AbstractCSQueue eAuto = (AbstractCSQueue) cs.
        getQueue("root.e.e1-auto");
    Assertions.assertTrue(eAuto.isDynamicQueue());
    ParentQueue e = (ParentQueue) cs.
        getQueue("root.e");

    // Try to remove a static child queue
    try {
      a.removeChildQueue(cs.getQueue("root.a.a1"));
      Assertions.fail("root.a.a1 is a static queue and should not be removed at " +
          "runtime");
    } catch (SchedulerDynamicEditException ignored) {
    }

    // Try to remove a dynamic queue with a different parent
    try {
      a.removeChildQueue(eAuto);
      Assertions.fail("root.a should not be able to remove root.e.e1-auto");
    } catch (SchedulerDynamicEditException ignored) {
    }

    a.removeChildQueue(aAuto);
    e.removeChildQueue(eAuto);

    aAuto = (AbstractCSQueue) cs.
        getQueue("root.a.a-auto");
    eAuto = (AbstractCSQueue) cs.
        getQueue("root.e.e1-auto");

    Assertions.assertNull(aAuto, "root.a.a-auto should have been removed");
    Assertions.assertNull(eAuto, "root.e.e1-auto should have been removed");
  }

  protected AbstractLeafQueue createQueue(String queuePath) throws YarnException,
      IOException {
    return autoQueueHandler.createQueue(new QueuePath(queuePath));
  }

  private void assertQueueMinResource(CSQueue queue, float expected) {
    Assertions.assertEquals(queue.getQueueResourceQuotas().getEffectiveMinResource()
            .getMemorySize(),
        1e-6, Math.round(expected * GB));
  }
}
