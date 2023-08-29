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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.HashSet;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;

public class TestCapacitySchedulerNewQueueAutoCreation
    extends TestCapacitySchedulerAutoCreatedQueueBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      org.apache.hadoop.yarn.server.resourcemanager
          .scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.class);
  public static final int GB = 1024;
  public static final int MAX_MEMORY = 1200;
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
        /   |   \
      a     b    e
    /
  a1
   */
  @Before
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

  @After
  public void tearDown() {
    if (mockRM != null) {
      mockRM.stop();
    }
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
    Assert.assertEquals(1 / 3f, c.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, c.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(400 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assert.assertEquals(((LeafQueue)c).getUserLimitFactor(), -1, 1e-6);
    Assert.assertEquals(((LeafQueue)c).getMaxAMResourcePerQueuePercent(), 1, 1e-6);

    // Now add another queue-d, in the same hierarchy
    createQueue("root.d-auto");

    // Because queue-d has the same weight of other sibling queue, its abs cap
    // become 1/4
    CSQueue d = cs.getQueue("root.d-auto");
    Assert.assertEquals(1 / 4f, d.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, d.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(300 * GB,
        d.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Now we check queue c again, it should also become 1/4 capacity
    Assert.assertEquals(1 / 4f, c.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, c.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(300 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Now we add a two-level queue, create leaf only
    // Now add another queue a2-auto, under root.a
    createQueue("root.a.a2-auto");

    // root.a has 1/4 abs resource, a2/a1 has the same weight, so a2 has 1/8 abs
    // capacity
    CSQueue a2 = cs.getQueue("root.a.a2-auto");
    Assert.assertEquals(1 / 8f, a2.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, a2.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(150 * GB,
        a2.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // try, create leaf + parent, will success
    createQueue("root.e-auto.e1-auto");

    // Now check capacity of e and e1 (under root we have 5 queues, so e1 get
    // 1/5 capacity
    CSQueue e = cs.getQueue("root.e-auto");
    Assert.assertEquals(1 / 5f, e.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, e.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(240 * GB,
        e.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Under e, there's only one queue, so e1/e have same capacity
    CSQueue e1 = cs.getQueue("root.e-auto.e1-auto");
    Assert.assertEquals(1 / 5f, e1.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, e1.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(240 * GB,
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
  public void testAutoCreateQueueWithSiblingsUnderRoot() throws Exception {
    startScheduler();

    createQueue("root.c-auto");

    // Check if queue c-auto got created
    CSQueue c = cs.getQueue("root.c-auto");
    Assert.assertEquals(1 / 3f, c.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, c.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(400 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Now add another queue-d, in the same hierarchy
    createQueue("root.d-auto");

    // Because queue-d has the same weight of other sibling queue, its abs cap
    // become 1/4
    CSQueue d = cs.getQueue("root.d-auto");
    Assert.assertEquals(1 / 4f, d.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, d.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(300 * GB,
        d.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Now we check queue c again, it should also become 1/4 capacity
    Assert.assertEquals(1 / 4f, c.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, c.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(300 * GB,
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
  public void testAutoCreateQueueStaticParentOneLevel() throws Exception {
    startScheduler();
    // Now we add a two-level queue, create leaf only
    // Now add another queue a2-auto, under root.a
    createQueue("root.a.a2-auto");

    // root.a has 1/2 abs resource, a2/a1 has the same weight, so a2 has 1/4 abs
    // capacity
    CSQueue a2 = cs.getQueue("root.a.a2-auto");
    Assert.assertEquals(1 / 4f, a2.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, a2.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(MAX_MEMORY * (1 / 4f) * GB,
        a2.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize(),
        1e-6);

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
  public void testAutoCreateQueueAutoParentTwoLevelsWithSiblings()
      throws Exception {
    startScheduler();
    csConf.setAutoQueueCreationV2Enabled("root.a.a2-auto", true);

    // root.a has 1/2 abs resource -> a1 and a2-auto same weight 1/4
    // -> a3-auto is alone with weight 1/4
    createQueue("root.a.a2-auto.a3-auto");
    CSQueue a3 = cs.getQueue("root.a.a2-auto.a3-auto");
    Assert.assertEquals(1 / 4f, a3.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, a3.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(MAX_MEMORY * (1 / 4f) * GB,
        a3.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize(),
        1e-6);

    // root.a has 1/2 abs resource -> a1 and a2-auto same weight 1/4
    // -> a3-auto and a4-auto same weight 1/8
    createQueue("root.a.a2-auto.a4-auto");
    CSQueue a4 = cs.getQueue("root.a.a2-auto.a4-auto");
    Assert.assertEquals(1 / 8f, a3.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, a3.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(MAX_MEMORY * (1 / 8f) * GB,
        a4.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize(),
        1e-6);
  }

  @Test(expected = SchedulerDynamicEditException.class)
  public void testAutoCreateQueueShouldFailWhenNonParentQueue()
      throws Exception {
    startScheduler();
    createQueue("root.a.a1.a2-auto");
  }

  @Test(expected = SchedulerDynamicEditException.class)
  public void testAutoCreateQueueWhenSiblingsNotInWeightMode()
      throws Exception {
    startScheduler();
    // If the new queue mode is used it's allowed to
    // create a new dynamic queue when the sibling is
    // not in weight mode
    assumeThat(csConf.isLegacyQueueMode(), is(true));
    csConf.setCapacity("root.a", 50f);
    csConf.setCapacity("root.b", 50f);
    csConf.setCapacity("root.a.a1", 100f);
    cs.reinitialize(csConf, mockRM.getRMContext());
    createQueue("root.a.a2-auto");
  }

  @Test()
  public void testAutoCreateMaximumQueueDepth()
      throws Exception {
    startScheduler();
    // By default, max depth is 2, therefore this is an invalid scenario
    Assert.assertThrows(SchedulerDynamicEditException.class,
        () -> createQueue("root.a.a3-auto.a4-auto.a5-auto"));

    // Set depth 3 for root.a, making it a valid scenario
    csConf.setMaximumAutoCreatedQueueDepth("root.a", 3);
    cs.reinitialize(csConf, mockRM.getRMContext());
    try {
      createQueue("root.a.a3-auto.a4-auto.a5-auto");
    } catch (SchedulerDynamicEditException sde) {
      LOG.error("%s", sde);
      Assert.fail("Depth is set for root.a, exception should not be thrown");
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
      Assert.fail("Depth is set globally, exception should not be thrown");
    }

    // Set depth on a dynamic queue, which has no effect on auto queue creation validation
    csConf.setMaximumAutoCreatedQueueDepth("root.a.a6-auto.a7-auto.a8-auto", 10);
    Assert.assertThrows(SchedulerDynamicEditException.class,
        () -> createQueue("root.a.a6-auto.a7-auto.a8-auto.a9-auto.a10-auto.a11-auto"));
  }

  @Test(expected = SchedulerDynamicEditException.class)
  public void testAutoCreateQueueShouldFailIfNotEnabledForParent()
      throws Exception {
    startScheduler();
    csConf.setAutoQueueCreationV2Enabled("root", false);
    cs.reinitialize(csConf, mockRM.getRMContext());
    createQueue("root.c-auto");
  }

  @Test
  public void testAutoCreateQueueRefresh() throws Exception {
    startScheduler();

    createBasicQueueStructureAndValidate();

    // Refresh the queue to make sure all queues are still exist.
    // (Basically, dynamic queues should not disappear after refresh).
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Double confirm, after refresh, we should still see root queue has 5
    // children.
    Assert.assertEquals(5, cs.getQueue("root").getChildQueues().size());
    Assert.assertNotNull(cs.getQueue("root.c-auto"));
  }

  @Test
  public void testConvertDynamicToStaticQueue() throws Exception {
    startScheduler();

    createBasicQueueStructureAndValidate();

    // Now, update root.a's weight to 6
    csConf.setNonLabeledQueueWeight("root.a", 6f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Double confirm, after refresh, we should still see root queue has 5
    // children.
    Assert.assertEquals(5, cs.getQueue("root").getChildQueues().size());

    // Get queue a
    CSQueue a = cs.getQueue("root.a");

    // a's abs resource should be 6/10, (since a.weight=6, all other 4 peers
    // have weight=1).
    Assert.assertEquals(6 / 10f, a.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(720 * GB,
        a.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assert.assertEquals(6f, a.getQueueCapacities().getWeight(), 1e-6);

    // Set queue c-auto's weight to 6, and mark c-auto to be static queue
    csConf.setQueues("root", new String[]{"a", "b", "c-auto"});
    csConf.setNonLabeledQueueWeight("root.c-auto", 6f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Get queue c
    CSQueue c = cs.getQueue("root.c-auto");

    // c's abs resource should be 6/15, (since a/c.weight=6, all other 3 peers
    // have weight=1).
    Assert.assertEquals(6 / 15f, c.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(480 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assert.assertEquals(6f, c.getQueueCapacities().getWeight(), 1e-6);

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
    Assert.assertEquals(e1NormalizedWeight, e1.getAbsoluteCapacity(), 1e-6);
    assertQueueMinResource(e1, MAX_MEMORY * e1NormalizedWeight);
    Assert.assertEquals(6f, e1.getQueueCapacities().getWeight(), 1e-6);
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
  public void testConvertDynamicParentToStaticParent() throws Exception {
    startScheduler();
    createQueue("root.d-auto.d1-auto");
    csConf.setQueues("root", new String[]{"a", "b", "d-auto"});
    csConf.setNonLabeledQueueWeight("root.a", 6f);
    csConf.setNonLabeledQueueWeight("root.d-auto", 1f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    CSQueue d = cs.getQueue("root.d-auto");

    Assert.assertEquals(1 / 8f, d.getAbsoluteCapacity(), 1e-6);
    assertQueueMinResource(d, MAX_MEMORY * (1 / 8f));
    Assert.assertEquals(1f, d.getQueueCapacities().getWeight(), 1e-6);

    CSQueue d1 = cs.getQueue("root.d-auto.d1-auto");
    Assert.assertEquals(1 / 8f, d1.getAbsoluteCapacity(), 1e-6);
    assertQueueMinResource(d1, MAX_MEMORY * (1 / 8f));
    Assert.assertEquals(1f, d1.getQueueCapacities().getWeight(), 1e-6);
  }

  @Test
  public void testAutoQueueCreationOnAppSubmission() throws Exception {
    startScheduler();

    submitApp(cs, USER0, USER0, "root.e-auto");

    AbstractCSQueue e = (AbstractCSQueue) cs.getQueue("root.e-auto");
    Assert.assertNotNull(e);
    Assert.assertTrue(e.isDynamicQueue());

    AbstractCSQueue user0 = (AbstractCSQueue) cs.getQueue(
        "root.e-auto." + USER0);
    Assert.assertNotNull(user0);
    Assert.assertTrue(user0.isDynamicQueue());
  }

  @Test
  public void testChildlessParentQueueWhenAutoQueueCreationEnabled()
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
    Assert.assertTrue("empty-auto-parent is not a LeafQueue",
        empty instanceof LeafQueue);
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
    Assert.assertTrue("empty-auto-parent is not a ParentQueue",
        empty instanceof AbstractParentQueue);
    Assert.assertEquals("empty-auto-parent has children",
        0, empty.getChildQueues().size());
    Assert.assertTrue("empty-auto-parent is not eligible " +
            "for auto queue creation",
        ((AbstractParentQueue)empty).isEligibleForAutoQueueCreation());
  }

  @Test
  public void testAutoQueueCreationWithDisabledMappingRules() throws Exception {
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
    Assert.assertNotNull(a2Auto);
  }

  @Test
  public void testAutoCreateQueueUserLimitDisabled() throws Exception {
    startScheduler();
    createBasicQueueStructureAndValidate();

    submitApp(cs, USER0, USER0, "root.e-auto");

    AbstractCSQueue e = (AbstractCSQueue) cs.getQueue("root.e-auto");
    Assert.assertNotNull(e);
    Assert.assertTrue(e.isDynamicQueue());

    AbstractCSQueue user0 = (AbstractCSQueue) cs.getQueue(
        "root.e-auto." + USER0);
    Assert.assertNotNull(user0);
    Assert.assertTrue(user0.isDynamicQueue());
    Assert.assertTrue(user0 instanceof LeafQueue);

    LeafQueue user0LeafQueue = (LeafQueue) user0;

    // Assert user limit factor is -1
    Assert.assertTrue(user0LeafQueue.getUserLimitFactor() == -1);

    // Assert user max applications not limited
    Assert.assertEquals(user0LeafQueue.getMaxApplicationsPerUser(),
        user0LeafQueue.getMaxApplications());

    // Assert AM Resource
    Assert.assertEquals(user0LeafQueue.getAMResourceLimit().getMemorySize(),
        user0LeafQueue.
            getMaxAMResourcePerQueuePercent() * MAX_MEMORY * GB, 1e-6);

    // Assert user limit (no limit) when limit factor is -1
    Assert.assertEquals(MAX_MEMORY * GB,
        user0LeafQueue.getEffectiveMaxCapacityDown("",
            user0LeafQueue.getMinimumAllocation()).getMemorySize(), 1e-6);
  }

  @Test
  public void testAutoQueueCreationMaxAppUpdate() throws Exception {
    startScheduler();

    // When no conf for max apps
    LeafQueue a1 =  (LeafQueue)cs.
        getQueue("root.a.a1");
    Assert.assertNotNull(a1);
    Assert.assertEquals(csConf.getMaximumSystemApplications()
            * a1.getAbsoluteCapacity(), a1.getMaxApplications(), 1);

    LeafQueue b = (LeafQueue)cs.
        getQueue("root.b");
    Assert.assertNotNull(b);
    Assert.assertEquals(csConf.getMaximumSystemApplications()
            * b.getAbsoluteCapacity(), b.getMaxApplications(), 1);

    createQueue("root.e");

    // Make sure other children queues
    // max app correct.
    LeafQueue e = (LeafQueue)cs.
        getQueue("root.e");
    Assert.assertNotNull(e);
    Assert.assertEquals(csConf.getMaximumSystemApplications()
            * e.getAbsoluteCapacity(), e.getMaxApplications(), 1);

    a1 =  (LeafQueue)cs.
        getQueue("root.a.a1");
    Assert.assertNotNull(a1);
    Assert.assertEquals(csConf.getMaximumSystemApplications()
            * a1.getAbsoluteCapacity(), a1.getMaxApplications(), 1);

    b = (LeafQueue)cs.
        getQueue("root.b");
    Assert.assertNotNull(b);
    Assert.assertEquals(csConf.getMaximumSystemApplications()
            * b.getAbsoluteCapacity(), b.getMaxApplications(), 1);

    // When update global max app per queue
    csConf.setGlobalMaximumApplicationsPerQueue(1000);
    cs.reinitialize(csConf, mockRM.getRMContext());
    Assert.assertEquals(1000, b.getMaxApplications());
    Assert.assertEquals(1000, a1.getMaxApplications());
    Assert.assertEquals(1000, e.getMaxApplications());

    // when set some queue for max apps
    csConf.setMaximumApplicationsPerQueue("root.e1", 50);
    createQueue("root.e1");
    LeafQueue e1 = (LeafQueue)cs.
        getQueue("root.e1");
    Assert.assertNotNull(e1);

    cs.reinitialize(csConf, mockRM.getRMContext());
    Assert.assertEquals(50, e1.getMaxApplications());
  }

  @Test(expected = SchedulerDynamicEditException.class)
  public void testAutoCreateQueueWithAmbiguousNonFullPathParentName()
      throws Exception {
    startScheduler();

    createQueue("root.a.a");
    createQueue("a.a");
  }

  @Test
  public void testAutoCreateQueueIfFirstExistingParentQueueIsNotStatic()
      throws Exception {
    startScheduler();

    // create a dynamic ParentQueue
    createQueue("root.a.a-parent-auto.a1-leaf-auto");
    Assert.assertNotNull(cs.getQueue("root.a.a-parent-auto"));

    // create a new dynamic LeafQueue under the existing ParentQueue
    createQueue("root.a.a-parent-auto.a2-leaf-auto");

    CSQueue a2Leaf = cs.getQueue("a2-leaf-auto");

    // Make sure a2-leaf-auto is under a-parent-auto
    Assert.assertEquals("root.a.a-parent-auto",
        a2Leaf.getParent().getQueuePath());
  }

  @Test
  public void testAutoCreateQueueIfAmbiguousQueueNames() throws Exception {
    startScheduler();

    AbstractCSQueue b = (AbstractCSQueue) cs.getQueue("root.b");
    Assert.assertFalse(b.isDynamicQueue());

    createQueue("root.a.b.b");

    AbstractCSQueue bAutoParent = (AbstractCSQueue) cs.getQueue("root.a.b");
    Assert.assertTrue(bAutoParent.isDynamicQueue());
    Assert.assertTrue(bAutoParent.hasChildQueues());

    AbstractCSQueue bAutoLeafQueue =
        (AbstractCSQueue) cs.getQueue("root.a.b.b");
    Assert.assertTrue(bAutoLeafQueue.isDynamicQueue());
    Assert.assertFalse(bAutoLeafQueue.hasChildQueues());
  }

  @Test
  public void testAutoCreateQueueMaxQueuesLimit() throws Exception {
    startScheduler();

    csConf.setAutoCreatedQueuesV2MaxChildQueuesLimit("root.e", 5);
    cs.reinitialize(csConf, mockRM.getRMContext());

    for (int i = 0; i < 5; ++i) {
      createQueue("root.e.q_" + i);
    }

    // Check if max queue limit can't be exceeded
    try {
      createQueue("root.e.q_6");
      Assert.fail("Can't exceed max queue limit.");
    } catch (Exception ex) {
      Assert.assertTrue(ex
          instanceof SchedulerDynamicEditException);
    }
  }

  @Test
  public void testAutoCreatedQueueTemplateConfig() throws Exception {
    startScheduler();
    csConf.set(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        "root.a.*") + "capacity", "6w");
    cs.reinitialize(csConf, mockRM.getRMContext());

    AbstractLeafQueue a2 = createQueue("root.a.a-auto.a2");
    Assert.assertEquals("weight is not set by template", 6f,
        a2.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals("user limit factor should be disabled with dynamic queues",
        -1f, a2.getUserLimitFactor(), 1e-6);
    Assert.assertEquals("maximum AM resource percent should be 1 with dynamic queues",
        1f, a2.getMaxAMResourcePerQueuePercent(), 1e-6);

    // Set the user-limit-factor and maximum-am-resource-percent via templates to ensure their
    // modified defaults are indeed overridden
    csConf.set(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        "root.a.*") + "user-limit-factor", "10");
    csConf.set(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        "root.a.*") + "maximum-am-resource-percent", "0.8");

    cs.reinitialize(csConf, mockRM.getRMContext());
    a2 = (LeafQueue) cs.getQueue("root.a.a-auto.a2");
    Assert.assertEquals("weight is overridden", 6f,
        a2.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals("user limit factor should be modified by templates",
        10f, a2.getUserLimitFactor(), 1e-6);
    Assert.assertEquals("maximum AM resource percent should be modified by templates",
        0.8f, a2.getMaxAMResourcePerQueuePercent(), 1e-6);


    csConf.setNonLabeledQueueWeight("root.a.a-auto.a2", 4f);
    cs.reinitialize(csConf, mockRM.getRMContext());
    Assert.assertEquals("weight is not explicitly set", 4f,
        a2.getQueueCapacities().getWeight(), 1e-6);

    csConf.setBoolean(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        "root.a") + CapacitySchedulerConfiguration
        .AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE, false);
    cs.reinitialize(csConf, mockRM.getRMContext());
    AbstractLeafQueue a3 = createQueue("root.a.a3");
    Assert.assertFalse("auto queue deletion should be turned off on a3",
        a3.isEligibleForAutoDeletion());

    // Set the capacity of label TEST
    csConf.set(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        "root.c") + "accessible-node-labels.TEST.capacity", "6w");
    csConf.setQueues("root", new String[]{"a", "b", "c"});
    csConf.setAutoQueueCreationV2Enabled("root.c", true);
    cs.reinitialize(csConf, mockRM.getRMContext());
    AbstractLeafQueue c1 = createQueue("root.c.c1");
    Assert.assertEquals("weight is not set for label TEST", 6f,
        c1.getQueueCapacities().getWeight("TEST"), 1e-6);
    cs.reinitialize(csConf, mockRM.getRMContext());
    c1 = (AbstractLeafQueue) cs.getQueue("root.c.c1");
    Assert.assertEquals("weight is not set for label TEST", 6f,
        c1.getQueueCapacities().getWeight("TEST"), 1e-6);
  }

  @Test
  public void testAutoCreatedQueueConfigChange() throws Exception {
    startScheduler();
    AbstractLeafQueue a2 = createQueue("root.a.a-auto.a2");
    csConf.setNonLabeledQueueWeight("root.a.a-auto.a2", 4f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    Assert.assertEquals("weight is not explicitly set", 4f,
        a2.getQueueCapacities().getWeight(), 1e-6);

    a2 = (AbstractLeafQueue) cs.getQueue("root.a.a-auto.a2");
    csConf.setState("root.a.a-auto.a2", QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());
    Assert.assertEquals("root.a.a-auto.a2 has not been stopped",
        QueueState.STOPPED, a2.getState());

    csConf.setState("root.a.a-auto.a2", QueueState.RUNNING);
    cs.reinitialize(csConf, mockRM.getRMContext());
    Assert.assertEquals("root.a.a-auto.a2 is not running",
        QueueState.RUNNING, a2.getState());
  }

  @Test
  public void testAutoCreateQueueState() throws Exception {
    startScheduler();

    createQueue("root.e.e1");
    csConf.setState("root.e", QueueState.STOPPED);
    csConf.setState("root.e.e1", QueueState.STOPPED);
    csConf.setState("root.a", QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Make sure the static queue is stopped
    Assert.assertEquals(cs.getQueue("root.a").getState(),
        QueueState.STOPPED);
    // If not set, default is the queue state of parent
    Assert.assertEquals(cs.getQueue("root.a.a1").getState(),
        QueueState.STOPPED);

    Assert.assertEquals(cs.getQueue("root.e").getState(),
        QueueState.STOPPED);
    Assert.assertEquals(cs.getQueue("root.e.e1").getState(),
        QueueState.STOPPED);

    // Make root.e state to RUNNING
    csConf.setState("root.e", QueueState.RUNNING);
    cs.reinitialize(csConf, mockRM.getRMContext());
    Assert.assertEquals(cs.getQueue("root.e.e1").getState(),
        QueueState.STOPPED);

    // Make root.e.e1 state to RUNNING
    csConf.setState("root.e.e1", QueueState.RUNNING);
    cs.reinitialize(csConf, mockRM.getRMContext());
    Assert.assertEquals(cs.getQueue("root.e.e1").getState(),
        QueueState.RUNNING);
  }

  @Test
  public void testAutoQueueCreationDepthLimitFromStaticParent()
      throws Exception {
    startScheduler();

    // a is the first existing queue here and it is static, therefore
    // the distance is 2
    createQueue("root.a.a-auto.a1-auto");
    Assert.assertNotNull(cs.getQueue("root.a.a-auto.a1-auto"));

    try {
      createQueue("root.a.a-auto.a2-auto.a3-auto");
      Assert.fail("Queue creation should not succeed because the distance " +
          "from the first static parent is above limit");
    } catch (SchedulerDynamicEditException ignored) {

    }

  }

  @Test
  public void testCapacitySchedulerAutoQueueDeletion() throws Exception {
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

    Assert.assertTrue(
        "No AutoCreatedQueueDeletionPolicy " +
            "is present in running monitors",
        cs.getSchedulingMonitorManager().
            isSameConfiguredPolicies(policies));

    ApplicationAttemptId a2App = submitApp(cs, USER0,
        "a2-auto", "root.a.a1-auto");

    // Wait a2 created successfully.
    GenericTestUtils.waitFor(()-> cs.getQueue(
        "root.a.a1-auto.a2-auto") != null,
        100, 2000);

    AbstractCSQueue a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    Assert.assertNotNull("a1 is not present", a1);
    AbstractCSQueue a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    Assert.assertNotNull("a2 is not present", a2);
    Assert.assertTrue("a2 is not a dynamic queue",
        a2.isDynamicQueue());

    // Now there are still 1 app in a2 queue.
    Assert.assertEquals(1, a2.getNumApplications());

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
    Assert.assertNotNull("a2 is not present", a2);

    // Make app finished.
    AppAttemptRemovedSchedulerEvent event =
        new AppAttemptRemovedSchedulerEvent(a2App,
            RMAppAttemptState.FINISHED, false);
    cs.handle(event);
    AppRemovedSchedulerEvent rEvent = new AppRemovedSchedulerEvent(
        a2App.getApplicationId(), RMAppState.FINISHED);
    cs.handle(rEvent);

    // Now there are no apps in a2 queue.
    Assert.assertEquals(0, a2.getNumApplications());

    // Wait the a2 deleted.
    GenericTestUtils.waitFor(() -> {
      AbstractCSQueue a2Tmp = (AbstractCSQueue) cs.getQueue(
            "root.a.a1-auto.a2-auto");
      return a2Tmp == null;
    }, 100, 3000);

    a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    Assert.assertNull("a2 is not deleted", a2);

    // The parent will not be deleted with child queues
    a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    Assert.assertNotNull("a1 is not present", a1);

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
    Assert.assertNull("a1 is not deleted", a1);
  }

  @Test
  public void testCapacitySchedulerAutoQueueDeletionDisabled()
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

    Assert.assertTrue(
        "No AutoCreatedQueueDeletionPolicy " +
            "is present in running monitors",
        cs.getSchedulingMonitorManager().
            isSameConfiguredPolicies(policies));

    ApplicationAttemptId a2App = submitApp(cs, USER0,
        "a2-auto", "root.a.a1-auto");

    // Wait a2 created successfully.
    GenericTestUtils.waitFor(()-> cs.getQueue(
        "root.a.a1-auto.a2-auto") != null,
        100, 2000);

    AbstractCSQueue a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    Assert.assertNotNull("a1 is not present", a1);
    AbstractCSQueue a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    Assert.assertNotNull("a2 is not present", a2);
    Assert.assertTrue("a2 is not a dynamic queue",
        a2.isDynamicQueue());

    // Make app finished.
    AppAttemptRemovedSchedulerEvent event =
        new AppAttemptRemovedSchedulerEvent(a2App,
            RMAppAttemptState.FINISHED, false);
    cs.handle(event);
    AppRemovedSchedulerEvent rEvent = new AppRemovedSchedulerEvent(
        a2App.getApplicationId(), RMAppState.FINISHED);
    cs.handle(rEvent);

    // Now there are no apps in a2 queue.
    Assert.assertEquals(0, a2.getNumApplications());

    // Wait the time expired.
    long l1 = a2.getLastSubmittedTimestamp();
    GenericTestUtils.waitFor(() -> {
      long duration = (Time.monotonicNow() - l1)/1000;
      return duration > csConf.getAutoExpiredDeletionTime();
    }, 100, 2000);

    // The auto deletion is no enabled for a2-auto
    a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    Assert.assertNotNull("a1 is not present", a1);
    a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    Assert.assertNotNull("a2 is not present", a2);
    Assert.assertTrue("a2 is not a dynamic queue",
        a2.isDynamicQueue());

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
    Assert.assertNull("a2 is not deleted", a2);
    // The parent will not be deleted with child queues
    a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    Assert.assertNotNull("a1 is not present", a1);

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
    Assert.assertNull("a1 is not deleted", a1);
  }

  @Test
  public void testAutoCreateQueueAfterRemoval() throws Exception {
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
    Assert.assertEquals(1 / 5f, e1.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, e1.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(240 * GB,
        e1.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check after removal e1.
    cs.removeQueue(e1);
    CSQueue e = cs.getQueue("root.e-auto");
    Assert.assertEquals(1 / 5f, e.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, e.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(240 * GB,
        e.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check after removal e.
    cs.removeQueue(e);
    CSQueue d = cs.getQueue("root.d-auto");
    Assert.assertEquals(1 / 4f, d.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, d.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(300 * GB,
        d.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check after removal d.
    cs.removeQueue(d);
    CSQueue c = cs.getQueue("root.c-auto");
    Assert.assertEquals(1 / 3f, c.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, c.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(400 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check after removal c.
    cs.removeQueue(c);
    CSQueue b = cs.getQueue("root.b");
    Assert.assertEquals(1 / 2f, b.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, b.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(600 * GB,
        b.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check can't remove static queue b.
    try {
      cs.removeQueue(b);
      Assert.fail("Can't remove static queue b!");
    } catch (Exception ex) {
      Assert.assertTrue(ex
          instanceof SchedulerDynamicEditException);
    }
    // Check a.
    CSQueue a = cs.getQueue("root.a");
    Assert.assertEquals(1 / 2f, a.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(1f, a.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(600 * GB,
        b.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
  }

  @Test
  public void testQueueInfoIfAmbiguousQueueNames() throws Exception {
    startScheduler();

    AbstractCSQueue b = (AbstractCSQueue) cs.
        getQueue("root.b");
    Assert.assertFalse(b.isDynamicQueue());
    Assert.assertEquals("root.b",
        b.getQueueInfo().getQueuePath());

    createQueue("root.a.b.b");

    AbstractCSQueue bAutoParent = (AbstractCSQueue) cs.
        getQueue("root.a.b");
    Assert.assertTrue(bAutoParent.isDynamicQueue());
    Assert.assertTrue(bAutoParent.hasChildQueues());
    Assert.assertEquals("root.a.b",
        bAutoParent.getQueueInfo().getQueuePath());

    AbstractCSQueue bAutoLeafQueue =
        (AbstractCSQueue) cs.getQueue("root.a.b.b");
    Assert.assertTrue(bAutoLeafQueue.isDynamicQueue());
    Assert.assertFalse(bAutoLeafQueue.hasChildQueues());
    Assert.assertEquals("root.a.b.b",
        bAutoLeafQueue.getQueueInfo().getQueuePath());

    // Make sure all queue name are ambiguous
    Assert.assertEquals("b",
        b.getQueueInfo().getQueueName());
    Assert.assertEquals("b",
        bAutoParent.getQueueInfo().getQueueName());
    Assert.assertEquals("b",
        bAutoLeafQueue.getQueueInfo().getQueueName());
  }

  @Test
  public void testRemoveDanglingAutoCreatedQueuesOnReinit() throws Exception {
    startScheduler();

    // Validate static parent deletion
    createQueue("root.a.a-auto");
    AbstractCSQueue aAuto = (AbstractCSQueue) cs.
        getQueue("root.a.a-auto");
    Assert.assertTrue(aAuto.isDynamicQueue());

    csConf.setState("root.a", QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());
    aAuto = (AbstractCSQueue) cs.
        getQueue("root.a.a-auto");
    Assert.assertEquals("root.a.a-auto is not in STOPPED state", QueueState.STOPPED, aAuto.getState());
    csConf.setQueues("root", new String[]{"b"});
    cs.reinitialize(csConf, mockRM.getRMContext());
    CSQueue aAutoNew = cs.getQueue("root.a.a-auto");
    Assert.assertNull(aAutoNew);

    submitApp(cs, USER0, "a-auto", "root.a");
    aAutoNew = cs.getQueue("root.a.a-auto");
    Assert.assertNotNull(aAutoNew);

    // Validate static grandparent deletion
    csConf.setQueues("root", new String[]{"a", "b"});
    csConf.setQueues("root.a", new String[]{"a1"});
    csConf.setAutoQueueCreationV2Enabled("root.a.a1", true);
    cs.reinitialize(csConf, mockRM.getRMContext());

    createQueue("root.a.a1.a1-auto");
    CSQueue a1Auto = cs.getQueue("root.a.a1.a1-auto");
    Assert.assertNotNull("a1-auto should exist", a1Auto);

    csConf.setQueues("root", new String[]{"b"});
    cs.reinitialize(csConf, mockRM.getRMContext());
    a1Auto = cs.getQueue("root.a.a1.a1-auto");
    Assert.assertNull("a1-auto has no parent and should not exist", a1Auto);

    // Validate dynamic parent deletion
    csConf.setState("root.b", QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());
    csConf.setAutoQueueCreationV2Enabled("root.b", true);
    cs.reinitialize(csConf, mockRM.getRMContext());

    createQueue("root.b.b-auto-parent.b-auto-leaf");
    CSQueue bAutoParent = cs.getQueue("root.b.b-auto-parent");
    Assert.assertNotNull("b-auto-parent should exist", bAutoParent);
    ParentQueue b = (ParentQueue) cs.getQueue("root.b");
    b.removeChildQueue(bAutoParent);

    cs.reinitialize(csConf, mockRM.getRMContext());

    bAutoParent = cs.getQueue("root.b.b-auto-parent");
    Assert.assertNull("b-auto-parent should not exist ", bAutoParent);
    CSQueue bAutoLeaf = cs.getQueue("root.b.b-auto-parent.b-auto-leaf");
    Assert.assertNull("b-auto-leaf should not exist " +
        "when its dynamic parent is removed", bAutoLeaf);
  }

  @Test
  public void testParentQueueDynamicChildRemoval() throws Exception {
    startScheduler();

    createQueue("root.a.a-auto");
    createQueue("root.a.a-auto");
    AbstractCSQueue aAuto = (AbstractCSQueue) cs.
        getQueue("root.a.a-auto");
    Assert.assertTrue(aAuto.isDynamicQueue());
    ParentQueue a = (ParentQueue) cs.
        getQueue("root.a");
    createQueue("root.e.e1-auto");
    AbstractCSQueue eAuto = (AbstractCSQueue) cs.
        getQueue("root.e.e1-auto");
    Assert.assertTrue(eAuto.isDynamicQueue());
    ParentQueue e = (ParentQueue) cs.
        getQueue("root.e");

    // Try to remove a static child queue
    try {
      a.removeChildQueue(cs.getQueue("root.a.a1"));
      Assert.fail("root.a.a1 is a static queue and should not be removed at " +
          "runtime");
    } catch (SchedulerDynamicEditException ignored) {
    }

    // Try to remove a dynamic queue with a different parent
    try {
      a.removeChildQueue(eAuto);
      Assert.fail("root.a should not be able to remove root.e.e1-auto");
    } catch (SchedulerDynamicEditException ignored) {
    }

    a.removeChildQueue(aAuto);
    e.removeChildQueue(eAuto);

    aAuto = (AbstractCSQueue) cs.
        getQueue("root.a.a-auto");
    eAuto = (AbstractCSQueue) cs.
        getQueue("root.e.e1-auto");

    Assert.assertNull("root.a.a-auto should have been removed", aAuto);
    Assert.assertNull("root.e.e1-auto should have been removed", eAuto);
  }

  @Test()
  public void testAutoCreateInvalidParent() throws Exception {
    startScheduler();
    Assert.assertThrows(SchedulerDynamicEditException.class,
        () -> createQueue("invalid.queue"));
    Assert.assertThrows(SchedulerDynamicEditException.class,
        () -> createQueue("invalid.queue.longer"));
    Assert.assertThrows(SchedulerDynamicEditException.class,
        () -> createQueue("invalidQueue"));
  }

  protected AbstractLeafQueue createQueue(String queuePath) throws YarnException,
      IOException {
    return autoQueueHandler.createQueue(new QueuePath(queuePath));
  }

  private void assertQueueMinResource(CSQueue queue, float expected) {
    Assert.assertEquals(Math.round(expected * GB),
        queue.getQueueResourceQuotas().getEffectiveMinResource()
            .getMemorySize(), 1e-6);
  }
}
