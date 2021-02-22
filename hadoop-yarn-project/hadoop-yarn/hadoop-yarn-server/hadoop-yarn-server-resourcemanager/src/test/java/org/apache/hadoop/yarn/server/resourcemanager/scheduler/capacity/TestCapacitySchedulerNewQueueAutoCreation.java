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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private CapacitySchedulerAutoQueueHandler autoQueueHandler;

  /*
  Create the following structure:
           root
        /       \
      a          b
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
  }

  private void startScheduler() throws Exception {
    RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(csConf);
    mockRM = new MockRM(csConf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    cs = (CapacityScheduler) mockRM.getResourceScheduler();
    cs.updatePlacementRules();
    mockRM.start();
    cs.start();
    autoQueueHandler = new CapacitySchedulerAutoQueueHandler(
        cs.getCapacitySchedulerQueueManager());
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
    csConf.setCapacity("root.a", 50f);
    csConf.setCapacity("root.b", 50f);
    csConf.setCapacity("root.a.a1", 100f);
    cs.reinitialize(csConf, mockRM.getRMContext());
    createQueue("root.a.a2-auto");
  }

  @Test(expected = SchedulerDynamicEditException.class)
  public void testAutoCreateQueueShouldFailIfDepthIsAboveLimit()
      throws Exception {
    startScheduler();
    createQueue("root.a.a3-auto.a4-auto.a5-auto");
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
        empty instanceof ParentQueue);
    Assert.assertEquals("empty-auto-parent has children",
        0, empty.getChildQueues().size());
    Assert.assertTrue("empty-auto-parent is not eligible " +
            "for auto queue creation",
        ((ParentQueue)empty).isEligibleForAutoQueueCreation());
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

    LeafQueue user0LeafQueue = (LeafQueue)user0;

    // Assert user limit factor is -1
    Assert.assertTrue(user0LeafQueue.getUserLimitFactor() == -1);

    // Assert user max applications not limited
    Assert.assertEquals(user0LeafQueue.getMaxApplicationsPerUser(),
        user0LeafQueue.getMaxApplications());

    // Assert AM Resource
    Assert.assertEquals(user0LeafQueue.getAMResourceLimit().getMemorySize(),
        user0LeafQueue.getMaxAMResourcePerQueuePercent()*MAX_MEMORY*GB, 1e-6);

    // Assert user limit (no limit) when limit factor is -1
    Assert.assertEquals(MAX_MEMORY*GB,
        user0LeafQueue.getEffectiveMaxCapacityDown("",
            user0LeafQueue.getMinimumAllocation()).getMemorySize(), 1e-6);
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

  private LeafQueue createQueue(String queuePath) throws YarnException {
    return autoQueueHandler.autoCreateQueue(
        CSQueueUtils.extractQueuePath(queuePath));
  }

  private void assertQueueMinResource(CSQueue queue, float expected) {
    Assert.assertEquals(Math.round(expected * GB),
        queue.getQueueResourceQuotas().getEffectiveMinResource()
            .getMemorySize(), 1e-6);
  }
}
