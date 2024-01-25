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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

public class TestAbsoluteResourceConfiguration {

  private static final int GB = 1024;
  private static final float DELTA = 0.001f;

  private static final String QUEUEA = "queueA";
  private static final String QUEUEB = "queueB";
  private static final String QUEUEC = "queueC";
  private static final String QUEUED = "queueD";
  private static final String QUEUEA1 = "queueA1";
  private static final String QUEUEA2 = "queueA2";
  private static final String QUEUEB1 = "queueB1";

  private static final QueuePath QUEUEA_FULL =
          new QueuePath(CapacitySchedulerConfiguration.ROOT, QUEUEA);
  private static final QueuePath QUEUEB_FULL =
          new QueuePath(CapacitySchedulerConfiguration.ROOT, QUEUEB);
  private static final QueuePath QUEUEC_FULL =
          new QueuePath(CapacitySchedulerConfiguration.ROOT, QUEUEC);
  private static final QueuePath QUEUED_FULL =
          new QueuePath(CapacitySchedulerConfiguration.ROOT, QUEUED);

  private static final QueuePath QUEUEA1_FULL =
          new QueuePath(QUEUEA_FULL.getFullPath() + "." + QUEUEA1);
  private static final QueuePath QUEUEA2_FULL =
          new QueuePath(QUEUEA_FULL.getFullPath() + "." + QUEUEA2);
  private static final QueuePath QUEUEB1_FULL =
          new QueuePath(QUEUEB_FULL.getFullPath() + "." + QUEUEB1);

  private static final Resource QUEUE_A_MINRES = Resource.newInstance(100 * GB,
      10);
  private static final Resource QUEUE_A_MAXRES = Resource.newInstance(200 * GB,
      30);
  private static final Resource QUEUE_A1_MINRES = Resource.newInstance(50 * GB,
      5);
  private static final Resource QUEUE_A2_MINRES = Resource.newInstance(50 * GB,
      5);
  private static final Resource QUEUE_B_MINRES = Resource.newInstance(50 * GB,
      10);
  private static final Resource QUEUE_B1_MINRES = Resource.newInstance(40 * GB,
      10);
  private static final Resource QUEUE_B_MAXRES = Resource.newInstance(150 * GB,
      30);
  private static final Resource QUEUE_C_MINRES = Resource.newInstance(25 * GB,
      5);
  private static final Resource QUEUE_C_MAXRES = Resource.newInstance(150 * GB,
      20);
  private static final Resource QUEUE_D_MINRES = Resource.newInstance(25 * GB,
      5);
  private static final Resource QUEUE_D_MAXRES = Resource.newInstance(150 * GB,
      20);
  private static final Resource QUEUEA_REDUCED = Resource.newInstance(64000, 6);
  private static final Resource QUEUEB_REDUCED = Resource.newInstance(32000, 6);
  private static final Resource QUEUEC_REDUCED = Resource.newInstance(16000, 3);
  private static final Resource QUEUEMAX_REDUCED = Resource.newInstance(128000,
      20);
  private static final Resource QUEUE_D_TEMPL_MINRES =
      Resource.newInstance(25 * GB, 5);
  private static final Resource QUEUE_D_TEMPL_MAXRES =
      Resource.newInstance(150 * GB, 20);
  public static final String X_LABEL = "X";
  public static final String Y_LABEL = "Y";

  private static Set<String> resourceTypes = new HashSet<>(
      Arrays.asList("memory", "vcores"));

  private CapacitySchedulerConfiguration setupNormalizationConfiguration() {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{QUEUEA, QUEUEB});
    csConf.setQueues(QUEUEA_FULL.getFullPath(), new String[]{QUEUEA1, QUEUEA2});

//    60, 28
    csConf.setMinimumResourceRequirement("", QUEUEA_FULL, Resource.newInstance(50 * GB, 20));
    csConf.setMinimumResourceRequirement("", QUEUEA1_FULL, Resource.newInstance(30 * GB, 15));
    csConf.setMinimumResourceRequirement("", QUEUEA2_FULL, Resource.newInstance(20 * GB, 5));
    csConf.setMinimumResourceRequirement("", QUEUEB_FULL, Resource.newInstance(10 * GB, 8));

    return csConf;
  }

  private CapacitySchedulerConfiguration setupSimpleQueueConfiguration(
      boolean isCapacityNeeded) {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{QUEUEA, QUEUEB, QUEUEC, QUEUED});

    // Set default capacities like normal configuration.
    if (isCapacityNeeded) {
      csConf.setCapacity(QUEUEA_FULL.getFullPath(), 50f);
      csConf.setCapacity(QUEUEB_FULL.getFullPath(), 25f);
      csConf.setCapacity(QUEUEC_FULL.getFullPath(), 25f);
      csConf.setCapacity(QUEUED_FULL.getFullPath(), 25f);
    }

    csConf.setAutoCreateChildQueueEnabled(QUEUED_FULL.getFullPath(), true);

    // Setup leaf queue template configs
    csConf.setAutoCreatedLeafQueueTemplateCapacityByLabel(QUEUED_FULL.getFullPath(), "",
        QUEUE_D_TEMPL_MINRES);
    csConf.setAutoCreatedLeafQueueTemplateMaxCapacity(QUEUED_FULL.getFullPath(), "",
        QUEUE_D_TEMPL_MAXRES);

    return csConf;
  }

  private CapacitySchedulerConfiguration setupComplexQueueConfiguration(
      boolean isCapacityNeeded) {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{QUEUEA, QUEUEB, QUEUEC});
    csConf.setQueues(QUEUEA_FULL.getFullPath(), new String[]{QUEUEA1, QUEUEA2});
    csConf.setQueues(QUEUEB_FULL.getFullPath(), new String[]{QUEUEB1});

    // Set default capacities like normal configuration.
    if (isCapacityNeeded) {
      csConf.setCapacity(QUEUEA_FULL.getFullPath(), 50f);
      csConf.setCapacity(QUEUEB_FULL.getFullPath(), 25f);
      csConf.setCapacity(QUEUEC_FULL.getFullPath(), 25f);
      csConf.setCapacity(QUEUEA1_FULL.getFullPath(), 50f);
      csConf.setCapacity(QUEUEA2_FULL.getFullPath(), 50f);
      csConf.setCapacity(QUEUEB1_FULL.getFullPath(), 100f);
    }

    return csConf;
  }

  private CapacitySchedulerConfiguration setupLabeledConfiguration(
      CapacitySchedulerConfiguration csConf) {
    csConf.setMinimumResourceRequirement("", QUEUEA_FULL, Resource.newInstance(20 * GB, 8));
    csConf.setMinimumResourceRequirement("", QUEUEB_FULL, Resource.newInstance(10 * GB, 3));
    csConf.setMinimumResourceRequirement("", QUEUEC_FULL, Resource.newInstance(10 * GB, 2));
    csConf.setMinimumResourceRequirement("", QUEUED_FULL, Resource.newInstance(10 * GB, 2));

    csConf.setMinimumResourceRequirement(X_LABEL, QUEUEA_FULL, Resource.newInstance(20 * GB, 8));
    csConf.setMinimumResourceRequirement(X_LABEL, QUEUEB_FULL, Resource.newInstance(10 * GB, 3));
    csConf.setMinimumResourceRequirement(X_LABEL, QUEUEC_FULL, Resource.newInstance(10 * GB, 2));
    csConf.setMinimumResourceRequirement(X_LABEL, QUEUED_FULL, Resource.newInstance(10 * GB, 2));

    csConf.setMinimumResourceRequirement(Y_LABEL, QUEUEA_FULL, Resource.newInstance(2 * GB, 1));
    csConf.setMinimumResourceRequirement(Y_LABEL, QUEUEB_FULL, Resource.newInstance(2 * GB, 1));
    csConf.setMinimumResourceRequirement(Y_LABEL, QUEUEC_FULL, Resource.newInstance(2 * GB, 1));
    csConf.setMinimumResourceRequirement(Y_LABEL, QUEUED_FULL, Resource.newInstance(2 * GB, 2));

    return csConf;
  }

  private CapacitySchedulerConfiguration setupMinMaxResourceConfiguration(
      CapacitySchedulerConfiguration csConf) {

    // Update min/max resource to queueA/B/C
    csConf.setMinimumResourceRequirement("", QUEUEA_FULL, QUEUE_A_MINRES);
    csConf.setMinimumResourceRequirement("", QUEUEB_FULL, QUEUE_B_MINRES);
    csConf.setMinimumResourceRequirement("", QUEUEC_FULL, QUEUE_C_MINRES);
    csConf.setMinimumResourceRequirement("", QUEUED_FULL, QUEUE_D_MINRES);

    csConf.setMaximumResourceRequirement("", QUEUEA_FULL, QUEUE_A_MAXRES);
    csConf.setMaximumResourceRequirement("", QUEUEB_FULL, QUEUE_B_MAXRES);
    csConf.setMaximumResourceRequirement("", QUEUEC_FULL, QUEUE_C_MAXRES);
    csConf.setMaximumResourceRequirement("", QUEUED_FULL, QUEUE_D_MAXRES);

    return csConf;
  }

  private CapacitySchedulerConfiguration setupComplexMinMaxResourceConfig(
      CapacitySchedulerConfiguration csConf) {
    // Update min/max resource to queueA/B/C
    csConf.setMinimumResourceRequirement("", QUEUEA_FULL, QUEUE_A_MINRES);
    csConf.setMinimumResourceRequirement("", QUEUEB_FULL, QUEUE_B_MINRES);
    csConf.setMinimumResourceRequirement("", QUEUEC_FULL, QUEUE_C_MINRES);
    csConf.setMinimumResourceRequirement("", QUEUEA1_FULL, QUEUE_A1_MINRES);
    csConf.setMinimumResourceRequirement("", QUEUEA2_FULL, QUEUE_A2_MINRES);
    csConf.setMinimumResourceRequirement("", QUEUEB1_FULL, QUEUE_B1_MINRES);

    csConf.setMaximumResourceRequirement("", QUEUEA_FULL, QUEUE_A_MAXRES);
    csConf.setMaximumResourceRequirement("", QUEUEB_FULL, QUEUE_B_MAXRES);
    csConf.setMaximumResourceRequirement("", QUEUEC_FULL, QUEUE_C_MAXRES);

    return csConf;
  }

  @Test
  public void testSimpleMinMaxResourceConfigurartionPerQueue()
      throws Exception {

    CapacitySchedulerConfiguration csConf = setupSimpleQueueConfiguration(false);
    setupMinMaxResourceConfiguration(csConf);

    Assert.assertEquals("Min resource configured for QUEUEA is not correct",
        QUEUE_A_MINRES,
        csConf.getMinimumResourceRequirement("", QUEUEA_FULL.getFullPath(), resourceTypes));
    Assert.assertEquals("Max resource configured for QUEUEA is not correct",
        QUEUE_A_MAXRES,
        csConf.getMaximumResourceRequirement("", QUEUEA_FULL.getFullPath(), resourceTypes));
    Assert.assertEquals("Min resource configured for QUEUEB is not correct",
        QUEUE_B_MINRES,
        csConf.getMinimumResourceRequirement("", QUEUEB_FULL.getFullPath(), resourceTypes));
    Assert.assertEquals("Max resource configured for QUEUEB is not correct",
        QUEUE_B_MAXRES,
        csConf.getMaximumResourceRequirement("", QUEUEB_FULL.getFullPath(), resourceTypes));
    Assert.assertEquals("Min resource configured for QUEUEC is not correct",
        QUEUE_C_MINRES,
        csConf.getMinimumResourceRequirement("", QUEUEC_FULL.getFullPath(), resourceTypes));
    Assert.assertEquals("Max resource configured for QUEUEC is not correct",
        QUEUE_C_MAXRES,
        csConf.getMaximumResourceRequirement("", QUEUEC_FULL.getFullPath(), resourceTypes));

    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    @SuppressWarnings("resource")
    MockRM rm = new MockRM(csConf);
    rm.start();

    // Add few nodes
    rm.registerNode("127.0.0.1:1234", 250 * GB, 40);

    // Get queue object to verify min/max resource configuration.
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    ManagedParentQueue parentQueue = (ManagedParentQueue) cs.getQueue(QUEUED);
    AutoCreatedLeafQueue d1 = new AutoCreatedLeafQueue(cs.getQueueContext(), "d1", parentQueue);
    cs.addQueue(d1);

    /**
     * After adding child queue d1, it occupies all entire resource
     * of Managed Parent queue
     */
    cs.getRootQueue().updateClusterResource(cs.getClusterResource(),
        new ResourceLimits(cs.getClusterResource()));

    Assert.assertEquals(QUEUE_D_TEMPL_MINRES,
        d1.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals(QUEUE_D_TEMPL_MINRES,
        d1.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals(QUEUE_D_TEMPL_MAXRES,
        d1.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals(QUEUE_D_TEMPL_MAXRES,
        d1.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    /**
     * After adding child queue d2, d1 + d2 > resource
     * of Managed Parent queue, d2 will change to 0.
     * d1 will occupy all entire resource
     * of Managed Parent queue.
     */
    AutoCreatedLeafQueue d2 = new AutoCreatedLeafQueue(cs.getQueueContext(), "d2", parentQueue);
    cs.addQueue(d2);

    cs.getRootQueue().updateClusterResource(cs.getClusterResource(),
        new ResourceLimits(cs.getClusterResource()));

    Assert.assertEquals(Resource.newInstance(0, 0),
        d2.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals(Resource.newInstance(0, 0),
        d2.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals(QUEUE_D_TEMPL_MAXRES,
        d2.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals(QUEUE_D_TEMPL_MAXRES,
        d2.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    Assert.assertEquals(QUEUE_D_TEMPL_MINRES,
        d1.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals(QUEUE_D_TEMPL_MINRES,
        d1.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals(QUEUE_D_TEMPL_MAXRES,
        d1.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals(QUEUE_D_TEMPL_MAXRES,
        d1.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    rm.close();
  }

  @Test
  public void testNormalizationAfterNodeRemoval() throws Exception {
    CapacitySchedulerConfiguration csConf = setupNormalizationConfiguration();
    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    MockRM rm = new MockRM(csConf);

    rm.start();
    rm.registerNode("h1:1234", 8 * GB, 4);
    rm.registerNode("h2:1234", 8 * GB, 4);
    rm.registerNode("h3:1234", 8 * GB, 4);
    MockNM nm = rm.registerNode("h4:1234", 8 * GB, 4);
    rm.registerNode("h5:1234", 28 * GB, 12);

    // Send a removal event to CS. MockRM#unregisterNode does not reflect the real world scenario,
    // therefore we manually need to invoke this removal event.
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    cs.handle(new NodeRemovedSchedulerEvent(rm.getRMContext().getRMNodes().get(nm.getNodeId())));

    Resource res = Resources.add(
        cs.getQueue(QUEUEA1_FULL.getFullPath()).getEffectiveCapacity(""),
        cs.getQueue(QUEUEA2_FULL.getFullPath()).getEffectiveCapacity(""));
    Resource resParent = cs.getQueue(QUEUEA_FULL.getFullPath()).getEffectiveCapacity("");

    // Check if there is no overcommitment on behalf of the child queues
    Assert.assertTrue(String.format("Summarized resource %s of all children is greater than " +
        "their parent's %s", res, resParent),
        Resources.lessThan(cs.getResourceCalculator(), cs.getClusterResource(), res, resParent));
    rm.stop();
  }

  @Test
  public void testEffectiveMinMaxResourceConfigurartionPerQueue()
      throws Exception {
    // create conf with basic queue configuration.
    CapacitySchedulerConfiguration csConf = setupSimpleQueueConfiguration(
        false);
    setupMinMaxResourceConfiguration(csConf);

    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    @SuppressWarnings("resource")
    MockRM rm = new MockRM(csConf);
    rm.start();

    // Add few nodes
    rm.registerNode("127.0.0.1:1234", 250 * GB, 40);

    // Get queue object to verify min/max resource configuration.
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    LeafQueue qA = (LeafQueue) cs.getQueue(QUEUEA);
    Assert.assertNotNull(qA);
    Assert.assertEquals("Min resource configured for QUEUEA is not correct",
        QUEUE_A_MINRES, qA.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEA is not correct",
        QUEUE_A_MINRES, qA.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    LeafQueue qB = (LeafQueue) cs.getQueue(QUEUEB);
    Assert.assertNotNull(qB);
    Assert.assertEquals("Min resource configured for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    LeafQueue qC = (LeafQueue) cs.getQueue(QUEUEC);
    Assert.assertNotNull(qC);
    Assert.assertEquals("Min resource configured for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    rm.stop();
  }

  @Test
  public void testSimpleValidateAbsoluteResourceConfig() throws Exception {
    /**
     * Queue structure is as follows.
     *    root
     *   / | \
     *   a b c
     *   / \ |
     *  a1 a2 b1
     *
     * Test below cases 1) Configure percentage based capacity and absolute
     * resource together. 2) As per above tree structure, ensure all values
     * could be retrieved. 3) Validate whether min resource cannot be more than
     * max resources. 4) Validate whether max resource of queue cannot be more
     * than its parent max resource.
     */
    // create conf with basic queue configuration.
    CapacitySchedulerConfiguration csConf = setupSimpleQueueConfiguration(
        false);
    setupMinMaxResourceConfiguration(csConf);
    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    @SuppressWarnings("resource")
    MockRM rm = new MockRM(csConf);
    rm.start();

    // Add few nodes
    rm.registerNode("127.0.0.1:1234", 250 * GB, 40);

    // Get queue object to verify min/max resource configuration.
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // 1. Create a new config with min/max.
    CapacitySchedulerConfiguration csConf1 = setupSimpleQueueConfiguration(
        true);
    setupMinMaxResourceConfiguration(csConf1);

    try {
      cs.reinitialize(csConf1, rm.getRMContext());
    } catch (IOException e) {
      Assert.fail();
    }
    rm.stop();

    // 2. Create a new config with min/max alone with a complex queue config.
    // Check all values could be fetched correctly.
    CapacitySchedulerConfiguration csConf2 = setupComplexQueueConfiguration(
        false);
    setupComplexMinMaxResourceConfig(csConf2);

    rm = new MockRM(csConf2);
    rm.start();
    rm.registerNode("127.0.0.1:1234", 250 * GB, 40);
    cs = (CapacityScheduler) rm.getResourceScheduler();

    LeafQueue qA1 = (LeafQueue) cs.getQueue(QUEUEA1);
    Assert.assertEquals("Effective Min resource for QUEUEA1 is not correct",
        QUEUE_A1_MINRES, qA1.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA1 is not correct",
        QUEUE_A_MAXRES, qA1.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    LeafQueue qA2 = (LeafQueue) cs.getQueue(QUEUEA2);
    Assert.assertEquals("Effective Min resource for QUEUEA2 is not correct",
        QUEUE_A2_MINRES, qA2.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA2 is not correct",
        QUEUE_A_MAXRES, qA2.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    LeafQueue qB1 = (LeafQueue) cs.getQueue(QUEUEB1);
    Assert.assertNotNull(qB1);
    Assert.assertEquals("Min resource configured for QUEUEB1 is not correct",
        QUEUE_B1_MINRES, qB1.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEB1 is not correct",
        QUEUE_B_MAXRES, qB1.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEB1 is not correct",
        QUEUE_B1_MINRES, qB1.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB1 is not correct",
        QUEUE_B_MAXRES, qB1.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    LeafQueue qC = (LeafQueue) cs.getQueue(QUEUEC);
    Assert.assertNotNull(qC);
    Assert.assertEquals("Min resource configured for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    // 3. Create a new config and make sure one queue's min resource is more
    // than its max resource configured.
    CapacitySchedulerConfiguration csConf3 = setupComplexQueueConfiguration(
        false);
    setupComplexMinMaxResourceConfig(csConf3);

    csConf3.setMinimumResourceRequirement("", QUEUEB1_FULL, QUEUE_B_MAXRES);
    csConf3.setMaximumResourceRequirement("", QUEUEB1_FULL, QUEUE_B1_MINRES);

    try {
      cs.reinitialize(csConf3, rm.getRMContext());
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e instanceof IOException);
      Assert.assertEquals(
          "Failed to re-init queues : Min resource configuration "
              + "<memory:153600, vCores:30> is greater than its "
              + "max value:<memory:40960, vCores:10> "
              + "in queue:root.queueB.queueB1",
          e.getMessage());
    }

    // 4. Create a new config and make sure one queue's max resource is more
    // than its preant's max resource configured.
    CapacitySchedulerConfiguration csConf4 = setupComplexQueueConfiguration(
        false);
    setupComplexMinMaxResourceConfig(csConf4);

    csConf4.setMaximumResourceRequirement("", QUEUEB1_FULL, QUEUE_A_MAXRES);

    try {
      cs.reinitialize(csConf4, rm.getRMContext());
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e instanceof IOException);
      Assert
          .assertEquals(
              "Failed to re-init queues : Max resource configuration "
                  + "<memory:204800, vCores:30> is greater than parents max value:"
                  + "<memory:153600, vCores:30> in queue:root.queueB.queueB1",
              e.getMessage());
    }
    rm.stop();
  }

  @Test
  public void testComplexValidateAbsoluteResourceConfig() throws Exception {
    /**
     * Queue structure is as follows.
     *   root
     *  / | \
     *  a b c
     * / \ |
     * a1 a2 b1
     *
     * Test below cases: 1) Parent and its child queues must use either
     * percentage based or absolute resource configuration. 2) Parent's min
     * resource must be more than sum of child's min resource.
     */

    // create conf with basic queue configuration.
    CapacitySchedulerConfiguration csConf = setupComplexQueueConfiguration(
        false);
    setupComplexMinMaxResourceConfig(csConf);
    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    @SuppressWarnings("resource")
    MockRM rm = new MockRM(csConf);
    rm.start();

    // Add few nodes
    rm.registerNode("127.0.0.1:1234", 250 * GB, 40);

    // 1. Explicitly set percentage based config for parent queues. This will
    // make Queue A,B and C with percentage based and A1,A2 or B1 with absolute
    // resource.
    csConf.setCapacity(QUEUEA_FULL.getFullPath(), 50f);
    csConf.setCapacity(QUEUEB_FULL.getFullPath(), 25f);
    csConf.setCapacity(QUEUEC_FULL.getFullPath(), 25f);

    // Get queue object to verify min/max resource configuration.
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    try {
      cs.reinitialize(csConf, rm.getRMContext());
      if (csConf.isLegacyQueueMode()) {
        Assert.fail("legacy queue mode does not support mixed queue modes");
      }
    } catch (IOException e) {
      if (!csConf.isLegacyQueueMode()) {
        Assert.fail("new queue mode supports mixed queue modes");
      }
      Assert.assertTrue(e.getMessage().contains("Failed to re-init queues"));
    }

    // 2. Create a new config and make sure one queue's min resource is more
    // than its max resource configured.
    CapacitySchedulerConfiguration csConf1 = setupComplexQueueConfiguration(
        false);
    setupComplexMinMaxResourceConfig(csConf1);

    // Configure QueueA with lesser resource than its children.
    csConf1.setMinimumResourceRequirement("", QUEUEA_FULL, QUEUE_A1_MINRES);

    try {
      cs.reinitialize(csConf1, rm.getRMContext());
      if (csConf.isLegacyQueueMode()) {
        Assert.fail("legacy queue mode enforces that parent.capacity >= sum(children.capacity)");
      }
    } catch (IOException e) {
      if (!csConf.isLegacyQueueMode()) {
        Assert.fail("new queue mode allows that parent.capacity >= sum(children.capacity)");
      }
      Assert.assertEquals("Failed to re-init queues : Parent Queues capacity: "
          + "<memory:51200, vCores:5> is less than to its children:"
          + "<memory:102400, vCores:10> for queue:queueA", e.getMessage());
    }
    rm.stop();
  }

  @Test
  public void testValidateAbsoluteResourceConfig() throws Exception {
    /**
     * Queue structure is as follows. root / a / \ a1 a2
     *
     * Test below cases: 1) Test ConfigType when resource is [memory=0]
     */

    // create conf with basic queue configuration.
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {QUEUEA, QUEUEB});
    csConf.setQueues(QUEUEA_FULL.getFullPath(), new String[] {QUEUEA1, QUEUEA2});

    // Set default capacities like normal configuration.
    csConf.setCapacity(QUEUEA_FULL.getFullPath(), "[memory=125]");
    csConf.setCapacity(QUEUEB_FULL.getFullPath(), "[memory=0]");
    csConf.setCapacity(QUEUEA1_FULL.getFullPath(), "[memory=100]");
    csConf.setCapacity(QUEUEA2_FULL.getFullPath(), "[memory=25]");

    // Update min/max resource to queueA
    csConf.setMinimumResourceRequirement("", QUEUEA_FULL, QUEUE_A_MINRES);
    csConf.setMaximumResourceRequirement("", QUEUEA_FULL, QUEUE_A_MAXRES);

    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    @SuppressWarnings("resource")
    MockRM rm = new MockRM(csConf);
    rm.start();

    // Add few nodes
    rm.registerNode("127.0.0.1:1234", 125 * GB, 20);

    // Set [memory=0] to one of the queue and see if reinitialization
    // doesnt throw exception saying "Parent queue 'root.A' and
    // child queue 'root.A.A2' should use either percentage
    // based capacityconfiguration or absolute resource together for label"
    csConf.setCapacity(QUEUEA1_FULL.getFullPath(), "[memory=125]");
    csConf.setCapacity(QUEUEA2_FULL.getFullPath(), "[memory=0]");

    // Get queue object to verify min/max resource configuration.
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    try {
      cs.reinitialize(csConf, rm.getRMContext());
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
    rm.stop();
  }

  @Test
  public void testDownscalingForLabels() throws Exception {
    CapacitySchedulerConfiguration csConf = setupSimpleQueueConfiguration(false);
    setupLabeledConfiguration(csConf);

    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    MockRM rm = new MockRM(csConf);
    rm.start();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 8 * GB, 5);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 8 * GB, 5);
    MockNM nm3 = rm.registerNode("127.0.0.3:1234", 8 * GB, 5);
    MockNM nm4 = rm.registerNode("127.0.0.4:1234", 8 * GB, 5);

    rm.getRMContext().getNodeLabelManager().addToCluserNodeLabelsWithDefaultExclusivity(
        ImmutableSet.of(X_LABEL, Y_LABEL));
    rm.getRMContext().getNodeLabelManager().addLabelsToNode(
        ImmutableMap.of(nm1.getNodeId(), ImmutableSet.of(X_LABEL),
            nm2.getNodeId(), ImmutableSet.of(X_LABEL),
            nm3.getNodeId(), ImmutableSet.of(X_LABEL),
            nm4.getNodeId(), ImmutableSet.of(Y_LABEL)));

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue root = cs.getRootQueue();
    root.updateClusterResource(cs.getClusterResource(), new ResourceLimits(cs.getClusterResource()));

    Resource childrenResource = root.getChildQueues().stream().map(q -> q.getEffectiveCapacity(
        X_LABEL)).reduce(Resources::add).orElse(Resource.newInstance(0, 0));

    Assert.assertTrue("Children of root have more resource than overall cluster resource",
        Resources.greaterThan(cs.getResourceCalculator(), cs.getClusterResource(),
            root.getEffectiveCapacity(X_LABEL), childrenResource));
    rm.stop();
  }

  @Test
  public void testEffectiveResourceAfterReducingClusterResource()
      throws Exception {
    // create conf with basic queue configuration.
    CapacitySchedulerConfiguration csConf = setupSimpleQueueConfiguration(
        false);
    setupMinMaxResourceConfiguration(csConf);

    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    @SuppressWarnings("resource")
    MockRM rm = new MockRM(csConf);
    rm.start();

    // Add few nodes
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 125 * GB, 20);
    rm.registerNode("127.0.0.2:1234", 125 * GB, 20);

    // Get queue object to verify min/max resource configuration.
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    LeafQueue qA = (LeafQueue) cs.getQueue(QUEUEA);
    Assert.assertNotNull(qA);
    Assert.assertEquals("Min resource configured for QUEUEA is not correct",
        QUEUE_A_MINRES, qA.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEA is not correct",
        QUEUE_A_MINRES, qA.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    LeafQueue qB = (LeafQueue) cs.getQueue(QUEUEB);
    Assert.assertNotNull(qB);
    Assert.assertEquals("Min resource configured for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    LeafQueue qC = (LeafQueue) cs.getQueue(QUEUEC);
    Assert.assertNotNull(qC);
    Assert.assertEquals("Min resource configured for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    // unregister one NM.
    rm.unRegisterNode(nm1);

    // After loosing one NM, effective min res of queueA will become just
    // above half. Hence A's min will be 60Gi and 6 cores and max will be
    // 128GB and 20 cores.
    Assert.assertEquals("Effective Min resource for QUEUEA is not correct",
        QUEUEA_REDUCED, qA.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA is not correct",
        QUEUEMAX_REDUCED, qA.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    Assert.assertEquals("Effective Min resource for QUEUEB is not correct",
        QUEUEB_REDUCED, qB.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB is not correct",
        QUEUEMAX_REDUCED, qB.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    Assert.assertEquals("Effective Min resource for QUEUEC is not correct",
        QUEUEC_REDUCED, qC.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEC is not correct",
        QUEUEMAX_REDUCED, qC.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());

    rm.stop();
  }

  @Test
  public void testEffectiveResourceAfterIncreasingClusterResource()
      throws Exception {
    // create conf with basic queue configuration.
    CapacitySchedulerConfiguration csConf = setupComplexQueueConfiguration(
        false);
    setupComplexMinMaxResourceConfig(csConf);

    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    @SuppressWarnings("resource")
    MockRM rm = new MockRM(csConf);
    rm.start();

    // Add few nodes
    rm.registerNode("127.0.0.1:1234", 125 * GB, 20);
    rm.registerNode("127.0.0.2:1234", 125 * GB, 20);

    // Get queue object to verify min/max resource configuration.
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    ParentQueue qA = (ParentQueue) cs.getQueue(QUEUEA);
    Assert.assertNotNull(qA);
    Assert.assertEquals("Min resource configured for QUEUEA is not correct",
        QUEUE_A_MINRES, qA.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEA is not correct",
        QUEUE_A_MINRES, qA.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEA is not correct",
        0.4, qA.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEA is not correct",
        0.8, qA.getAbsoluteMaximumCapacity(), DELTA);

    ParentQueue qB = (ParentQueue) cs.getQueue(QUEUEB);
    Assert.assertNotNull(qB);
    Assert.assertEquals("Min resource configured for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEB is not correct",
        0.2, qB.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEB is not correct",
        0.6, qB.getAbsoluteMaximumCapacity(), DELTA);

    LeafQueue qC = (LeafQueue) cs.getQueue(QUEUEC);
    Assert.assertNotNull(qC);
    Assert.assertEquals("Min resource configured for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEC is not correct",
        0.1, qC.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEC is not correct",
        0.6, qC.getAbsoluteMaximumCapacity(), DELTA);

    LeafQueue qA1 = (LeafQueue) cs.getQueue(QUEUEA1);
    Assert.assertEquals("Effective Min resource for QUEUEA1 is not correct",
        QUEUE_A1_MINRES, qA1.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA1 is not correct",
        QUEUE_A_MAXRES, qA1.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEA1 is not correct",
        0.2, qA1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEA1 is not correct",
        0.8, qA1.getAbsoluteMaximumCapacity(), DELTA);

    LeafQueue qA2 = (LeafQueue) cs.getQueue(QUEUEA2);
    Assert.assertEquals("Effective Min resource for QUEUEA2 is not correct",
        QUEUE_A2_MINRES, qA2.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA2 is not correct",
        QUEUE_A_MAXRES, qA2.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEA2 is not correct",
        0.2, qA2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEA2 is not correct",
        0.8, qA2.getAbsoluteMaximumCapacity(), DELTA);

    LeafQueue qB1 = (LeafQueue) cs.getQueue(QUEUEB1);
    Assert.assertEquals("Min resource configured for QUEUEB1 is not correct",
        QUEUE_B1_MINRES, qB1.usageTracker.getQueueResourceQuotas().getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEB1 is not correct",
        QUEUE_B_MAXRES, qB1.usageTracker.getQueueResourceQuotas().getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEB1 is not correct",
        QUEUE_B1_MINRES, qB1.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB1 is not correct",
        QUEUE_B_MAXRES, qB1.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEB1 is not correct",
        0.16, qB1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEB1 is not correct",
        0.6, qB1.getAbsoluteMaximumCapacity(), DELTA);

    // add new NM.
    rm.registerNode("127.0.0.3:1234", 125 * GB, 20);

    // There will be no change in effective resource when nodes are added.
    // Since configured capacity was based on initial node capacity, a
    // re configurations is needed to use this added capacity.
    Assert.assertEquals("Effective Min resource for QUEUEA is not correct",
        QUEUE_A_MINRES, qA.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEA is not correct",
        0.266, qA.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEA is not correct",
        0.533, qA.getAbsoluteMaximumCapacity(), DELTA);

    Assert.assertEquals("Effective Min resource for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEB is not correct",
        0.133, qB.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEB is not correct",
        0.4, qB.getAbsoluteMaximumCapacity(), DELTA);

    Assert.assertEquals("Effective Min resource for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEC is not correct",
        0.066, qC.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEC is not correct",
        0.4, qC.getAbsoluteMaximumCapacity(), DELTA);

    Assert.assertEquals("Effective Min resource for QUEUEB1 is not correct",
        QUEUE_B1_MINRES, qB1.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB1 is not correct",
        QUEUE_B_MAXRES, qB1.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEB1 is not correct",
        0.106, qB1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEB1 is not correct",
        0.4, qB1.getAbsoluteMaximumCapacity(), DELTA);

    Assert.assertEquals("Effective Min resource for QUEUEA1 is not correct",
        QUEUE_A1_MINRES, qA1.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA1 is not correct",
        QUEUE_A_MAXRES, qA1.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEA1 is not correct",
        0.133, qA1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEA1 is not correct",
        0.533, qA1.getAbsoluteMaximumCapacity(), DELTA);

    Assert.assertEquals("Effective Min resource for QUEUEA2 is not correct",
        QUEUE_A2_MINRES, qA2.usageTracker.getQueueResourceQuotas().getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA2 is not correct",
        QUEUE_A_MAXRES, qA2.usageTracker.getQueueResourceQuotas().getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEA2 is not correct",
        0.133, qA2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEA2 is not correct",
            0.533, qA2.getAbsoluteMaximumCapacity(), DELTA);

    rm.stop();
  }
}
