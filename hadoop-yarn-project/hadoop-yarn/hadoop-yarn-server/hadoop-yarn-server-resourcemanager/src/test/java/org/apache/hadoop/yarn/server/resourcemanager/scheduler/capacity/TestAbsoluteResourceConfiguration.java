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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.Assert;
import org.junit.Test;

public class TestAbsoluteResourceConfiguration {

  private static final int GB = 1024;
  private static final float DELTA = 0.001f;

  private static final String QUEUEA = "queueA";
  private static final String QUEUEB = "queueB";
  private static final String QUEUEC = "queueC";
  private static final String QUEUEA1 = "queueA1";
  private static final String QUEUEA2 = "queueA2";
  private static final String QUEUEB1 = "queueB1";

  private static final String QUEUEA_FULL = CapacitySchedulerConfiguration.ROOT
      + "." + QUEUEA;
  private static final String QUEUEB_FULL = CapacitySchedulerConfiguration.ROOT
      + "." + QUEUEB;
  private static final String QUEUEC_FULL = CapacitySchedulerConfiguration.ROOT
      + "." + QUEUEC;
  private static final String QUEUEA1_FULL = QUEUEA_FULL + "." + QUEUEA1;
  private static final String QUEUEA2_FULL = QUEUEA_FULL + "." + QUEUEA2;
  private static final String QUEUEB1_FULL = QUEUEB_FULL + "." + QUEUEB1;

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
  private static final Resource QUEUE_C_MINRES = Resource.newInstance(50 * GB,
      10);
  private static final Resource QUEUE_C_MAXRES = Resource.newInstance(150 * GB,
      20);
  private static final Resource QUEUEA_REDUCED = Resource.newInstance(64000, 6);
  private static final Resource QUEUEB_REDUCED = Resource.newInstance(32000, 6);
  private static final Resource QUEUEC_REDUCED = Resource.newInstance(32000, 6);
  private static final Resource QUEUEMAX_REDUCED = Resource.newInstance(128000,
      20);

  private static Set<String> resourceTypes = new HashSet<>(
      Arrays.asList("memory", "vcores"));

  private CapacitySchedulerConfiguration setupSimpleQueueConfiguration(
      boolean isCapacityNeeded) {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{QUEUEA, QUEUEB, QUEUEC});

    // Set default capacities like normal configuration.
    if (isCapacityNeeded) {
      csConf.setCapacity(QUEUEA_FULL, 50f);
      csConf.setCapacity(QUEUEB_FULL, 25f);
      csConf.setCapacity(QUEUEC_FULL, 25f);
    }

    return csConf;
  }

  private CapacitySchedulerConfiguration setupComplexQueueConfiguration(
      boolean isCapacityNeeded) {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{QUEUEA, QUEUEB, QUEUEC});
    csConf.setQueues(QUEUEA_FULL, new String[]{QUEUEA1, QUEUEA2});
    csConf.setQueues(QUEUEB_FULL, new String[]{QUEUEB1});

    // Set default capacities like normal configuration.
    if (isCapacityNeeded) {
      csConf.setCapacity(QUEUEA_FULL, 50f);
      csConf.setCapacity(QUEUEB_FULL, 25f);
      csConf.setCapacity(QUEUEC_FULL, 25f);
      csConf.setCapacity(QUEUEA1_FULL, 50f);
      csConf.setCapacity(QUEUEA2_FULL, 50f);
      csConf.setCapacity(QUEUEB1_FULL, 100f);
    }

    return csConf;
  }

  private CapacitySchedulerConfiguration setupMinMaxResourceConfiguration(
      CapacitySchedulerConfiguration csConf) {
    // Update min/max resource to queueA/B/C
    csConf.setMinimumResourceRequirement("", QUEUEA_FULL, QUEUE_A_MINRES);
    csConf.setMinimumResourceRequirement("", QUEUEB_FULL, QUEUE_B_MINRES);
    csConf.setMinimumResourceRequirement("", QUEUEC_FULL, QUEUE_C_MINRES);

    csConf.setMaximumResourceRequirement("", QUEUEA_FULL, QUEUE_A_MAXRES);
    csConf.setMaximumResourceRequirement("", QUEUEB_FULL, QUEUE_B_MAXRES);
    csConf.setMaximumResourceRequirement("", QUEUEC_FULL, QUEUE_C_MAXRES);

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
  public void testSimpleMinMaxResourceConfigurartionPerQueue() {

    CapacitySchedulerConfiguration csConf = setupSimpleQueueConfiguration(true);
    setupMinMaxResourceConfiguration(csConf);

    Assert.assertEquals("Min resource configured for QUEUEA is not correct",
        QUEUE_A_MINRES,
        csConf.getMinimumResourceRequirement("", QUEUEA_FULL, resourceTypes));
    Assert.assertEquals("Max resource configured for QUEUEA is not correct",
        QUEUE_A_MAXRES,
        csConf.getMaximumResourceRequirement("", QUEUEA_FULL, resourceTypes));
    Assert.assertEquals("Min resource configured for QUEUEB is not correct",
        QUEUE_B_MINRES,
        csConf.getMinimumResourceRequirement("", QUEUEB_FULL, resourceTypes));
    Assert.assertEquals("Max resource configured for QUEUEB is not correct",
        QUEUE_B_MAXRES,
        csConf.getMaximumResourceRequirement("", QUEUEB_FULL, resourceTypes));
    Assert.assertEquals("Min resource configured for QUEUEC is not correct",
        QUEUE_C_MINRES,
        csConf.getMinimumResourceRequirement("", QUEUEC_FULL, resourceTypes));
    Assert.assertEquals("Max resource configured for QUEUEC is not correct",
        QUEUE_C_MAXRES,
        csConf.getMaximumResourceRequirement("", QUEUEC_FULL, resourceTypes));
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
        QUEUE_A_MINRES, qA.queueResourceQuotas.getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.queueResourceQuotas.getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEA is not correct",
        QUEUE_A_MINRES, qA.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.queueResourceQuotas.getEffectiveMaxResource());

    LeafQueue qB = (LeafQueue) cs.getQueue(QUEUEB);
    Assert.assertNotNull(qB);
    Assert.assertEquals("Min resource configured for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.queueResourceQuotas.getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.queueResourceQuotas.getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.queueResourceQuotas.getEffectiveMaxResource());

    LeafQueue qC = (LeafQueue) cs.getQueue(QUEUEC);
    Assert.assertNotNull(qC);
    Assert.assertEquals("Min resource configured for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.queueResourceQuotas.getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.queueResourceQuotas.getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.queueResourceQuotas.getEffectiveMaxResource());

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
        QUEUE_A1_MINRES, qA1.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA1 is not correct",
        QUEUE_A_MAXRES, qA1.queueResourceQuotas.getEffectiveMaxResource());

    LeafQueue qA2 = (LeafQueue) cs.getQueue(QUEUEA2);
    Assert.assertEquals("Effective Min resource for QUEUEA2 is not correct",
        QUEUE_A2_MINRES, qA2.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA2 is not correct",
        QUEUE_A_MAXRES, qA2.queueResourceQuotas.getEffectiveMaxResource());

    LeafQueue qB1 = (LeafQueue) cs.getQueue(QUEUEB1);
    Assert.assertNotNull(qB1);
    Assert.assertEquals("Min resource configured for QUEUEB1 is not correct",
        QUEUE_B1_MINRES, qB1.queueResourceQuotas.getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEB1 is not correct",
        QUEUE_B_MAXRES, qB1.queueResourceQuotas.getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEB1 is not correct",
        QUEUE_B1_MINRES, qB1.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB1 is not correct",
        QUEUE_B_MAXRES, qB1.queueResourceQuotas.getEffectiveMaxResource());

    LeafQueue qC = (LeafQueue) cs.getQueue(QUEUEC);
    Assert.assertNotNull(qC);
    Assert.assertEquals("Min resource configured for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.queueResourceQuotas.getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.queueResourceQuotas.getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.queueResourceQuotas.getEffectiveMaxResource());

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
    csConf.setCapacity(QUEUEA_FULL, 50f);
    csConf.setCapacity(QUEUEB_FULL, 25f);
    csConf.setCapacity(QUEUEC_FULL, 25f);

    // Get queue object to verify min/max resource configuration.
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    try {
      cs.reinitialize(csConf, rm.getRMContext());
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e instanceof IOException);
      Assert.assertEquals(
          "Failed to re-init queues : Parent queue 'root.queueA' "
              + "and child queue 'root.queueA.queueA1'"
              + " should use either percentage based"
              + " capacity configuration or absolute resource together.",
          e.getMessage());
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
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e instanceof IOException);
      Assert.assertEquals("Failed to re-init queues : Parent Queues capacity: "
          + "<memory:51200, vCores:5> is less than to its children:"
          + "<memory:102400, vCores:10> for queue:queueA", e.getMessage());
    }
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
        QUEUE_A_MINRES, qA.queueResourceQuotas.getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.queueResourceQuotas.getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEA is not correct",
        QUEUE_A_MINRES, qA.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.queueResourceQuotas.getEffectiveMaxResource());

    LeafQueue qB = (LeafQueue) cs.getQueue(QUEUEB);
    Assert.assertNotNull(qB);
    Assert.assertEquals("Min resource configured for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.queueResourceQuotas.getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.queueResourceQuotas.getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.queueResourceQuotas.getEffectiveMaxResource());

    LeafQueue qC = (LeafQueue) cs.getQueue(QUEUEC);
    Assert.assertNotNull(qC);
    Assert.assertEquals("Min resource configured for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.queueResourceQuotas.getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.queueResourceQuotas.getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.queueResourceQuotas.getEffectiveMaxResource());

    // unregister one NM.
    rm.unRegisterNode(nm1);

    // After loosing one NM, effective min res of queueA will become just
    // above half. Hence A's min will be 60Gi and 6 cores and max will be
    // 128GB and 20 cores.
    Assert.assertEquals("Effective Min resource for QUEUEA is not correct",
        QUEUEA_REDUCED, qA.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA is not correct",
        QUEUEMAX_REDUCED, qA.queueResourceQuotas.getEffectiveMaxResource());

    Assert.assertEquals("Effective Min resource for QUEUEB is not correct",
        QUEUEB_REDUCED, qB.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB is not correct",
        QUEUEMAX_REDUCED, qB.queueResourceQuotas.getEffectiveMaxResource());

    Assert.assertEquals("Effective Min resource for QUEUEC is not correct",
        QUEUEC_REDUCED, qC.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEC is not correct",
        QUEUEMAX_REDUCED, qC.queueResourceQuotas.getEffectiveMaxResource());

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
        QUEUE_A_MINRES, qA.queueResourceQuotas.getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.queueResourceQuotas.getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEA is not correct",
        QUEUE_A_MINRES, qA.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.queueResourceQuotas.getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEA is not correct",
        0.4, qA.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEA is not correct",
        0.8, qA.getAbsoluteMaximumCapacity(), DELTA);

    ParentQueue qB = (ParentQueue) cs.getQueue(QUEUEB);
    Assert.assertNotNull(qB);
    Assert.assertEquals("Min resource configured for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.queueResourceQuotas.getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.queueResourceQuotas.getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.queueResourceQuotas.getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEB is not correct",
        0.2, qB.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEB is not correct",
        0.6, qB.getAbsoluteMaximumCapacity(), DELTA);

    LeafQueue qC = (LeafQueue) cs.getQueue(QUEUEC);
    Assert.assertNotNull(qC);
    Assert.assertEquals("Min resource configured for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.queueResourceQuotas.getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.queueResourceQuotas.getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.queueResourceQuotas.getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEC is not correct",
        0.2, qC.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEC is not correct",
        0.6, qC.getAbsoluteMaximumCapacity(), DELTA);

    LeafQueue qA1 = (LeafQueue) cs.getQueue(QUEUEA1);
    Assert.assertEquals("Effective Min resource for QUEUEA1 is not correct",
        QUEUE_A1_MINRES, qA1.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA1 is not correct",
        QUEUE_A_MAXRES, qA1.queueResourceQuotas.getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEA1 is not correct",
        0.2, qA1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEA1 is not correct",
        0.8, qA1.getAbsoluteMaximumCapacity(), DELTA);

    LeafQueue qA2 = (LeafQueue) cs.getQueue(QUEUEA2);
    Assert.assertEquals("Effective Min resource for QUEUEA2 is not correct",
        QUEUE_A2_MINRES, qA2.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA2 is not correct",
        QUEUE_A_MAXRES, qA2.queueResourceQuotas.getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEA2 is not correct",
        0.2, qA2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEA2 is not correct",
        0.8, qA2.getAbsoluteMaximumCapacity(), DELTA);

    LeafQueue qB1 = (LeafQueue) cs.getQueue(QUEUEB1);
    Assert.assertEquals("Min resource configured for QUEUEB1 is not correct",
        QUEUE_B1_MINRES, qB1.queueResourceQuotas.getConfiguredMinResource());
    Assert.assertEquals("Max resource configured for QUEUEB1 is not correct",
        QUEUE_B_MAXRES, qB1.queueResourceQuotas.getConfiguredMaxResource());
    Assert.assertEquals("Effective Min resource for QUEUEB1 is not correct",
        QUEUE_B1_MINRES, qB1.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB1 is not correct",
        QUEUE_B_MAXRES, qB1.queueResourceQuotas.getEffectiveMaxResource());
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
        QUEUE_A_MINRES, qA.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA is not correct",
        QUEUE_A_MAXRES, qA.queueResourceQuotas.getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEA is not correct",
        0.266, qA.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEA is not correct",
        0.533, qA.getAbsoluteMaximumCapacity(), DELTA);

    Assert.assertEquals("Effective Min resource for QUEUEB is not correct",
        QUEUE_B_MINRES, qB.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB is not correct",
        QUEUE_B_MAXRES, qB.queueResourceQuotas.getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEB is not correct",
        0.133, qB.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEB is not correct",
        0.4, qB.getAbsoluteMaximumCapacity(), DELTA);

    Assert.assertEquals("Effective Min resource for QUEUEC is not correct",
        QUEUE_C_MINRES, qC.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEC is not correct",
        QUEUE_C_MAXRES, qC.queueResourceQuotas.getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEC is not correct",
        0.133, qC.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEC is not correct",
        0.4, qC.getAbsoluteMaximumCapacity(), DELTA);

    Assert.assertEquals("Effective Min resource for QUEUEB1 is not correct",
        QUEUE_B1_MINRES, qB1.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEB1 is not correct",
        QUEUE_B_MAXRES, qB1.queueResourceQuotas.getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEB1 is not correct",
        0.106, qB1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEB1 is not correct",
        0.4, qB1.getAbsoluteMaximumCapacity(), DELTA);

    Assert.assertEquals("Effective Min resource for QUEUEA1 is not correct",
        QUEUE_A1_MINRES, qA1.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA1 is not correct",
        QUEUE_A_MAXRES, qA1.queueResourceQuotas.getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEA1 is not correct",
        0.133, qA1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEA1 is not correct",
        0.533, qA1.getAbsoluteMaximumCapacity(), DELTA);

    Assert.assertEquals("Effective Min resource for QUEUEA2 is not correct",
        QUEUE_A2_MINRES, qA2.queueResourceQuotas.getEffectiveMinResource());
    Assert.assertEquals("Effective Max resource for QUEUEA2 is not correct",
        QUEUE_A_MAXRES, qA2.queueResourceQuotas.getEffectiveMaxResource());
    Assert.assertEquals("Absolute capacity for QUEUEA2 is not correct",
        0.133, qA2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Absolute Max capacity for QUEUEA2 is not correct",
            0.533, qA2.getAbsoluteMaximumCapacity(), DELTA);

    rm.stop();
  }
}
