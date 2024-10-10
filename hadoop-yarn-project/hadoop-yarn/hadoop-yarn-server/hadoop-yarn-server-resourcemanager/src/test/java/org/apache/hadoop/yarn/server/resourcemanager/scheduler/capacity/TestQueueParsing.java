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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.PriorityUtilizationQueueOrderingPolicy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;

import static java.util.Collections.emptySet;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils.toSet;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;

public class TestQueueParsing {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestQueueParsing.class);
  private static final String A_PATH = CapacitySchedulerConfiguration.ROOT + ".a";
  private static final String B_PATH = CapacitySchedulerConfiguration.ROOT + ".b";
  private static final String C_PATH = CapacitySchedulerConfiguration.ROOT + ".c";
  private static final String A1_PATH = A_PATH + ".a1";
  private static final String A2_PATH = A_PATH + ".a2";
  private static final String B1_PATH = B_PATH + ".b1";
  private static final String B2_PATH = B_PATH + ".b2";
  private static final String B3_PATH = B_PATH + ".b3";
  private static final String B4_PATH = B_PATH + ".b4";
  private static final String C1_PATH = C_PATH + ".c1";
  private static final String C2_PATH = C_PATH + ".c2";
  private static final String C3_PATH = C_PATH + ".c3";
  private static final String C4_PATH = C_PATH + ".c4";
  private static final String C11_PATH = C1_PATH + ".c11";
  private static final String C12_PATH = C1_PATH + ".c12";
  private static final String C13_PATH = C1_PATH + ".c13";
  private static final String AX_PATH = "root.a.x";
  private static final String AY_PATH = "root.a.y";
  private static final String X_PATH = "root.x";

  private static final QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  private static final QueuePath A = new QueuePath(A_PATH);
  private static final QueuePath B = new QueuePath(B_PATH);
  private static final QueuePath C = new QueuePath(C_PATH);
  private static final QueuePath A1 = new QueuePath(A1_PATH);
  private static final QueuePath A2 = new QueuePath(A2_PATH);
  private static final QueuePath B1 = new QueuePath(B1_PATH);
  private static final QueuePath B2 = new QueuePath(B2_PATH);
  private static final QueuePath B3 = new QueuePath(B3_PATH);
  private static final QueuePath B4 = new QueuePath(B4_PATH);
  private static final QueuePath C1 = new QueuePath(C1_PATH);
  private static final QueuePath C2 = new QueuePath(C2_PATH);
  private static final QueuePath C3 = new QueuePath(C3_PATH);
  private static final QueuePath C4 = new QueuePath(C4_PATH);
  private static final QueuePath C11 = new QueuePath(C11_PATH);
  private static final QueuePath C12 = new QueuePath(C12_PATH);
  private static final QueuePath C13 = new QueuePath(C13_PATH);
  private static final QueuePath AX = new QueuePath(AX_PATH);
  private static final QueuePath AY = new QueuePath(AY_PATH);
  private static final QueuePath X = new QueuePath(X_PATH);
  private static final double DELTA = 0.001;
  private static final int GB = 1024;

  private RMNodeLabelsManager nodeLabelManager;

  @Before
  public void setup() throws Exception{
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
            ResourceScheduler.class);
    Resource clusterResource = Resources.createResource(2 * 8 * GB, 2 * 16);

    nodeLabelManager = new NullRMNodeLabelsManager();
    nodeLabelManager.init(conf);
    ((NullRMNodeLabelsManager)nodeLabelManager)
        .setResourceForLabel(CommonNodeLabelsManager.NO_LABEL, clusterResource);
  }

  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(ROOT, new String[] {"a", "b", "c"});

    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setCapacity(B, 20);
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 70);

    LOG.info("Setup top-level queues");

    // Define 2nd-level queues
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setCapacity(A1, 30);
    conf.setMaximumCapacity(A1, 45);
    conf.setCapacity(A2, 70);
    conf.setMaximumCapacity(A2, 85);

    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setCapacity(B1, 50);
    conf.setMaximumCapacity(B1, 85);
    conf.setCapacity(B2, 30);
    conf.setMaximumCapacity(B2, 35);
    conf.setCapacity(B3, 20);
    conf.setMaximumCapacity(B3, 35);

    conf.setQueues(C, new String[] {"c1", "c2", "c3", "c4"});
    conf.setCapacity(C1, 50);
    conf.setMaximumCapacity(C1, 55);
    conf.setCapacity(C2, 10);
    conf.setMaximumCapacity(C2, 25);
    conf.setCapacity(C3, 35);
    conf.setMaximumCapacity(C3, 38);
    conf.setCapacity(C4, 5);
    conf.setMaximumCapacity(C4, 5);

    LOG.info("Setup 2nd-level queues");

    // Define 3rd-level queues
    conf.setQueues(C1, new String[] {"c11", "c12", "c13"});
    conf.setCapacity(C11, 15);
    conf.setMaximumCapacity(C11, 30);
    conf.setCapacity(C12, 45);
    conf.setMaximumCapacity(C12, 70);
    conf.setCapacity(C13, 40);
    conf.setMaximumCapacity(C13, 40);

    LOG.info("Setup 3rd-level queues");
  }

  private void setupQueueConfigurationWithSpacesShouldBeTrimmed(
          CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.set(
            QueuePrefixes.getQueuePrefix(ROOT)
                    + CapacitySchedulerConfiguration.QUEUES, " a ,b, c");

    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setCapacity(B, 20);
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 70);
  }

  private void setupNestedQueueConfigurationWithSpacesShouldBeTrimmed(
          CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.set(
            QueuePrefixes.getQueuePrefix(ROOT)
                    + CapacitySchedulerConfiguration.QUEUES, " a ,b, c");

    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setCapacity(B, 20);
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 70);

    // sub queues for A
    conf.set(QueuePrefixes.getQueuePrefix(A)
            + CapacitySchedulerConfiguration.QUEUES, "a1, a2 ");

    conf.setCapacity(A1, 60);
    conf.setCapacity(A2, 40);
  }

  private void setupQueueConfigurationWithoutLabels(CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(ROOT, new String[] {"a", "b"});

    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setCapacity(B, 90);

    LOG.info("Setup top-level queues");

    // Define 2nd-level queues
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setCapacity(A1, 30);
    conf.setMaximumCapacity(A1, 45);
    conf.setCapacity(A2, 70);
    conf.setMaximumCapacity(A2, 85);

    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setCapacity(B1, 50);
    conf.setMaximumCapacity(B1, 85);
    conf.setCapacity(B2, 30);
    conf.setMaximumCapacity(B2, 35);
    conf.setCapacity(B3, 20);
    conf.setMaximumCapacity(B3, 35);
  }

  private void setupQueueConfigurationWithLabels(CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setCapacityByLabel(ROOT, "red", 100);
    conf.setCapacityByLabel(ROOT, "blue", 100);

    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setCapacity(B, 90);

    LOG.info("Setup top-level queues");

    // Define 2nd-level queues
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(A, "red", 50);
    conf.setMaximumCapacityByLabel(A, "red", 50);
    conf.setCapacityByLabel(A, "blue", 50);

    conf.setCapacity(A1, 30);
    conf.setMaximumCapacity(A1, 45);
    conf.setCapacityByLabel(A1, "red", 50);
    conf.setCapacityByLabel(A1, "blue", 100);

    conf.setCapacity(A2, 70);
    conf.setMaximumCapacity(A2, 85);
    conf.setAccessibleNodeLabels(A2, ImmutableSet.of("red"));
    conf.setCapacityByLabel(A2, "red", 50);
    conf.setMaximumCapacityByLabel(A2, "red", 60);

    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setAccessibleNodeLabels(B, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(B, "red", 50);
    conf.setCapacityByLabel(B, "blue", 50);

    conf.setCapacity(B1, 50);
    conf.setMaximumCapacity(B1, 85);
    conf.setCapacityByLabel(B1, "red", 50);
    conf.setCapacityByLabel(B1, "blue", 50);

    conf.setCapacity(B2, 30);
    conf.setMaximumCapacity(B2, 35);
    conf.setCapacityByLabel(B2, "red", 25);
    conf.setCapacityByLabel(B2, "blue", 25);

    conf.setCapacity(B3, 20);
    conf.setMaximumCapacity(B3, 35);
    conf.setCapacityByLabel(B3, "red", 25);
    conf.setCapacityByLabel(B3, "blue", 25);
  }

  private void setupQueueConfigurationWithLabelsAndReleaseCheck(
      CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setCapacityByLabel(ROOT, "red", 100);
    conf.setCapacityByLabel(ROOT, "blue", 100);

    // The cap <= max-cap check is not needed
    conf.setCapacity(A, 50);
    conf.setMaximumCapacity(A, 100);

    conf.setCapacity(B, 50);
    conf.setMaximumCapacity(B, 100);

    LOG.info("Setup top-level queues");

    // Define 2nd-level queues
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(A, "red", 50);
    conf.setMaximumCapacityByLabel(A, "red", 100);
    conf.setCapacityByLabel(A, "blue", 30);
    conf.setMaximumCapacityByLabel(A, "blue", 50);

    conf.setCapacity(A1, 60);
    conf.setMaximumCapacity(A1, 60);
    conf.setCapacityByLabel(A1, "red", 60);
    conf.setMaximumCapacityByLabel(A1, "red", 30);
    conf.setCapacityByLabel(A1, "blue", 100);
    conf.setMaximumCapacityByLabel(A1, "blue", 100);

    conf.setCapacity(A2, 40);
    conf.setMaximumCapacity(A2, 85);
    conf.setAccessibleNodeLabels(A2, ImmutableSet.of("red"));
    conf.setCapacityByLabel(A2, "red", 40);
    conf.setMaximumCapacityByLabel(A2, "red", 60);

    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setAccessibleNodeLabels(B, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(B, "red", 50);
    conf.setMaximumCapacityByLabel(B, "red", 100);
    conf.setCapacityByLabel(B, "blue", 70);
    conf.setMaximumCapacityByLabel(B, "blue", 100);

    conf.setCapacity(B1, 10);
    conf.setMaximumCapacity(B1, 10);
    conf.setCapacityByLabel(B1, "red", 60);
    conf.setMaximumCapacityByLabel(B1, "red", 30);
    conf.setCapacityByLabel(B1, "blue", 50);
    conf.setMaximumCapacityByLabel(B1, "blue", 100);

    conf.setCapacity(B2, 80);
    conf.setMaximumCapacity(B2, 40);
    conf.setCapacityByLabel(B2, "red", 30);
    conf.setCapacityByLabel(B2, "blue", 25);

    conf.setCapacity(B3, 10);
    conf.setMaximumCapacity(B3, 25);
    conf.setCapacityByLabel(B3, "red", 10);
    conf.setCapacityByLabel(B3, "blue", 25);
  }

  private void setupQueueConfigurationWithLabelsInherit(
          CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(ROOT, new String[] {"a", "b"});
    conf.setCapacityByLabel(ROOT, "red", 100);
    conf.setCapacityByLabel(ROOT, "blue", 100);

    // Set A configuration
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(A, "red", 100);
    conf.setCapacityByLabel(A, "blue", 100);

    // Set B configuraiton
    conf.setCapacity(B, 90);
    conf.setAccessibleNodeLabels(B, CommonNodeLabelsManager.EMPTY_STRING_SET);

    // Define 2nd-level queues
    conf.setCapacity(A1, 30);
    conf.setMaximumCapacity(A1, 45);
    conf.setCapacityByLabel(A1, "red", 50);
    conf.setCapacityByLabel(A1, "blue", 100);

    conf.setCapacity(A2, 70);
    conf.setMaximumCapacity(A2, 85);
    conf.setAccessibleNodeLabels(A2, ImmutableSet.of("red"));
    conf.setCapacityByLabel(A2, "red", 50);
  }

  private void setupQueueConfigurationWithSingleLevel(
          CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(ROOT, new String[] {"a", "b"});

    // Set A configuration
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(A, "red", 90);
    conf.setCapacityByLabel(A, "blue", 90);

    // Set B configuraiton
    conf.setCapacity(B, 90);
    conf.setAccessibleNodeLabels(B, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(B, "red", 10);
    conf.setCapacityByLabel(B, "blue", 10);
  }

  public MockRM createMockRMWithoutLabels(YarnConfiguration conf) throws Exception {
    return createMockRMWithLabels(conf, emptySet());
  }

  public MockRM createMockRMWithLabels(
      YarnConfiguration conf, Set<String> nodeLabels) throws Exception {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(nodeLabels);
    Map<NodeId, Set<String>> nodeIdMap = new HashMap<>();
    int i = 0;
    for (String label : nodeLabels) {
      nodeIdMap.put(NodeId.newInstance("h" + ++i, 0), toSet(label));
    }
    nodeLabelManager.addLabelsToNode(nodeIdMap);

    MockRM rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelManager;
      }
    };
    rm.getRMContext().setNodeLabelManager(nodeLabelManager);
    rm.start();

    for (NodeId key : nodeIdMap.keySet()) {
      rm.registerNode(key.toString(), 8 * GB);
    }

    return rm;
  }

  @Test
  public void testQueueParsing() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithoutLabels(conf);
    CapacityScheduler capacityScheduler = (CapacityScheduler) rm.getResourceScheduler();

    CSQueue a = capacityScheduler.getQueue("a");
    Assert.assertEquals(0.10, a.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a.getAbsoluteMaximumCapacity(), DELTA);

    CSQueue b1 = capacityScheduler.getQueue("b1");
    Assert.assertEquals(0.2 * 0.5, b1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Parent B has no MAX_CAP",
        0.85, b1.getAbsoluteMaximumCapacity(), DELTA);

    CSQueue c12 = capacityScheduler.getQueue("c12");
    Assert.assertEquals(0.7 * 0.5 * 0.45, c12.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.7 * 0.55 * 0.7,
        c12.getAbsoluteMaximumCapacity(), DELTA);
  }

  @Test (expected=java.lang.IllegalArgumentException.class)
  public void testRootQueueParsing() throws Exception {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    // non-100 percent value will throw IllegalArgumentException
    csConf.setCapacity(ROOT, 90);
  }
  
  @Test
  public void testQueueParsingReinitializeWithLabels() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithoutLabels(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithLabels(conf, ImmutableSet.of("red", "blue"));

    CapacityScheduler capacityScheduler = (CapacityScheduler) rm.getResourceScheduler();
    csConf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithLabels(csConf);
    conf = new YarnConfiguration(csConf);
    capacityScheduler.reinitialize(conf, rm.getRMContext());
    checkQueueLabels(capacityScheduler);
    ServiceOperations.stopQuietly(rm);
  }

  @Test
  public void testQueueParsingWithLabels() throws Exception {
    CapacitySchedulerConfiguration csConf =
            new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithLabels(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithLabels(conf, ImmutableSet.of("red", "blue"));

    CapacityScheduler capacityScheduler = (CapacityScheduler) rm.getResourceScheduler();
    checkQueueLabels(capacityScheduler);
    ServiceOperations.stopQuietly(rm);
  }

  private void checkQueueLabels(CapacityScheduler capacityScheduler) {
    // queue-A is red, blue
    Assert.assertTrue(capacityScheduler.getQueue("a").getAccessibleNodeLabels()
        .containsAll(ImmutableSet.of("red", "blue")));

    // queue-A1 inherits A's configuration
    Assert.assertTrue(capacityScheduler.getQueue("a1")
        .getAccessibleNodeLabels().containsAll(ImmutableSet.of("red", "blue")));

    // queue-A2 is "red"
    Assert.assertEquals(1, capacityScheduler.getQueue("a2")
        .getAccessibleNodeLabels().size());
    Assert.assertTrue(capacityScheduler.getQueue("a2")
        .getAccessibleNodeLabels().contains("red"));

    // queue-B is "red"/"blue"
    Assert.assertTrue(capacityScheduler.getQueue("b").getAccessibleNodeLabels()
        .containsAll(ImmutableSet.of("red", "blue")));

    // queue-B2 inherits "red"/"blue"
    Assert.assertTrue(capacityScheduler.getQueue("b2")
        .getAccessibleNodeLabels().containsAll(ImmutableSet.of("red", "blue")));

    // check capacity of A2
    CSQueue qA2 = capacityScheduler.getQueue("a2");
    Assert.assertEquals(0.7, qA2.getCapacity(), DELTA);
    Assert.assertEquals(0.5, qA2.getQueueCapacities().getCapacity("red"), DELTA);
    Assert.assertEquals(0.07, qA2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.25, qA2.getQueueCapacities().getAbsoluteCapacity("red"), DELTA);
    Assert.assertEquals(0.1275, qA2.getAbsoluteMaximumCapacity(), DELTA);
    Assert.assertEquals(0.3, qA2.getQueueCapacities().getAbsoluteMaximumCapacity("red"), DELTA);

    // check capacity of B3
    CSQueue qB3 = capacityScheduler.getQueue("b3");
    Assert.assertEquals(0.18, qB3.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.125, qB3.getQueueCapacities().getAbsoluteCapacity("red"), DELTA);
    Assert.assertEquals(0.35, qB3.getAbsoluteMaximumCapacity(), DELTA);
    Assert.assertEquals(1, qB3.getQueueCapacities().getAbsoluteMaximumCapacity("red"), DELTA);
  }

  @Test
  public void testQueueParsingWithLeafQueueDisableElasticity() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithLabelsAndReleaseCheck(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithLabels(conf, ImmutableSet.of("red", "blue"));

    CapacityScheduler capacityScheduler = (CapacityScheduler) rm.getResourceScheduler();
    checkQueueLabelsWithLeafQueueDisableElasticity(capacityScheduler);
    ServiceOperations.stopQuietly(rm);
  }

  private void checkQueueLabelsWithLeafQueueDisableElasticity(
      CapacityScheduler capacityScheduler) {
    // queue-A is red, blue
    Assert.assertTrue(capacityScheduler.getQueue("a").getAccessibleNodeLabels()
            .containsAll(ImmutableSet.of("red", "blue")));

    // queue-A1 inherits A's configuration
    Assert.assertTrue(capacityScheduler.getQueue("a1")
            .getAccessibleNodeLabels().containsAll(ImmutableSet.of("red", "blue")));

    // queue-A2 is "red"
    Assert.assertEquals(1, capacityScheduler.getQueue("a2")
            .getAccessibleNodeLabels().size());
    Assert.assertTrue(capacityScheduler.getQueue("a2")
            .getAccessibleNodeLabels().contains("red"));

    // queue-B is "red"/"blue"
    Assert.assertTrue(capacityScheduler.getQueue("b").getAccessibleNodeLabels()
            .containsAll(ImmutableSet.of("red", "blue")));

    // queue-B2 inherits "red"/"blue"
    Assert.assertTrue(capacityScheduler.getQueue("b2")
            .getAccessibleNodeLabels().containsAll(ImmutableSet.of("red", "blue")));

    // check capacity of A2
    CSQueue qA2 = capacityScheduler.getQueue("a2");
    Assert.assertEquals(0.4, qA2.getCapacity(), DELTA);
    Assert.assertEquals(0.4, qA2.getQueueCapacities()
            .getCapacity("red"), DELTA);
    Assert.assertEquals(0.2, qA2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.2, qA2.getQueueCapacities()
            .getAbsoluteCapacity("red"), DELTA);
    Assert.assertEquals(0.85, qA2.getAbsoluteMaximumCapacity(), DELTA);
    Assert.assertEquals(0.6, qA2.getQueueCapacities()
            .getAbsoluteMaximumCapacity("red"), DELTA);

    // check disable elasticity at leaf queue level without label
    CSQueue qB2 = capacityScheduler.getQueue("b2");
    Assert.assertEquals(0.4, qB2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.4, qB2.getAbsoluteMaximumCapacity(), DELTA);

    // check disable elasticity at leaf queue level with label
    CSQueue qA1 = capacityScheduler.getQueue("a1");
    Assert.assertEquals(0.3, qA1.getQueueCapacities().
            getAbsoluteCapacity("red"), DELTA);
    Assert.assertEquals(0.3, qA1.getQueueCapacities().
            getAbsoluteMaximumCapacity("red"), DELTA);

    CSQueue qB1 = capacityScheduler.getQueue("b1");
    Assert.assertEquals(0.3, qB1.getQueueCapacities()
            .getAbsoluteCapacity("red"), DELTA);
    Assert.assertEquals(0.3, qB1.getQueueCapacities()
            .getAbsoluteMaximumCapacity("red"), DELTA);

    // check capacity of B3
    CSQueue qB3 = capacityScheduler.getQueue("b3");
    Assert.assertEquals(0.05, qB3.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.175, qB3.getQueueCapacities()
            .getAbsoluteCapacity("blue"), DELTA);
    Assert.assertEquals(0.25, qB3.getAbsoluteMaximumCapacity(), DELTA);
    Assert.assertEquals(1, qB3.getQueueCapacities()
            .getAbsoluteMaximumCapacity("blue"), DELTA);
  }

  @Test
  public void testQueueParsingWithLabelsInherit() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithLabelsInherit(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithLabels(conf, ImmutableSet.of("red", "blue"));

    CapacityScheduler capacityScheduler = (CapacityScheduler) rm.getResourceScheduler();
    checkQueueLabelsInheritConfig(capacityScheduler);
    ServiceOperations.stopQuietly(rm);
  }

  private void checkQueueLabelsInheritConfig(CapacityScheduler capacityScheduler) {
    // queue-A is red, blue
    Assert.assertTrue(capacityScheduler.getQueue("a").getAccessibleNodeLabels()
            .containsAll(ImmutableSet.of("red", "blue")));

    // queue-A1 inherits A's configuration
    Assert.assertTrue(capacityScheduler.getQueue("a1")
            .getAccessibleNodeLabels().containsAll(ImmutableSet.of("red", "blue")));

    // queue-A2 is "red"
    Assert.assertEquals(1, capacityScheduler.getQueue("a2")
            .getAccessibleNodeLabels().size());
    Assert.assertTrue(capacityScheduler.getQueue("a2")
            .getAccessibleNodeLabels().contains("red"));

    // queue-B is "red"/"blue"
    Assert.assertTrue(capacityScheduler.getQueue("b").getAccessibleNodeLabels()
            .isEmpty());
  }
  
  @Test
  public void testQueueParsingWhenLabelsNotExistedInNodeLabelManager() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithLabels(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithoutLabels(conf);

    ServiceOperations.stopQuietly(rm);
  }
  
  @Test
  public void testQueueParsingWhenLabelsInheritedNotExistedInNodeLabelManager() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithLabelsInherit(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithoutLabels(conf);

    ServiceOperations.stopQuietly(rm);
  }
  
  @Test
  public void testSingleLevelQueueParsingWhenLabelsNotExistedInNodeLabelManager() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithSingleLevel(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithoutLabels(conf);

    ServiceOperations.stopQuietly(rm);
  }
  
  @Test
  public void testQueueParsingWhenLabelsNotExist() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithLabels(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithoutLabels(conf);

    ServiceOperations.stopQuietly(rm);
  }
  
  @Test
  public void testQueueParsingWithUnusedLabels() throws Exception {
    // Initialize a cluster with labels, but don't use them, reinitialize
    // shouldn't fail
    Set<String> labels = ImmutableSet.of("red", "blue");
    CapacitySchedulerConfiguration csConf =
            new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    csConf.setAccessibleNodeLabels(ROOT, labels);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithLabels(conf, labels);

    CapacityScheduler capacityScheduler = (CapacityScheduler) rm.getResourceScheduler();
    capacityScheduler.reinitialize(csConf, rm.getRMContext());

    // check root queue's capacity by label -- they should be all zero
    CSQueue root = capacityScheduler.getQueue(CapacitySchedulerConfiguration.ROOT);
    Assert.assertEquals(0, root.getQueueCapacities().getCapacity("red"), DELTA);
    Assert.assertEquals(0, root.getQueueCapacities().getCapacity("blue"), DELTA);

    CSQueue a = capacityScheduler.getQueue("a");
    Assert.assertEquals(0.10, a.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a.getAbsoluteMaximumCapacity(), DELTA);

    CSQueue b1 = capacityScheduler.getQueue("b1");
    Assert.assertEquals(0.2 * 0.5, b1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Parent B has no MAX_CAP", 0.85,
        b1.getAbsoluteMaximumCapacity(), DELTA);

    CSQueue c12 = capacityScheduler.getQueue("c12");
    Assert.assertEquals(0.7 * 0.5 * 0.45, c12.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.7 * 0.55 * 0.7, c12.getAbsoluteMaximumCapacity(),
        DELTA);
    ServiceOperations.stopQuietly(rm);
  }
  
  @Test
  public void testQueueParsingShouldTrimSpaces() throws Exception {
    CapacitySchedulerConfiguration csConf = 
      new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithSpacesShouldBeTrimmed(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithoutLabels(conf);

    CapacityScheduler capacityScheduler = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue a = capacityScheduler.getQueue("a");
    Assert.assertNotNull(a);
    Assert.assertEquals(0.10, a.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a.getAbsoluteMaximumCapacity(), DELTA);
    
    CSQueue c = capacityScheduler.getQueue("c");
    Assert.assertNotNull(c);
    Assert.assertEquals(0.70, c.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.70, c.getAbsoluteMaximumCapacity(), DELTA);
  }
  
  @Test
  public void testNestedQueueParsingShouldTrimSpaces() throws Exception {
    CapacitySchedulerConfiguration csConf = 
      new CapacitySchedulerConfiguration();
    setupNestedQueueConfigurationWithSpacesShouldBeTrimmed(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithoutLabels(conf);
    CapacityScheduler capacityScheduler = (CapacityScheduler) rm.getResourceScheduler();

    CSQueue a = capacityScheduler.getQueue("a");
    Assert.assertNotNull(a);
    Assert.assertEquals(0.10, a.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a.getAbsoluteMaximumCapacity(), DELTA);
    
    CSQueue c = capacityScheduler.getQueue("c");
    Assert.assertNotNull(c);
    Assert.assertEquals(0.70, c.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.70, c.getAbsoluteMaximumCapacity(), DELTA);
    
    CSQueue a1 = capacityScheduler.getQueue("a1");
    Assert.assertNotNull(a1);
    Assert.assertEquals(0.10 * 0.6, a1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a1.getAbsoluteMaximumCapacity(), DELTA);
    
    CSQueue a2 = capacityScheduler.getQueue("a2");
    Assert.assertNotNull(a2);
    Assert.assertEquals(0.10 * 0.4, a2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a2.getAbsoluteMaximumCapacity(), DELTA);
  }
  
  /**
   * Test init a queue configuration, children's capacity for a given label
   * doesn't equals to 100%. This expect IllegalArgumentException thrown.
   */
  @Test(expected = ServiceStateException.class)
  public void testQueueParsingFailWhenSumOfChildrenNonLabeledCapacityNot100Percent()
      throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();

    // If the new queue mode is used it's allowed to leave
    // some of the resources of a parent queue unallocated
    assumeThat(csConf.isLegacyQueueMode(), is(true));
    setupQueueConfiguration(csConf);
    csConf.setCapacity(C2, 5);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithLabels(conf, ImmutableSet.of("red", "blue"));
    ServiceOperations.stopQuietly(rm);
  }
  
  /**
   * Test init a queue configuration, children's capacity for a given label
   * doesn't equals to 100%. This expect IllegalArgumentException thrown.
   */
  @Test(expected = ServiceStateException.class)
  public void testQueueParsingFailWhenSumOfChildrenLabeledCapacityNot100Percent()
      throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();

    // If the new queue mode is used it's allowed to leave
    // some of the resources of a parent queue unallocated
    assumeThat(csConf.isLegacyQueueMode(), is(true));

    setupQueueConfigurationWithLabels(csConf);
    csConf.setCapacityByLabel(B3, "red", 24);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithLabels(conf, ImmutableSet.of("red", "blue"));
    ServiceOperations.stopQuietly(rm);
  }

  /**
   * Test init a queue configuration, children's capacity for a given label
   * doesn't equals to 100%. This expect IllegalArgumentException thrown.
   */
  @Test(expected = ServiceStateException.class)
  public void testQueueParsingWithSumOfChildLabelCapacityNot100PercentWithWildCard()
      throws Exception {
    CapacitySchedulerConfiguration csConf =
            new CapacitySchedulerConfiguration();

    // If the new queue mode is used it's allowed to leave
    // some of the resources of a parent queue unallocated
    assumeThat(csConf.isLegacyQueueMode(), is(true));

    setupQueueConfigurationWithLabels(csConf);
    csConf.setCapacityByLabel(B3, "red", 24);
    csConf.setAccessibleNodeLabels(ROOT, ImmutableSet.of(RMNodeLabelsManager.ANY));
    csConf.setAccessibleNodeLabels(B, ImmutableSet.of(RMNodeLabelsManager.ANY));
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithLabels(conf, ImmutableSet.of("red", "blue"));
    ServiceOperations.stopQuietly(rm);
  }
  
  @Test(expected = IOException.class)
  public void testQueueParsingWithMoveQueue() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setQueues(ROOT, new String[] {"a"});
    csConf.setQueues(A, new String[] {"x", "y"});
    csConf.setCapacity(A, 100);
    csConf.setCapacity(AX, 50);
    csConf.setCapacity(AY, 50);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithoutLabels(conf);
    CapacityScheduler capacityScheduler = (CapacityScheduler) rm.getResourceScheduler();

    csConf.setQueues(ROOT, new String[] {"a", "x"});
    csConf.setQueues(A, new String[] {"y"});
    csConf.setCapacity(X, 50);
    csConf.setCapacity(A, 50);
    csConf.setCapacity(AY, 100);
    
    capacityScheduler.reinitialize(csConf, rm.getRMContext());
    ServiceOperations.stopQuietly(rm);
  }

  @Test(timeout = 60000, expected = ServiceStateException.class)
  public void testRMStartWrongNodeCapacity() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    Set<String> labels = ImmutableSet.of("x", "y", "z");

    // If the new queue mode is used it's allowed to leave
    // some of the resources of a parent queue unallocated
    assumeThat(csConf.isLegacyQueueMode(), is(true));

    // Define top-level queues
    csConf.setQueues(ROOT, new String[] {"a"});
    csConf.setCapacityByLabel(ROOT, "x", 100);
    csConf.setCapacityByLabel(ROOT, "y", 100);
    csConf.setCapacityByLabel(ROOT, "z", 100);

    csConf.setCapacity(A, 100);
    csConf.setAccessibleNodeLabels(A, labels);
    csConf.setCapacityByLabel(A, "x", 100);
    csConf.setCapacityByLabel(A, "y", 100);
    csConf.setCapacityByLabel(A, "z", 70);

    YarnConfiguration conf = new YarnConfiguration(csConf);
    MockRM rm = createMockRMWithLabels(conf, labels);
    ServiceOperations.stopQuietly(rm);
  }


  @Test
  public void testQueueOrderingPolicyUpdatedAfterReinitialize() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithoutLabels(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithoutLabels(conf);
    CapacityScheduler capacityScheduler = (CapacityScheduler) rm.getResourceScheduler();

    // Add a new b4 queue
    csConf.setQueues(B,
        new String[] { "b1", "b2", "b3", "b4" });
    csConf.setCapacity(B4, 0f);
    ParentQueue bQ = (ParentQueue) capacityScheduler.getQueue("b");
    checkEqualsToQueueSet(bQ.getChildQueues(),
        new String[] { "b1", "b2", "b3" });
    capacityScheduler.reinitialize(csConf, rm.getRMContext());

    // Check child queue of b
    checkEqualsToQueueSet(bQ.getChildQueues(),
        new String[] { "b1", "b2", "b3", "b4" });

    PriorityUtilizationQueueOrderingPolicy queueOrderingPolicy =
        (PriorityUtilizationQueueOrderingPolicy) bQ.getQueueOrderingPolicy();
    checkEqualsToQueueSet(queueOrderingPolicy.getQueues(),
        new String[] { "b1", "b2", "b3", "b4" });

    ServiceOperations.stopQuietly(rm);
  }

  @Test(timeout = 60000)
  public void testQueueCapacityWithWeight() throws Exception {
    Set<String> labels = ImmutableSet.of("x", "y", "z");

    CapacitySchedulerConfiguration csConf =
            new CapacitySchedulerConfiguration();
    // Define top-level queues
    csConf.setQueues(ROOT, new String[] {"a"});
    csConf.setLabeledQueueWeight(ROOT, "x", 100);
    csConf.setLabeledQueueWeight(ROOT, "y", 100);
    csConf.setLabeledQueueWeight(ROOT, "z", 100);

    csConf.setNonLabeledQueueWeight(A, 100);
    csConf.setAccessibleNodeLabels(A, labels);
    csConf.setLabeledQueueWeight(A, "x", 100);
    csConf.setLabeledQueueWeight(A, "y", 100);
    csConf.setLabeledQueueWeight(A, "z", 70);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithLabels(conf, labels);

    verifyQueueAbsCapacity(rm, CapacitySchedulerConfiguration.ROOT, "", 1f);
    verifyQueueAbsCapacity(rm, CapacitySchedulerConfiguration.ROOT, "x", 1f);
    verifyQueueAbsCapacity(rm, CapacitySchedulerConfiguration.ROOT, "y", 1f);
    verifyQueueAbsCapacity(rm, CapacitySchedulerConfiguration.ROOT, "z", 1f);

    verifyQueueAbsCapacity(rm, A_PATH, "", 1f);
    verifyQueueAbsCapacity(rm, A_PATH, "x", 1f);
    verifyQueueAbsCapacity(rm, A_PATH, "y", 1f);
    verifyQueueAbsCapacity(rm, A_PATH, "z", 1f);
    ServiceOperations.stopQuietly(rm);
  }

  @Test
  public void testQueueParsingWithDefaultUserLimitValues() throws Exception {
    CapacitySchedulerConfiguration csConf =
            new CapacitySchedulerConfiguration();

    final String queueA = CapacitySchedulerConfiguration.ROOT + ".a";
    final String queueB = CapacitySchedulerConfiguration.ROOT + ".b";

    // Define top-level queues
    csConf.setQueues(ROOT, new String[] {"a", "b"});

    // Set default value
    csConf.setDefaultUserLimit(20);
    csConf.setDefaultUserLimitFactor(2.0f);

    // Set A configuration and let B use default values
    csConf.setCapacity(A, 50);
    csConf.setUserLimit(A, 15);
    csConf.setUserLimitFactor(A, 1.5f);
    csConf.setCapacity(B, 50);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    MockRM rm = createMockRMWithoutLabels(conf);

    // Test
    CapacityScheduler capacityScheduler = (CapacityScheduler) rm.getResourceScheduler();

    Assert.assertEquals(15,
            ((LeafQueue)capacityScheduler.getQueue(A_PATH)).getUserLimit(), DELTA);
    Assert.assertEquals(1.5,
            ((LeafQueue)capacityScheduler.getQueue(A_PATH)).getUserLimitFactor(), DELTA);
    Assert.assertEquals(20,
            ((LeafQueue)capacityScheduler.getQueue(B_PATH)).getUserLimit(), DELTA);
    Assert.assertEquals(2.0,
            ((LeafQueue)capacityScheduler.getQueue(B_PATH)).getUserLimitFactor(), DELTA);

    // Use hadoop default value
    csConf = new CapacitySchedulerConfiguration();

    csConf.setQueues(ROOT, new String[] {"a", "b"});
    csConf.setCapacity(A, 50);
    csConf.setUserLimit(A, 15);
    csConf.setUserLimitFactor(A, 1.5f);
    csConf.setCapacity(B, 50);

    capacityScheduler.reinitialize(csConf, rm.getRMContext());

    Assert.assertEquals(15,
            ((LeafQueue)capacityScheduler.getQueue(A_PATH)).getUserLimit(), DELTA);
    Assert.assertEquals(1.5,
            ((LeafQueue)capacityScheduler.getQueue(A_PATH)).getUserLimitFactor(), DELTA);
    Assert.assertEquals(100,
            ((LeafQueue)capacityScheduler.getQueue(B_PATH)).getUserLimit(), DELTA);
    Assert.assertEquals(1,
            ((LeafQueue)capacityScheduler.getQueue(B_PATH)).getUserLimitFactor(), DELTA);
  }

  private void verifyQueueAbsCapacity(MockRM rm, String queuePath, String label,
      float expectedAbsCapacity) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = cs.getQueue(queuePath);
    Assert.assertEquals(expectedAbsCapacity,
        queue.getQueueCapacities().getAbsoluteCapacity(label), 1e-6);
  }

  private void checkEqualsToQueueSet(List<CSQueue> queues, String[] queueNames) {
    Set<String> existedQueues = new HashSet<>();
    for (CSQueue q : queues) {
      existedQueues.add(q.getQueueShortName());
    }
    for (String q : queueNames) {
      Assert.assertTrue(existedQueues.remove(q));
    }
    Assert.assertTrue(existedQueues.isEmpty());
  }
}
