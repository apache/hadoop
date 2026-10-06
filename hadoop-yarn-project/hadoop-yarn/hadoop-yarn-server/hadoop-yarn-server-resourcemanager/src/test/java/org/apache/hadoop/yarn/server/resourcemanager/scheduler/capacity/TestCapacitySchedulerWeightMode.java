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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestCapacitySchedulerWeightMode {
  private static final String DEFAULT_PATH = CapacitySchedulerConfiguration.ROOT + ".default";
  private static final String A_PATH = CapacitySchedulerConfiguration.ROOT + ".a";
  private static final String B_PATH = CapacitySchedulerConfiguration.ROOT + ".b";
  private static final String A1_PATH = A_PATH + ".a1";
  private static final String B1_PATH = B_PATH + ".b1";
  private static final String B2_PATH = B_PATH + ".b2";
  private static final QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  private static final QueuePath DEFAULT = new QueuePath(DEFAULT_PATH);
  private static final QueuePath A = new QueuePath(A_PATH);
  private static final QueuePath B = new QueuePath(B_PATH);
  private static final QueuePath A1 = new QueuePath(A1_PATH);
  private static final QueuePath B1 = new QueuePath(B1_PATH);
  private static final QueuePath B2 = new QueuePath(B2_PATH);
  private static final QueuePath C = new QueuePath(CapacitySchedulerConfiguration.ROOT + ".c");
  private static final QueuePath D = new QueuePath(CapacitySchedulerConfiguration.ROOT + ".d");
  private static final String MIXED_MODES_PREFIX = "Parent queue 'root' have children queue "
      + "used mixed of  weight mode, percentage and absolute mode, it is not allowed, please "
      + "double check, details:";

  private YarnConfiguration conf;

  RMNodeLabelsManager mgr;

  @BeforeEach
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  public static <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }

  public static CapacitySchedulerConfiguration getConfigWithInheritedAccessibleNodeLabel(
      Configuration config) {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        config);

    // Define top-level queues
    conf.setQueues(ROOT,
        new String[] { "a"});

    conf.setCapacityByLabel(A, RMNodeLabelsManager.NO_LABEL, 100f);
    conf.setCapacityByLabel(A, "newLabel", 100f);
    conf.setAccessibleNodeLabels(A, toSet("newLabel"));
    conf.setAllowZeroCapacitySum(A, true);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1" });
    conf.setCapacityByLabel(A1, RMNodeLabelsManager.NO_LABEL, 100f);

    return conf;
  }


  /*
   * Queue structure:
   *                      root (*)
   *                  ________________
   *                 /                \
   *               a x(weight=100), y(w=50)   b y(w=50), z(w=100)
   *               ________________    ______________
   *              /                   /              \
   *             a1 ([x,y]: w=100)    b1(no)          b2([y,z]: w=100)
   */
  public static Configuration getCSConfWithQueueLabelsWeightOnly(
      Configuration config) {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        config);

    // Define top-level queues
    conf.setQueues(ROOT,
        new String[] { "a", "b" });
    conf.setLabeledQueueWeight(ROOT, "x", 100);
    conf.setLabeledQueueWeight(ROOT, "y", 100);
    conf.setLabeledQueueWeight(ROOT, "z", 100);

    conf.setLabeledQueueWeight(A, RMNodeLabelsManager.NO_LABEL, 1);
    conf.setMaximumCapacity(A, 10);
    conf.setAccessibleNodeLabels(A, toSet("x", "y"));
    conf.setLabeledQueueWeight(A, "x", 100);
    conf.setLabeledQueueWeight(A, "y", 50);

    conf.setLabeledQueueWeight(B, RMNodeLabelsManager.NO_LABEL, 9);
    conf.setMaximumCapacity(B, 100);
    conf.setAccessibleNodeLabels(B, toSet("y", "z"));
    conf.setLabeledQueueWeight(B, "y", 50);
    conf.setLabeledQueueWeight(B, "z", 100);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1" });
    conf.setLabeledQueueWeight(A1, RMNodeLabelsManager.NO_LABEL, 100);
    conf.setMaximumCapacity(A1, 100);
    conf.setAccessibleNodeLabels(A1, toSet("x", "y"));
    conf.setDefaultNodeLabelExpression(A1, "x");
    conf.setLabeledQueueWeight(A1, "x", 100);
    conf.setLabeledQueueWeight(A1, "y", 100);

    conf.setQueues(B, new String[] { "b1", "b2" });
    conf.setLabeledQueueWeight(B1, RMNodeLabelsManager.NO_LABEL, 50);
    conf.setMaximumCapacity(B1, 50);
    conf.setAccessibleNodeLabels(B1, RMNodeLabelsManager.EMPTY_STRING_SET);

    conf.setLabeledQueueWeight(B2, RMNodeLabelsManager.NO_LABEL, 50);
    conf.setMaximumCapacity(B2, 50);
    conf.setAccessibleNodeLabels(B2, toSet("y", "z"));
    conf.setLabeledQueueWeight(B2, "y", 100);
    conf.setLabeledQueueWeight(B2, "z", 100);

    return conf;
  }

  /*
   * Queue structure:
   *                      root (*)
   *                  _______________________
   *                 /                       \
   *               a x(weight=100), y(w=50)   b y(w=50), z(w=100)
   *               ________________             ______________
   *              /                           /              \
   *             a1 ([x,y]: pct=100%)    b1(no)          b2([y,z]: percent=100%)
   *
   * Parent uses weight, child uses percentage
   */
  public static Configuration getCSConfWithLabelsParentUseWeightChildUsePct(
      Configuration config) {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        config);

    // Define top-level queues
    conf.setQueues(ROOT,
        new String[] { "a", "b" });
    conf.setLabeledQueueWeight(ROOT, "x", 100);
    conf.setLabeledQueueWeight(ROOT, "y", 100);
    conf.setLabeledQueueWeight(ROOT, "z", 100);

    conf.setLabeledQueueWeight(A, RMNodeLabelsManager.NO_LABEL, 1);
    conf.setMaximumCapacity(A, 10);
    conf.setAccessibleNodeLabels(A, toSet("x", "y"));
    conf.setLabeledQueueWeight(A, "x", 100);
    conf.setLabeledQueueWeight(A, "y", 50);

    conf.setLabeledQueueWeight(B, RMNodeLabelsManager.NO_LABEL, 9);
    conf.setMaximumCapacity(B, 100);
    conf.setAccessibleNodeLabels(B, toSet("y", "z"));
    conf.setLabeledQueueWeight(B, "y", 50);
    conf.setLabeledQueueWeight(B, "z", 100);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1" });
    conf.setCapacityByLabel(A1, RMNodeLabelsManager.NO_LABEL, 100);
    conf.setMaximumCapacity(A1, 100);
    conf.setAccessibleNodeLabels(A1, toSet("x", "y"));
    conf.setDefaultNodeLabelExpression(A1, "x");
    conf.setCapacityByLabel(A1, "x", 100);
    conf.setCapacityByLabel(A1, "y", 100);

    conf.setQueues(B, new String[] { "b1", "b2" });
    conf.setCapacityByLabel(B1, RMNodeLabelsManager.NO_LABEL, 50);
    conf.setMaximumCapacity(B1, 50);
    conf.setAccessibleNodeLabels(B1, RMNodeLabelsManager.EMPTY_STRING_SET);

    conf.setCapacityByLabel(B2, RMNodeLabelsManager.NO_LABEL, 50);
    conf.setMaximumCapacity(B2, 50);
    conf.setAccessibleNodeLabels(B2, toSet("y", "z"));
    conf.setCapacityByLabel(B2, "y", 100);
    conf.setCapacityByLabel(B2, "z", 100);

    return conf;
  }

  /*
   * Queue structure:
   *                      root (*)
   *                  _______________________
   *                 /                       \
   *               a x(=100%), y(50%)   b y(=50%), z(=100%)
   *               ________________             ______________
   *              /                           /              \
   *             a1 ([x,y]: w=1)    b1(no)          b2([y,z]: w=1)
   *
   * Parent uses percentages, child uses weights
   */
  public static Configuration getCSConfWithLabelsParentUsePctChildUseWeight(
      Configuration config) {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(
        config);

    // Define top-level queues
    conf.setQueues(ROOT,
        new String[] { "a", "b" });
    conf.setCapacityByLabel(ROOT, "x", 100);
    conf.setCapacityByLabel(ROOT, "y", 100);
    conf.setCapacityByLabel(ROOT, "z", 100);

    conf.setCapacityByLabel(A, RMNodeLabelsManager.NO_LABEL, 10);
    conf.setMaximumCapacity(A, 10);
    conf.setAccessibleNodeLabels(A, toSet("x", "y"));
    conf.setCapacityByLabel(A, "x", 100);
    conf.setCapacityByLabel(A, "y", 50);

    conf.setCapacityByLabel(B, RMNodeLabelsManager.NO_LABEL, 90);
    conf.setMaximumCapacity(B, 100);
    conf.setAccessibleNodeLabels(B, toSet("y", "z"));
    conf.setCapacityByLabel(B, "y", 50);
    conf.setCapacityByLabel(B, "z", 100);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1" });
    conf.setLabeledQueueWeight(A1, RMNodeLabelsManager.NO_LABEL, 1);
    conf.setMaximumCapacity(A1, 100);
    conf.setAccessibleNodeLabels(A1, toSet("x", "y"));
    conf.setDefaultNodeLabelExpression(A1, "x");
    conf.setLabeledQueueWeight(A1, "x", 1);
    conf.setLabeledQueueWeight(A1, "y", 1);

    conf.setQueues(B, new String[] { "b1", "b2" });
    conf.setLabeledQueueWeight(B1, RMNodeLabelsManager.NO_LABEL, 1);
    conf.setMaximumCapacity(B1, 50);
    conf.setAccessibleNodeLabels(B1, RMNodeLabelsManager.EMPTY_STRING_SET);

    conf.setLabeledQueueWeight(B2, RMNodeLabelsManager.NO_LABEL, 1);
    conf.setMaximumCapacity(B2, 50);
    conf.setAccessibleNodeLabels(B2, toSet("y", "z"));
    conf.setLabeledQueueWeight(B2, "y", 1);
    conf.setLabeledQueueWeight(B2, "z", 1);

    return conf;
  }

  /**
   * This is an identical test of
   * @see {@link TestNodeLabelContainerAllocation#testContainerAllocateWithComplexLabels()}
   * The only difference is, instead of using label, it uses weight mode
   * @throws Exception
   */
  @Test
  @Timeout(value = 300)
  public void testContainerAllocateWithComplexLabelsWeightOnly() throws Exception {
    internalTestContainerAllocationWithNodeLabel(
        getCSConfWithQueueLabelsWeightOnly(conf));
  }

  /**
   * This is an identical test of
   * @see {@link TestNodeLabelContainerAllocation#testContainerAllocateWithComplexLabels()}
   * The only difference is, instead of using label, it uses weight mode:
   * Parent uses weight, child uses percent
   * @throws Exception
   */
  @Test
  @Timeout(value = 300)
  public void testContainerAllocateWithComplexLabelsWeightAndPercentMixed1() throws Exception {
    internalTestContainerAllocationWithNodeLabel(
        getCSConfWithLabelsParentUseWeightChildUsePct(conf));
  }

  /**
   * This is an identical test of
   * @see {@link TestNodeLabelContainerAllocation#testContainerAllocateWithComplexLabels()}
   * The only difference is, instead of using label, it uses weight mode:
   * Parent uses percent, child uses weight
   * @throws Exception
   */
  @Test
  @Timeout(value = 300)
  public void testContainerAllocateWithComplexLabelsWeightAndPercentMixed2() throws Exception {
    internalTestContainerAllocationWithNodeLabel(
        getCSConfWithLabelsParentUsePctChildUseWeight(conf));
  }

  /**
   * This checks whether the parent prints the correct log about the
   * configured mode.
   */
  @Test
  @Timeout(value = 300)
  public void testGetCapacityOrWeightStringUsingWeights() throws IOException {
    try (MockRM rm = new MockRM(
        getCSConfWithQueueLabelsWeightOnly(conf))) {
      rm.start();
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

      String capacityOrWeightString = ((ParentQueue) cs.getQueue(A.getFullPath()))
          .getCapacityOrWeightString();
      validateCapacityOrWeightString(capacityOrWeightString, true);

      capacityOrWeightString = ((LeafQueue) cs.getQueue(A1.getFullPath()))
          .getCapacityOrWeightString();
      validateCapacityOrWeightString(capacityOrWeightString, true);

      capacityOrWeightString = ((LeafQueue) cs.getQueue(A1.getFullPath()))
          .getExtendedCapacityOrWeightString();
      validateCapacityOrWeightString(capacityOrWeightString, true);
    }
  }

  /**
   * This checks whether the parent prints the correct log about the
   * configured mode.
   */
  @Test
  @Timeout(value = 300)
  public void testGetCapacityOrWeightStringParentPctLeafWeights()
      throws IOException {
    try (MockRM rm = new MockRM(
        getCSConfWithLabelsParentUseWeightChildUsePct(conf))) {
      rm.start();
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

      String capacityOrWeightString = ((ParentQueue) cs.getQueue(A.getFullPath()))
          .getCapacityOrWeightString();
      validateCapacityOrWeightString(capacityOrWeightString, true);

      capacityOrWeightString = ((LeafQueue) cs.getQueue(A1.getFullPath()))
          .getCapacityOrWeightString();
      validateCapacityOrWeightString(capacityOrWeightString, false);

      capacityOrWeightString = ((LeafQueue) cs.getQueue(A1.getFullPath()))
          .getExtendedCapacityOrWeightString();
      validateCapacityOrWeightString(capacityOrWeightString, false);
    }
  }

  /**
   * This test ensures that while iterating through a parent's Node Labels
   * (when calculating the normalized weights) the parent's Node Labels won't
   * be added to the children with weight -1. If the parent
   * has a node label that a specific child doesn't the normalized calling the
   * normalized weight setter will be skipped. The queue root.b has access to
   * the labels "x" and "y", but root.b.b1 won't. For more information see
   * YARN-10807.
   * @throws Exception
   */
  @Test
  public void testChildAccessibleNodeLabelsWeightMode() throws Exception {
    MockRM rm = new MockRM(getCSConfWithQueueLabelsWeightOnly(conf));
    rm.start();

    CapacityScheduler cs =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    LeafQueue b1 = (LeafQueue) cs.getQueue(B1.getFullPath());

    assertNotNull(b1);
    assertTrue(b1.getAccessibleNodeLabels().isEmpty());

    Set<String> b1ExistingNodeLabels = ((CSQueue) b1).getQueueCapacities()
        .getExistingNodeLabels();
    assertEquals(1, b1ExistingNodeLabels.size());
    assertEquals("", b1ExistingNodeLabels.iterator().next());

    rm.close();
  }

  /**
   * Tests whether weight is correctly reset to -1. See YARN-11016 for further details.
   * @throws IOException if reinitialization fails
   */
  @Test()
  public void testAccessibleNodeLabelsInheritanceNoWeightMode() throws IOException {
    CapacitySchedulerConfiguration newConf = getConfigWithInheritedAccessibleNodeLabel(conf);

    MockRM rm = new MockRM(newConf);
    CapacityScheduler cs =
        (CapacityScheduler) rm.getRMContext().getScheduler();

    Resource clusterResource = Resource.newInstance(1024, 2);
    cs.getRootQueue().updateClusterResource(clusterResource, new ResourceLimits(clusterResource));

    try {
      cs.reinitialize(newConf, rm.getRMContext());
    } catch (Exception e) {
      fail("Reinitialization failed with " + e);
    }
  }

  @Test
  public void testQueueInfoWeight() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.init(conf);
    rm.start();

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(
        conf);
    csConf.setQueues(ROOT,
        new String[] {"a", "b", "default"});
    csConf.setNonLabeledQueueWeight(A, 1);
    csConf.setNonLabeledQueueWeight(B, 2);
    csConf.setNonLabeledQueueWeight(DEFAULT, 3);

    // Check queue info capacity
    CapacityScheduler cs =
        (CapacityScheduler)rm.getRMContext().getScheduler();
    cs.reinitialize(csConf, rm.getRMContext());

    LeafQueue a = (LeafQueue)
        cs.getQueue("root.a");
    assertNotNull(a);
    assertEquals(a.getQueueCapacities().getWeight(),
        a.getQueueInfo(false,
        false).getWeight(), 1e-6);

    LeafQueue b = (LeafQueue)
        cs.getQueue("root.b");
    assertNotNull(b);
    assertEquals(b.getQueueCapacities().getWeight(),
        b.getQueueInfo(false,
        false).getWeight(), 1e-6);
    rm.close();
  }

  @Test
  public void testSiblingsMixingWeightAndPercentageRejected() {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
    csConf.setQueues(ROOT, new String[] {"a", "b"});
    csConf.setCapacity(A, 50f);
    csConf.setNonLabeledQueueWeight(B, 1);

    assertEquals(MIXED_MODES_PREFIX + "{Queue=root.a, label= uses percentage mode}. "
        + "{Queue=root.b, label= uses weight mode}. "
        + "{Queue=root.b, label= uses percentage mode}. ", getStartupError(csConf));
  }

  @Test
  public void testAbsoluteAndPercentageSiblingsDependOnDeclarationOrder()
      throws IOException {
    CapacitySchedulerConfiguration absoluteFirst = new CapacitySchedulerConfiguration(conf);
    absoluteFirst.setQueues(ROOT, new String[] {"a", "b"});
    absoluteFirst.setCapacity(A, "[memory=1024,vcores=1]");
    absoluteFirst.setCapacity(B, 50f);
    assertEquals(MIXED_MODES_PREFIX + "{Queue=root.a, label= uses absolute mode}. "
        + "{Queue=root.b, label= uses percentage mode}. ", getStartupError(absoluteFirst));

    // The mixed mode check only remembers a percentage sibling seen after the
    // last absolute one, so listing the percentage queue first is accepted.
    CapacitySchedulerConfiguration percentageFirst =
        new CapacitySchedulerConfiguration(conf);
    percentageFirst.setQueues(ROOT, new String[] {"b", "a"});
    percentageFirst.setCapacity(A, "[memory=1024,vcores=1]");
    percentageFirst.setCapacity(B, 50f);
    try (MockRM rm = new MockRM(percentageFirst)) {
      rm.start();
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      assertEquals(Resource.newInstance(1024, 1), cs.getQueue(A.getFullPath())
          .getQueueResourceQuotas().getConfiguredMinResource());
    }
  }

  @Test
  public void testAbsoluteVectorWithWeightUnitsRejectedNextToAbsoluteSibling() {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
    csConf.setQueues(ROOT, new String[] {"a", "c"});
    csConf.setCapacity(A, "[memory=40960,vcores=40]");
    csConf.setCapacity(C, "[memory=1w,vcores=1w]");

    assertEquals(MIXED_MODES_PREFIX + "{Queue=root.a, label= uses absolute mode}. "
        + "{Queue=root.c, label= uses weight mode}. "
        + "{Queue=root.c, label= uses absolute mode}. ", getStartupError(csConf));
  }

  @Test
  public void testPerLabelMixingOfWeightAndPercentageRejected() {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
    csConf.setQueues(ROOT, new String[] {"a", "b"});
    csConf.setCapacityByLabel(A, "x", 50f);
    csConf.setLabeledQueueWeight(B, "x", 1);

    // The percentage flag is not reset per queue, so root.b is also listed as
    // using percentage mode.
    assertEquals(MIXED_MODES_PREFIX + "{Queue=root.a, label=x uses percentage mode}. "
        + "{Queue=root.b, label= uses percentage mode}. "
        + "{Queue=root.b, label=x uses weight mode}. "
        + "{Queue=root.b, label=x uses percentage mode}. ", getStartupError(csConf));
  }

  @ParameterizedTest
  @CsvSource({"150, 100, 150.0", "100, -1, -1.0"})
  public void testLabeledCapacityOutOfRangeRejected(float capacity, float maximumCapacity,
      String illegalValue) {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
    csConf.setQueues(ROOT, new String[] {"a"});
    csConf.setCapacity(A, 100f);
    csConf.setAccessibleNodeLabels(A, toSet("x"));
    csConf.setCapacityByLabel(A, "x", capacity);
    csConf.setMaximumCapacityByLabel(A, "x", maximumCapacity);

    assertEquals("Illegal capacity of " + illegalValue + " for node-label=x in queue=root.a, "
        + "valid capacity should in range of [0, 100].", getStartupError(csConf));
  }

  @Test
  public void testWeightAbove10000Rejected() {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
    csConf.setQueues(ROOT, new String[] {"a", "b"});
    csConf.setNonLabeledQueueWeight(A, 20000);
    csConf.setNonLabeledQueueWeight(B, 1);

    assertEquals("Illegal weight=20000.0 for queue=root.alabel=. "
        + "Acceptable values: [0, 10000], -1 is same as not set", getStartupError(csConf));
  }

  @Test
  public void testZeroWeightAndZeroPercentSiblingsAccepted() throws IOException {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
    csConf.setQueues(ROOT, new String[] {"a", "b", "c", "d"});
    csConf.setCapacity(A, 0f);
    csConf.setNonLabeledQueueWeight(B, 2);
    csConf.setNonLabeledQueueWeight(C, 1);
    csConf.setNonLabeledQueueWeight(D, 0);

    try (MockRM rm = new MockRM(csConf)) {
      rm.start();
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      assertEquals(0f, cs.getQueue(A.getFullPath()).getAbsoluteCapacity(), 1e-6);
      assertEquals(2f / 3, cs.getQueue(B.getFullPath()).getAbsoluteCapacity(), 1e-6);
      assertEquals(1f / 3, cs.getQueue(C.getFullPath()).getAbsoluteCapacity(), 1e-6);
      QueueCapacities d = cs.getQueue(D.getFullPath()).getQueueCapacities();
      assertEquals(0f, d.getWeight());
      assertEquals(0f, d.getNormalizedWeight());
      assertEquals(0f, d.getAbsoluteCapacity());
    }
  }

  private static String getStartupError(CapacitySchedulerConfiguration csConf) {
    try (MockRM rm = new MockRM(csConf)) {
      rm.start();
    } catch (Exception e) {
      return ExceptionUtils.getRootCause(e).getMessage();
    }
    return fail("Expected the scheduler configuration to be rejected");
  }

  private void internalTestContainerAllocationWithNodeLabel(
      Configuration csConf) throws Exception {
    /*
     * Queue structure:
     *                      root (*)
     *                  ________________
     *                 /                \
     *               a x(100%), y(50%)   b y(50%), z(100%)
     *               ________________    ______________
     *              /                   /              \
     *             a1 (x,y)         b1(no)              b2(y,z)
     *               100%                          y = 100%, z = 100%
     *
     * Node structure:
     * h1 : x
     * h2 : y
     * h3 : y
     * h4 : z
     * h5 : NO
     *
     * Total resource:
     * x: 4G
     * y: 6G
     * z: 2G
     * *: 2G
     *
     * Resource of
     * a1: x=4G, y=3G, NO=0.2G
     * b1: NO=0.9G (max=1G)
     * b2: y=3, z=2G, NO=0.9G (max=1G)
     *
     * Each node can only allocate two containers
     */

    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y", "z"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0),
        toSet("x"), NodeId.newInstance("h2", 0), toSet("y"),
        NodeId.newInstance("h3", 0), toSet("y"), NodeId.newInstance("h4", 0),
        toSet("z"), NodeId.newInstance("h5", 0),
        RMNodeLabelsManager.EMPTY_STRING_SET));

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 2048);
    MockNM nm2 = rm1.registerNode("h2:1234", 2048);
    MockNM nm3 = rm1.registerNode("h3:1234", 2048);
    MockNM nm4 = rm1.registerNode("h4:1234", 2048);
    MockNM nm5 = rm1.registerNode("h5:1234", 2048);

    ContainerId containerId;

    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data2);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // request a container (label = y). can be allocated on nm2
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "y");
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2L);
    assertTrue(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am1.getApplicationAttemptId(), containerId, rm1,
        "h2");

    // launch an app to queue b1 (label = y), and check all container will
    // be allocated in h5
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b1")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm5);

    // request a container for AM, will succeed
    // and now b1's queue capacity will be used, cannot allocate more containers
    // (Maximum capacity reached)
    am2.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am2.getApplicationAttemptId(), 2);
    assertFalse(rm1.waitForState(nm4, containerId,
        RMContainerState.ALLOCATED));
    assertFalse(rm1.waitForState(nm5, containerId,
        RMContainerState.ALLOCATED));

    // launch an app to queue b2
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b2")
            .withUnmanagedAM(false)
            .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm5);

    // request a container. try to allocate on nm1 (label = x) and nm3 (label =
    // y,z). Will successfully allocate on nm3
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "y");
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 2);
    assertFalse(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED));
    assertTrue(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h3");

    // try to allocate container (request label = z) on nm4 (label = y,z).
    // Will successfully allocate on nm4 only.
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "z");
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 3L);
    assertTrue(rm1.waitForState(nm4, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h4");

    rm1.close();
  }

  private void checkTaskContainersHost(ApplicationAttemptId attemptId,
      ContainerId containerId, ResourceManager rm, String host) {
    YarnScheduler scheduler = rm.getRMContext().getScheduler();
    SchedulerAppReport appReport = scheduler.getSchedulerAppInfo(attemptId);

    assertTrue(appReport.getLiveContainers().size() > 0);
    for (RMContainer c : appReport.getLiveContainers()) {
      if (c.getContainerId().equals(containerId)) {
        assertEquals(host, c.getAllocatedNode().getHost());
      }
    }
  }

  private void validateCapacityOrWeightString(String capacityOrWeightString,
      boolean shouldContainWeight) {
    assertEquals(shouldContainWeight,
        capacityOrWeightString.contains("weight"));
    assertEquals(shouldContainWeight,
        capacityOrWeightString.contains("normalizedWeight"));
    assertEquals(!shouldContainWeight,
        capacityOrWeightString.contains("capacity"));

  }
}
