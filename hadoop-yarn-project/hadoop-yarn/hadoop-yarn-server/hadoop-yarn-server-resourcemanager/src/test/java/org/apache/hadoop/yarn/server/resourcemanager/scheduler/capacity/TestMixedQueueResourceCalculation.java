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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning.QueueUpdateWarningType;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ROOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.GB;

public class TestMixedQueueResourceCalculation extends CapacitySchedulerQueueCalculationTestBase {
  private static final long MEMORY = 16384;
  private static final long VCORES = 16;
  private static final String C_VECTOR_WITH_WARNING = createCapacityVector(weight(3),
      absolute(VCORES * 0.25));
  private static final String A11_VECTOR_WITH_WARNING = createCapacityVector(weight(1),
      absolute(VCORES * 0.25));
  private static final String A1_VECTOR_WITH_WARNING = createCapacityVector(absolute(2048),
      absolute(VCORES * 0.25));
  private static final String C_VECTOR_NO_REMAINING_RESOURCE = createCapacityVector(weight(3),
      absolute(VCORES * 0.25));
  private static final String A1_VECTOR_NO_REMAINING_RESOURCE = createCapacityVector(weight(1),
      absolute(VCORES * 0.25));

  private static final Resource A12_EXPECTED_MAX_RESOURCE_MAX_WARNINGS =
      createResource(MEMORY * 0.5, VCORES);
  private static final Resource A11_EXPECTED_MAX_RESOURCE_MAX_WARNINGS =
      createResource(MEMORY * 0.5, 0.1 * VCORES);
  private static final Resource A11_EXPECTED_MIN_RESOURCE_MAX_WARNINGS =
      createResource(0.5 * 0.5 * MEMORY, 0.1 * VCORES);
  private static final Resource A12_EXPECTED_MIN_RESOURCE_MAX_WARNINGS =
      createResource(0.5 * 0.5 * MEMORY, 0);
  private static final String A11_MAX_VECTOR_MAX_WARNINGS =
      createCapacityVector(absolute(MEMORY), percentage(10));
  private static final String A1_MAX_VECTOR_MAX_WARNINGS =
      createCapacityVector(absolute(MEMORY * 0.5),
      percentage(100));

  private static final Resource UPDATE_RESOURCE = Resource.newInstance(16384, 16);
  private static final Resource ZERO_RESOURCE = Resource.newInstance(0, 0);

  private static final Resource A_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(2486, 9);
  private static final Resource A1_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(621, 4);
  private static final Resource A11_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(217, 1);
  private static final Resource A12_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(403, 3);
  private static final Resource A2_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(1865, 5);
  private static final Resource B_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(8095, 3);
  private static final Resource B1_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(8095, 3);
  private static final Resource C_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(5803, 4);

  private static final Resource B_WARNING_RESOURCE = Resource.newInstance(8096, 4);
  private static final Resource B1_WARNING_RESOURCE = Resource.newInstance(8096, 3);
  private static final Resource A_WARNING_RESOURCE = Resource.newInstance(8288, 12);
  private static final Resource A1_WARNING_RESOURCE = Resource.newInstance(2048, 4);
  private static final Resource A2_WARNING_RESOURCE = Resource.newInstance(2048, 8);
  private static final Resource A12_WARNING_RESOURCE = Resource.newInstance(2048, 4);

  private static final String A_VECTOR_ZERO_RESOURCE =
      createCapacityVector(percentage(100), weight(6));
  private static final String B_VECTOR_ZERO_RESOURCE =
      createCapacityVector(absolute(MEMORY), absolute(VCORES * 0.5));

  private static final String A_MAX_VECTOR_DIFFERENT_MIN_MAX =
      createCapacityVector(absolute(MEMORY), percentage(80));
  private static final Resource B_EXPECTED_MAX_RESOURCE_DIFFERENT_MIN_MAX =
      Resource.newInstance(MEMORY, (int) (VCORES * 0.5));
  private static final Resource A_EXPECTED_MAX_RESOURCE_DIFFERENT_MIN_MAX =
      Resource.newInstance(MEMORY, (int) (VCORES * 0.8));
  private static final String B_MAX_VECTOR_DIFFERENT_MIN_MAX =
      createCapacityVector(absolute(MEMORY), absolute(VCORES * 0.5));
  private static final String A_MIN_VECTOR_DIFFERENT_MIN_MAX =
      createCapacityVector(percentage(50), absolute(VCORES * 0.5));
  private static final String B_MIN_VECTOR_DIFFERENT_MIN_MAX =
      createCapacityVector(weight(6), percentage(100));
  private static final String B_INVALID_MAX_VECTOR =
      createCapacityVector(absolute(MEMORY), weight(10));

  private static final String X_LABEL = "x";
  private static final String Y_LABEL = "y";
  private static final String Z_LABEL = "z";

  private static final String H1_NODE = "h1";
  private static final String H2_NODE = "h2";
  private static final String H3_NODE = "h3";
  private static final String H4_NODE = "h4";
  private static final String H5_NODE = "h5";
  private static final int H1_MEMORY = 60 * GB;
  private static final int H1_VCORES = 60;
  private static final int H2_MEMORY = 10 * GB;
  private static final int H2_VCORES = 25;
  private static final int H3_VCORES = 35;
  private static final int H3_MEMORY = 10 * GB;
  private static final int H4_MEMORY = 10 * GB;
  private static final int H4_VCORES = 15;

  private static final String A11_MIN_VECTOR_MAX_WARNINGS =
      createCapacityVector(percentage(50), percentage(100));
  private static final String A12_MIN_VECTOR_MAX_WARNINGS =
      createCapacityVector(percentage(50), percentage(0));

  private static final Resource A_EXPECTED_MIN_RESOURCE_NO_LABEL = createResource(2048, 8);
  private static final Resource A1_EXPECTED_MIN_RESOURCE_NO_LABEL = createResource(1024, 5);
  private static final Resource A2_EXPECTED_MIN_RESOURCE_NO_LABEL = createResource(1024, 2);
  private static final Resource B_EXPECTED_MIN_RESOURCE_NO_LABEL = createResource(3072, 8);
  private static final Resource A_EXPECTED_MIN_RESOURCE_X_LABEL = createResource(30720, 30);
  private static final Resource A1_EXPECTED_MIN_RESOURCE_X_LABEL = createResource(20480, 0);
  private static final Resource A2_EXPECTED_MIN_RESOURCE_X_LABEL = createResource(10240, 30);
  private static final Resource B_EXPECTED_MIN_RESOURCE_X_LABEL = createResource(30720, 30);
  private static final Resource A_EXPECTED_MIN_RESOURCE_Y_LABEL = createResource(8096, 42);
  private static final Resource A1_EXPECTED_MIN_RESOURCE_Y_LABEL = createResource(6186, 21);
  private static final Resource A2_EXPECTED_MIN_RESOURCE_Y_LABEL = createResource(1910, 21);
  private static final Resource B_EXPECTED_MIN_RESOURCE_Y_LABEL = createResource(12384, 18);
  private static final Resource A_EXPECTED_MIN_RESOURCE_Z_LABEL = createResource(7168, 11);
  private static final Resource A1_EXPECTED_MIN_RESOURCE_Z_LABEL = createResource(6451, 4);
  private static final Resource A2_EXPECTED_MIN_RESOURCE_Z_LABEL = createResource(716, 7);
  private static final Resource B_EXPECTED_MIN_RESOURCE_Z_LABEL = createResource(3072, 4);
  private static final Resource EMPTY_LABEL_RESOURCE = Resource.newInstance(5 * GB, 16);

  private static final String A_VECTOR_NO_LABEL =
      createCapacityVector(absolute(2048), percentage(50));
  private static final String A1_VECTOR_NO_LABEL =
      createCapacityVector(absolute(1024), percentage(70));
  private static final String A2_VECTOR_NO_LABEL =
      createCapacityVector(absolute(1024), percentage(30));
  private static final String B_VECTOR_NO_LABEL =
      createCapacityVector(weight(3), percentage(50));
  private static final String A_VECTOR_X_LABEL =
      createCapacityVector(percentage(50), weight(3));
  private static final String A1_VECTOR_X_LABEL =
      createCapacityVector(absolute(20480), percentage(10));
  private static final String A2_VECTOR_X_LABEL =
      createCapacityVector(absolute(10240), absolute(30));
  private static final String B_VECTOR_X_LABEL =
      createCapacityVector(percentage(50), percentage(50));
  private static final String A_VECTOR_Y_LABEL =
      createCapacityVector(absolute(8096), weight(1));
  private static final String A1_VECTOR_Y_LABEL =
      createCapacityVector(absolute(6186), weight(3));
  private static final String A2_VECTOR_Y_LABEL =
      createCapacityVector(weight(3), weight(3));
  private static final String B_VECTOR_Y_LABEL =
      createCapacityVector(percentage(100), percentage(30));
  private static final String A_VECTOR_Z_LABEL =
      createCapacityVector(percentage(70), absolute(11));
  private static final String A1_VECTOR_Z_LABEL =
      createCapacityVector(percentage(90), percentage(40));
  private static final String A2_VECTOR_Z_LABEL =
      createCapacityVector(percentage(10), weight(4));
  private static final String B_VECTOR_Z_LABEL =
      createCapacityVector(percentage(30), absolute(4));

  private static final String A_VECTOR_NO_REMAINING_RESOURCE =
      createCapacityVector(percentage(30), weight(6));
  private static final String A11_VECTOR_NO_REMAINING_RESOURCE =
      createCapacityVector(percentage(35), percentage(25));
  private static final String A12_VECTOR_NO_REMAINING_RESOURCE =
      createCapacityVector(percentage(65), percentage(75));
  private static final String A2_VECTOR_NO_REMAINING_RESOURCE =
      createCapacityVector(weight(3), percentage(100));
  private static final String B_VECTOR_NO_REMAINING_RESOURCE =
      createCapacityVector(absolute(8095), percentage(30));
  private static final String B1_VECTOR_NO_REMAINING_RESOURCE =
      createCapacityVector(weight(5), absolute(3));
  private static final String A_VECTOR_WITH_WARNINGS =
      createCapacityVector(percentage(100), weight(6));
  private static final String A12_VECTOR_WITH_WARNING =
      createCapacityVector(percentage(100), percentage(100));
  private static final String A2_VECTOR_WITH_WARNING =
      createCapacityVector(absolute(2048), percentage(100));
  private static final String B_VECTOR_WITH_WARNING =
      createCapacityVector(absolute(8096), percentage(30));
  private static final String B1_VECTOR_WITH_WARNING =
      createCapacityVector(absolute(10256), absolute(3));

  @Override
  public void setUp() throws Exception {
    super.setUp();
    csConf.setLegacyQueueModeEnabled(false);
  }

  /**
   * Tests a complex scenario in which no warning or remaining resource is generated during the
   * update phase (except for rounding leftovers, eg. 1 memory or 1 vcores).
   *
   *          -root-
   *        /    \  \
   *       A      B  C
   *      / \     |
   *     A1  A2   B1
   *   /  \
   *  A11 A12
   *
   * @throws IOException if update is failed
   */
  @Test
  public void testComplexHierarchyWithoutRemainingResource() throws IOException {
    setupQueueHierarchyWithoutRemainingResource();

    QueueAssertionBuilder assertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .assertEffectiveMinResource(A_COMPLEX_NO_REMAINING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            A_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .withQueue(A1)
        .assertEffectiveMinResource(A1_COMPLEX_NO_REMAINING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            A1_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .withQueue(A11)
        .assertEffectiveMinResource(A11_COMPLEX_NO_REMAINING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            A11_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .withQueue(A12)
        .assertEffectiveMinResource(A12_COMPLEX_NO_REMAINING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            A12_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .withQueue(A2)
        .assertEffectiveMinResource(A2_COMPLEX_NO_REMAINING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            A2_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .withQueue(B)
        .assertEffectiveMinResource(B_COMPLEX_NO_REMAINING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            B_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .withQueue(B1)
        .assertEffectiveMinResource(B1_COMPLEX_NO_REMAINING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            B1_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .withQueue(C)
        .assertEffectiveMinResource(C_COMPLEX_NO_REMAINING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            C_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .build();

    update(assertionBuilder, UPDATE_RESOURCE);
  }

  /**
   * Tests a complex scenario in which several validation warnings are generated during the update
   * phase.
   *
   *          -root-
   *        /    \  \
   *       A      B  C
   *      / \     |
   *     A1  A2   B1
   *   /  \
   *  A11 A12
   *
   * @throws IOException if update is failed
   */
  @Test
  public void testComplexHierarchyWithWarnings() throws IOException {
    setupQueueHierarchyWithWarnings();
    QueueAssertionBuilder assertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .assertEffectiveMinResource(A_WARNING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            A_WARNING_RESOURCE, UPDATE_RESOURCE))
        .withQueue(A1)
        .assertEffectiveMinResource(A1_WARNING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            A1_WARNING_RESOURCE, UPDATE_RESOURCE))
        .withQueue(A2)
        .assertEffectiveMinResource(A2_WARNING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            A2_WARNING_RESOURCE, UPDATE_RESOURCE))
        .withQueue(A11)
        .assertEffectiveMinResource(ZERO_RESOURCE)
        .assertAbsoluteCapacity(0)
        .withQueue(A12)
        .assertEffectiveMinResource(A12_WARNING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            A12_WARNING_RESOURCE, UPDATE_RESOURCE))
        .withQueue(B)
        .assertEffectiveMinResource(B_WARNING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            B_WARNING_RESOURCE, UPDATE_RESOURCE))
        .withQueue(B1)
        .assertEffectiveMinResource(B1_WARNING_RESOURCE)
        .assertAbsoluteCapacity(resourceCalculator.divide(UPDATE_RESOURCE,
            B1_WARNING_RESOURCE, UPDATE_RESOURCE))
        .withQueue(C)
        .assertEffectiveMinResource(ZERO_RESOURCE)
        .assertAbsoluteCapacity(0)
        .build();

    QueueCapacityUpdateContext updateContext = update(assertionBuilder, UPDATE_RESOURCE);
    Optional<QueueUpdateWarning> queueCZeroResourceWarning = getSpecificWarning(
        updateContext.getUpdateWarnings(), QueueUpdateWarningType.QUEUE_ZERO_RESOURCE, C);
    Optional<QueueUpdateWarning> queueARemainingResourceWarning = getSpecificWarning(
        updateContext.getUpdateWarnings(), QueueUpdateWarningType.BRANCH_UNDERUTILIZED, A);
    Optional<QueueUpdateWarning> queueBDownscalingWarning = getSpecificWarning(
        updateContext.getUpdateWarnings(), QueueUpdateWarningType.BRANCH_DOWNSCALED, B);
    Optional<QueueUpdateWarning> queueA11ZeroResourceWarning = getSpecificWarning(
        updateContext.getUpdateWarnings(), QueueUpdateWarningType.QUEUE_ZERO_RESOURCE, A11);

    Assert.assertTrue(queueCZeroResourceWarning.isPresent());
    Assert.assertTrue(queueARemainingResourceWarning.isPresent());
    Assert.assertTrue(queueBDownscalingWarning.isPresent());
    Assert.assertTrue(queueA11ZeroResourceWarning.isPresent());
  }

  @Test
  public void testZeroResourceIfNoMemory() throws IOException {
    csConf.setCapacityVector(A, NO_LABEL, A_VECTOR_ZERO_RESOURCE);
    csConf.setCapacityVector(B, NO_LABEL, B_VECTOR_ZERO_RESOURCE);

    QueueAssertionBuilder assertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .assertEffectiveMinResource(ZERO_RESOURCE)
        .withQueue(B)
        .assertEffectiveMinResource(createResource(MEMORY, VCORES * 0.5))
        .build();

    QueueCapacityUpdateContext updateContext = update(assertionBuilder, UPDATE_RESOURCE);
    Optional<QueueUpdateWarning> queueAZeroResourceWarning = getSpecificWarning(
        updateContext.getUpdateWarnings(), QueueUpdateWarningType.QUEUE_ZERO_RESOURCE, A);
    Optional<QueueUpdateWarning> rootUnderUtilizedWarning = getSpecificWarning(
        updateContext.getUpdateWarnings(), QueueUpdateWarningType.BRANCH_UNDERUTILIZED, ROOT);
    Assert.assertTrue(queueAZeroResourceWarning.isPresent());
    Assert.assertTrue(rootUnderUtilizedWarning.isPresent());
  }

  @Test
  public void testDifferentMinimumAndMaximumCapacityTypes() throws IOException {
    csConf.setCapacityVector(A, NO_LABEL, A_MIN_VECTOR_DIFFERENT_MIN_MAX);
    csConf.setMaximumCapacityVector(A, NO_LABEL, A_MAX_VECTOR_DIFFERENT_MIN_MAX);
    csConf.setCapacityVector(B, NO_LABEL, B_MIN_VECTOR_DIFFERENT_MIN_MAX);
    csConf.setMaximumCapacityVector(B, NO_LABEL, B_MAX_VECTOR_DIFFERENT_MIN_MAX);

    QueueAssertionBuilder assertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(UPDATE_RESOURCE, 0.5d))
        .assertEffectiveMaxResource(A_EXPECTED_MAX_RESOURCE_DIFFERENT_MIN_MAX)
        .withQueue(B)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(UPDATE_RESOURCE, 0.5d))
        .assertEffectiveMaxResource(B_EXPECTED_MAX_RESOURCE_DIFFERENT_MIN_MAX)
        .build();

    QueueCapacityUpdateContext updateContext = update(assertionBuilder, UPDATE_RESOURCE);
    Assert.assertEquals(0, updateContext.getUpdateWarnings().size());

    // WEIGHT capacity type for maximum capacity is not supported
    csConf.setMaximumCapacityVector(B, NO_LABEL, B_INVALID_MAX_VECTOR);
    try {
      cs.reinitialize(csConf, mockRM.getRMContext());
      update(assertionBuilder, UPDATE_RESOURCE);
      Assert.fail("WEIGHT maximum capacity type is not supported, an error should be thrown when " +
          "set up");
    } catch (IOException ignored) {
    }
  }

  @Test
  public void testMaximumResourceWarnings() throws IOException {
    csConf.setMaximumCapacityVector(A1, NO_LABEL, A1_MAX_VECTOR_MAX_WARNINGS);
    csConf.setCapacityVector(A11, NO_LABEL, A11_MIN_VECTOR_MAX_WARNINGS);
    csConf.setCapacityVector(A12, NO_LABEL, A12_MIN_VECTOR_MAX_WARNINGS);
    csConf.setMaximumCapacityVector(A11, NO_LABEL, A11_MAX_VECTOR_MAX_WARNINGS);

    QueueAssertionBuilder assertionBuilder = createAssertionBuilder()
        .withQueue(A11)
        .assertEffectiveMinResource(A11_EXPECTED_MIN_RESOURCE_MAX_WARNINGS)
        .assertEffectiveMaxResource(A11_EXPECTED_MAX_RESOURCE_MAX_WARNINGS)
        .withQueue(A12)
        .assertEffectiveMinResource(A12_EXPECTED_MIN_RESOURCE_MAX_WARNINGS)
        .assertEffectiveMaxResource(A12_EXPECTED_MAX_RESOURCE_MAX_WARNINGS)
        .build();

    QueueCapacityUpdateContext updateContext = update(assertionBuilder, UPDATE_RESOURCE);
    Optional<QueueUpdateWarning> queueA11ExceedsParentMaxResourceWarning = getSpecificWarning(
        updateContext.getUpdateWarnings(), QueueUpdateWarningType.QUEUE_MAX_RESOURCE_EXCEEDS_PARENT,
        A11);
    Optional<QueueUpdateWarning> queueA11MinExceedsMaxWarning = getSpecificWarning(
        updateContext.getUpdateWarnings(), QueueUpdateWarningType.QUEUE_EXCEEDS_MAX_RESOURCE, A11);
    Assert.assertTrue(queueA11ExceedsParentMaxResourceWarning.isPresent());
    Assert.assertTrue(queueA11MinExceedsMaxWarning.isPresent());
  }

  @Test
  public void testNodeLabels() throws Exception {
    setLabeledQueueConfigs();

    QueueAssertionBuilder assertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .assertEffectiveMinResource(A_EXPECTED_MIN_RESOURCE_NO_LABEL, NO_LABEL)
        .withQueue(A1)
        .assertEffectiveMinResource(A1_EXPECTED_MIN_RESOURCE_NO_LABEL, NO_LABEL)
        .withQueue(A2)
        .assertEffectiveMinResource(A2_EXPECTED_MIN_RESOURCE_NO_LABEL, NO_LABEL)
        .withQueue(B)
        .assertEffectiveMinResource(B_EXPECTED_MIN_RESOURCE_NO_LABEL, NO_LABEL)
        .withQueue(A)
        .assertEffectiveMinResource(A_EXPECTED_MIN_RESOURCE_X_LABEL, X_LABEL)
        .withQueue(A1)
        .assertEffectiveMinResource(A1_EXPECTED_MIN_RESOURCE_X_LABEL, X_LABEL)
        .withQueue(A2)
        .assertEffectiveMinResource(A2_EXPECTED_MIN_RESOURCE_X_LABEL, X_LABEL)
        .withQueue(B)
        .assertEffectiveMinResource(B_EXPECTED_MIN_RESOURCE_X_LABEL, X_LABEL)
        .withQueue(A)
        .assertEffectiveMinResource(A_EXPECTED_MIN_RESOURCE_Y_LABEL, Y_LABEL)
        .withQueue(A1)
        .assertEffectiveMinResource(A1_EXPECTED_MIN_RESOURCE_Y_LABEL, Y_LABEL)
        .withQueue(A2)
        .assertEffectiveMinResource(A2_EXPECTED_MIN_RESOURCE_Y_LABEL, Y_LABEL)
        .withQueue(B)
        .assertEffectiveMinResource(B_EXPECTED_MIN_RESOURCE_Y_LABEL, Y_LABEL)
        .withQueue(A)
        .assertEffectiveMinResource(A_EXPECTED_MIN_RESOURCE_Z_LABEL, Z_LABEL)
        .withQueue(A1)
        .assertEffectiveMinResource(A1_EXPECTED_MIN_RESOURCE_Z_LABEL, Z_LABEL)
        .withQueue(A2)
        .assertEffectiveMinResource(A2_EXPECTED_MIN_RESOURCE_Z_LABEL, Z_LABEL)
        .withQueue(B)
        .assertEffectiveMinResource(B_EXPECTED_MIN_RESOURCE_Z_LABEL, Z_LABEL)
        .build();

    update(assertionBuilder, UPDATE_RESOURCE, EMPTY_LABEL_RESOURCE);
  }

  private void setLabeledQueueConfigs() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of(X_LABEL, Y_LABEL, Z_LABEL));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance(H1_NODE, 0),
        TestUtils.toSet(X_LABEL), NodeId.newInstance(H2_NODE, 0),
        TestUtils.toSet(Y_LABEL), NodeId.newInstance(H3_NODE, 0),
        TestUtils.toSet(Y_LABEL), NodeId.newInstance(H4_NODE, 0),
        TestUtils.toSet(Z_LABEL), NodeId.newInstance(H5_NODE, 0),
        RMNodeLabelsManager.EMPTY_STRING_SET));

    mockRM.registerNode("h1:1234", H1_MEMORY, H1_VCORES); // label = x
    mockRM.registerNode("h2:1234", H2_MEMORY, H2_VCORES); // label = y
    mockRM.registerNode("h3:1234", H3_MEMORY, H3_VCORES); // label = y
    mockRM.registerNode("h4:1234", H4_MEMORY, H4_VCORES); // label = z

    csConf.setCapacityVector(A, NO_LABEL, A_VECTOR_NO_LABEL);
    csConf.setCapacityVector(A1, NO_LABEL, A1_VECTOR_NO_LABEL);
    csConf.setCapacityVector(A2, NO_LABEL, A2_VECTOR_NO_LABEL);
    csConf.setCapacityVector(B, NO_LABEL, B_VECTOR_NO_LABEL);

    csConf.setCapacityVector(A, X_LABEL, A_VECTOR_X_LABEL);
    csConf.setCapacityVector(A1, X_LABEL, A1_VECTOR_X_LABEL);
    csConf.setCapacityVector(A2, X_LABEL, A2_VECTOR_X_LABEL);
    csConf.setCapacityVector(B, X_LABEL, B_VECTOR_X_LABEL);

    csConf.setCapacityVector(A, Y_LABEL, A_VECTOR_Y_LABEL);
    csConf.setCapacityVector(A1, Y_LABEL, A1_VECTOR_Y_LABEL);
    csConf.setCapacityVector(A2, Y_LABEL, A2_VECTOR_Y_LABEL);
    csConf.setCapacityVector(B, Y_LABEL, B_VECTOR_Y_LABEL);

    csConf.setCapacityVector(A, Z_LABEL, A_VECTOR_Z_LABEL);
    csConf.setCapacityVector(A1, Z_LABEL, A1_VECTOR_Z_LABEL);
    csConf.setCapacityVector(A2, Z_LABEL, A2_VECTOR_Z_LABEL);
    csConf.setCapacityVector(B, Z_LABEL, B_VECTOR_Z_LABEL);

    cs.reinitialize(csConf, mockRM.getRMContext());
  }

  private void setupQueueHierarchyWithoutRemainingResource() throws IOException {
    csConf.setState(B, QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());
    setQueues();

    csConf.setState(B, QueueState.RUNNING);
    csConf.setCapacityVector(A, NO_LABEL, A_VECTOR_NO_REMAINING_RESOURCE);
    csConf.setCapacityVector(A1, NO_LABEL, A1_VECTOR_NO_REMAINING_RESOURCE);
    csConf.setCapacityVector(A11, NO_LABEL, A11_VECTOR_NO_REMAINING_RESOURCE);
    csConf.setCapacityVector(A12, NO_LABEL, A12_VECTOR_NO_REMAINING_RESOURCE);
    csConf.setCapacityVector(A2, NO_LABEL, A2_VECTOR_NO_REMAINING_RESOURCE);
    csConf.setCapacityVector(B, NO_LABEL, B_VECTOR_NO_REMAINING_RESOURCE);
    csConf.setCapacityVector(B1, NO_LABEL, B1_VECTOR_NO_REMAINING_RESOURCE);
    csConf.setCapacityVector(C, NO_LABEL, C_VECTOR_NO_REMAINING_RESOURCE);

    cs.reinitialize(csConf, mockRM.getRMContext());
  }

  private void setupQueueHierarchyWithWarnings() throws IOException {
    csConf.setState(B, QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());
    setQueues();

    csConf.setState(B, QueueState.RUNNING);
    csConf.setCapacityVector(A, NO_LABEL, A_VECTOR_WITH_WARNINGS);
    csConf.setCapacityVector(A1, NO_LABEL, A1_VECTOR_WITH_WARNING);
    csConf.setCapacityVector(A11, NO_LABEL, A11_VECTOR_WITH_WARNING);
    csConf.setCapacityVector(A12, NO_LABEL, A12_VECTOR_WITH_WARNING);
    csConf.setCapacityVector(A2, NO_LABEL, A2_VECTOR_WITH_WARNING);
    csConf.setCapacityVector(B, NO_LABEL, B_VECTOR_WITH_WARNING);
    csConf.setCapacityVector(B1, NO_LABEL, B1_VECTOR_WITH_WARNING);
    csConf.setCapacityVector(C, NO_LABEL, C_VECTOR_WITH_WARNING);

    cs.reinitialize(csConf, mockRM.getRMContext());
  }

  private void setQueues() {
    csConf.setQueues("root", new String[]{"a", "b", "c"});
    csConf.setQueues(A, new String[]{"a1", "a2"});
    csConf.setQueues(B, new String[]{"b1"});
  }

  private Optional<QueueUpdateWarning> getSpecificWarning(
      Collection<QueueUpdateWarning> warnings, QueueUpdateWarningType warningTypeToSelect,
      String queue) {
    return warnings.stream().filter((w) -> w.getWarningType().equals(warningTypeToSelect)
        && w.getQueue().equals(queue)).findFirst();
  }
}
