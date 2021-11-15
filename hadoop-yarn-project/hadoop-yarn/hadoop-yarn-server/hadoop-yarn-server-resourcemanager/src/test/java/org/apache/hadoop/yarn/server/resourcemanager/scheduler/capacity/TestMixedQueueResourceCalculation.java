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
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning.QueueUpdateWarningType;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ROOT;

public class TestMixedQueueResourceCalculation extends CapacitySchedulerQueueCalculationTestBase {
  private static final long MEMORY = 16384;
  private static final long VCORES = 16;

  private static final Resource UPDATE_RESOURCE = Resource.newInstance(16384, 16);
  private static final Resource ZERO_RESOURCE = Resource.newInstance(0, 0);

  public static final Resource A_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(2486, 9);
  public static final Resource A1_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(621, 4);
  public static final Resource A11_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(217, 1);
  public static final Resource A12_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(403, 3);
  public static final Resource A2_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(1865, 5);
  public static final Resource B_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(8095, 3);
  public static final Resource B1_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(8095, 3);
  public static final Resource C_COMPLEX_NO_REMAINING_RESOURCE = Resource.newInstance(5803, 4);

  public static final Resource B_WARNING_RESOURCE = Resource.newInstance(8096, 3);
  public static final Resource B1_WARNING_RESOURCE = Resource.newInstance(8096, 3);
  public static final Resource A_WARNING_RESOURCE = Resource.newInstance(8288, 9);
  public static final Resource A1_WARNING_RESOURCE = Resource.newInstance(2048, 4);
  public static final Resource A2_WARNING_RESOURCE = Resource.newInstance(2048, 5);
  public static final Resource A12_WARNING_RESOURCE = Resource.newInstance(2048, 4);

  @Override
  public void setUp() throws Exception {
    super.setUp();
    csConf.setLegacyQueueModeEnabled(false);
  }

  @Test
  public void testComplexHierarchyWithoutRemainingResource() throws IOException {
    setupQueueHierarchyWithoutRemainingResource();

    QueueAssertionBuilder assertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .toExpect(A_COMPLEX_NO_REMAINING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, A_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .withQueue(A1)
        .toExpect(A1_COMPLEX_NO_REMAINING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, A1_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .withQueue(A11)
        .toExpect(A11_COMPLEX_NO_REMAINING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, A11_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .withQueue(A12)
        .toExpect(A12_COMPLEX_NO_REMAINING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, A12_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .withQueue(A2)
        .toExpect(A2_COMPLEX_NO_REMAINING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, A2_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .withQueue(B)
        .toExpect(B_COMPLEX_NO_REMAINING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, B_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .withQueue(B1)
        .toExpect(B1_COMPLEX_NO_REMAINING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, B1_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .withQueue(C)
        .toExpect(C_COMPLEX_NO_REMAINING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, C_COMPLEX_NO_REMAINING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .build();

    update(assertionBuilder, UPDATE_RESOURCE);
  }

  @Test
  public void testComplexHierarchyWithWarnings() throws IOException {
    setupQueueHierarchyWithWarnings();
    QueueAssertionBuilder assertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .toExpect(A_WARNING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, A_WARNING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .withQueue(A1)
        .toExpect(A1_WARNING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, A1_WARNING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .withQueue(A2)
        .toExpect(A2_WARNING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, A2_WARNING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .withQueue(A11)
        .toExpect(ZERO_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(0)
        .assertAbsoluteCapacity()
        .withQueue(A12)
        .toExpect(A12_WARNING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, A12_WARNING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .withQueue(B)
        .toExpect(B_WARNING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, B_WARNING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .withQueue(B1)
        .toExpect(B1_WARNING_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(resourceCalculator.divide(UPDATE_RESOURCE, B1_WARNING_RESOURCE, UPDATE_RESOURCE))
        .assertAbsoluteCapacity()
        .withQueue(C)
        .toExpect(ZERO_RESOURCE)
        .assertEffectiveMinResource()
        .toExpect(0)
        .assertAbsoluteCapacity()
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
    Optional<QueueUpdateWarning> queueA12ZeroResourceWarning = getSpecificWarning(
        updateContext.getUpdateWarnings(), QueueUpdateWarningType.QUEUE_ZERO_RESOURCE, A12);

    Assert.assertTrue(queueCZeroResourceWarning.isPresent());
    Assert.assertTrue(queueARemainingResourceWarning.isPresent());
    Assert.assertTrue(queueBDownscalingWarning.isPresent());
    Assert.assertTrue(queueA11ZeroResourceWarning.isPresent());
    Assert.assertTrue(queueA12ZeroResourceWarning.isPresent());
  }

  @Test
  public void testZeroResourceIfNoMemory() throws IOException {
    csConf.setCapacityVector(A, "", createMemoryVcoresVector(percentage(100), weight(6)));
    csConf.setCapacityVector(B, "", createMemoryVcoresVector(absolute(MEMORY), absolute(VCORES * 0.5)));

    QueueAssertionBuilder assertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .toExpect(ZERO_RESOURCE)
        .assertEffectiveMinResource()
        .withQueue(B)
        .toExpect(createResource(MEMORY, VCORES * 0.5))
        .assertEffectiveMinResource()
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
    csConf.setCapacityVector(A, "", createMemoryVcoresVector(percentage(50), absolute(VCORES * 0.5)));
    csConf.setMaximumCapacityVector(A, "", createMemoryVcoresVector(absolute(MEMORY), percentage(80)));
    csConf.setCapacityVector(B, "", createMemoryVcoresVector(weight(6), percentage(100)));
    csConf.setMaximumCapacityVector(B, "", createMemoryVcoresVector(absolute(MEMORY), absolute(VCORES * 0.5)));

    QueueAssertionBuilder assertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .toExpect(ResourceUtils.multiply(UPDATE_RESOURCE, 0.5f))
        .assertEffectiveMinResource()
        .toExpect(Resource.newInstance(MEMORY, (int) (VCORES * 0.8)))
        .assertEffectiveMaxResource()
        .withQueue(B)
        .toExpect(ResourceUtils.multiply(UPDATE_RESOURCE, 0.5f))
        .assertEffectiveMinResource()
        .toExpect(Resource.newInstance(MEMORY, (int) (VCORES * 0.5)))
        .assertEffectiveMaxResource()
        .build();

    QueueCapacityUpdateContext updateContext = update(assertionBuilder, UPDATE_RESOURCE);
    Assert.assertEquals(0, updateContext.getUpdateWarnings().size());

    // WEIGHT capacity type for maximum capacity is not supported
    csConf.setMaximumCapacityVector(B, "", createMemoryVcoresVector(absolute(MEMORY), weight(10)));
    try {
      cs.reinitialize(csConf, mockRM.getRMContext());
      update(assertionBuilder, UPDATE_RESOURCE);
      Assert.fail("WEIGHT maximum capacity type is not supported, an error should be thrown when set up");
    } catch (IllegalStateException ignored) {
    }
  }

  @Test
  public void testMaximumResourceWarnings() throws IOException {
    csConf.setMaximumCapacityVector(A1, "", createMemoryVcoresVector(absolute(MEMORY * 0.5), percentage(100)));
    csConf.setCapacityVector(A11, "", createMemoryVcoresVector(percentage(50), percentage(100)));
    csConf.setCapacityVector(A12, "", createMemoryVcoresVector(percentage(50), percentage(0)));
    csConf.setMaximumCapacityVector(A11, "", createMemoryVcoresVector(absolute(MEMORY), percentage(10)));

    QueueAssertionBuilder assertionBuilder = createAssertionBuilder()
        .withQueue(A11)
        .toExpect(createResource(0.5 * 0.5 * MEMORY, 0.1 * VCORES))
        .assertEffectiveMinResource()
        .toExpect(createResource(MEMORY * 0.5, 0.1 * VCORES))
        .assertEffectiveMaxResource()
        .withQueue(A12)
        .toExpect(createResource(0.5 * 0.5 * MEMORY, 0))
        .assertEffectiveMinResource()
        .toExpect(createResource(MEMORY * 0.5, VCORES))
        .assertEffectiveMaxResource()
        .build();

    QueueCapacityUpdateContext updateContext = update(assertionBuilder, UPDATE_RESOURCE);
    Optional<QueueUpdateWarning> queueA11ExceedsParentMaxResourceWarning = getSpecificWarning(
        updateContext.getUpdateWarnings(), QueueUpdateWarningType.QUEUE_MAX_RESOURCE_EXCEEDS_PARENT, A11);
    Optional<QueueUpdateWarning> queueA11MinExceedsMaxWarning = getSpecificWarning(
        updateContext.getUpdateWarnings(), QueueUpdateWarningType.QUEUE_EXCEEDS_MAX_RESOURCE, A11);
    Assert.assertTrue(queueA11ExceedsParentMaxResourceWarning.isPresent());
    Assert.assertTrue(queueA11MinExceedsMaxWarning.isPresent());
  }

  private void setupQueueHierarchyWithoutRemainingResource() throws IOException {
    csConf.setState(B, QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());
    setQueues();

    csConf.setState(B, QueueState.RUNNING);
    csConf.setCapacityVector(A, "", createMemoryVcoresVector(percentage(30), weight(6)));
    csConf.setCapacityVector(A1, "", createMemoryVcoresVector(weight(1), absolute(VCORES * 0.25)));
    csConf.setCapacityVector(A11, "", createMemoryVcoresVector(percentage(35), percentage(25)));
    csConf.setCapacityVector(A12, "", createMemoryVcoresVector(percentage(65), percentage(75)));
    csConf.setCapacityVector(A2, "", createMemoryVcoresVector(weight(3), percentage(100)));
    csConf.setCapacityVector(B, "", createMemoryVcoresVector(absolute(8095), percentage(30)));
    csConf.setCapacityVector(B1, "", createMemoryVcoresVector(weight(5), absolute(3)));
    csConf.setCapacityVector(C, "", createMemoryVcoresVector(weight(3), absolute(VCORES * 0.25)));

    cs.reinitialize(csConf, mockRM.getRMContext());
  }

  private void setupQueueHierarchyWithWarnings() throws IOException {
    csConf.setState(B, QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());
    setQueues();

    Resource.newInstance(0, 0); // C
    Resource.newInstance(0, 0); // A12

    csConf.setState(B, QueueState.RUNNING);
    csConf.setCapacityVector(A, "", createMemoryVcoresVector(percentage(100), weight(6)));
    csConf.setCapacityVector(A1, "", createMemoryVcoresVector(absolute(2048), absolute(VCORES * 0.25)));
    csConf.setCapacityVector(A11, "", createMemoryVcoresVector(weight(1), absolute(VCORES * 0.25)));
    csConf.setCapacityVector(A12, "", createMemoryVcoresVector(percentage(100), percentage(100)));
    csConf.setCapacityVector(A2, "", createMemoryVcoresVector(absolute(2048), percentage(100)));
    csConf.setCapacityVector(B, "", createMemoryVcoresVector(absolute(8096), percentage(30)));
    csConf.setCapacityVector(B1, "", createMemoryVcoresVector(absolute(10256), absolute(3)));
    csConf.setCapacityVector(C, "", createMemoryVcoresVector(weight(3), absolute(VCORES * 0.25)));

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
    return warnings.stream().filter((w) -> w.getWarningType().equals(warningTypeToSelect) && w.getQueue().equals(
        queue)).findFirst();
  }
}
