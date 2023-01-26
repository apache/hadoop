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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.GB;

public class TestUniformQueueResourceCalculation extends CapacitySchedulerQueueCalculationTestBase {

  private static final Resource QUEUE_A_RES = Resource.newInstance(80 * GB,
      10);
  private static final Resource QUEUE_B_RES = Resource.newInstance(170 * GB,
      30);
  private static final Resource QUEUE_A1_RES = Resource.newInstance(50 * GB,
      4);
  private static final Resource QUEUE_A2_RES = Resource.newInstance(30 * GB,
      6);
  private static final Resource QUEUE_A11_RES = Resource.newInstance(40 * GB,
      2);
  private static final Resource QUEUE_A12_RES = Resource.newInstance(10 * GB,
      2);
  private static final Resource UPDATE_RES = Resource.newInstance(250 * GB, 40);
  private static final Resource PERCENTAGE_ALL_RES = Resource.newInstance(10 * GB, 20);

  public static final double A_CAPACITY = 0.3;
  public static final double B_CAPACITY = 0.7;
  public static final double A1_CAPACITY = 0.17;
  public static final double A11_CAPACITY = 0.25;
  public static final double A12_CAPACITY = 0.75;
  public static final double A2_CAPACITY = 0.83;

  public static final float A_WEIGHT = 3;
  public static final float B_WEIGHT = 6;
  public static final float A1_WEIGHT = 2;
  public static final float A11_WEIGHT = 5;
  public static final float A12_WEIGHT = 8;
  public static final float A2_WEIGHT = 3;

  public static final double A_NORMALIZED_WEIGHT = A_WEIGHT / (A_WEIGHT + B_WEIGHT);
  public static final double B_NORMALIZED_WEIGHT = B_WEIGHT / (A_WEIGHT + B_WEIGHT);
  public static final double A1_NORMALIZED_WEIGHT = A1_WEIGHT / (A1_WEIGHT + A2_WEIGHT);
  public static final double A2_NORMALIZED_WEIGHT = A2_WEIGHT / (A1_WEIGHT + A2_WEIGHT);
  public static final double A11_NORMALIZED_WEIGHT = A11_WEIGHT / (A11_WEIGHT + A12_WEIGHT);
  public static final double A12_NORMALIZED_WEIGHT = A12_WEIGHT / (A11_WEIGHT + A12_WEIGHT);

  @Test
  public void testWeightResourceCalculation() throws IOException {
    csConf.setNonLabeledQueueWeight(A, A_WEIGHT);
    csConf.setNonLabeledQueueWeight(B, B_WEIGHT);
    csConf.setNonLabeledQueueWeight(A1, A1_WEIGHT);
    csConf.setNonLabeledQueueWeight(A11, A11_WEIGHT);
    csConf.setNonLabeledQueueWeight(A12, A12_WEIGHT);
    csConf.setNonLabeledQueueWeight(A2, A2_WEIGHT);

    QueueAssertionBuilder queueAssertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .assertEffectiveMinResource(ResourceUtils.multiplyRound(UPDATE_RES, A_NORMALIZED_WEIGHT))
        .assertAbsoluteCapacity(A_NORMALIZED_WEIGHT)
        .withQueue(B)
        .assertEffectiveMinResource(ResourceUtils.multiplyRound(UPDATE_RES, B_NORMALIZED_WEIGHT))
        .assertAbsoluteCapacity(B_NORMALIZED_WEIGHT)
        .withQueue(A1)
        .assertEffectiveMinResource(ResourceUtils.multiplyRound(UPDATE_RES,
            A_NORMALIZED_WEIGHT * A1_NORMALIZED_WEIGHT))
        .assertAbsoluteCapacity(A_NORMALIZED_WEIGHT * A1_NORMALIZED_WEIGHT)
        .withQueue(A2)
        .assertEffectiveMinResource(ResourceUtils.multiplyRound(UPDATE_RES,
            A_NORMALIZED_WEIGHT * A2_NORMALIZED_WEIGHT))
        .assertAbsoluteCapacity(A_NORMALIZED_WEIGHT * A2_NORMALIZED_WEIGHT)
        .withQueue(A11)
        .assertEffectiveMinResource(ResourceUtils.multiplyRound(UPDATE_RES,
            A_NORMALIZED_WEIGHT * A1_NORMALIZED_WEIGHT * A11_NORMALIZED_WEIGHT))
        .assertAbsoluteCapacity(A_NORMALIZED_WEIGHT * A1_NORMALIZED_WEIGHT * A11_NORMALIZED_WEIGHT)
        .withQueue(A12)
        .assertEffectiveMinResource(ResourceUtils.multiplyRound(UPDATE_RES,
            A_NORMALIZED_WEIGHT * A1_NORMALIZED_WEIGHT * A12_NORMALIZED_WEIGHT))
        .assertAbsoluteCapacity(A_NORMALIZED_WEIGHT * A1_NORMALIZED_WEIGHT * A12_NORMALIZED_WEIGHT)
        .build();

    update(queueAssertionBuilder, UPDATE_RES);
  }

  @Test
  public void testPercentageResourceCalculation() throws IOException {
    csConf.setCapacity(A, (float) (A_CAPACITY * 100));
    csConf.setCapacity(B, (float) (B_CAPACITY * 100));
    csConf.setCapacity(A1, (float) (A1_CAPACITY * 100));
    csConf.setCapacity(A11, (float) (A11_CAPACITY * 100));
    csConf.setCapacity(A12, (float) (A12_CAPACITY * 100));
    csConf.setCapacity(A2, (float) (A2_CAPACITY * 100));

    QueueAssertionBuilder queueAssertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(PERCENTAGE_ALL_RES, A_CAPACITY))
        .assertCapacity(A_CAPACITY)
        .assertAbsoluteCapacity(A_CAPACITY)
        .withQueue(B)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(PERCENTAGE_ALL_RES, B_CAPACITY))
        .assertCapacity(B_CAPACITY)
        .assertAbsoluteCapacity(B_CAPACITY)
        .withQueue(A1)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(PERCENTAGE_ALL_RES,
            A_CAPACITY * A1_CAPACITY))
        .assertCapacity(A1_CAPACITY)
        .assertAbsoluteCapacity(A_CAPACITY * A1_CAPACITY)
        .withQueue(A2)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(PERCENTAGE_ALL_RES,
            A_CAPACITY * A2_CAPACITY))
        .assertCapacity(A2_CAPACITY)
        .assertAbsoluteCapacity(A_CAPACITY * A2_CAPACITY)
        .withQueue(A11)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(PERCENTAGE_ALL_RES,
            A11_CAPACITY * A_CAPACITY * A1_CAPACITY))
        .assertCapacity(A11_CAPACITY)
        .assertAbsoluteCapacity(A11_CAPACITY * A_CAPACITY * A1_CAPACITY)
        .withQueue(A12)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(PERCENTAGE_ALL_RES,
            A12_CAPACITY * A_CAPACITY * A1_CAPACITY))
        .assertCapacity(A12_CAPACITY)
        .assertAbsoluteCapacity(A12_CAPACITY * A_CAPACITY * A1_CAPACITY)
        .build();

    update(queueAssertionBuilder, PERCENTAGE_ALL_RES);
  }

  @Test
  public void testAbsoluteResourceCalculation() throws IOException {
    csConf.setMinimumResourceRequirement("", new QueuePath(A), QUEUE_A_RES);
    csConf.setMinimumResourceRequirement("", new QueuePath(B), QUEUE_B_RES);
    csConf.setMinimumResourceRequirement("", new QueuePath(A1), QUEUE_A1_RES);
    csConf.setMinimumResourceRequirement("", new QueuePath(A2), QUEUE_A2_RES);
    csConf.setMinimumResourceRequirement("", new QueuePath(A11), QUEUE_A11_RES);
    csConf.setMinimumResourceRequirement("", new QueuePath(A12), QUEUE_A12_RES);

    QueueAssertionBuilder queueAssertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .assertEffectiveMinResource(QUEUE_A_RES)
        .withQueue(B)
        .assertEffectiveMinResource(QUEUE_B_RES)
        .withQueue(A1)
        .assertEffectiveMinResource(QUEUE_A1_RES)
        .withQueue(A2)
        .assertEffectiveMinResource(QUEUE_A2_RES)
        .withQueue(A11)
        .assertEffectiveMinResource(QUEUE_A11_RES)
        .withQueue(A12)
        .assertEffectiveMinResource(QUEUE_A12_RES)
        .build();

    update(queueAssertionBuilder, UPDATE_RES);

    QueueAssertionBuilder queueAssertionHalfClusterResource = createAssertionBuilder()
        .withQueue(A)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(QUEUE_A_RES, 0.5f))
        .withQueue(B)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(QUEUE_B_RES, 0.5f))
        .withQueue(A1)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(QUEUE_A1_RES, 0.5f))
        .withQueue(A2)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(QUEUE_A2_RES, 0.5f))
        .withQueue(A11)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(QUEUE_A11_RES, 0.5f))
        .withQueue(A12)
        .assertEffectiveMinResource(ResourceUtils.multiplyFloor(QUEUE_A12_RES, 0.5f))
        .build();

    update(queueAssertionHalfClusterResource, ResourceUtils.multiplyFloor(UPDATE_RES, 0.5f));
  }

}