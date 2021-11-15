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
  private static final Resource QUEUE_B_RES = Resource.newInstance( 170 * GB,
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

  public static final float A_CAPACITY = 0.3f;
  public static final float B_CAPACITY = 0.7f;
  public static final float A1_CAPACITY = 0.17f;
  public static final float A11_CAPACITY = 0.25f;
  public static final float A12_CAPACITY = 0.75f;
  public static final float A2_CAPACITY = 0.83f;

  public static final float A_WEIGHT = 3;
  public static final float B_WEIGHT = 6;
  public static final float A1_WEIGHT = 2;
  public static final float A11_WEIGHT = 5;
  public static final float A12_WEIGHT = 8;
  public static final float A2_WEIGHT = 3;

  public static final float A_NORMALIZED_WEIGHT = A_WEIGHT / (A_WEIGHT + B_WEIGHT);
  public static final float B_NORMALIZED_WEIGHT = B_WEIGHT / (A_WEIGHT + B_WEIGHT);
  public static final float A1_NORMALIZED_WEIGHT = A1_WEIGHT / (A1_WEIGHT + A2_WEIGHT);
  public static final float A2_NORMALIZED_WEIGHT = A2_WEIGHT / (A1_WEIGHT + A2_WEIGHT);
  public static final float A11_NORMALIZED_WEIGHT = A11_WEIGHT / (A11_WEIGHT + A12_WEIGHT);
  public static final float A12_NORMALIZED_WEIGHT = A12_WEIGHT / (A11_WEIGHT + A12_WEIGHT);

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
        .toExpect(ResourceUtils.multiplyRound(UPDATE_RES, A_NORMALIZED_WEIGHT))
        .assertEffectiveMinResource()
        .toExpect(A_NORMALIZED_WEIGHT)
        .assertAbsoluteCapacity()
        .withQueue(B)
        .toExpect(ResourceUtils.multiplyRound(UPDATE_RES, B_NORMALIZED_WEIGHT))
        .assertEffectiveMinResource()
        .toExpect(B_NORMALIZED_WEIGHT)
        .assertAbsoluteCapacity()
        .withQueue(A1)
        .toExpect(ResourceUtils.multiplyRound(UPDATE_RES, A_NORMALIZED_WEIGHT * A1_NORMALIZED_WEIGHT))
        .assertEffectiveMinResource()
        .toExpect(A_NORMALIZED_WEIGHT * A1_NORMALIZED_WEIGHT)
        .assertAbsoluteCapacity()
        .withQueue(A2)
        .toExpect(ResourceUtils.multiplyRound(UPDATE_RES, A_NORMALIZED_WEIGHT * A2_NORMALIZED_WEIGHT))
        .assertEffectiveMinResource()
        .toExpect(A_NORMALIZED_WEIGHT * A2_NORMALIZED_WEIGHT)
        .assertAbsoluteCapacity()
        .withQueue(A11)
        .toExpect(ResourceUtils.multiplyRound(UPDATE_RES, A_NORMALIZED_WEIGHT * A1_NORMALIZED_WEIGHT * A11_NORMALIZED_WEIGHT))
        .assertEffectiveMinResource()
        .toExpect(A_NORMALIZED_WEIGHT * A1_NORMALIZED_WEIGHT * A11_NORMALIZED_WEIGHT)
        .assertAbsoluteCapacity()
        .withQueue(A12)
        .toExpect(ResourceUtils.multiplyRound(UPDATE_RES, A_NORMALIZED_WEIGHT * A1_NORMALIZED_WEIGHT * A12_NORMALIZED_WEIGHT))
        .assertEffectiveMinResource()
        .toExpect(A_NORMALIZED_WEIGHT * A1_NORMALIZED_WEIGHT * A12_NORMALIZED_WEIGHT)
        .assertAbsoluteCapacity()
        .build();

    update(queueAssertionBuilder, UPDATE_RES);
  }

  @Test
  public void testPercentageResourceCalculation() throws IOException {
    csConf.setCapacity(A, A_CAPACITY * 100);
    csConf.setCapacity(B, B_CAPACITY * 100);
    csConf.setCapacity(A1, A1_CAPACITY * 100);
    csConf.setCapacity(A11, A11_CAPACITY * 100);
    csConf.setCapacity(A12, A12_CAPACITY * 100);
    csConf.setCapacity(A2, A2_CAPACITY * 100);

    QueueAssertionBuilder queueAssertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .toExpect(ResourceUtils.multiply(PERCENTAGE_ALL_RES, A_CAPACITY))
        .assertEffectiveMinResource()
        .toExpect(A_CAPACITY)
        .assertCapacity()
        .toExpect(A_CAPACITY)
        .assertAbsoluteCapacity()
        .withQueue(B)
        .toExpect(ResourceUtils.multiply(PERCENTAGE_ALL_RES, B_CAPACITY))
        .assertEffectiveMinResource()
        .toExpect(B_CAPACITY)
        .assertCapacity()
        .toExpect(B_CAPACITY)
        .assertAbsoluteCapacity()
        .withQueue(A1)
        .toExpect(ResourceUtils.multiply(PERCENTAGE_ALL_RES, A_CAPACITY * A1_CAPACITY))
        .assertEffectiveMinResource()
        .toExpect(A1_CAPACITY)
        .assertCapacity()
        .toExpect(A_CAPACITY * A1_CAPACITY)
        .assertAbsoluteCapacity()
        .withQueue(A2)
        .toExpect(ResourceUtils.multiply(PERCENTAGE_ALL_RES, A_CAPACITY * A2_CAPACITY))
        .assertEffectiveMinResource()
        .toExpect(A2_CAPACITY)
        .assertCapacity()
        .toExpect(A_CAPACITY * A2_CAPACITY)
        .assertAbsoluteCapacity()
        .withQueue(A11)
        .toExpect(ResourceUtils.multiply(PERCENTAGE_ALL_RES, A11_CAPACITY * A_CAPACITY * A1_CAPACITY))
        .assertEffectiveMinResource()
        .toExpect(A11_CAPACITY)
        .assertCapacity()
        .toExpect(A11_CAPACITY * A_CAPACITY * A1_CAPACITY)
        .assertAbsoluteCapacity()
        .withQueue(A12)
        .toExpect(ResourceUtils.multiply(PERCENTAGE_ALL_RES, A12_CAPACITY * A_CAPACITY * A1_CAPACITY))
        .assertEffectiveMinResource()
        .toExpect(A12_CAPACITY)
        .assertCapacity()
        .toExpect(A12_CAPACITY * A_CAPACITY * A1_CAPACITY)
        .assertAbsoluteCapacity()
        .build();

    update(queueAssertionBuilder, PERCENTAGE_ALL_RES);
  }

  @Test
  public void testAbsoluteResourceCalculation() throws IOException {
    csConf.setMinimumResourceRequirement("", A, QUEUE_A_RES);
    csConf.setMinimumResourceRequirement("", B, QUEUE_B_RES);
    csConf.setMinimumResourceRequirement("", A1, QUEUE_A1_RES);
    csConf.setMinimumResourceRequirement("", A2, QUEUE_A2_RES);
    csConf.setMinimumResourceRequirement("", A11, QUEUE_A11_RES);
    csConf.setMinimumResourceRequirement("", A12, QUEUE_A12_RES);

    QueueAssertionBuilder queueAssertionBuilder = createAssertionBuilder()
        .withQueue(A)
        .toExpect(QUEUE_A_RES)
        .assertEffectiveMinResource()
        .withQueue(B)
        .toExpect(QUEUE_B_RES)
        .assertEffectiveMinResource()
        .withQueue(A1)
        .toExpect(QUEUE_A1_RES)
        .assertEffectiveMinResource()
        .withQueue(A2)
        .toExpect(QUEUE_A2_RES)
        .assertEffectiveMinResource()
        .withQueue(A11)
        .toExpect(QUEUE_A11_RES)
        .assertEffectiveMinResource()
        .withQueue(A12)
        .toExpect(QUEUE_A12_RES)
        .assertEffectiveMinResource()
        .build();

    update(queueAssertionBuilder, UPDATE_RES);

    QueueAssertionBuilder queueAssertionHalfClusterResource = createAssertionBuilder()
        .withQueue(A)
        .toExpect(ResourceUtils.multiply(QUEUE_A_RES, 0.5f))
        .assertEffectiveMinResource()
        .withQueue(B)
        .toExpect(ResourceUtils.multiply(QUEUE_B_RES, 0.5f))
        .assertEffectiveMinResource()
        .withQueue(A1)
        .toExpect(ResourceUtils.multiply(QUEUE_A1_RES, 0.5f))
        .assertEffectiveMinResource()
        .withQueue(A2)
        .toExpect(ResourceUtils.multiply(QUEUE_A2_RES, 0.5f))
        .assertEffectiveMinResource()
        .withQueue(A11)
        .toExpect(ResourceUtils.multiply(QUEUE_A11_RES, 0.5f))
        .assertEffectiveMinResource()
        .withQueue(A12)
        .toExpect(ResourceUtils.multiply(QUEUE_A12_RES, 0.5f))
        .assertEffectiveMinResource()
        .build();

    update(queueAssertionHalfClusterResource, ResourceUtils.multiply(UPDATE_RES, 0.5f));
  }

}