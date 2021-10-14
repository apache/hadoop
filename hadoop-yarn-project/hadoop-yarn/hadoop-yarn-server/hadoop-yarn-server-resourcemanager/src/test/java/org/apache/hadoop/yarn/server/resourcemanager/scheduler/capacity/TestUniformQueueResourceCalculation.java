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
      15);
  private static final Resource QUEUE_B_RES = Resource.newInstance( 170 * GB,
      25);
  private static final Resource QUEUE_A1_RES = Resource.newInstance(50 * GB,
      5);
  private static final Resource QUEUE_A2_RES = Resource.newInstance(30 * GB,
      5);
  private static final Resource UPDATE_RES = Resource.newInstance(250 * GB, 40);
  private static final Resource PERCENTAGE_ALL_RES = Resource.newInstance(10 * GB, 20);
  private static final Resource WEIGHT_ALL_RES = Resource.newInstance(10 * GB, 20);

  @Test
  public void testWeightResourceCalculation() throws IOException {
//    CapacitySchedulerQueueCapacityHandler queueController =
//        new CapacitySchedulerQueueCapacityHandler(mgr);
//    update(WEIGHT_ALL_RES);
//    queueController.update(WEIGHT_ALL_RES, cs.getQueue("root.a"));
//    CSQueue a = cs.getQueue("root.a");
//
//    Assert.assertEquals(6 * GB, a.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize(), 1e-6);
  }

  @Test
  public void testPercentageResourceCalculation() throws IOException {
    csConf.setCapacity("root.a", 30);
    csConf.setCapacity("root.b", 70);
    csConf.setCapacity("root.a.a1", 17);
    csConf.setCapacity("root.a.a2", 83);

    QueueAssertionBuilder queueAssertionBuilder = createAssertionBuilder()
        .withQueue("root.a")
        .toExpect(ResourceUtils.multiply(PERCENTAGE_ALL_RES, 0.3f))
        .assertEffectiveMinResource()
        .toExpect(0.3f)
        .assertCapacity()
        .toExpect(0.3f)
        .assertAbsoluteCapacity()

        .withQueue("root.b")
        .toExpect(ResourceUtils.multiply(PERCENTAGE_ALL_RES, 0.7f))
        .assertEffectiveMinResource()
        .toExpect(0.7f)
        .assertCapacity()
        .toExpect(0.7f)
        .assertAbsoluteCapacity()

        .withQueue("root.a.a1")
        .toExpect(ResourceUtils.multiply(PERCENTAGE_ALL_RES, 0.3f * 0.17f))
        .assertEffectiveMinResource()
        .toExpect(0.17f)
        .assertCapacity()
        .toExpect(0.3f * 0.17f)
        .assertAbsoluteCapacity()

        .withQueue("root.a.a2")
        .toExpect(ResourceUtils.multiply(PERCENTAGE_ALL_RES, 0.3f * 0.83f))
        .assertEffectiveMinResource()
        .toExpect(0.83f)
        .assertCapacity()
        .toExpect(0.3f * 0.83f)
        .assertAbsoluteCapacity()
        .build();

    update(queueAssertionBuilder, PERCENTAGE_ALL_RES);
  }

  @Test
  public void testAbsoluteResourceCalculation() throws IOException {
    csConf.setMinimumResourceRequirement("", "root.a", QUEUE_A_RES);
    csConf.setMinimumResourceRequirement("", "root.b", QUEUE_B_RES);
    csConf.setMinimumResourceRequirement("", "root.a.a1", QUEUE_A1_RES);
    csConf.setMinimumResourceRequirement("", "root.a.a2", QUEUE_A2_RES);

    QueueAssertionBuilder queueAssertionBuilder = createAssertionBuilder()
        .withQueue("root.a")
        .toExpect(QUEUE_A_RES)
        .assertEffectiveMinResource()

        .withQueue("root.b")
        .toExpect(QUEUE_B_RES)
        .assertEffectiveMinResource()

        .withQueue("root.a.a1")
        .toExpect(QUEUE_A1_RES)
        .assertEffectiveMinResource()

        .withQueue("root.a.a2")
        .toExpect(QUEUE_A2_RES)
        .assertEffectiveMinResource()
        .build();

    update(queueAssertionBuilder, UPDATE_RES);

    QueueAssertionBuilder queueAssertionHalfClusterResource = createAssertionBuilder()
        .withQueue("root.a")
        .toExpect(ResourceUtils.multiply(QUEUE_A_RES, 0.5f))
        .assertEffectiveMinResource()

        .withQueue("root.b")
        .toExpect(ResourceUtils.multiply(QUEUE_B_RES, 0.5f))
        .assertEffectiveMinResource()

        .withQueue("root.a.a1")
        .toExpect(ResourceUtils.multiply(QUEUE_A1_RES, 0.5f))
        .assertEffectiveMinResource()

        .withQueue("root.a.a2")
        .toExpect(ResourceUtils.multiply(QUEUE_A2_RES, 0.5f))
        .assertEffectiveMinResource()
        .build();

    update(queueAssertionHalfClusterResource, ResourceUtils.multiply(UPDATE_RES, 0.5f));
  }

}