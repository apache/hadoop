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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueResourceVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueVectorResourceType;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestQueueCapacityConfigParser {

  private static final String QUEUE = "root.test";
  private static final String ABSOLUTE_RESOURCE = "[memory-mb=12Gi, vcores=6, yarn.io/gpu=10]";
  private static final String MIXED_RESOURCE = "[memory-mb=1024, vcores=50%, yarn.io/gpu=6w]";
  private static final String RESOURCE_TYPES = "yarn.io/gpu";

  private final QueueCapacityConfigParser capacityConfigParser
      = new QueueCapacityConfigParser();

  @Test
  public void testPercentageCapacityConfig() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setCapacity(QUEUE, 50);

    QueueCapacityVector percentageResourceVector = capacityConfigParser.parse(conf, QUEUE, "");
    List<QueueResourceVectorEntry> resources = Lists.newArrayList(percentageResourceVector.iterator());

    Assert.assertEquals(QueueVectorResourceType.PERCENTAGE, resources.get(0).getVectorResourceType());
    Assert.assertEquals(50f, resources.get(0).getResourceValue(), 1e-6);

    Assert.assertEquals(QueueVectorResourceType.PERCENTAGE, resources.get(1).getVectorResourceType());
    Assert.assertEquals(50f, resources.get(1).getResourceValue(), 1e-6);

    QueueCapacityVector rootResourceVector = capacityConfigParser.parse(conf,
        CapacitySchedulerConfiguration.ROOT, "");
    List<QueueResourceVectorEntry> rootResources =
        Lists.newArrayList(rootResourceVector.iterator());

    Assert.assertEquals(QueueVectorResourceType.PERCENTAGE,
        rootResources.get(0).getVectorResourceType());
    Assert.assertEquals(100f,
        rootResources.get(0).getResourceValue(), 1e-6);

    Assert.assertEquals(QueueVectorResourceType.PERCENTAGE,
        rootResources.get(1).getVectorResourceType());
    Assert.assertEquals(100f,
        rootResources.get(1).getResourceValue(), 1e-6);
  }

  @Test
  public void testWeightCapacityConfig() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setNonLabeledQueueWeight(QUEUE, 6);

    QueueCapacityVector weightResourceVector = capacityConfigParser.parse(conf, QUEUE, "");
    List<QueueResourceVectorEntry> resources = Lists.newArrayList(weightResourceVector.iterator());

    Assert.assertEquals(QueueVectorResourceType.WEIGHT, resources.get(0).getVectorResourceType());
    Assert.assertEquals(6f, resources.get(0).getResourceValue(), 1e-6);

    Assert.assertEquals(QueueVectorResourceType.WEIGHT, resources.get(1).getVectorResourceType());
    Assert.assertEquals(6f, resources.get(1).getResourceValue(), 1e-6);
  }

  @Test
  public void testAbsoluteResourceCapacityConfig() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE) + CapacitySchedulerConfiguration.CAPACITY, ABSOLUTE_RESOURCE);
    conf.set(YarnConfiguration.RESOURCE_TYPES, RESOURCE_TYPES);
    ResourceUtils.resetResourceTypes(conf);

    QueueCapacityVector absoluteResourceVector = capacityConfigParser.parse(conf, QUEUE, "");

    Assert.assertEquals(QueueVectorResourceType.ABSOLUTE, absoluteResourceVector.getResource("memory-mb").getVectorResourceType());
    Assert.assertEquals(12 * 1024, absoluteResourceVector.getResource("memory-mb").getResourceValue(), 1e-6);

    Assert.assertEquals(QueueVectorResourceType.ABSOLUTE, absoluteResourceVector.getResource("vcores").getVectorResourceType());
    Assert.assertEquals(6f, absoluteResourceVector.getResource("vcores").getResourceValue(), 1e-6);

    Assert.assertEquals(QueueVectorResourceType.ABSOLUTE, absoluteResourceVector.getResource("yarn.io/gpu").getVectorResourceType());
    Assert.assertEquals(10f, absoluteResourceVector.getResource("yarn.io/gpu").getResourceValue(), 1e-6);
  }

  @Test
  public void testMixedCapacityConfig() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE)
        + CapacitySchedulerConfiguration.CAPACITY, MIXED_RESOURCE);
    conf.set(YarnConfiguration.RESOURCE_TYPES, RESOURCE_TYPES);
    ResourceUtils.resetResourceTypes(conf);

    QueueCapacityVector mixedResourceVector =
        capacityConfigParser.parse(conf, QUEUE, "");

    Assert.assertEquals(QueueVectorResourceType.ABSOLUTE,
        mixedResourceVector.getResource("memory-mb").getVectorResourceType());
    Assert.assertEquals(1024, mixedResourceVector.getResource("memory-mb").getResourceValue(), 1e-6);

    Assert.assertEquals(QueueVectorResourceType.PERCENTAGE,
        mixedResourceVector.getResource("vcores").getVectorResourceType());
    Assert.assertEquals(50f,
        mixedResourceVector.getResource("vcores").getResourceValue(), 1e-6);

    Assert.assertEquals(QueueVectorResourceType.WEIGHT,
        mixedResourceVector.getResource("yarn.io/gpu").getVectorResourceType());
    Assert.assertEquals(6f,
        mixedResourceVector.getResource("yarn.io/gpu").getResourceValue(), 1e-6);
  }

  @Test
  public void testInvalidCapacityConfigs() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE)
        + CapacitySchedulerConfiguration.CAPACITY, "[invalid]");

    QueueCapacityVector invalidResourceCapacity =
        capacityConfigParser.parse(conf, QUEUE, "");
    List<QueueResourceVectorEntry> resources =
        Lists.newArrayList(invalidResourceCapacity.iterator());
    Assert.assertEquals(resources.size(), 0);

    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE)
        + CapacitySchedulerConfiguration.CAPACITY, "");

    QueueCapacityVector emptyCapacity =
        capacityConfigParser.parse(conf, QUEUE, "");
    List<QueueResourceVectorEntry> emptyResources =
        Lists.newArrayList(emptyCapacity.iterator());
    Assert.assertEquals(emptyResources.size(), 0);

    conf.unset(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE)
        + CapacitySchedulerConfiguration.CAPACITY);

    QueueCapacityVector nonSetCapacity =
        capacityConfigParser.parse(conf, QUEUE, "");
    List<QueueResourceVectorEntry> nonSetResources =
        Lists.newArrayList(nonSetCapacity.iterator());
    Assert.assertEquals(nonSetResources.size(), 0);
  }
}