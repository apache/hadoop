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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;
import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestQueueMetricsForCustomResources.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils.EPSILON;

public class TestQueueCapacityConfigParser {

  private static final String ALL_RESOURCE_TEMPLATE = "[memory-mb=%s, vcores=%s, yarn.io/gpu=%s]";
  private static final String MEMORY_VCORE_TEMPLATE = "[memory-mb=%s, vcores=%s]";

  private static final String MEMORY_ABSOLUTE = "12Gi";
  private static final float VCORE_ABSOLUTE = 6;
  private static final float GPU_ABSOLUTE = 10;

  private static final float PERCENTAGE_VALUE = 50f;
  private static final float MEMORY_MIXED = 1024;
  private static final float WEIGHT_VALUE = 6;

  private static final String QUEUE = "root.test";

  private static final String ABSOLUTE_RESOURCE = String.format(
      ALL_RESOURCE_TEMPLATE, MEMORY_ABSOLUTE, VCORE_ABSOLUTE, GPU_ABSOLUTE);
  private static final String ABSOLUTE_RESOURCE_MEMORY_VCORE = String.format(
      MEMORY_VCORE_TEMPLATE, MEMORY_ABSOLUTE, VCORE_ABSOLUTE);
  private static final String MIXED_RESOURCE = String.format(
      ALL_RESOURCE_TEMPLATE, MEMORY_MIXED, PERCENTAGE_VALUE + "%", WEIGHT_VALUE + "w");
  private static final String RESOURCE_TYPES = GPU_URI;

  public static final String NONEXISTINGSUFFIX = "50nonexistingsuffix";
  public static final String EMPTY_BRACKET = "[]";
  public static final String INVALID_CAPACITY_BRACKET = "[invalid]";
  public static final String INVALID_CAPACITY_FORMAT = "[memory-100,vcores-60]";

  private final QueueCapacityConfigParser capacityConfigParser
      = new QueueCapacityConfigParser();

  @Test
  public void testPercentageCapacityConfig() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setCapacity(QUEUE, PERCENTAGE_VALUE);

    QueueCapacityVector percentageCapacityVector = capacityConfigParser.parse(conf, QUEUE,
        NO_LABEL);
    QueueCapacityVectorEntry memory = percentageCapacityVector.getResource(MEMORY_URI);
    QueueCapacityVectorEntry vcore = percentageCapacityVector.getResource(VCORES_URI);

    Assert.assertEquals(QueueCapacityType.PERCENTAGE, memory.getVectorResourceType());
    Assert.assertEquals(PERCENTAGE_VALUE, memory.getResourceValue(), EPSILON);

    Assert.assertEquals(QueueCapacityType.PERCENTAGE, vcore.getVectorResourceType());
    Assert.assertEquals(PERCENTAGE_VALUE, vcore.getResourceValue(), EPSILON);

    QueueCapacityVector rootCapacityVector = capacityConfigParser.parse(conf,
        CapacitySchedulerConfiguration.ROOT, NO_LABEL);

    QueueCapacityVectorEntry memoryRoot = rootCapacityVector.getResource(MEMORY_URI);
    QueueCapacityVectorEntry vcoreRoot = rootCapacityVector.getResource(VCORES_URI);

    Assert.assertEquals(QueueCapacityType.PERCENTAGE, memoryRoot.getVectorResourceType());
    Assert.assertEquals(100f, memoryRoot.getResourceValue(), EPSILON);

    Assert.assertEquals(QueueCapacityType.PERCENTAGE, vcoreRoot.getVectorResourceType());
    Assert.assertEquals(100f, vcoreRoot.getResourceValue(), EPSILON);
  }

  @Test
  public void testWeightCapacityConfig() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setNonLabeledQueueWeight(QUEUE, WEIGHT_VALUE);

    QueueCapacityVector weightCapacityVector = capacityConfigParser.parse(conf, QUEUE, NO_LABEL);

    QueueCapacityVectorEntry memory = weightCapacityVector.getResource(MEMORY_URI);
    QueueCapacityVectorEntry vcore = weightCapacityVector.getResource(VCORES_URI);

    Assert.assertEquals(QueueCapacityType.WEIGHT, memory.getVectorResourceType());
    Assert.assertEquals(WEIGHT_VALUE, memory.getResourceValue(), EPSILON);

    Assert.assertEquals(QueueCapacityType.WEIGHT, vcore.getVectorResourceType());
    Assert.assertEquals(WEIGHT_VALUE, vcore.getResourceValue(), EPSILON);
  }

  @Test
  public void testAbsoluteCapacityVectorConfig() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE) +
        CapacitySchedulerConfiguration.CAPACITY, ABSOLUTE_RESOURCE);
    conf.set(YarnConfiguration.RESOURCE_TYPES, RESOURCE_TYPES);
    ResourceUtils.resetResourceTypes(conf);

    QueueCapacityVector absoluteCapacityVector = capacityConfigParser.parse(conf, QUEUE, NO_LABEL);

    Assert.assertEquals(QueueCapacityType.ABSOLUTE, absoluteCapacityVector.getResource(MEMORY_URI)
        .getVectorResourceType());
    Assert.assertEquals(12 * GB, absoluteCapacityVector.getResource(MEMORY_URI)
        .getResourceValue(), EPSILON);

    Assert.assertEquals(QueueCapacityType.ABSOLUTE, absoluteCapacityVector.getResource(VCORES_URI)
        .getVectorResourceType());
    Assert.assertEquals(VCORE_ABSOLUTE, absoluteCapacityVector.getResource(VCORES_URI)
        .getResourceValue(), EPSILON);

    Assert.assertEquals(QueueCapacityType.ABSOLUTE, absoluteCapacityVector.getResource(GPU_URI)
        .getVectorResourceType());
    Assert.assertEquals(GPU_ABSOLUTE, absoluteCapacityVector.getResource(GPU_URI)
        .getResourceValue(), EPSILON);

    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE) +
        CapacitySchedulerConfiguration.CAPACITY, ABSOLUTE_RESOURCE_MEMORY_VCORE);
    QueueCapacityVector withoutGpuVector = capacityConfigParser.parse(conf, QUEUE, NO_LABEL);

    Assert.assertEquals(3, withoutGpuVector.getResourceCount());
    Assert.assertEquals(0f, withoutGpuVector.getResource(GPU_URI).getResourceValue(), EPSILON);
  }

  @Test
  public void testMixedCapacityConfig() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE)
        + CapacitySchedulerConfiguration.CAPACITY, MIXED_RESOURCE);
    conf.set(YarnConfiguration.RESOURCE_TYPES, RESOURCE_TYPES);
    ResourceUtils.resetResourceTypes(conf);

    QueueCapacityVector mixedCapacityVector =
        capacityConfigParser.parse(conf, QUEUE, NO_LABEL);

    Assert.assertEquals(QueueCapacityType.ABSOLUTE,
        mixedCapacityVector.getResource(MEMORY_URI).getVectorResourceType());
    Assert.assertEquals(MEMORY_MIXED, mixedCapacityVector.getResource(MEMORY_URI)
        .getResourceValue(), EPSILON);

    Assert.assertEquals(QueueCapacityType.PERCENTAGE,
        mixedCapacityVector.getResource(VCORES_URI).getVectorResourceType());
    Assert.assertEquals(PERCENTAGE_VALUE,
        mixedCapacityVector.getResource(VCORES_URI).getResourceValue(), EPSILON);

    Assert.assertEquals(QueueCapacityType.WEIGHT,
        mixedCapacityVector.getResource(GPU_URI).getVectorResourceType());
    Assert.assertEquals(WEIGHT_VALUE,
        mixedCapacityVector.getResource(GPU_URI).getResourceValue(), EPSILON);

    // Test undefined capacity type default value
    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE)
        + CapacitySchedulerConfiguration.CAPACITY, ABSOLUTE_RESOURCE_MEMORY_VCORE);

    QueueCapacityVector mixedCapacityVectorWithGpuUndefined =
        capacityConfigParser.parse(conf, QUEUE, NO_LABEL);
    Assert.assertEquals(QueueCapacityType.ABSOLUTE,
        mixedCapacityVectorWithGpuUndefined.getResource(MEMORY_URI).getVectorResourceType());
    Assert.assertEquals(0, mixedCapacityVectorWithGpuUndefined.getResource(GPU_URI)
        .getResourceValue(), EPSILON);

  }

  @Test
  public void testInvalidCapacityConfigs() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();

    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE)
        + CapacitySchedulerConfiguration.CAPACITY, NONEXISTINGSUFFIX);
    QueueCapacityVector capacityVectorWithInvalidSuffix =
        capacityConfigParser.parse(conf, QUEUE, NO_LABEL);
    List<QueueCapacityVectorEntry> entriesWithInvalidSuffix =
        Lists.newArrayList(capacityVectorWithInvalidSuffix.iterator());
    Assert.assertEquals(0, entriesWithInvalidSuffix.size());

    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE)
        + CapacitySchedulerConfiguration.CAPACITY, INVALID_CAPACITY_FORMAT);
    QueueCapacityVector invalidDelimiterCapacityVector =
        capacityConfigParser.parse(conf, QUEUE, NO_LABEL);
    List<QueueCapacityVectorEntry> invalidDelimiterEntries =
        Lists.newArrayList(invalidDelimiterCapacityVector.iterator());
    Assert.assertEquals(0, invalidDelimiterEntries.size());

    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE)
        + CapacitySchedulerConfiguration.CAPACITY, INVALID_CAPACITY_BRACKET);
    QueueCapacityVector invalidCapacityVector =
        capacityConfigParser.parse(conf, QUEUE, NO_LABEL);
    List<QueueCapacityVectorEntry> resources =
        Lists.newArrayList(invalidCapacityVector.iterator());
    Assert.assertEquals(0, resources.size());

    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE)
        + CapacitySchedulerConfiguration.CAPACITY, EMPTY_BRACKET);
    QueueCapacityVector emptyBracketCapacityVector =
        capacityConfigParser.parse(conf, QUEUE, NO_LABEL);
    List<QueueCapacityVectorEntry> emptyEntries =
        Lists.newArrayList(emptyBracketCapacityVector.iterator());
    Assert.assertEquals(0, emptyEntries.size());

    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE)
        + CapacitySchedulerConfiguration.CAPACITY, "");
    QueueCapacityVector emptyCapacity =
        capacityConfigParser.parse(conf, QUEUE, NO_LABEL);
    List<QueueCapacityVectorEntry> emptyResources =
        Lists.newArrayList(emptyCapacity.iterator());
    Assert.assertEquals(emptyResources.size(), 0);

    conf.unset(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE)
        + CapacitySchedulerConfiguration.CAPACITY);
    QueueCapacityVector nonSetCapacity =
        capacityConfigParser.parse(conf, QUEUE, NO_LABEL);
    List<QueueCapacityVectorEntry> nonSetResources =
        Lists.newArrayList(nonSetCapacity.iterator());
    Assert.assertEquals(nonSetResources.size(), 0);
  }
}