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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;
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
    QueueCapacityVector percentageCapacityVector =
        capacityConfigParser.parse(Float.toString(PERCENTAGE_VALUE), QUEUE);
    QueueCapacityVectorEntry memory = percentageCapacityVector.getResource(MEMORY_URI);
    QueueCapacityVectorEntry vcore = percentageCapacityVector.getResource(VCORES_URI);

    Assert.assertEquals(ResourceUnitCapacityType.PERCENTAGE, memory.getVectorResourceType());
    Assert.assertEquals(PERCENTAGE_VALUE, memory.getResourceValue(), EPSILON);

    Assert.assertEquals(ResourceUnitCapacityType.PERCENTAGE, vcore.getVectorResourceType());
    Assert.assertEquals(PERCENTAGE_VALUE, vcore.getResourceValue(), EPSILON);

    QueueCapacityVector rootCapacityVector =
        capacityConfigParser.parse(Float.toString(PERCENTAGE_VALUE),
        CapacitySchedulerConfiguration.ROOT);

    QueueCapacityVectorEntry memoryRoot = rootCapacityVector.getResource(MEMORY_URI);
    QueueCapacityVectorEntry vcoreRoot = rootCapacityVector.getResource(VCORES_URI);

    Assert.assertEquals(ResourceUnitCapacityType.PERCENTAGE, memoryRoot.getVectorResourceType());
    Assert.assertEquals(100f, memoryRoot.getResourceValue(), EPSILON);

    Assert.assertEquals(ResourceUnitCapacityType.PERCENTAGE, vcoreRoot.getVectorResourceType());
    Assert.assertEquals(100f, vcoreRoot.getResourceValue(), EPSILON);
  }

  @Test
  public void testWeightCapacityConfig() {
    QueueCapacityVector weightCapacityVector = capacityConfigParser.parse(WEIGHT_VALUE + "w",
        QUEUE);

    QueueCapacityVectorEntry memory = weightCapacityVector.getResource(MEMORY_URI);
    QueueCapacityVectorEntry vcore = weightCapacityVector.getResource(VCORES_URI);

    Assert.assertEquals(ResourceUnitCapacityType.WEIGHT, memory.getVectorResourceType());
    Assert.assertEquals(WEIGHT_VALUE, memory.getResourceValue(), EPSILON);

    Assert.assertEquals(ResourceUnitCapacityType.WEIGHT, vcore.getVectorResourceType());
    Assert.assertEquals(WEIGHT_VALUE, vcore.getResourceValue(), EPSILON);
  }

  @Test
  public void testAbsoluteCapacityVectorConfig() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(CapacitySchedulerConfiguration.getQueuePrefix(QUEUE) +
        CapacitySchedulerConfiguration.CAPACITY, ABSOLUTE_RESOURCE);
    conf.set(YarnConfiguration.RESOURCE_TYPES, RESOURCE_TYPES);
    ResourceUtils.resetResourceTypes(conf);

    QueueCapacityVector absoluteCapacityVector = capacityConfigParser.parse(ABSOLUTE_RESOURCE,
        QUEUE);

    Assert.assertEquals(ResourceUnitCapacityType.ABSOLUTE,
        absoluteCapacityVector.getResource(MEMORY_URI).getVectorResourceType());
    Assert.assertEquals(12 * GB, absoluteCapacityVector.getResource(MEMORY_URI)
        .getResourceValue(), EPSILON);

    Assert.assertEquals(ResourceUnitCapacityType.ABSOLUTE,
        absoluteCapacityVector.getResource(VCORES_URI).getVectorResourceType());
    Assert.assertEquals(VCORE_ABSOLUTE, absoluteCapacityVector.getResource(VCORES_URI)
        .getResourceValue(), EPSILON);

    Assert.assertEquals(ResourceUnitCapacityType.ABSOLUTE,
        absoluteCapacityVector.getResource(GPU_URI).getVectorResourceType());
    Assert.assertEquals(GPU_ABSOLUTE, absoluteCapacityVector.getResource(GPU_URI)
        .getResourceValue(), EPSILON);

    QueueCapacityVector withoutGpuVector = capacityConfigParser
        .parse(ABSOLUTE_RESOURCE_MEMORY_VCORE, QUEUE);

    Assert.assertEquals(3, withoutGpuVector.getResourceCount());
    Assert.assertEquals(0f, withoutGpuVector.getResource(GPU_URI).getResourceValue(), EPSILON);
  }

  @Test
  public void testMixedCapacityConfig() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.set(YarnConfiguration.RESOURCE_TYPES, RESOURCE_TYPES);
    ResourceUtils.resetResourceTypes(conf);

    QueueCapacityVector mixedCapacityVector =
        capacityConfigParser.parse(MIXED_RESOURCE, QUEUE);

    Assert.assertEquals(ResourceUnitCapacityType.ABSOLUTE,
        mixedCapacityVector.getResource(MEMORY_URI).getVectorResourceType());
    Assert.assertEquals(MEMORY_MIXED, mixedCapacityVector.getResource(MEMORY_URI)
        .getResourceValue(), EPSILON);

    Assert.assertEquals(ResourceUnitCapacityType.PERCENTAGE,
        mixedCapacityVector.getResource(VCORES_URI).getVectorResourceType());
    Assert.assertEquals(PERCENTAGE_VALUE,
        mixedCapacityVector.getResource(VCORES_URI).getResourceValue(), EPSILON);

    Assert.assertEquals(ResourceUnitCapacityType.WEIGHT,
        mixedCapacityVector.getResource(GPU_URI).getVectorResourceType());
    Assert.assertEquals(WEIGHT_VALUE,
        mixedCapacityVector.getResource(GPU_URI).getResourceValue(), EPSILON);

    // Test undefined capacity type default value
    QueueCapacityVector mixedCapacityVectorWithGpuUndefined =
        capacityConfigParser.parse(ABSOLUTE_RESOURCE_MEMORY_VCORE, QUEUE);
    Assert.assertEquals(ResourceUnitCapacityType.ABSOLUTE,
        mixedCapacityVectorWithGpuUndefined.getResource(MEMORY_URI).getVectorResourceType());
    Assert.assertEquals(0, mixedCapacityVectorWithGpuUndefined.getResource(GPU_URI)
        .getResourceValue(), EPSILON);

  }

  @Test
  public void testInvalidCapacityConfigs() {
    QueueCapacityVector capacityVectorWithInvalidSuffix =
        capacityConfigParser.parse(NONEXISTINGSUFFIX, QUEUE);
    List<QueueCapacityVectorEntry> entriesWithInvalidSuffix =
        Lists.newArrayList(capacityVectorWithInvalidSuffix.iterator());
    Assert.assertEquals(0, entriesWithInvalidSuffix.size());

    QueueCapacityVector invalidDelimiterCapacityVector =
        capacityConfigParser.parse(INVALID_CAPACITY_FORMAT, QUEUE);
    List<QueueCapacityVectorEntry> invalidDelimiterEntries =
        Lists.newArrayList(invalidDelimiterCapacityVector.iterator());
    Assert.assertEquals(0, invalidDelimiterEntries.size());

    QueueCapacityVector invalidCapacityVector =
        capacityConfigParser.parse(INVALID_CAPACITY_BRACKET, QUEUE);
    List<QueueCapacityVectorEntry> resources =
        Lists.newArrayList(invalidCapacityVector.iterator());
    Assert.assertEquals(0, resources.size());

    QueueCapacityVector emptyBracketCapacityVector =
        capacityConfigParser.parse(EMPTY_BRACKET, QUEUE);
    List<QueueCapacityVectorEntry> emptyEntries =
        Lists.newArrayList(emptyBracketCapacityVector.iterator());
    Assert.assertEquals(0, emptyEntries.size());

    QueueCapacityVector emptyCapacity =
        capacityConfigParser.parse("", QUEUE);
    List<QueueCapacityVectorEntry> emptyResources =
        Lists.newArrayList(emptyCapacity.iterator());
    Assert.assertEquals(emptyResources.size(), 0);

    QueueCapacityVector nonSetCapacity =
        capacityConfigParser.parse(null, QUEUE);
    List<QueueCapacityVectorEntry> nonSetResources =
        Lists.newArrayList(nonSetCapacity.iterator());
    Assert.assertEquals(nonSetResources.size(), 0);
  }
}