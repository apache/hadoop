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

import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils.EPSILON;

public class TestQueueCapacityVector {
  private static final String CUSTOM_RESOURCE = "custom";
  public static final String MIXED_CAPACITY_VECTOR_STRING =
      "[custom=3.0,memory-mb=10.0w,vcores=6.0%]";

  private final YarnConfiguration conf = new YarnConfiguration();

  @Before
  public void setUp() {
    conf.set(YarnConfiguration.RESOURCE_TYPES, CUSTOM_RESOURCE);
    ResourceUtils.resetResourceTypes(conf);
  }

  @Test
  public void getResourceNamesByCapacityType() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();

    capacityVector.setResource(MEMORY_URI, 10, QueueCapacityType.PERCENTAGE);
    capacityVector.setResource(VCORES_URI, 6, QueueCapacityType.PERCENTAGE);

    // custom is not set, defaults to 0
    Assert.assertEquals(1, capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.ABSOLUTE).size());
    Assert.assertTrue(capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.ABSOLUTE).contains(CUSTOM_RESOURCE));

    Assert.assertEquals(2, capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.PERCENTAGE).size());
    Assert.assertTrue(capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.PERCENTAGE).contains(VCORES_URI));
    Assert.assertTrue(capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.PERCENTAGE).contains(MEMORY_URI));
    Assert.assertEquals(10, capacityVector.getResource(MEMORY_URI).getResourceValue(), EPSILON);
    Assert.assertEquals(6, capacityVector.getResource(VCORES_URI).getResourceValue(), EPSILON);
  }

  @Test
  public void isResourceOfType() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();

    capacityVector.setResource(MEMORY_URI, 10, QueueCapacityType.WEIGHT);
    capacityVector.setResource(VCORES_URI, 6, QueueCapacityType.PERCENTAGE);
    capacityVector.setResource(CUSTOM_RESOURCE, 3, QueueCapacityType.ABSOLUTE);

    Assert.assertTrue(capacityVector.isResourceOfType(MEMORY_URI, QueueCapacityType.WEIGHT));
    Assert.assertTrue(capacityVector.isResourceOfType(VCORES_URI, QueueCapacityType.PERCENTAGE));
    Assert.assertTrue(capacityVector.isResourceOfType(CUSTOM_RESOURCE, QueueCapacityType.ABSOLUTE));
  }

  @Test
  public void testIterator() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();
    List<QueueCapacityVectorEntry> entries = Lists.newArrayList(capacityVector);

    Assert.assertEquals(3, entries.size());

    QueueCapacityVector emptyCapacityVector = new QueueCapacityVector();
    List<QueueCapacityVectorEntry> emptyEntries = Lists.newArrayList(emptyCapacityVector);

    Assert.assertEquals(0, emptyEntries.size());
  }

  @Test
  public void testToString() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();

    capacityVector.setResource(MEMORY_URI, 10, QueueCapacityType.WEIGHT);
    capacityVector.setResource(VCORES_URI, 6, QueueCapacityType.PERCENTAGE);
    capacityVector.setResource(CUSTOM_RESOURCE, 3, QueueCapacityType.ABSOLUTE);

    Assert.assertEquals(MIXED_CAPACITY_VECTOR_STRING, capacityVector.toString());

    QueueCapacityVector emptyCapacityVector = new QueueCapacityVector();
    Assert.assertEquals("[]", emptyCapacityVector.toString());
  }
}