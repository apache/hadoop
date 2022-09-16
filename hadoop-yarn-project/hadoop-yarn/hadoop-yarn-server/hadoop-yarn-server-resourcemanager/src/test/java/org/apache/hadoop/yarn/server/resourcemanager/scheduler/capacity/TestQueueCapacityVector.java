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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils.EPSILON;

public class TestQueueCapacityVector {
  private static final String CUSTOM_RESOURCE = "custom";
  public static final String MIXED_CAPACITY_VECTOR_STRING =
      "[custom=3.0,memory-mb=10.0w,vcores=6.0%]";

  private final YarnConfiguration conf = new YarnConfiguration();

  @BeforeEach
  public void setUp() {
    conf.set(YarnConfiguration.RESOURCE_TYPES, CUSTOM_RESOURCE);
    ResourceUtils.resetResourceTypes(conf);
  }

  @Test
  void getResourceNamesByCapacityType() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();

    capacityVector.setResource(MEMORY_URI, 10, QueueCapacityType.PERCENTAGE);
    capacityVector.setResource(VCORES_URI, 6, QueueCapacityType.PERCENTAGE);

    // custom is not set, defaults to 0
    Assertions.assertEquals(1, capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.ABSOLUTE).size());
    Assertions.assertTrue(capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.ABSOLUTE).contains(CUSTOM_RESOURCE));

    Assertions.assertEquals(2, capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.PERCENTAGE).size());
    Assertions.assertTrue(capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.PERCENTAGE).contains(VCORES_URI));
    Assertions.assertTrue(capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.PERCENTAGE).contains(MEMORY_URI));
    Assertions.assertEquals(capacityVector.getResource(MEMORY_URI).getResourceValue(), EPSILON, 10);
    Assertions.assertEquals(capacityVector.getResource(VCORES_URI).getResourceValue(), EPSILON, 6);
  }

  @Test
  void isResourceOfType() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();

    capacityVector.setResource(MEMORY_URI, 10, QueueCapacityType.WEIGHT);
    capacityVector.setResource(VCORES_URI, 6, QueueCapacityType.PERCENTAGE);
    capacityVector.setResource(CUSTOM_RESOURCE, 3, QueueCapacityType.ABSOLUTE);

    Assertions.assertTrue(capacityVector.isResourceOfType(MEMORY_URI, QueueCapacityType.WEIGHT));
    Assertions.assertTrue(capacityVector.isResourceOfType(VCORES_URI, QueueCapacityType.PERCENTAGE));
    Assertions.assertTrue(capacityVector.isResourceOfType(CUSTOM_RESOURCE, QueueCapacityType.ABSOLUTE));
  }

  @Test
  void testIterator() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();
    List<QueueCapacityVectorEntry> entries = Lists.newArrayList(capacityVector);

    Assertions.assertEquals(3, entries.size());

    QueueCapacityVector emptyCapacityVector = new QueueCapacityVector();
    List<QueueCapacityVectorEntry> emptyEntries = Lists.newArrayList(emptyCapacityVector);

    Assertions.assertEquals(0, emptyEntries.size());
  }

  @Test
  void testToString() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();

    capacityVector.setResource(MEMORY_URI, 10, QueueCapacityType.WEIGHT);
    capacityVector.setResource(VCORES_URI, 6, QueueCapacityType.PERCENTAGE);
    capacityVector.setResource(CUSTOM_RESOURCE, 3, QueueCapacityType.ABSOLUTE);

    Assertions.assertEquals(MIXED_CAPACITY_VECTOR_STRING, capacityVector.toString());

    QueueCapacityVector emptyCapacityVector = new QueueCapacityVector();
    Assertions.assertEquals("[]", emptyCapacityVector.toString());
  }
}