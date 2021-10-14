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

public class TestQueueCapacityVector {

  private final YarnConfiguration conf = new YarnConfiguration();

  @Before
  public void setUp() {
    conf.set(YarnConfiguration.RESOURCE_TYPES, "custom");
    ResourceUtils.resetResourceTypes(conf);
  }

  @Test
  public void getResourceNamesByCapacityType() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();

    capacityVector.setResource("memory-mb", 10, QueueCapacityType.PERCENTAGE);
    capacityVector.setResource("vcores", 6, QueueCapacityType.PERCENTAGE);

    // custom is not set, defaults to 0
    Assert.assertEquals(1, capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.ABSOLUTE).size());
    Assert.assertTrue(capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.ABSOLUTE).contains("custom"));

    Assert.assertEquals(2, capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.PERCENTAGE).size());
    Assert.assertTrue(capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.PERCENTAGE).contains("vcores"));
    Assert.assertTrue(capacityVector.getResourceNamesByCapacityType(
        QueueCapacityType.PERCENTAGE).contains("memory-mb"));
  }

  @Test
  public void isResourceOfType() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();

    capacityVector.setResource("memory-mb", 10, QueueCapacityType.WEIGHT);
    capacityVector.setResource("vcores", 6, QueueCapacityType.PERCENTAGE);
    capacityVector.setResource("custom", 3, QueueCapacityType.ABSOLUTE);

    Assert.assertTrue(capacityVector.isResourceOfType("memory-mb", QueueCapacityType.WEIGHT));
    Assert.assertTrue(capacityVector.isResourceOfType("vcores", QueueCapacityType.PERCENTAGE));
    Assert.assertTrue(capacityVector.isResourceOfType("custom", QueueCapacityType.ABSOLUTE));
  }

  @Test
  public void iterator() {
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

    capacityVector.setResource("memory-mb", 10, QueueCapacityType.WEIGHT);
    capacityVector.setResource("vcores", 6, QueueCapacityType.PERCENTAGE);
    capacityVector.setResource("custom", 3, QueueCapacityType.ABSOLUTE);

    Assert.assertEquals("[custom=3.0,memory-mb=10.0w,vcores=6.0%]", capacityVector.toString());

    QueueCapacityVector emptyCapacityVector = new QueueCapacityVector();
    Assert.assertEquals("[]", emptyCapacityVector.toString());
  }
}