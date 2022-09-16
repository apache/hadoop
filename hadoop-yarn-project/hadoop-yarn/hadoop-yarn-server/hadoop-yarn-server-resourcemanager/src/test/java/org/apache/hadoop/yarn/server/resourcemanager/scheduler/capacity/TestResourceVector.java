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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils.EPSILON;

public class TestResourceVector {
  private final static String CUSTOM_RESOURCE = "custom";

  private final YarnConfiguration conf = new YarnConfiguration();

  @BeforeEach
  public void setUp() {
    conf.set(YarnConfiguration.RESOURCE_TYPES, CUSTOM_RESOURCE);
    ResourceUtils.resetResourceTypes(conf);
  }

  @Test
  void testCreation() {
    ResourceVector zeroResourceVector = ResourceVector.newInstance();
    Assertions.assertEquals(zeroResourceVector.getValue(MEMORY_URI), EPSILON, 0);
    Assertions.assertEquals(zeroResourceVector.getValue(VCORES_URI), EPSILON, 0);
    Assertions.assertEquals(zeroResourceVector.getValue(CUSTOM_RESOURCE), EPSILON, 0);

    ResourceVector uniformResourceVector = ResourceVector.of(10);
    Assertions.assertEquals(uniformResourceVector.getValue(MEMORY_URI), EPSILON, 10);
    Assertions.assertEquals(uniformResourceVector.getValue(VCORES_URI), EPSILON, 10);
    Assertions.assertEquals(uniformResourceVector.getValue(CUSTOM_RESOURCE), EPSILON, 10);

    Map<String, Long> customResources = new HashMap<>();
    customResources.put(CUSTOM_RESOURCE, 2L);
    Resource resource = Resource.newInstance(10, 5, customResources);
    ResourceVector resourceVectorFromResource = ResourceVector.of(resource);
    Assertions.assertEquals(resourceVectorFromResource.getValue(MEMORY_URI), EPSILON, 10);
    Assertions.assertEquals(resourceVectorFromResource.getValue(VCORES_URI), EPSILON, 5);
    Assertions.assertEquals(resourceVectorFromResource.getValue(CUSTOM_RESOURCE), EPSILON, 2);
  }

  @Test
  void testSubtract() {
    ResourceVector lhsResourceVector = ResourceVector.of(13);
    ResourceVector rhsResourceVector = ResourceVector.of(5);
    lhsResourceVector.subtract(rhsResourceVector);

    Assertions.assertEquals(lhsResourceVector.getValue(MEMORY_URI), EPSILON, 8);
    Assertions.assertEquals(lhsResourceVector.getValue(VCORES_URI), EPSILON, 8);
    Assertions.assertEquals(lhsResourceVector.getValue(CUSTOM_RESOURCE), EPSILON, 8);

    ResourceVector negativeResourceVector = ResourceVector.of(-100);

    // Check whether overflow causes any issues
    negativeResourceVector.subtract(ResourceVector.of(Float.MAX_VALUE));
    Assertions.assertEquals(negativeResourceVector.getValue(MEMORY_URI), EPSILON, -Float.MAX_VALUE);
    Assertions.assertEquals(negativeResourceVector.getValue(VCORES_URI), EPSILON, -Float.MAX_VALUE);
    Assertions.assertEquals(negativeResourceVector.getValue(CUSTOM_RESOURCE), EPSILON,
        -Float.MAX_VALUE);

  }

  @Test
  void testIncrement() {
    ResourceVector resourceVector = ResourceVector.of(13);
    resourceVector.increment(MEMORY_URI, 5);

    Assertions.assertEquals(resourceVector.getValue(MEMORY_URI), EPSILON, 18);
    Assertions.assertEquals(resourceVector.getValue(VCORES_URI), EPSILON, 13);
    Assertions.assertEquals(resourceVector.getValue(CUSTOM_RESOURCE), EPSILON, 13);

    // Check whether overflow causes any issues
    ResourceVector maxFloatResourceVector = ResourceVector.of(Float.MAX_VALUE);
    maxFloatResourceVector.increment(MEMORY_URI, 100);
    Assertions.assertEquals(maxFloatResourceVector.getValue(MEMORY_URI), EPSILON, Float.MAX_VALUE);
  }

  @Test
  void testEquals() {
    ResourceVector resourceVector = ResourceVector.of(13);
    ResourceVector resourceVectorOther = ResourceVector.of(14);
    Resource resource = Resource.newInstance(13, 13);

    Assertions.assertNotEquals(null, resourceVector);
    Assertions.assertNotEquals(resourceVectorOther, resourceVector);
    Assertions.assertNotEquals(resource, resourceVector);

    ResourceVector resourceVectorOne = ResourceVector.of(1);
    resourceVectorOther.subtract(resourceVectorOne);

    Assertions.assertEquals(resourceVectorOther, resourceVector);
  }
}