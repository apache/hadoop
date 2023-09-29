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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils.EPSILON;

public class TestResourceVector {
  private final static String CUSTOM_RESOURCE = "custom";

  private final YarnConfiguration conf = new YarnConfiguration();

  @Before
  public void setUp() {
    conf.set(YarnConfiguration.RESOURCE_TYPES, CUSTOM_RESOURCE);
    ResourceUtils.resetResourceTypes(conf);
  }

  @Test
  public void testCreation() {
    ResourceVector zeroResourceVector = ResourceVector.newInstance();
    Assert.assertEquals(0, zeroResourceVector.getValue(MEMORY_URI), EPSILON);
    Assert.assertEquals(0, zeroResourceVector.getValue(VCORES_URI), EPSILON);
    Assert.assertEquals(0, zeroResourceVector.getValue(CUSTOM_RESOURCE), EPSILON);

    ResourceVector uniformResourceVector = ResourceVector.of(10);
    Assert.assertEquals(10, uniformResourceVector.getValue(MEMORY_URI), EPSILON);
    Assert.assertEquals(10, uniformResourceVector.getValue(VCORES_URI), EPSILON);
    Assert.assertEquals(10, uniformResourceVector.getValue(CUSTOM_RESOURCE), EPSILON);

    Map<String, Long> customResources = new HashMap<>();
    customResources.put(CUSTOM_RESOURCE, 2L);
    Resource resource = Resource.newInstance(10, 5, customResources);
    ResourceVector resourceVectorFromResource = ResourceVector.of(resource);
    Assert.assertEquals(10, resourceVectorFromResource.getValue(MEMORY_URI), EPSILON);
    Assert.assertEquals(5, resourceVectorFromResource.getValue(VCORES_URI), EPSILON);
    Assert.assertEquals(2, resourceVectorFromResource.getValue(CUSTOM_RESOURCE), EPSILON);
  }

  @Test
  public void testSubtract() {
    ResourceVector lhsResourceVector = ResourceVector.of(13);
    ResourceVector rhsResourceVector = ResourceVector.of(5);
    lhsResourceVector.decrement(rhsResourceVector);

    Assert.assertEquals(8, lhsResourceVector.getValue(MEMORY_URI), EPSILON);
    Assert.assertEquals(8, lhsResourceVector.getValue(VCORES_URI), EPSILON);
    Assert.assertEquals(8, lhsResourceVector.getValue(CUSTOM_RESOURCE), EPSILON);

    ResourceVector negativeResourceVector = ResourceVector.of(-100);

    // Check whether overflow causes any issues
    negativeResourceVector.decrement(ResourceVector.of(Float.MAX_VALUE));
    Assert.assertEquals(-Float.MAX_VALUE, negativeResourceVector.getValue(MEMORY_URI), EPSILON);
    Assert.assertEquals(-Float.MAX_VALUE, negativeResourceVector.getValue(VCORES_URI), EPSILON);
    Assert.assertEquals(-Float.MAX_VALUE, negativeResourceVector.getValue(CUSTOM_RESOURCE),
        EPSILON);

  }

  @Test
  public void testIncrement() {
    ResourceVector resourceVector = ResourceVector.of(13);
    resourceVector.increment(MEMORY_URI, 5);

    Assert.assertEquals(18, resourceVector.getValue(MEMORY_URI), EPSILON);
    Assert.assertEquals(13, resourceVector.getValue(VCORES_URI), EPSILON);
    Assert.assertEquals(13, resourceVector.getValue(CUSTOM_RESOURCE), EPSILON);

    // Check whether overflow causes any issues
    ResourceVector maxFloatResourceVector = ResourceVector.of(Float.MAX_VALUE);
    maxFloatResourceVector.increment(MEMORY_URI, 100);
    Assert.assertEquals(Float.MAX_VALUE, maxFloatResourceVector.getValue(MEMORY_URI), EPSILON);
  }

  @Test
  public void testEquals() {
    ResourceVector resourceVector = ResourceVector.of(13);
    ResourceVector resourceVectorOther = ResourceVector.of(14);
    Resource resource = Resource.newInstance(13, 13);

    Assert.assertNotEquals(null, resourceVector);
    Assert.assertNotEquals(resourceVectorOther, resourceVector);
    Assert.assertNotEquals(resource, resourceVector);

    ResourceVector resourceVectorOne = ResourceVector.of(1);
    resourceVectorOther.decrement(resourceVectorOne);

    Assert.assertEquals(resourceVectorOther, resourceVector);
  }
}