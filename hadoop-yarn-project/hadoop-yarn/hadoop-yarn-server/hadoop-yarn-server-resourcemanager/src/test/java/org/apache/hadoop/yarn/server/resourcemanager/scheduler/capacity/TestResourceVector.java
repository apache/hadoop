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

public class TestResourceVector {
  private final YarnConfiguration conf = new YarnConfiguration();

  @Before
  public void setUp() {
    conf.set(YarnConfiguration.RESOURCE_TYPES, "custom");
    ResourceUtils.resetResourceTypes(conf);
  }

  @Test
  public void testCreation() {
    ResourceVector zeroResourceVector = ResourceVector.newInstance();
    Assert.assertEquals(0, zeroResourceVector.getValue("memory-mb"), 1e-6);
    Assert.assertEquals(0, zeroResourceVector.getValue("vcores"), 1e-6);
    Assert.assertEquals(0, zeroResourceVector.getValue("custom"), 1e-6);

    ResourceVector uniformResourceVector = ResourceVector.of(10);
    Assert.assertEquals(10, uniformResourceVector.getValue("memory-mb"), 1e-6);
    Assert.assertEquals(10, uniformResourceVector.getValue("vcores"), 1e-6);
    Assert.assertEquals(10, uniformResourceVector.getValue("custom"), 1e-6);

    Map<String, Long> customResources = new HashMap<>();
    customResources.put("custom", 2L);
    Resource resource = Resource.newInstance(10, 5, customResources);
    ResourceVector resourceVectorFromResource = ResourceVector.of(resource);
    Assert.assertEquals(10, resourceVectorFromResource.getValue("memory-mb"), 1e-6);
    Assert.assertEquals(5, resourceVectorFromResource.getValue("vcores"), 1e-6);
    Assert.assertEquals(2, resourceVectorFromResource.getValue("custom"), 1e-6);
  }

  @Test
  public void subtract() {
    ResourceVector lhsResourceVector = ResourceVector.of(13);
    ResourceVector rhsResourceVector = ResourceVector.of(5);
    lhsResourceVector.subtract(rhsResourceVector);

    Assert.assertEquals(8, lhsResourceVector.getValue("memory-mb"), 1e-6);
    Assert.assertEquals(8, lhsResourceVector.getValue("vcores"), 1e-6);
    Assert.assertEquals(8, lhsResourceVector.getValue("custom"), 1e-6);
  }

  @Test
  public void increment() {
    ResourceVector resourceVector = ResourceVector.of(13);
    resourceVector.increment("memory-mb", 5);

    Assert.assertEquals(18, resourceVector.getValue("memory-mb"), 1e-6);
    Assert.assertEquals(13, resourceVector.getValue("vcores"), 1e-6);
    Assert.assertEquals(13, resourceVector.getValue("custom"), 1e-6);

    // Check whether overflow causes any issues
    ResourceVector maxFloatResourceVector = ResourceVector.of(Float.MAX_VALUE);
    maxFloatResourceVector.increment("memory-mb", 100);
    Assert.assertEquals(Float.MAX_VALUE, maxFloatResourceVector.getValue("memory-mb"), 1e-6);
  }

  @Test
  public void testEquals() {
    ResourceVector resourceVector = ResourceVector.of(13);
    ResourceVector resourceVectorOther = ResourceVector.of(14);
    Resource resource = Resource.newInstance(13, 13);

    Assert.assertFalse(resourceVector.equals(null));
    Assert.assertFalse(resourceVector.equals(resourceVectorOther));
    Assert.assertFalse(resourceVector.equals(resource));

    ResourceVector resourceVectorOne = ResourceVector.of(1);
    resourceVectorOther.subtract(resourceVectorOne);

    Assert.assertTrue(resourceVector.equals(resourceVectorOther));
  }
}