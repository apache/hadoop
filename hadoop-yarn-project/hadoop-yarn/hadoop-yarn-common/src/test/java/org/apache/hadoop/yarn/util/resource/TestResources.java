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

package org.apache.hadoop.yarn.util.resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.apache.hadoop.yarn.util.resource.Resources.componentwiseMin;
import static org.apache.hadoop.yarn.util.resource.Resources.componentwiseMax;
import static org.apache.hadoop.yarn.util.resource.Resources.add;
import static org.apache.hadoop.yarn.util.resource.Resources.multiplyAndRoundUp;
import static org.apache.hadoop.yarn.util.resource.Resources.subtract;
import static org.apache.hadoop.yarn.util.resource.Resources.multiply;
import static org.apache.hadoop.yarn.util.resource.Resources.multiplyAndAddTo;
import static org.apache.hadoop.yarn.util.resource.Resources.multiplyAndRoundDown;
import static org.apache.hadoop.yarn.util.resource.Resources.fitsIn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestResources {
  private static final String INVALID_RESOURCE_MSG = "Invalid resource value";

  static class ExtendedResources extends Resources {
    public static Resource unbounded() {
      return new FixedValueResource("UNBOUNDED", Long.MAX_VALUE);
    }

    public static Resource none() {
      return new FixedValueResource("NONE", 0L);
    }
  }

  private static final String EXTRA_RESOURCE_TYPE = "resource2";
  private String resourceTypesFile;

  private void setupExtraResourceType() throws Exception {
    Configuration conf = new YarnConfiguration();
    resourceTypesFile =
        TestResourceUtils.setupResourceTypes(conf, "resource-types-3.xml");
  }

  private void unsetExtraResourceType() {
    deleteResourceTypesFile();
    ResourceUtils.resetResourceTypes();
  }

  private void deleteResourceTypesFile() {
    if (resourceTypesFile != null && !resourceTypesFile.isEmpty()) {
      File resourceFile = new File(resourceTypesFile);
      resourceFile.delete();
    }
  }

  @Before
  public void setup() throws Exception {
    setupExtraResourceType();
  }

  @After
  public void teardown() {
    deleteResourceTypesFile();
  }

  public Resource createResource(long memory, int vCores) {
    return Resource.newInstance(memory, vCores);
  }

  public Resource createResource(long memory, int vCores, long resource2) {
    Resource ret = Resource.newInstance(memory, vCores);
    ret.setResourceInformation(EXTRA_RESOURCE_TYPE,
        ResourceInformation.newInstance(EXTRA_RESOURCE_TYPE, resource2));
    return ret;
  }

  @Test(timeout = 10000)
  public void testCompareToWithUnboundedResource() {
    unsetExtraResourceType();
    Resource unboundedClone = Resources.clone(ExtendedResources.unbounded());
    assertTrue(unboundedClone
        .compareTo(createResource(Long.MAX_VALUE, Integer.MAX_VALUE)) == 0);
    assertTrue(unboundedClone.compareTo(createResource(Long.MAX_VALUE, 0)) > 0);
    assertTrue(
        unboundedClone.compareTo(createResource(0, Integer.MAX_VALUE)) > 0);
  }

  @Test(timeout = 10000)
  public void testCompareToWithNoneResource() {
    assertTrue(Resources.none().compareTo(createResource(0, 0)) == 0);
    assertTrue(Resources.none().compareTo(createResource(1, 0)) < 0);
    assertTrue(Resources.none().compareTo(createResource(0, 1)) < 0);
    assertTrue(Resources.none().compareTo(createResource(0, 0, 0)) == 0);
    assertTrue(Resources.none().compareTo(createResource(1, 0, 0)) < 0);
    assertTrue(Resources.none().compareTo(createResource(0, 1, 0)) < 0);
    assertTrue(Resources.none().compareTo(createResource(0, 0, 1)) < 0);
  }

  @Test(timeout = 1000)
  public void testFitsIn() {
    assertTrue(fitsIn(createResource(1, 1), createResource(2, 2)));
    assertTrue(fitsIn(createResource(2, 2), createResource(2, 2)));
    assertFalse(fitsIn(createResource(2, 2), createResource(1, 1)));
    assertFalse(fitsIn(createResource(1, 2), createResource(2, 1)));
    assertFalse(fitsIn(createResource(2, 1), createResource(1, 2)));
    assertTrue(fitsIn(createResource(1, 1, 1), createResource(2, 2, 2)));
    assertTrue(fitsIn(createResource(1, 1, 0), createResource(2, 2, 0)));
    assertTrue(fitsIn(createResource(1, 1, 1), createResource(2, 2, 2)));
  }

  @Test(timeout = 1000)
  public void testComponentwiseMin() {
    assertEquals(createResource(1, 1),
        componentwiseMin(createResource(1, 1), createResource(2, 2)));
    assertEquals(createResource(1, 1),
        componentwiseMin(createResource(2, 2), createResource(1, 1)));
    assertEquals(createResource(1, 1),
        componentwiseMin(createResource(1, 2), createResource(2, 1)));
    assertEquals(createResource(1, 1, 1),
        componentwiseMin(createResource(1, 1, 1), createResource(2, 2, 2)));
    assertEquals(createResource(1, 1, 0),
        componentwiseMin(createResource(2, 2, 2), createResource(1, 1)));
    assertEquals(createResource(1, 1, 2),
        componentwiseMin(createResource(1, 2, 2), createResource(2, 1, 3)));
  }

  @Test
  public void testComponentwiseMax() {
    assertEquals(createResource(2, 2),
        componentwiseMax(createResource(1, 1), createResource(2, 2)));
    assertEquals(createResource(2, 2),
        componentwiseMax(createResource(2, 2), createResource(1, 1)));
    assertEquals(createResource(2, 2),
        componentwiseMax(createResource(1, 2), createResource(2, 1)));
    assertEquals(createResource(2, 2, 2),
        componentwiseMax(createResource(1, 1, 1), createResource(2, 2, 2)));
    assertEquals(createResource(2, 2, 2),
        componentwiseMax(createResource(2, 2, 2), createResource(1, 1)));
    assertEquals(createResource(2, 2, 3),
        componentwiseMax(createResource(1, 2, 2), createResource(2, 1, 3)));
    assertEquals(createResource(2, 2, 1),
        componentwiseMax(createResource(2, 2, 0), createResource(2, 1, 1)));
  }

  @Test
  public void testAdd() {
    assertEquals(createResource(2, 3),
        add(createResource(1, 1), createResource(1, 2)));
    assertEquals(createResource(3, 2),
        add(createResource(1, 1), createResource(2, 1)));
    assertEquals(createResource(2, 2, 0),
        add(createResource(1, 1, 0), createResource(1, 1, 0)));
    assertEquals(createResource(2, 2, 3),
        add(createResource(1, 1, 1), createResource(1, 1, 2)));
  }

  @Test
  public void testSubtract() {
    assertEquals(createResource(1, 0),
        subtract(createResource(2, 1), createResource(1, 1)));
    assertEquals(createResource(0, 1),
        subtract(createResource(1, 2), createResource(1, 1)));
    assertEquals(createResource(2, 2, 0),
        subtract(createResource(3, 3, 0), createResource(1, 1, 0)));
    assertEquals(createResource(1, 1, 2),
        subtract(createResource(2, 2, 3), createResource(1, 1, 1)));
  }

  @Test
  public void testClone() {
    assertEquals(createResource(1, 1), Resources.clone(createResource(1, 1)));
    assertEquals(createResource(1, 1, 0),
        Resources.clone(createResource(1, 1)));
    assertEquals(createResource(1, 1),
        Resources.clone(createResource(1, 1, 0)));
    assertEquals(createResource(1, 1, 2),
        Resources.clone(createResource(1, 1, 2)));
  }

  @Test
  public void testMultiply() {
    assertEquals(createResource(4, 2), multiply(createResource(2, 1), 2));
    assertEquals(createResource(4, 2, 0), multiply(createResource(2, 1), 2));
    assertEquals(createResource(2, 4), multiply(createResource(1, 2), 2));
    assertEquals(createResource(2, 4, 0), multiply(createResource(1, 2), 2));
    assertEquals(createResource(6, 6, 0), multiply(createResource(3, 3, 0), 2));
    assertEquals(createResource(4, 4, 6), multiply(createResource(2, 2, 3), 2));
  }

  @Test(timeout=10000)
  public void testMultiplyRoundUp() {
    final double by = 0.5;
    final String memoryErrorMsg = "Invalid memory size.";
    final String vcoreErrorMsg = "Invalid virtual core number.";
    Resource resource = Resources.createResource(1, 1);
    Resource result = Resources.multiplyAndRoundUp(resource, by);
    assertEquals(memoryErrorMsg, result.getMemorySize(), 1);
    assertEquals(vcoreErrorMsg, result.getVirtualCores(), 1);

    resource = Resources.createResource(2, 2);
    result = Resources.multiplyAndRoundUp(resource, by);
    assertEquals(memoryErrorMsg, result.getMemorySize(), 1);
    assertEquals(vcoreErrorMsg, result.getVirtualCores(), 1);

    resource = Resources.createResource(0, 0);
    result = Resources.multiplyAndRoundUp(resource, by);
    assertEquals(memoryErrorMsg, result.getMemorySize(), 0);
    assertEquals(vcoreErrorMsg, result.getVirtualCores(), 0);
  }

  @Test
  public void testMultiplyAndRoundUpCustomResources() {
    assertEquals(INVALID_RESOURCE_MSG, createResource(5, 2, 8),
        multiplyAndRoundUp(createResource(3, 1, 5), 1.5));
    assertEquals(INVALID_RESOURCE_MSG, createResource(5, 2, 0),
        multiplyAndRoundUp(createResource(3, 1, 0), 1.5));
    assertEquals(INVALID_RESOURCE_MSG, createResource(5, 5, 0),
        multiplyAndRoundUp(createResource(3, 3, 0), 1.5));
    assertEquals(INVALID_RESOURCE_MSG, createResource(8, 3, 13),
        multiplyAndRoundUp(createResource(3, 1, 5), 2.5));
    assertEquals(INVALID_RESOURCE_MSG, createResource(8, 3, 0),
        multiplyAndRoundUp(createResource(3, 1, 0), 2.5));
    assertEquals(INVALID_RESOURCE_MSG, createResource(8, 8, 0),
        multiplyAndRoundUp(createResource(3, 3, 0), 2.5));
  }

  @Test
  public void testMultiplyAndRoundDown() {
    assertEquals(INVALID_RESOURCE_MSG, createResource(4, 1),
        multiplyAndRoundDown(createResource(3, 1), 1.5));
    assertEquals(INVALID_RESOURCE_MSG, createResource(4, 1, 0),
        multiplyAndRoundDown(createResource(3, 1), 1.5));
    assertEquals(INVALID_RESOURCE_MSG, createResource(1, 4),
        multiplyAndRoundDown(createResource(1, 3), 1.5));
    assertEquals(INVALID_RESOURCE_MSG, createResource(1, 4, 0),
        multiplyAndRoundDown(createResource(1, 3), 1.5));
    assertEquals(INVALID_RESOURCE_MSG, createResource(7, 7, 0),
        multiplyAndRoundDown(createResource(3, 3, 0), 2.5));
    assertEquals(INVALID_RESOURCE_MSG, createResource(2, 2, 7),
        multiplyAndRoundDown(createResource(1, 1, 3), 2.5));
  }

  @Test
  public void testMultiplyAndAddTo() throws Exception {
    unsetExtraResourceType();
    setupExtraResourceType();
    assertEquals(createResource(6, 4),
        multiplyAndAddTo(createResource(3, 1), createResource(2, 2), 1.5));
    assertEquals(createResource(6, 4, 0),
        multiplyAndAddTo(createResource(3, 1), createResource(2, 2), 1.5));
    assertEquals(createResource(4, 7),
        multiplyAndAddTo(createResource(1, 1), createResource(2, 4), 1.5));
    assertEquals(createResource(4, 7, 0),
        multiplyAndAddTo(createResource(1, 1), createResource(2, 4), 1.5));
    assertEquals(createResource(6, 4, 0),
        multiplyAndAddTo(createResource(3, 1, 0), createResource(2, 2, 0),
            1.5));
    assertEquals(createResource(6, 4, 6),
        multiplyAndAddTo(createResource(3, 1, 2), createResource(2, 2, 3),
            1.5));
  }

  @Test
  public void testCreateResourceWithSameLongValue() throws Exception {
    unsetExtraResourceType();
    setupExtraResourceType();

    Resource res = ResourceUtils.createResourceWithSameValue(11L);
    assertEquals(11L, res.getMemorySize());
    assertEquals(11, res.getVirtualCores());
    assertEquals(11L, res.getResourceInformation(EXTRA_RESOURCE_TYPE).getValue());
  }

  @Test
  public void testCreateResourceWithSameIntValue() throws Exception {
    unsetExtraResourceType();
    setupExtraResourceType();

    Resource res = ResourceUtils.createResourceWithSameValue(11);
    assertEquals(11, res.getMemorySize());
    assertEquals(11, res.getVirtualCores());
    assertEquals(11, res.getResourceInformation(EXTRA_RESOURCE_TYPE).getValue());
  }

  @Test
  public void testCreateSimpleResourceWithSameLongValue() {
    Resource res = ResourceUtils.createResourceWithSameValue(11L);
    assertEquals(11L, res.getMemorySize());
    assertEquals(11, res.getVirtualCores());
  }

  @Test
  public void testCreateSimpleResourceWithSameIntValue() {
    Resource res = ResourceUtils.createResourceWithSameValue(11);
    assertEquals(11, res.getMemorySize());
    assertEquals(11, res.getVirtualCores());
  }
}
