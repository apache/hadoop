/*
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

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestResourceCalculator {
  private final ResourceCalculator resourceCalculator;

  @Parameterized.Parameters
  public static Collection<ResourceCalculator[]> getParameters() {
    return Arrays.asList(new ResourceCalculator[][] {
        { new DefaultResourceCalculator() },
        { new DominantResourceCalculator() } });
  }

  @Before
  public void setupNoExtraResource() {
    // This has to run before each test because we don't know when
    // setupExtraResource() might be called
    ResourceUtils.resetResourceTypes(new Configuration());
  }

  private static void setupExtraResource() {
    Configuration conf = new Configuration();

    conf.set(YarnConfiguration.RESOURCE_TYPES, "test");
    ResourceUtils.resetResourceTypes(conf);
  }

  public TestResourceCalculator(ResourceCalculator rs) {
    this.resourceCalculator = rs;
  }
  
  @Test(timeout = 10000)
  public void testFitsIn() {

    if (resourceCalculator instanceof DefaultResourceCalculator) {
      Assert.assertTrue(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(2, 1)));
      Assert.assertTrue(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(2, 2)));
      Assert.assertTrue(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(1, 2)));
      Assert.assertTrue(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(1, 1)));
      Assert.assertFalse(resourceCalculator.fitsIn(
          Resource.newInstance(2, 1), Resource.newInstance(1, 2)));
    } else if (resourceCalculator instanceof DominantResourceCalculator) {
      Assert.assertFalse(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(2, 1)));
      Assert.assertTrue(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(2, 2)));
      Assert.assertTrue(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(1, 2)));
      Assert.assertFalse(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(1, 1)));
      Assert.assertFalse(resourceCalculator.fitsIn(
          Resource.newInstance(2, 1), Resource.newInstance(1, 2)));
    }
  }

  private Resource newResource(long memory, int cpu) {
    Resource res = Resource.newInstance(memory, cpu);

    return res;
  }

  private Resource newResource(long memory, int cpu, int test) {
    Resource res = newResource(memory, cpu);

    res.setResourceValue("test", test);

    return res;
  }

  /**
   * Test that the compare() method returns the expected result (0, -1, or 1).
   * If the expected result is not 0, this method will also test the resources
   * in the opposite order and check for the negative of the expected result.
   *
   * @param cluster the cluster resource
   * @param res1 the LHS resource
   * @param res2 the RHS resource
   * @param expected the expected result
   */
  private void assertComparison(Resource cluster, Resource res1, Resource res2,
      int expected) {
    int actual = resourceCalculator.compare(cluster, res1, res2);

    assertEquals(String.format("Resource comparison did not give the expected "
        + "result for %s v/s %s", res1.toString(), res2.toString()),
        expected, actual);

    if (expected != 0) {
      // Try again with args in the opposite order and the negative of the
      // expected result.
      actual = resourceCalculator.compare(cluster, res2, res1);
      assertEquals(String.format("Resource comparison did not give the "
          + "expected result for %s v/s %s", res2.toString(), res1.toString()),
          expected * -1, actual);
    }
  }

  @Test
  public void testCompareWithOnlyMandatory() {
    // This test is necessary because there are optimizations that are only
    // triggered when only the mandatory resources are configured.

    // Keep cluster resources even so that the numbers are easy to understand
    Resource cluster = newResource(4, 4);

    assertComparison(cluster, newResource(1, 1), newResource(1, 1), 0);
    assertComparison(cluster, newResource(0, 0), newResource(0, 0), 0);
    assertComparison(cluster, newResource(2, 2), newResource(1, 1), 1);
    assertComparison(cluster, newResource(2, 2), newResource(0, 0), 1);

    if (resourceCalculator instanceof DefaultResourceCalculator) {
      testCompareDefaultWithOnlyMandatory(cluster);
    } else if (resourceCalculator instanceof DominantResourceCalculator) {
      testCompareDominantWithOnlyMandatory(cluster);
    }
  }

  private void testCompareDefaultWithOnlyMandatory(Resource cluster) {
    assertComparison(cluster, newResource(1, 1), newResource(1, 1), 0);
    assertComparison(cluster, newResource(1, 2), newResource(1, 1), 0);
    assertComparison(cluster, newResource(1, 1), newResource(1, 0), 0);
    assertComparison(cluster, newResource(2, 1), newResource(1, 1), 1);
    assertComparison(cluster, newResource(2, 1), newResource(1, 2), 1);
    assertComparison(cluster, newResource(2, 1), newResource(1, 0), 1);
  }

  private void testCompareDominantWithOnlyMandatory(Resource cluster) {
    assertComparison(cluster, newResource(2, 1), newResource(2, 1), 0);
    assertComparison(cluster, newResource(2, 1), newResource(1, 2), 0);
    assertComparison(cluster, newResource(2, 1), newResource(1, 1), 1);
    assertComparison(cluster, newResource(2, 2), newResource(2, 1), 1);
    assertComparison(cluster, newResource(2, 2), newResource(1, 2), 1);
    assertComparison(cluster, newResource(3, 1), newResource(3, 0), 1);
  }

  @Test
  public void testCompare() {
    // Test with 3 resources
    setupExtraResource();

    // Keep cluster resources even so that the numbers are easy to understand
    Resource cluster = newResource(4L, 4, 4);

    assertComparison(cluster, newResource(1, 1, 1), newResource(1, 1, 1), 0);
    assertComparison(cluster, newResource(0, 0, 0), newResource(0, 0, 0), 0);
    assertComparison(cluster, newResource(2, 2, 2), newResource(1, 1, 1), 1);
    assertComparison(cluster, newResource(2, 2, 2), newResource(0, 0, 0), 1);

    if (resourceCalculator instanceof DefaultResourceCalculator) {
      testCompareDefault(cluster);
    } else if (resourceCalculator instanceof DominantResourceCalculator) {
      testCompareDominant(cluster);
      testCompareDominantZeroValueResource();
    }
  }

  private void testCompareDefault(Resource cluster) {
    assertComparison(cluster, newResource(1, 1, 2), newResource(1, 1, 1), 0);
    assertComparison(cluster, newResource(1, 2, 1), newResource(1, 1, 1), 0);
    assertComparison(cluster, newResource(1, 2, 2), newResource(1, 1, 1), 0);
    assertComparison(cluster, newResource(1, 2, 2), newResource(1, 0, 0), 0);
    assertComparison(cluster, newResource(2, 1, 1), newResource(1, 1, 1), 1);
    assertComparison(cluster, newResource(2, 1, 1), newResource(1, 2, 1), 1);
    assertComparison(cluster, newResource(2, 1, 1), newResource(1, 1, 2), 1);
    assertComparison(cluster, newResource(2, 1, 1), newResource(1, 2, 2), 1);
    assertComparison(cluster, newResource(2, 1, 1), newResource(1, 0, 0), 1);
  }

  /**
   * Verify compare when one or all the resource are zero.
   */
  private void testCompareDominantZeroValueResource(){
    Resource cluster = newResource(4L, 4, 0);
    assertComparison(cluster, newResource(2, 1, 1), newResource(1, 1, 2), 1);
    assertComparison(cluster, newResource(2, 2, 1), newResource(1, 2, 2), 1);
    assertComparison(cluster, newResource(2, 2, 1), newResource(2, 2, 2), 0);
    assertComparison(cluster, newResource(0, 2, 1), newResource(0, 2, 2), 0);
    assertComparison(cluster, newResource(0, 1, 2), newResource(1, 1, 2), -1);
    assertComparison(cluster, newResource(1, 1, 2), newResource(2, 1, 2), -1);

    // cluster resource zero
    cluster = newResource(0, 0, 0);
    assertComparison(cluster, newResource(2, 1, 1), newResource(1, 1, 1), 1);
    assertComparison(cluster, newResource(2, 2, 2), newResource(1, 1, 1), 1);
    assertComparison(cluster, newResource(2, 1, 1), newResource(1, 2, 1), 0);
    assertComparison(cluster, newResource(1, 1, 1), newResource(1, 1, 1), 0);
    assertComparison(cluster, newResource(1, 1, 1), newResource(1, 1, 2), -1);
    assertComparison(cluster, newResource(1, 1, 1), newResource(1, 2, 1), -1);
  }

  private void testCompareDominant(Resource cluster) {
    assertComparison(cluster, newResource(2, 1, 1), newResource(2, 1, 1), 0);
    assertComparison(cluster, newResource(2, 1, 1), newResource(1, 2, 1), 0);
    assertComparison(cluster, newResource(2, 1, 1), newResource(1, 1, 2), 0);
    assertComparison(cluster, newResource(2, 1, 0), newResource(0, 1, 2), 0);
    assertComparison(cluster, newResource(2, 2, 1), newResource(1, 2, 2), 0);
    assertComparison(cluster, newResource(2, 2, 1), newResource(2, 1, 2), 0);
    assertComparison(cluster, newResource(2, 2, 1), newResource(2, 2, 1), 0);
    assertComparison(cluster, newResource(2, 2, 0), newResource(2, 0, 2), 0);
    assertComparison(cluster, newResource(3, 2, 1), newResource(3, 2, 1), 0);
    assertComparison(cluster, newResource(3, 2, 1), newResource(3, 1, 2), 0);
    assertComparison(cluster, newResource(3, 2, 1), newResource(1, 2, 3), 0);
    assertComparison(cluster, newResource(3, 2, 1), newResource(1, 3, 2), 0);
    assertComparison(cluster, newResource(3, 2, 1), newResource(2, 1, 3), 0);
    assertComparison(cluster, newResource(3, 2, 1), newResource(2, 3, 1), 0);
    assertComparison(cluster, newResource(2, 1, 1), newResource(1, 1, 1), 1);
    assertComparison(cluster, newResource(2, 1, 1), newResource(1, 1, 0), 1);
    assertComparison(cluster, newResource(2, 2, 1), newResource(2, 1, 1), 1);
    assertComparison(cluster, newResource(2, 2, 1), newResource(1, 2, 1), 1);
    assertComparison(cluster, newResource(2, 2, 1), newResource(1, 1, 2), 1);
    assertComparison(cluster, newResource(2, 2, 1), newResource(0, 2, 2), 1);
    assertComparison(cluster, newResource(2, 2, 2), newResource(2, 1, 1), 1);
    assertComparison(cluster, newResource(2, 2, 2), newResource(1, 2, 1), 1);
    assertComparison(cluster, newResource(2, 2, 2), newResource(1, 1, 2), 1);
    assertComparison(cluster, newResource(2, 2, 2), newResource(2, 2, 1), 1);
    assertComparison(cluster, newResource(2, 2, 2), newResource(2, 1, 2), 1);
    assertComparison(cluster, newResource(2, 2, 2), newResource(1, 2, 2), 1);
    assertComparison(cluster, newResource(3, 2, 1), newResource(2, 2, 2), 1);
    assertComparison(cluster, newResource(3, 1, 1), newResource(2, 2, 2), 1);
    assertComparison(cluster, newResource(3, 1, 1), newResource(3, 1, 0), 1);
    assertComparison(cluster, newResource(3, 1, 1), newResource(3, 0, 0), 1);
  }

  @Test(timeout = 10000)
  public void testCompareWithEmptyCluster() {
    Resource clusterResource = Resource.newInstance(0, 0);

    // For lhs == rhs
    Resource lhs = Resource.newInstance(0, 0);
    Resource rhs = Resource.newInstance(0, 0);
    assertResourcesOperations(clusterResource, lhs, rhs, false, true, false,
        true, lhs, lhs);

    // lhs > rhs
    lhs = Resource.newInstance(1, 1);
    rhs = Resource.newInstance(0, 0);
    assertResourcesOperations(clusterResource, lhs, rhs, false, false, true,
        true, lhs, rhs);

    // For lhs < rhs
    lhs = Resource.newInstance(0, 0);
    rhs = Resource.newInstance(1, 1);
    assertResourcesOperations(clusterResource, lhs, rhs, true, true, false,
        false, rhs, lhs);

    if (!(resourceCalculator instanceof DominantResourceCalculator)) {
      return;
    }

    // verify for 2 dimensional resources i.e memory and cpu
    // dominant resource types
    lhs = Resource.newInstance(1, 0);
    rhs = Resource.newInstance(0, 1);
    assertResourcesOperations(clusterResource, lhs, rhs, false, true, false,
        true, lhs, lhs);

    lhs = Resource.newInstance(0, 1);
    rhs = Resource.newInstance(1, 0);
    assertResourcesOperations(clusterResource, lhs, rhs, false, true, false,
        true, lhs, lhs);

    lhs = Resource.newInstance(1, 1);
    rhs = Resource.newInstance(1, 0);
    assertResourcesOperations(clusterResource, lhs, rhs, false, false, true,
        true, lhs, rhs);

    lhs = Resource.newInstance(0, 1);
    rhs = Resource.newInstance(1, 1);
    assertResourcesOperations(clusterResource, lhs, rhs, true, true, false,
        false, rhs, lhs);

  }

  private void assertResourcesOperations(Resource clusterResource,
      Resource lhs, Resource rhs, boolean lessThan, boolean lessThanOrEqual,
      boolean greaterThan, boolean greaterThanOrEqual, Resource max,
      Resource min) {

    assertEquals("Less Than operation is wrongly calculated.", lessThan,
        Resources.lessThan(resourceCalculator, clusterResource, lhs, rhs));

    assertEquals(
        "Less Than Or Equal To operation is wrongly calculated.",
        lessThanOrEqual, Resources.lessThanOrEqual(resourceCalculator,
            clusterResource, lhs, rhs));

    assertEquals("Greater Than operation is wrongly calculated.",
        greaterThan,
        Resources.greaterThan(resourceCalculator, clusterResource, lhs, rhs));

    assertEquals(
        "Greater Than Or Equal To operation is wrongly calculated.",
        greaterThanOrEqual, Resources.greaterThanOrEqual(resourceCalculator,
            clusterResource, lhs, rhs));

    assertEquals("Max(value) Operation wrongly calculated.", max,
        Resources.max(resourceCalculator, clusterResource, lhs, rhs));

    assertEquals("Min(value) operation is wrongly calculated.", min,
        Resources.min(resourceCalculator, clusterResource, lhs, rhs));
  }

  /**
   * Test resource normalization.
   */
  @Test(timeout = 10000)
  public void testNormalize() {
    // requested resources value cannot be an arbitrary number.
    Resource ask = Resource.newInstance(1111, 2);
    Resource min = Resource.newInstance(1024, 1);
    Resource max = Resource.newInstance(8 * 1024, 8);
    Resource increment = Resource.newInstance(1024, 4);
    if (resourceCalculator instanceof DefaultResourceCalculator) {
      Resource result = Resources.normalize(resourceCalculator,
          ask, min, max, increment);

      assertEquals(2 * 1024, result.getMemorySize());
    } else if (resourceCalculator instanceof DominantResourceCalculator) {
      Resource result = Resources.normalize(resourceCalculator,
          ask, min, max, increment);

      assertEquals(2 * 1024, result.getMemorySize());
      assertEquals(4, result.getVirtualCores());
    }

    // if resources asked are less than minimum resource, then normalize it to
    // minimum resource.
    ask = Resource.newInstance(512, 0);
    min = Resource.newInstance(2 * 1024, 2);
    max = Resource.newInstance(8 * 1024, 8);
    increment = Resource.newInstance(1024, 1);
    if (resourceCalculator instanceof DefaultResourceCalculator) {
      Resource result = Resources.normalize(resourceCalculator,
          ask, min, max, increment);

      assertEquals(2 * 1024, result.getMemorySize());
    } else if (resourceCalculator instanceof DominantResourceCalculator) {
      Resource result = Resources.normalize(resourceCalculator,
          ask, min, max, increment);

      assertEquals(2 * 1024, result.getMemorySize());
      assertEquals(2, result.getVirtualCores());
    }

    // if resources asked are larger than maximum resource, then normalize it to
    // maximum resources.
    ask = Resource.newInstance(9 * 1024, 9);
    min = Resource.newInstance(2 * 1024, 2);
    max = Resource.newInstance(8 * 1024, 8);
    increment = Resource.newInstance(1024, 1);
    if (resourceCalculator instanceof DefaultResourceCalculator) {
      Resource result = Resources.normalize(resourceCalculator,
          ask, min, max, increment);

      assertEquals(8 * 1024, result.getMemorySize());
    } else if (resourceCalculator instanceof DominantResourceCalculator) {
      Resource result = Resources.normalize(resourceCalculator,
          ask, min, max, increment);

      assertEquals(8 * 1024, result.getMemorySize());
      assertEquals(8, result.getVirtualCores());
    }

    // if increment is 0, use minimum resource as the increment resource.
    ask = Resource.newInstance(1111, 2);
    min = Resource.newInstance(2 * 1024, 2);
    max = Resource.newInstance(8 * 1024, 8);
    increment = Resource.newInstance(0, 0);
    if (resourceCalculator instanceof DefaultResourceCalculator) {
      Resource result = Resources.normalize(resourceCalculator,
          ask, min, max, increment);

      assertEquals(2 * 1024, result.getMemorySize());
    } else if (resourceCalculator instanceof DominantResourceCalculator) {
      Resource result = Resources.normalize(resourceCalculator,
          ask, min, max, increment);

      assertEquals(2 * 1024, result.getMemorySize());
      assertEquals(2, result.getVirtualCores());
    }
  }
}