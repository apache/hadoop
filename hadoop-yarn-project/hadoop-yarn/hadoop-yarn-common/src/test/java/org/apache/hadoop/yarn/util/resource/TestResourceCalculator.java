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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestResourceCalculator {
  private static final String EXTRA_RESOURCE_NAME = "test";

  private ResourceCalculator resourceCalculator;

  public static Collection<Object[]> getParameters() {
    return Arrays.asList(new Object[][]{
        {"DefaultResourceCalculator", new DefaultResourceCalculator()},
        {"DominantResourceCalculator", new DominantResourceCalculator()}});
  }

  @BeforeEach
  public void setupNoExtraResource() {
    // This has to run before each test because we don't know when
    // setupExtraResource() might be called
    ResourceUtils.resetResourceTypes(new Configuration());
  }

  private static void setupExtraResource() {
    Configuration conf = new Configuration();

    conf.set(YarnConfiguration.RESOURCE_TYPES, EXTRA_RESOURCE_NAME);
    ResourceUtils.resetResourceTypes(conf);
  }

  public void initTestResourceCalculator(String name, ResourceCalculator rs) {
    this.resourceCalculator = rs;
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  @Timeout(10000)
  void testFitsIn(String name, ResourceCalculator rs) {

    initTestResourceCalculator(name, rs);

    if (resourceCalculator instanceof DefaultResourceCalculator) {
      assertTrue(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(2, 1)));
      assertTrue(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(2, 2)));
      assertTrue(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(1, 2)));
      assertTrue(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(1, 1)));
      assertFalse(resourceCalculator.fitsIn(
          Resource.newInstance(2, 1), Resource.newInstance(1, 2)));
    } else if (resourceCalculator instanceof DominantResourceCalculator) {
      assertFalse(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(2, 1)));
      assertTrue(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(2, 2)));
      assertTrue(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(1, 2)));
      assertFalse(resourceCalculator.fitsIn(
          Resource.newInstance(1, 2), Resource.newInstance(1, 1)));
      assertFalse(resourceCalculator.fitsIn(
          Resource.newInstance(2, 1), Resource.newInstance(1, 2)));
    }
  }

  private Resource newResource(long memory, int cpu) {
    Resource res = Resource.newInstance(memory, cpu);

    return res;
  }

  private Resource newResource(long memory, int cpu, int extraResource) {
    Resource res = newResource(memory, cpu);

    res.setResourceValue(EXTRA_RESOURCE_NAME, extraResource);

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

    assertEquals(expected, actual, String.format("Resource comparison did not give the expected "
        + "result for %s v/s %s", res1.toString(), res2.toString()));

    if (expected != 0) {
      // Try again with args in the opposite order and the negative of the
      // expected result.
      actual = resourceCalculator.compare(cluster, res2, res1);
      assertEquals(expected * -1, actual, String.format("Resource comparison did not give the "
          + "expected result for %s v/s %s", res2.toString(), res1.toString()));
    }
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  void testCompareWithOnlyMandatory(String name, ResourceCalculator rs) {
    initTestResourceCalculator(name, rs);
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

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  void testCompare(String name, ResourceCalculator rs) {
    initTestResourceCalculator(name, rs);
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
  private void testCompareDominantZeroValueResource() {
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

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  @Timeout(10000)
  void testCompareWithEmptyCluster(String name, ResourceCalculator rs) {
    initTestResourceCalculator(name, rs);
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

    assertEquals(lessThan,
        Resources.lessThan(resourceCalculator, clusterResource, lhs, rhs),
        "Less Than operation is wrongly calculated.");

    assertEquals(
        lessThanOrEqual, Resources.lessThanOrEqual(resourceCalculator,
            clusterResource, lhs, rhs), "Less Than Or Equal To operation is wrongly calculated.");

    assertEquals(greaterThan,
        Resources.greaterThan(resourceCalculator, clusterResource, lhs, rhs),
        "Greater Than operation is wrongly calculated.");

    assertEquals(greaterThanOrEqual,
        Resources.greaterThanOrEqual(resourceCalculator, clusterResource, lhs, rhs),
        "Greater Than Or Equal To operation is wrongly calculated.");

    assertEquals(max,
        Resources.max(resourceCalculator, clusterResource, lhs, rhs),
        "Max(value) Operation wrongly calculated.");

    assertEquals(min,
        Resources.min(resourceCalculator, clusterResource, lhs, rhs),
        "Min(value) operation is wrongly calculated.");
  }

  /**
   * Test resource normalization.
   */
  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  @Timeout(10000)
  void testNormalize(String name, ResourceCalculator rs) {
    initTestResourceCalculator(name, rs);
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

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  void testDivisionByZeroRatioDenominatorIsZero(String name, ResourceCalculator rs) {
    initTestResourceCalculator(name, rs);
    float ratio = resourceCalculator.ratio(newResource(1, 1), newResource(0,
        0));
    assertEquals(Float.POSITIVE_INFINITY, ratio, 0.00001);
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  void testDivisionByZeroRatioNumeratorAndDenominatorIsZero(String name, ResourceCalculator rs) {
    initTestResourceCalculator(name, rs);
    float ratio = resourceCalculator.ratio(newResource(0, 0), newResource(0,
        0));
    assertEquals(0.0, ratio, 0.00001);
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  void testFitsInDiagnosticsCollector(String name, ResourceCalculator rs) {
    initTestResourceCalculator(name, rs);
    if (resourceCalculator instanceof DefaultResourceCalculator) {
      // required-resource = (0, 0)
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 0),
              newResource(0, 0)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 0),
              newResource(0, 1)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 0),
              newResource(1, 0)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 0),
              newResource(1, 1)));

      // required-resource = (0, 1)
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 1),
              newResource(0, 0)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 1),
              newResource(0, 1)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 1),
              newResource(1, 0)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 1),
              newResource(1, 1)));

      // required-resource = (1, 0)
      assertEquals(ImmutableSet.of(ResourceInformation.MEMORY_URI),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 0),
              newResource(0, 0)));
      assertEquals(ImmutableSet.of(ResourceInformation.MEMORY_URI),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 0),
              newResource(0, 1)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 0),
              newResource(1, 0)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 0),
              newResource(1, 1)));

      // required-resource = (1, 1)
      assertEquals(ImmutableSet.of(ResourceInformation.MEMORY_URI),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 1),
              newResource(0, 0)));
      assertEquals(ImmutableSet.of(ResourceInformation.MEMORY_URI),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 1),
              newResource(0, 1)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 1),
              newResource(1, 0)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 1),
              newResource(1, 1)));
    } else if (resourceCalculator instanceof DominantResourceCalculator) {
      // required-resource = (0, 0)
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 0),
              newResource(0, 0)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 0),
              newResource(0, 1)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 0),
              newResource(1, 0)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 0),
              newResource(1, 1)));

      // required-resource = (0, 1)
      assertEquals(ImmutableSet.of(ResourceInformation.VCORES_URI),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 1),
              newResource(0, 0)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 1),
              newResource(0, 1)));
      assertEquals(ImmutableSet.of(ResourceInformation.VCORES_URI),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 1),
              newResource(1, 0)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(0, 1),
              newResource(1, 1)));

      // required-resource = (1, 0)
      assertEquals(ImmutableSet.of(ResourceInformation.MEMORY_URI),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 0),
              newResource(0, 0)));
      assertEquals(ImmutableSet.of(ResourceInformation.MEMORY_URI),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 0),
              newResource(0, 1)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 0),
              newResource(1, 0)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 0),
              newResource(1, 1)));

      // required-resource = (1, 1)
      assertEquals(ImmutableSet.of(ResourceInformation.MEMORY_URI,
          ResourceInformation.VCORES_URI), resourceCalculator
          .getInsufficientResourceNames(newResource(1, 1), newResource(0, 0)));
      assertEquals(ImmutableSet.of(ResourceInformation.MEMORY_URI),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 1),
              newResource(0, 1)));
      assertEquals(ImmutableSet.of(ResourceInformation.VCORES_URI),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 1),
              newResource(1, 0)));
      assertEquals(ImmutableSet.of(),
          resourceCalculator.getInsufficientResourceNames(newResource(1, 1),
              newResource(1, 1)));
    }
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  void testRatioWithNoExtraResource(String name, ResourceCalculator rs) {
    initTestResourceCalculator(name, rs);
    //setup
    Resource resource1 = newResource(1, 1);
    Resource resource2 = newResource(2, 1);

    //act
    float ratio = resourceCalculator.ratio(resource1, resource2);

    //assert
    if (resourceCalculator instanceof DefaultResourceCalculator) {
      double ratioOfMemories = 0.5;
      assertEquals(ratioOfMemories, ratio, 0.00001);
    } else if (resourceCalculator instanceof DominantResourceCalculator) {
      double ratioOfCPUs = 1.0;
      assertEquals(ratioOfCPUs, ratio, 0.00001);
    }
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  void testRatioWithExtraResource(String name, ResourceCalculator rs) {
    initTestResourceCalculator(name, rs);
    //setup
    setupExtraResource();
    Resource resource1 = newResource(1, 1, 2);
    Resource resource2 = newResource(2, 1, 1);

    //act
    float ratio = resourceCalculator.ratio(resource1, resource2);

    //assert
    if (resourceCalculator instanceof DefaultResourceCalculator) {
      double ratioOfMemories = 0.5;
      assertEquals(ratioOfMemories, ratio, 0.00001);
    } else if (resourceCalculator instanceof DominantResourceCalculator) {
      double ratioOfExtraResources = 2.0;
      assertEquals(ratioOfExtraResources, ratio, 0.00001);
    }
  }
}