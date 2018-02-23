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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Comparator;
import java.util.Map;

import org.apache.curator.shaded.com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FakeSchedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy.DominantResourceFairnessComparatorN;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy.DominantResourceFairnessComparator2;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * comparator.compare(sched1, sched2) < 0 means that sched1 should get a
 * container before sched2
 */
public class TestDominantResourceFairnessPolicy {
  @Before
  public void setup() {
    addResources("test");
  }

  private Comparator<Schedulable> createComparator(int clusterMem,
      int clusterCpu) {
    DominantResourceFairnessPolicy policy =
        new DominantResourceFairnessPolicy();
    FSContext fsContext = mock(FSContext.class);
    when(fsContext.getClusterResource()).
        thenReturn(Resources.createResource(clusterMem, clusterCpu));
    policy.initialize(fsContext);
    return policy.getComparator();
  }
  
  private Schedulable createSchedulable(int memUsage, int cpuUsage) {
    return createSchedulable(memUsage, cpuUsage, 1.0f, 0, 0);
  }
  
  private Schedulable createSchedulable(int memUsage, int cpuUsage,
      int minMemShare, int minCpuShare) {
    return createSchedulable(memUsage, cpuUsage, 1.0f,
        minMemShare, minCpuShare);
  }
  
  private Schedulable createSchedulable(int memUsage, int cpuUsage,
      float weights) {
    return createSchedulable(memUsage, cpuUsage, weights, 0, 0);
  }

  private Schedulable createSchedulable(int memUsage, int cpuUsage,
      float weights, int minMemShare, int minCpuShare) {
    Resource usage = BuilderUtils.newResource(memUsage, cpuUsage);
    Resource minShare = BuilderUtils.newResource(minMemShare, minCpuShare);
    return new FakeSchedulable(minShare,
        Resources.createResource(Integer.MAX_VALUE, Integer.MAX_VALUE),
        weights, Resources.none(), usage, 0l);
  }
  
  @Test
  public void testSameDominantResource() {
    Comparator c = createComparator(8000, 4);
    Schedulable s1 = createSchedulable(1000, 1);
    Schedulable s2 = createSchedulable(2000, 1);

    assertTrue("Comparison didn't return a value less than 0",
        c.compare(s1, s2) < 0);
  }
  
  @Test
  public void testSameDominantResource2() {
    ResourceUtils.resetResourceTypes(new Configuration());
    testSameDominantResource();
  }

  @Test
  public void testDifferentDominantResource() {
    Comparator c = createComparator(8000, 8);
    Schedulable s1 = createSchedulable(4000, 3);
    Schedulable s2 = createSchedulable(2000, 5);

    assertTrue("Comparison didn't return a value less than 0",
        c.compare(s1, s2) < 0);
  }
  
  @Test
  public void testDifferentDominantResource2() {
    ResourceUtils.resetResourceTypes(new Configuration());
    testDifferentDominantResource();
  }

  @Test
  public void testOneIsNeedy() {
    Comparator c = createComparator(8000, 8);
    Schedulable s1 = createSchedulable(2000, 5, 0, 6);
    Schedulable s2 = createSchedulable(4000, 3, 0, 0);

    assertTrue("Comparison didn't return a value less than 0",
        c.compare(s1, s2) < 0);
  }
  
  @Test
  public void testOneIsNeedy2() {
    ResourceUtils.resetResourceTypes(new Configuration());
    testOneIsNeedy();
  }

  @Test
  public void testBothAreNeedy() {
    Comparator c = createComparator(8000, 100);
    // dominant share is 2000/8000
    Schedulable s1 = createSchedulable(2000, 5);
    // dominant share is 4000/8000
    Schedulable s2 = createSchedulable(4000, 3);

    assertTrue("Comparison didn't return a value less than 0",
        c.compare(s1, s2) < 0);

    // dominant min share is 2/3
    s1 = createSchedulable(2000, 5, 3000, 6);
    // dominant min share is 4/5
    s2 = createSchedulable(4000, 3, 5000, 4);

    assertTrue("Comparison didn't return a value less than 0",
        c.compare(s1, s2) < 0);
  }
  
  @Test
  public void testBothAreNeedy2() {
    ResourceUtils.resetResourceTypes(new Configuration());
    testBothAreNeedy();
  }

  @Test
  public void testEvenWeightsSameDominantResource() {
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(3000, 1, 2.0f),
        createSchedulable(2000, 1)) < 0);
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(1000, 3, 2.0f),
        createSchedulable(1000, 2)) < 0);
  }
  
  @Test
  public void testEvenWeightsSameDominantResource2() {
    ResourceUtils.resetResourceTypes(new Configuration());
    testEvenWeightsSameDominantResource();
  }

  @Test
  public void testEvenWeightsDifferentDominantResource() {
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(1000, 3, 2.0f),
        createSchedulable(2000, 1)) < 0);
    assertTrue(createComparator(8000, 8).compare(
        createSchedulable(3000, 1, 2.0f),
        createSchedulable(1000, 2)) < 0);
  }
  
  @Test
  public void testEvenWeightsDifferentDominantResource2() {
    ResourceUtils.resetResourceTypes(new Configuration());
    testEvenWeightsDifferentDominantResource();
  }

  @Test
  public void testSortShares() {
    float[][] ratios1 = {{0.3f, 2.0f}, {0.2f, 1.0f}, {0.4f, 0.1f}};
    float[][] ratios2 = {{0.2f, 9.0f}, {0.3f, 2.0f}, {0.25f, 0.1f}};
    float[][] expected1 = {{0.4f, 0.1f}, {0.3f, 2.0f}, {0.2f, 1.0f}};
    float[][] expected2 = {{0.3f, 2.0f}, {0.25f, 0.1f}, {0.2f, 9.0f}};
    DominantResourceFairnessComparatorN comparator =
        new DominantResourceFairnessComparatorN();

    comparator.sortRatios(ratios1, ratios2);

    for (int i = 0; i < ratios1.length; i++) {
      Assert.assertArrayEquals("The shares array was not sorted into the "
          + "expected order: incorrect inner array encountered",
          expected1[i], ratios1[i], 0.00001f);
      Assert.assertArrayEquals("The shares array was not sorted into the "
          + "expected order: incorrect inner array encountered",
          expected2[i], ratios2[i], 0.00001f);
    }
  }

  @Test
  public void testCalculateClusterAndFairRatios() {
    Map<String, Integer> index = ResourceUtils.getResourceTypeIndex();
    Resource used = Resources.createResource(10, 5);
    Resource capacity = Resources.createResource(100, 10);
    float[][] shares = new float[3][2];
    DominantResourceFairnessComparatorN comparator =
        new DominantResourceFairnessComparatorN();

    used.setResourceValue("test", 2L);
    capacity.setResourceValue("test", 5L);

    int dominant = comparator.calculateClusterAndFairRatios(used, capacity,
        shares, 1.0f);

    assertEquals("Calculated usage ratio for memory (10MB out of 100MB) is "
        + "incorrect", 0.1,
        shares[index.get(ResourceInformation.MEMORY_MB.getName())][0], .00001);
    assertEquals("Calculated usage ratio for vcores (5 out of 10) is "
        + "incorrect", 0.5,
        shares[index.get(ResourceInformation.VCORES.getName())][0], .00001);
    assertEquals("Calculated usage ratio for test resource (2 out of 5) is "
        + "incorrect", 0.4, shares[index.get("test")][0], .00001);
    assertEquals("The wrong dominant resource index was returned",
        index.get(ResourceInformation.VCORES.getName()).intValue(),
        dominant);
  }

  @Test
  public void testCalculateClusterAndFairRatios2() {
    ResourceUtils.resetResourceTypes(new Configuration());
    Resource used = Resources.createResource(10, 5);
    Resource capacity = Resources.createResource(100, 10);
    double[] shares = new double[2];
    DominantResourceFairnessComparator2 comparator =
        new DominantResourceFairnessComparator2();
    int dominant =
        comparator.calculateClusterAndFairRatios(used.getResources(), 1.0f,
            capacity.getResources(), shares);

    assertEquals("Calculated usage ratio for memory (10MB out of 100MB) is "
        + "incorrect", 0.1, shares[Resource.MEMORY_INDEX], .00001);
    assertEquals("Calculated usage ratio for vcores (5 out of 10) is "
        + "incorrect", 0.5, shares[Resource.VCORES_INDEX], .00001);
    assertEquals("The wrong dominant resource index was returned",
        Resource.VCORES_INDEX, dominant);
  }

  @Test
  public void testCalculateMinShareRatios() {
    Map<String, Integer> index = ResourceUtils.getResourceTypeIndex();
    Resource used = Resources.createResource(10, 5);
    Resource minShares = Resources.createResource(5, 10);
    float[][] ratios = new float[3][3];
    DominantResourceFairnessComparatorN comparator =
        new DominantResourceFairnessComparatorN();

    used.setResourceValue("test", 2L);
    minShares.setResourceValue("test", 0L);

    comparator.calculateMinShareRatios(used, minShares, ratios);

    assertEquals("Calculated min share ratio for memory (10MB out of 5MB) is "
        + "incorrect", 2.0,
        ratios[index.get(ResourceInformation.MEMORY_MB.getName())][2], .00001f);
    assertEquals("Calculated min share ratio for vcores (5 out of 10) is "
        + "incorrect", 0.5,
        ratios[index.get(ResourceInformation.VCORES.getName())][2], .00001f);
    assertEquals("Calculated min share ratio for test resource (0 out of 5) is "
        + "incorrect", Float.POSITIVE_INFINITY, ratios[index.get("test")][2],
        0.00001f);
  }

  @Test
  public void testCalculateMinShareRatios2() {
    ResourceUtils.resetResourceTypes(new Configuration());
    Resource used = Resources.createResource(10, 5);
    Resource minShares = Resources.createResource(5, 10);
    DominantResourceFairnessComparator2 comparator =
        new DominantResourceFairnessComparator2();

    double[] ratios =
        comparator.calculateMinShareRatios(used.getResources(),
            minShares.getResources());

    assertEquals("Calculated min share ratio for memory (10MB out of 5MB) is "
        + "incorrect", 2.0, ratios[Resource.MEMORY_INDEX], .00001f);
    assertEquals("Calculated min share ratio for vcores (5 out of 10) is "
        + "incorrect", 0.5, ratios[Resource.VCORES_INDEX], .00001f);
  }

  @Test
  public void testCompareShares() {
    float[][] ratios1 = {
        {0.4f, 0.1f, 2.0f},
        {0.3f, 2.0f, 0.1f},
        {0.2f, 1.0f, 9.0f}
    };
    float[][] ratios2 = {
        {0.3f, 2.0f, 1.0f},
        {0.2f, 0.1f, 0.5f},
        {0.2f, 1.0f, 2.0f}
    };
    float[][] ratios3 = {
        {0.3f, 2.0f, 1.0f},
        {0.2f, 0.1f, 2.0f},
        {0.1f, 2.0f, 1.0f}
    };
    DominantResourceFairnessComparatorN comparator =
        new DominantResourceFairnessComparatorN();

    int ret = comparator.compareRatios(ratios1, ratios2, 0);

    assertEquals("Expected the first array to be larger because the first "
        + "usage ratio element is larger", 1, ret);

    ret = comparator.compareRatios(ratios2, ratios1, 0);

    assertEquals("Expected the first array to be smaller because the first "
        + "usage ratio element is smaller", -1, ret);

    ret = comparator.compareRatios(ratios1, ratios1, 0);

    assertEquals("Expected the arrays to be equal, since they're the same "
        + "array", 0, ret);

    ret = comparator.compareRatios(ratios2, ratios2, 0);

    assertEquals("Expected the arrays to be equal, since they're the same "
        + "array", 0, ret);

    ret = comparator.compareRatios(ratios3, ratios3, 0);

    assertEquals("Expected the arrays to be equal, since they're the same "
        + "array", 0, ret);

    ret = comparator.compareRatios(ratios2, ratios3, 0);

    assertEquals("Expected the first array to be larger because the last "
        + "usage ratio element is larger, and all other elements are equal",
        1, ret);

    ret = comparator.compareRatios(ratios1, ratios2, 1);

    assertEquals("Expected the first array to be smaller because the first "
        + "fair share ratio element is smaller", -1, ret);

    ret = comparator.compareRatios(ratios2, ratios1, 1);

    assertEquals("Expected the first array to be larger because the first "
        + "fair share ratio element is larger", 1, ret);

    ret = comparator.compareRatios(ratios1, ratios1, 1);

    assertEquals("Expected the arrays to be equal, since they're the same "
        + "array", 0, ret);

    ret = comparator.compareRatios(ratios2, ratios2, 1);

    assertEquals("Expected the arrays to be equal, since they're the same "
        + "array", 0, ret);

    ret = comparator.compareRatios(ratios3, ratios3, 1);

    assertEquals("Expected the arrays to be equal, since they're the same "
        + "array", 0, ret);

    ret = comparator.compareRatios(ratios2, ratios3, 1);

    assertEquals("Expected the first array to be smaller because the last "
        + "usage ratio element is smaller, and all other elements are equal",
        -1, ret);

    ret = comparator.compareRatios(ratios1, ratios2, 2);

    assertEquals("Expected the first array to be larger because the first "
        + "min share ratio element is larger", 1, ret);

    ret = comparator.compareRatios(ratios2, ratios1, 2);

    assertEquals("Expected the first array to be smaller because the first "
        + "min share ratio element is smaller", -1, ret);

    ret = comparator.compareRatios(ratios1, ratios1, 2);

    assertEquals("Expected the arrays to be equal, since they're the same "
        + "array", 0, ret);

    ret = comparator.compareRatios(ratios2, ratios2, 2);

    assertEquals("Expected the arrays to be equal, since they're the same "
        + "array", 0, ret);

    ret = comparator.compareRatios(ratios3, ratios3, 2);

    assertEquals("Expected the arrays to be equal, since they're the same "
        + "array", 0, ret);

    ret = comparator.compareRatios(ratios2, ratios3, 2);

    assertEquals("Expected the first array to be smaller because the second "
        + "min share ratio element is smaller, and all the first elements are "
        + "equal", -1, ret);
  }

  @Test
  public void testCompareSchedulablesWithClusterResourceChanges(){
    Schedulable schedulable1 = createSchedulable(2000, 1);
    Schedulable schedulable2 = createSchedulable(1000, 2);

    // schedulable1 has share weights [1/2, 1/5], schedulable2 has share
    // weights [1/4, 2/5], schedulable1 > schedulable2 since 1/2 > 2/5
    assertTrue(createComparator(4000, 5)
        .compare(schedulable1, schedulable2) > 0);

    // share weights have changed because of the cluster resource change.
    // schedulable1 has share weights [1/4, 1/6], schedulable2 has share
    // weights [1/8, 1/3], schedulable1 < schedulable2 since 1/4 < 1/3
    assertTrue(createComparator(8000, 6)
        .compare(schedulable1, schedulable2) < 0);
  }

  private static void addResources(String... resources) {
    Configuration conf = new Configuration();

    // Add a third resource to the allowed set
    conf.set(YarnConfiguration.RESOURCE_TYPES, Joiner.on(',').join(resources));
    ResourceUtils.resetResourceTypes(conf);
  }
}
