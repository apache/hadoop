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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.ComputeFairShares;
import org.junit.Before;
import org.junit.Test;

/**
 * Exercise the computeFairShares method in SchedulingAlgorithms.
 */
public class TestComputeFairShares {
  private List<Schedulable> scheds;
  
  @Before
  public void setUp() throws Exception {
    scheds = new ArrayList<Schedulable>();
  }
  
  /** 
   * Basic test - pools with different demands that are all higher than their
   * fair share (of 10 slots) should each get their fair share.
   */
  @Test
  public void testEqualSharing() {
    scheds.add(new FakeSchedulable());
    scheds.add(new FakeSchedulable());
    scheds.add(new FakeSchedulable());
    scheds.add(new FakeSchedulable());
    ComputeFairShares.computeShares(scheds,
        Resources.createResource(40), ResourceType.MEMORY);
    verifyMemoryShares(10, 10, 10, 10);
  }
  
  /**
   * In this test, pool 4 has a smaller demand than the 40 / 4 = 10 slots that
   * it would be assigned with equal sharing. It should only get the 3 slots
   * it demands. The other pools must then split the remaining 37 slots, but
   * pool 3, with 11 slots demanded, is now below its share of 37/3 ~= 12.3,
   * so it only gets 11 slots. Pools 1 and 2 split the rest and get 13 each. 
   */
  @Test
  public void testLowMaxShares() {
    scheds.add(new FakeSchedulable(0, 100));
    scheds.add(new FakeSchedulable(0, 50));
    scheds.add(new FakeSchedulable(0, 11));
    scheds.add(new FakeSchedulable(0, 3));
    ComputeFairShares.computeShares(scheds,
        Resources.createResource(40), ResourceType.MEMORY);
    verifyMemoryShares(13, 13, 11, 3);
  }

    
  /**
   * In this test, some pools have minimum shares set. Pool 1 has a min share
   * of 20 so it gets 20 slots. Pool 2 also has a min share of 20, but its
   * demand is only 10 so it can only get 10 slots. The remaining pools have
   * 10 slots to split between them. Pool 4 gets 3 slots because its demand is
   * only 3, and pool 3 gets the remaining 7 slots. Pool 4 also had a min share
   * of 2 slots but this should not affect the outcome.
   */
  @Test
  public void testMinShares() {
    scheds.add(new FakeSchedulable(20));
    scheds.add(new FakeSchedulable(18));
    scheds.add(new FakeSchedulable(0));
    scheds.add(new FakeSchedulable(2));
    ComputeFairShares.computeShares(scheds,
        Resources.createResource(40), ResourceType.MEMORY);
    verifyMemoryShares(20, 18, 0, 2);
  }
  
  /**
   * Basic test for weighted shares with no minimum shares and no low demands.
   * Each pool should get slots in proportion to its weight.
   */
  @Test
  public void testWeightedSharing() {
    scheds.add(new FakeSchedulable(0, 2.0));
    scheds.add(new FakeSchedulable(0, 1.0));
    scheds.add(new FakeSchedulable(0, 1.0));
    scheds.add(new FakeSchedulable(0, 0.5));
    ComputeFairShares.computeShares(scheds,
        Resources.createResource(45), ResourceType.MEMORY);
    verifyMemoryShares(20, 10, 10, 5);
  }
  
  /**
   * Weighted sharing test where pools 1 and 2 are now given lower demands than
   * above. Pool 1 stops at 10 slots, leaving 35. If the remaining pools split
   * this into a 1:1:0.5 ratio, they would get 14:14:7 slots respectively, but
   * pool 2's demand is only 11, so it only gets 11. The remaining 2 pools split
   * the 24 slots left into a 1:0.5 ratio, getting 16 and 8 slots respectively.
   */
  @Test
  public void testWeightedSharingWithMaxShares() {
    scheds.add(new FakeSchedulable(0, 10, 2.0));
    scheds.add(new FakeSchedulable(0, 11, 1.0));
    scheds.add(new FakeSchedulable(0, 30, 1.0));
    scheds.add(new FakeSchedulable(0, 20, 0.5));
    ComputeFairShares.computeShares(scheds,
        Resources.createResource(45), ResourceType.MEMORY);
    verifyMemoryShares(10, 11, 16, 8);
  }


  /**
   * Weighted fair sharing test with min shares. As in the min share test above,
   * pool 1 has a min share greater than its demand so it only gets its demand.
   * Pool 3 has a min share of 15 even though its weight is very small, so it
   * gets 15 slots. The remaining pools share the remaining 20 slots equally,
   * getting 10 each. Pool 3's min share of 5 slots doesn't affect this.
   */
  @Test
  public void testWeightedSharingWithMinShares() {
    scheds.add(new FakeSchedulable(20, 2.0));
    scheds.add(new FakeSchedulable(0, 1.0));
    scheds.add(new FakeSchedulable(5, 1.0));
    scheds.add(new FakeSchedulable(15, 0.5));
    ComputeFairShares.computeShares(scheds,
        Resources.createResource(45), ResourceType.MEMORY);
    verifyMemoryShares(20, 5, 5, 15);
  }

  /**
   * Test that shares are computed accurately even when the number of slots is
   * very large.
   */
  @Test
  public void testLargeShares() {
    int million = 1000 * 1000;
    scheds.add(new FakeSchedulable());
    scheds.add(new FakeSchedulable());
    scheds.add(new FakeSchedulable());
    scheds.add(new FakeSchedulable());
    ComputeFairShares.computeShares(scheds,
        Resources.createResource(40 * million), ResourceType.MEMORY);
    verifyMemoryShares(10 * million, 10 * million, 10 * million, 10 * million);
  }
  
  /**
   * Test that being called on an empty list doesn't confuse the algorithm.
   */
  @Test
  public void testEmptyList() {
    ComputeFairShares.computeShares(scheds,
        Resources.createResource(40), ResourceType.MEMORY);
    verifyMemoryShares();
  }
  
  /**
   * Test that CPU works as well as memory
   */
  @Test
  public void testCPU() {
    scheds.add(new FakeSchedulable(Resources.createResource(0, 20),
        new ResourceWeights(2.0f)));
    scheds.add(new FakeSchedulable(Resources.createResource(0, 0),
        new ResourceWeights(1.0f)));
    scheds.add(new FakeSchedulable(Resources.createResource(0, 5),
        new ResourceWeights(1.0f)));
    scheds.add(new FakeSchedulable(Resources.createResource(0, 15),
        new ResourceWeights(0.5f)));
    ComputeFairShares.computeShares(scheds,
        Resources.createResource(0, 45), ResourceType.CPU);
    verifyCPUShares(20, 5, 5, 15);
  }
  
  /**
   * Check that a given list of shares have been assigned to this.scheds.
   */
  private void verifyMemoryShares(int... shares) {
    Assert.assertEquals(scheds.size(), shares.length);
    for (int i = 0; i < shares.length; i++) {
      Assert.assertEquals(shares[i], scheds.get(i).getFairShare().getMemory());
    }
  }
  
  /**
   * Check that a given list of shares have been assigned to this.scheds.
   */
  private void verifyCPUShares(int... shares) {
    Assert.assertEquals(scheds.size(), shares.length);
    for (int i = 0; i < shares.length; i++) {
      Assert.assertEquals(shares[i], scheds.get(i).getFairShare().getVirtualCores());
    }
  }
}
