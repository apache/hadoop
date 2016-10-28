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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Comparator;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestSchedulingPolicy {
  private static final Log LOG = LogFactory.getLog(TestSchedulingPolicy.class);

  @Test(timeout = 1000)
  public void testParseSchedulingPolicy()
      throws AllocationConfigurationException {

    // Class name
    SchedulingPolicy sm = SchedulingPolicy
        .parse(FairSharePolicy.class.getName());
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FairSharePolicy.NAME));

    // Canonical name
    sm = SchedulingPolicy.parse(FairSharePolicy.class
        .getCanonicalName());
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FairSharePolicy.NAME));

    // Class
    sm = SchedulingPolicy.getInstance(FairSharePolicy.class);
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FairSharePolicy.NAME));

    // Shortname - drf
    sm = SchedulingPolicy.parse("drf");
    assertTrue("Invalid scheduler name",
        sm.getName().equals(DominantResourceFairnessPolicy.NAME));
    
    // Shortname - fair
    sm = SchedulingPolicy.parse("fair");
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FairSharePolicy.NAME));

    // Shortname - fifo
    sm = SchedulingPolicy.parse("fifo");
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FifoPolicy.NAME));
  }

  /**
   * Trivial tests that make sure
   * {@link SchedulingPolicy#isApplicableTo(SchedulingPolicy, byte)} works as
   * expected for the possible values of depth
   * 
   * @throws AllocationConfigurationException
   */
  @Test(timeout = 1000)
  public void testIsApplicableTo() throws AllocationConfigurationException {
    final String ERR = "Broken SchedulingPolicy#isApplicableTo";
    
    // fifo
    SchedulingPolicy policy = SchedulingPolicy.parse("fifo");
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_LEAF));
    assertFalse(ERR, SchedulingPolicy.isApplicableTo(
        SchedulingPolicy.parse("fifo"), SchedulingPolicy.DEPTH_INTERMEDIATE));
    assertFalse(ERR, SchedulingPolicy.isApplicableTo(
        SchedulingPolicy.parse("fifo"), SchedulingPolicy.DEPTH_ROOT));

    
    // fair
    policy = SchedulingPolicy.parse("fair"); 
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_LEAF));
    assertTrue(ERR, SchedulingPolicy.isApplicableTo(policy,
        SchedulingPolicy.DEPTH_INTERMEDIATE));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_ROOT));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_PARENT));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_ANY));
    
    // drf
    policy = SchedulingPolicy.parse("drf"); 
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_LEAF));
    assertTrue(ERR, SchedulingPolicy.isApplicableTo(policy,
        SchedulingPolicy.DEPTH_INTERMEDIATE));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_ROOT));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_PARENT));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_ANY));
    
    policy = Mockito.mock(SchedulingPolicy.class);
    Mockito.when(policy.getApplicableDepth()).thenReturn(
        SchedulingPolicy.DEPTH_PARENT);
    assertTrue(ERR, SchedulingPolicy.isApplicableTo(policy,
        SchedulingPolicy.DEPTH_INTERMEDIATE));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_ROOT));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_PARENT));
    assertFalse(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_ANY));
  }

  /**
   * Test whether {@link FairSharePolicy.FairShareComparator} is transitive.
   */
  @Test
  public void testFairShareComparatorTransitivity() {
    FairSharePolicy policy = new FairSharePolicy();
    Comparator<Schedulable> fairShareComparator = policy.getComparator();
    FairShareComparatorTester tester =
        new FairShareComparatorTester(fairShareComparator);
    tester.testTransitivity();
  }


  /**
   * This class is responsible for testing the transitivity of
   * {@link FairSharePolicy.FairShareComparator}. We will generate
   * a lot of triples(each triple contains three {@link Schedulable}),
   * and then we verify transitivity by using each triple.
   *
   * <p>How to generate:</p>
   * For each field in {@link Schedulable} we all have a data collection. We
   * combine these data to construct a {@link Schedulable}, and generate all
   * cases of triple by DFS(depth first search algorithm). We can get 100% code
   * coverage by DFS.
   */
  private class FairShareComparatorTester {
    private Comparator<Schedulable> fairShareComparator;

    // Use the following data collections to generate three Schedulable.
    private Resource minShare = Resource.newInstance(0, 1);

    private Resource demand = Resource.newInstance(4, 1);

    private String[] nameCollection = {"A", "B", "C"};

    private long[] startTimeColloection = {1L, 2L, 3L};

    private Resource[] usageCollection = {
        Resource.newInstance(0, 1), Resource.newInstance(2, 1),
        Resource.newInstance(4, 1) };

    private ResourceWeights[] weightsCollection = {
        new ResourceWeights(0.0f), new ResourceWeights(1.0f),
        new ResourceWeights(2.0f) };



    public FairShareComparatorTester(
        Comparator<Schedulable> fairShareComparator) {
      this.fairShareComparator = fairShareComparator;
    }

    public void testTransitivity() {
      generateAndTest(new Stack<Schedulable>());
    }

    private void generateAndTest(Stack<Schedulable> genSchedulable) {
      if (genSchedulable.size() == 3) {
        // We get three Schedulable objects, let's use them to check the
        // comparator.
        Assert.assertTrue("The comparator must ensure transitivity",
            checkTransitivity(genSchedulable));
        return;
      }

      for (int i = 0; i < nameCollection.length; i++) {
        for (int j = 0; j < startTimeColloection.length; j++) {
          for (int k = 0; k < usageCollection.length; k++) {
            for (int t = 0; t < weightsCollection.length; t++) {
              genSchedulable.push(createSchedulable(i, j, k, t));
              generateAndTest(genSchedulable);
              genSchedulable.pop();
            }
          }
        }
      }

    }

    private Schedulable createSchedulable(
        int nameIdx, int startTimeIdx, int usageIdx, int weightsIdx) {
      return new MockSchedulable(minShare, demand, nameCollection[nameIdx],
        startTimeColloection[startTimeIdx], usageCollection[usageIdx],
        weightsCollection[weightsIdx]);
    }

    private boolean checkTransitivity(
        Collection<Schedulable> schedulableObjs) {

      Assert.assertEquals(3, schedulableObjs.size());
      Schedulable[] copy = schedulableObjs.toArray(new Schedulable[3]);

      if (fairShareComparator.compare(copy[0], copy[1]) > 0) {
        swap(copy, 0, 1);
      }

      if (fairShareComparator.compare(copy[1], copy[2]) > 0) {
        swap(copy, 1, 2);

        if (fairShareComparator.compare(copy[0], copy[1]) > 0) {
          swap(copy, 0, 1);
        }
      }

      // Here, we have got the following condition:
      // copy[0] <= copy[1] && copy[1] <= copy[2]
      //
      // So, just check copy[0] <= copy[2]
      if (fairShareComparator.compare(copy[0], copy[2]) <= 0) {
        return true;
      } else {
        LOG.fatal("Failure data: " + copy[0] + " " + copy[1] + " " + copy[2]);
        return false;
      }
    }

    private void swap(Schedulable[] array, int x, int y) {
      Schedulable tmp = array[x];
      array[x] = array[y];
      array[y] = tmp;
    }


    private class MockSchedulable implements Schedulable {
      private Resource minShare;
      private Resource demand;
      private String name;
      private long startTime;
      private Resource usage;
      private ResourceWeights weights;

      public MockSchedulable(Resource minShare, Resource demand, String name,
          long startTime, Resource usage, ResourceWeights weights) {
        this.minShare = minShare;
        this.demand = demand;
        this.name = name;
        this.startTime = startTime;
        this.usage = usage;
        this.weights = weights;
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public Resource getDemand() {
        return demand;
      }

      @Override
      public Resource getResourceUsage() {
        return usage;
      }

      @Override
      public Resource getMinShare() {
        return minShare;
      }

      @Override
      public ResourceWeights getWeights() {
        return weights;
      }

      @Override
      public long getStartTime() {
        return startTime;
      }

      @Override
      public Resource getMaxShare() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Priority getPriority() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void updateDemand() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Resource assignContainer(FSSchedulerNode node) {
        throw new UnsupportedOperationException();
      }

      @Override
      public RMContainer preemptContainer() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Resource getFairShare() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void setFairShare(Resource fairShare) {
        throw new UnsupportedOperationException();
      }

      @Override
      public String toString() {
        return "{name:" + name + ", start:" + startTime + ", usage:" + usage +
            ", weights:" + weights + ", demand:" + demand +
            ", minShare:" + minShare + "}";
      }
    }
  }

}
