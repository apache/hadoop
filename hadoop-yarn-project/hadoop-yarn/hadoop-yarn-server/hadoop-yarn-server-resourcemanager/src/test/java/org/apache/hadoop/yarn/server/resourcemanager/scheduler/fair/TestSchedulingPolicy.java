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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Comparator;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSchedulingPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSchedulingPolicy.class);
  private final static String ALLOC_FILE =
      new File(FairSchedulerTestBase.TEST_DIR, "test-queues").getAbsolutePath();
  private FairSchedulerConfiguration conf;
  private FairScheduler scheduler;

  @Before
  public void setUp() throws Exception {
    scheduler = new FairScheduler();
    conf = new FairSchedulerConfiguration();
    // since this runs outside of the normal context we need to set one
    RMContext rmContext = mock(RMContext.class);
    PlacementManager placementManager = new PlacementManager();
    when(rmContext.getQueuePlacementManager()).thenReturn(placementManager);
    scheduler.setRMContext(rmContext);
  }

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
    private Resource[] demandCollection = {
        Resource.newInstance(0, 0), Resource.newInstance(4, 1) };

    private String[] nameCollection = {"A", "B", "C"};

    private long[] startTimeColloection = {1L, 2L, 3L};

    private Resource[] usageCollection = {
        Resource.newInstance(0, 1), Resource.newInstance(2, 1),
        Resource.newInstance(4, 1) };

    private float[] weightsCollection = {0.0f, 1.0f, 2.0f};

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
              for (int m = 0; m < demandCollection.length; m++) {
                genSchedulable.push(createSchedulable(m, i, j, k, t));
                generateAndTest(genSchedulable);
                genSchedulable.pop();
              }
            }
          }
        }
      }

    }

    private Schedulable createSchedulable(
        int demandId, int nameIdx, int startTimeIdx,
        int usageIdx, int weightsIdx) {
      return new MockSchedulable(minShare, demandCollection[demandId],
        nameCollection[nameIdx], startTimeColloection[startTimeIdx],
        usageCollection[usageIdx], weightsCollection[weightsIdx]);
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
        LOG.error("Failure data: " + copy[0] + " " + copy[1] + " " + copy[2]);
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
      private float weights;

      public MockSchedulable(Resource minShare, Resource demand, String name,
          long startTime, Resource usage, float weights) {
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
      public float getWeight() {
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

      @Override
      public boolean isPreemptable() {
        return true;
      }
    }
  }

  @Test
  public void testSchedulingPolicyViolation() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("<schedulingPolicy>fair</schedulingPolicy>");
    out.println("    <queue name=\"child1\">");
    out.println("    <schedulingPolicy>drf</schedulingPolicy>");
    out.println("    </queue>");
    out.println("    <queue name=\"child2\">");
    out.println("    <schedulingPolicy>fair</schedulingPolicy>");
    out.println("    </queue>");
    out.println("</queue>");
    out.println("<defaultQueueSchedulingPolicy>drf" +
        "</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);

    FSQueue child1 = scheduler.getQueueManager().getQueue("child1");
    assertNull("Queue 'child1' should be null since its policy isn't allowed to"
        + " be 'drf' if its parent policy is 'fair'.", child1);

    // dynamic queue
    FSQueue dynamicQueue = scheduler.getQueueManager().
        getLeafQueue("dynamicQueue", true);
    assertNull("Dynamic queue should be null since it isn't allowed to be 'drf'"
        + " policy if its parent policy is 'fair'.", dynamicQueue);

    // Set child1 to 'fair' and child2 to 'drf', the reload the allocation file.
    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("<schedulingPolicy>fair</schedulingPolicy>");
    out.println("    <queue name=\"child1\">");
    out.println("    <schedulingPolicy>fair</schedulingPolicy>");
    out.println("    </queue>");
    out.println("    <queue name=\"child2\">");
    out.println("    <schedulingPolicy>drf</schedulingPolicy>");
    out.println("    </queue>");
    out.println("</queue>");
    out.println("<defaultQueueSchedulingPolicy>drf" +
        "</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, null);
    child1 = scheduler.getQueueManager().getQueue("child1");
    assertNotNull("Queue 'child1' should be not null since its policy is "
        + "allowed to be 'fair' if its parent policy is 'fair'.", child1);

    // Detect the policy violation of Child2, keep the original policy instead
    // of setting the new policy.
    FSQueue child2 = scheduler.getQueueManager().getQueue("child2");
    assertTrue("Queue 'child2' should be 'fair' since its new policy 'drf' "
        + "is not allowed.", child2.getPolicy() instanceof FairSharePolicy);
  }

  @Test
  public void testSchedulingPolicyViolationInTheMiddleLevel()
      throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("<schedulingPolicy>fair</schedulingPolicy>");
    out.println("  <queue name=\"level2\">");
    out.println("    <schedulingPolicy>fair</schedulingPolicy>");
    out.println("    <queue name=\"level3\">");
    out.println("    <schedulingPolicy>drf</schedulingPolicy>");
    out.println("       <queue name=\"leaf\">");
    out.println("       <schedulingPolicy>fair</schedulingPolicy>");
    out.println("       </queue>");
    out.println("    </queue>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);

    FSQueue level2 = scheduler.getQueueManager().getQueue("level2");
    assertNotNull("Queue 'level2' shouldn't be null since its policy is allowed"
        + " to be 'fair' if its parent policy is 'fair'.", level2);
    FSQueue level3 = scheduler.getQueueManager().getQueue("level2.level3");
    assertNull("Queue 'level3' should be null since its policy isn't allowed"
        + " to be 'drf' if its parent policy is 'fair'.", level3);
    FSQueue leaf = scheduler.getQueueManager().getQueue("level2.level3.leaf");
    assertNull("Queue 'leaf' should be null since its parent failed to create.",
        leaf);
  }

  @Test
  public void testFIFOPolicyOnlyForLeafQueues()
      throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("  <queue name=\"intermediate\">");
    out.println("    <schedulingPolicy>fifo</schedulingPolicy>");
    out.println("    <queue name=\"leaf\">");
    out.println("    <schedulingPolicy>fair</schedulingPolicy>");
    out.println("    </queue>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);

    FSQueue intermediate = scheduler.getQueueManager().getQueue("intermediate");
    assertNull("Queue 'intermediate' should be null since 'fifo' is only for "
        + "leaf queue.", intermediate);

    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("  <queue name=\"intermediate\">");
    out.println("    <schedulingPolicy>fair</schedulingPolicy>");
    out.println("    <queue name=\"leaf\">");
    out.println("    <schedulingPolicy>fifo</schedulingPolicy>");
    out.println("    </queue>");
    out.println("  </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, null);

    assertNotNull(scheduler.getQueueManager().getQueue("intermediate"));

    FSQueue leaf = scheduler.getQueueManager().getQueue("intermediate.leaf");
    assertNotNull("Queue 'leaf' should be null since 'fifo' is only for "
        + "leaf queue.", leaf);
  }

  @Test
  public void testPolicyReinitilization() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("<schedulingPolicy>fair</schedulingPolicy>");
    out.println("    <queue name=\"child1\">");
    out.println("    <schedulingPolicy>fair</schedulingPolicy>");
    out.println("    </queue>");
    out.println("    <queue name=\"child2\">");
    out.println("    <schedulingPolicy>fair</schedulingPolicy>");
    out.println("    </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);

    // Set child1 to 'drf' which is not allowed, then reload the allocation file
    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("<schedulingPolicy>fair</schedulingPolicy>");
    out.println("    <queue name=\"child1\">");
    out.println("    <schedulingPolicy>drf</schedulingPolicy>");
    out.println("    </queue>");
    out.println("    <queue name=\"child2\">");
    out.println("    <schedulingPolicy>fifo</schedulingPolicy>");
    out.println("    </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, null);

    FSQueue child1 = scheduler.getQueueManager().getQueue("child1");
    assertTrue("Queue 'child1' should still be 'fair' since 'drf' isn't allowed"
            + " if its parent policy is 'fair'.",
        child1.getPolicy() instanceof FairSharePolicy);
    FSQueue child2 = scheduler.getQueueManager().getQueue("child2");
    assertTrue("Queue 'child2' should still be 'fair' there is a policy"
            + " violation while reinitialization.",
        child2.getPolicy() instanceof FairSharePolicy);

    // Set both child1 and root to 'drf', then reload the allocation file
    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("<schedulingPolicy>drf</schedulingPolicy>");
    out.println("    <queue name=\"child1\">");
    out.println("    <schedulingPolicy>drf</schedulingPolicy>");
    out.println("    </queue>");
    out.println("    <queue name=\"child2\">");
    out.println("    <schedulingPolicy>fifo</schedulingPolicy>");
    out.println("    </queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.reinitialize(conf, null);

    child1 = scheduler.getQueueManager().getQueue("child1");
    assertTrue("Queue 'child1' should be 'drf' since both 'root' and 'child1'"
            + " are 'drf'.",
        child1.getPolicy() instanceof DominantResourceFairnessPolicy);
    child2 = scheduler.getQueueManager().getQueue("child2");
    assertTrue("Queue 'child2' should still be 'fifo' there is no policy"
            + " violation while reinitialization.",
        child2.getPolicy() instanceof FifoPolicy);
  }
}
