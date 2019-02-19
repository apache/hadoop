/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPreemptionForQueueWithPriorities
    extends ProportionalCapacityPreemptionPolicyMockFramework {
  @Before
  public void setup() {
    rc = new DefaultResourceCalculator();
    super.setup();
    policy = new ProportionalCapacityPreemptionPolicy(rmContext, cs, mClock);
  }

  @Test
  public void testPreemptionForHighestPriorityUnderutilizedQueue()
      throws IOException {
    /**
     * The simplest test of queue with priorities, Queue structure is:
     *
     * <pre>
     *        root
     *       / |  \
     *      a  b   c
     * </pre>
     *
     * For priorities
     * - a=1
     * - b/c=2
     *
     * So c will preempt more resource from a, till a reaches guaranteed
     * resource.
     */
    String labelsConfig = "=100,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100]);" + //root
            "-a(=[30 100 40 50]){priority=1};" + // a
            "-b(=[30 100 59 50]){priority=2};" + // b
            "-c(=[40 100 1 25]){priority=2}";    // c
    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t(1,1,n1,,40,false);" + // app1 in a
        "b\t(1,1,n1,,59,false);" + // app2 in b
        "c\t(1,1,n1,,1,false);";   // app3 in c

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // 10 preempted from app1, 15 preempted from app2, and nothing preempted
    // from app3
    verify(mDisp, times(10)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(15)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }

  @Test
  public void testPreemptionForLowestPriorityUnderutilizedQueue()
      throws IOException {
    /**
     * Similar to above, make sure we can still make sure less utilized queue
     * can get resource first regardless of priority.
     *
     * Queue structure is:
     *
     * <pre>
     *        root
     *       / |  \
     *      a  b   c
     * </pre>
     *
     * For priorities
     * - a=1
     * - b=2
     * - c=0
     *
     * So c will preempt more resource from a, till a reaches guaranteed
     * resource.
     */
    String labelsConfig = "=100,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100]);" + //root
            "-a(=[30 100 40 50]){priority=1};" + // a
            "-b(=[30 100 59 50]){priority=2};" + // b
            "-c(=[40 100 1 25]){priority=0}";    // c
    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
            "a\t(1,1,n1,,40,false);" + // app1 in a
            "b\t(1,1,n1,,59,false);" + // app2 in b
            "c\t(1,1,n1,,1,false);";   // app3 in c

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // 10 preempted from app1, 15 preempted from app2, and nothing preempted
    // from app3
    verify(mDisp, times(10)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(15)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }

  @Test
  public void testPreemptionWontHappenBetweenSatisfiedQueues()
      throws IOException {
    /**
     * No preemption happen if a queue is already satisfied, regardless of
     * priority
     *
     * Queue structure is:
     *
     * <pre>
     *        root
     *       / |  \
     *      a  b   c
     * </pre>
     *
     * For priorities
     * - a=1
     * - b=1
     * - c=2
     *
     * When c is satisfied, it will not preempt any resource from other queues
     */
    String labelsConfig = "=100,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100]);" + //root
            "-a(=[30 100 0 0]){priority=1};" + // a
            "-b(=[30 100 40 50]){priority=1};" + // b
            "-c(=[40 100 60 25]){priority=2}";   // c
    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "b\t(1,1,n1,,40,false);" + // app1 in b
        "c\t(1,1,n1,,60,false)"; // app2 in c

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // Nothing preempted
    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testPreemptionForMultipleQueuesInTheSamePriorityBuckets()
      throws IOException {
    /**
     * When a cluster has different priorities, each priority has multiple
     * queues, preemption policy should try to balance resource between queues
     * with same priority by ratio of their capacities
     *
     * Queue structure is:
     *
     * <pre>
     * root
     * - a (capacity=10), p=1
     * - b (capacity=15), p=1
     * - c (capacity=20), p=2
     * - d (capacity=25), p=2
     * - e (capacity=30), p=2
     * </pre>
     */
    String labelsConfig = "=100,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100]);" + //root
            "-a(=[10 100 35 50]){priority=1};" + // a
            "-b(=[15 100 25 50]){priority=1};" + // b
            "-c(=[20 100 39 50]){priority=2};" + // c
            "-d(=[25 100 0 0]){priority=2};" + // d
            "-e(=[30 100 1 99]){priority=2}";   // e
    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t(1,1,n1,,35,false);" + // app1 in a
        "b\t(1,1,n1,,25,false);" + // app2 in b
            "c\t(1,1,n1,,39,false);" + // app3 in c
            "e\t(1,1,n1,,1,false)"; // app4 in e

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // 23 preempted from app1, 6 preempted from app2, and nothing preempted
    // from app3/app4
    // (After preemption, a has 35 - 23 = 12, b has 25 - 6 = 19, so a:b after
    //  preemption is 1.58, close to 1.50)
    verify(mDisp, times(23)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(6)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
  }

  @Test
  public void testPreemptionForPriorityAndDisablePreemption()
      throws IOException {
    /**
     * When a cluster has different priorities, each priority has multiple
     * queues, preemption policy should try to balance resource between queues
     * with same priority by ratio of their capacities.
     *
     * But also we need to make sure preemption disable will be honered
     * regardless of priority.
     *
     * Queue structure is:
     *
     * <pre>
     * root
     * - a (capacity=10), p=1
     * - b (capacity=15), p=1
     * - c (capacity=20), p=2
     * - d (capacity=25), p=2
     * - e (capacity=30), p=2
     * </pre>
     */
    String labelsConfig = "=100,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100]);" + //root
            "-a(=[10 100 35 50]){priority=1,disable_preemption=true};" + // a
            "-b(=[15 100 25 50]){priority=1};" + // b
            "-c(=[20 100 39 50]){priority=2};" + // c
            "-d(=[25 100 0 0]){priority=2};" + // d
            "-e(=[30 100 1 99]){priority=2}";   // e
    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t(1,1,n1,,35,false);" + // app1 in a
            "b\t(1,1,n1,,25,false);" + // app2 in b
            "c\t(1,1,n1,,39,false);" + // app3 in c
            "e\t(1,1,n1,,1,false)"; // app4 in e

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // We suppose to preempt some resource from A, but now since queueA
    // disables preemption, so we need to preempt some resource from B and
    // some from C even if C has higher priority than A
    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(9)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
    verify(mDisp, times(19)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
  }

  @Test
  public void testPriorityPreemptionForHierarchicalOfQueues()
      throws IOException {
    /**
     * When a queue has multiple hierarchy and different priorities:
     *
     * <pre>
     * root
     * - a (capacity=30), p=1
     *   - a1 (capacity=40), p=1
     *   - a2 (capacity=60), p=1
     * - b (capacity=30), p=1
     *   - b1 (capacity=50), p=1
     *   - b2 (capacity=50), p=2
     * - c (capacity=40), p=1
     * </pre>
     */
    String labelsConfig = "=100,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100]);" + //root
            "-a(=[29 100 40 50]){priority=1};" + // a
            "--a1(=[12 100 20 50]){priority=1};" + // a1
            "--a2(=[17 100 20 50]){priority=1};" + // a2
            "-b(=[31 100 59 50]){priority=1};" + // b
            "--b1(=[16 100 30 50]){priority=1};" + // b1
            "--b2(=[15 100 29 50]){priority=2};" + // b2
            "-c(=[40 100 1 30]){priority=1}";   // c
    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a1\t(1,1,n1,,20,false);" + // app1 in a1
            "a2\t(1,1,n1,,20,false);" + // app2 in a2
            "b1\t(1,1,n1,,30,false);" + // app3 in b1
            "b2\t(1,1,n1,,29,false);" + // app4 in b2
            "c\t(1,1,n1,,1,false)"; // app5 in c


    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // Preemption should first divide capacities between a / b, and b2 should
    // get less preemption than b1 (because b2 has higher priority)
    verify(mDisp, times(6)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(1)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
    verify(mDisp, times(13)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
    verify(mDisp, times(10)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
  }

  @Test
  public void testPriorityPreemptionWithMandatoryResourceForHierarchicalOfQueues()
      throws Exception {
    /**
     * Queue structure is:
     *
     * <pre>
     *           root
     *           /  \
     *          a    b
     *        /  \  /  \
     *       a1  a2 b1  b2
     * </pre>
     *
     * a2 is underserved and need more resource. b2 will be preemptable.
     */

    String labelsConfig = "=100:200,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100:200 100:200 100:200 100:200]);" + //root
            "-a(=[50:100 100:200 20:40 60:100]){priority=1};" + // a
            "--a1(=[10:20 100:200 10:30 30:20]){priority=1};" + // a1
            "--a2(=[40:80 100:200 10:10 30:80]){priority=1};" + // a2
            "-b(=[50:100 100:200 80:160 40:100]){priority=1};" + // b
            "--b1(=[20:40 100:200 20:40 20:70]){priority=2};" + // b1
            "--b2(=[30:60 100:200 60:120 20:30]){priority=1}";// b2

    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a1\t(1,1:4,n1,,10,false);" + // app1 in a1
            "a2\t(1,1:1,n1,,10,false);" + // app2 in a2
            "b1\t(1,3:4,n1,,10,false);" + // app3 in b1
            "b2\t(1,20:40,n1,,3,false)";  // app4 in b2


    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig, true);
    policy.editSchedule();

    // Preemption should first divide capacities between a / b, and b1 should
    // get less preemption than b2 (because b1 has higher priority)
    verify(mDisp, times(3)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
    verify(mDisp, times(2)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
  }

  @Test
  public void testPriorityPreemptionWithMultipleResource()
      throws Exception {
    String RESOURCE_1 = "res1";

    riMap.put(RESOURCE_1, ResourceInformation.newInstance(RESOURCE_1, "", 0,
        ResourceTypes.COUNTABLE, 0, Integer.MAX_VALUE));

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);

    /**
     * Queue structure is:
     *
     * <pre>
     *           root
     *           /  \
     *          a    b
     *        /  \
     *       a1  a2
     * </pre>
     *
     * a1 and a2 are using most of resources.
     * b needs more resources which is under served.
     */
    String labelsConfig =
        "=100:100:10,true;";
    String nodesConfig =
        "n1=;"; // n1 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100:100:10 100:100:10 100:100:10 100:100:10]);" + //root
            "-a(=[50:60:3 100:100:10 80:90:10 30:20:4]){priority=1};" + // a
            "--a1(=[20:15:3 100:50:10 60:50:10 0]){priority=1};" + // a1
            "--a2(=[30:45 100:50:10 20:40 30:20:4]){priority=2};" + // a2
            "-b(=[50:40:7 100:100:10 20:10 30:10:2]){priority=1}"; // b

    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a1\t" // app1 in a1
            + "(1,6:5:1,n1,,10,false);" +
            "a2\t" // app2 in a2
            + "(1,2:4,n1,,10,false);" +
            "b\t" // app3 in b
            + "(1,2:1,n1,,10,false)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig, true);
    policy.editSchedule();

    // Preemption should first divide capacities between a / b, and a2 should
    // get less preemption than a1 (because a2 has higher priority). More
    // specifically, a2 will not get preempted since the resource preempted
    // from a1 can satisfy b already.
    verify(mDisp, times(7)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));

    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));

    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }

  @Test
  public void test3ResourceTypesInterQueuePreemption() throws IOException {
    rc = new DominantResourceCalculator();
    when(cs.getResourceCalculator()).thenReturn(rc);

    // Initialize resource map
    String RESOURCE_1 = "res1";
    riMap.put(RESOURCE_1, ResourceInformation.newInstance(RESOURCE_1, "", 0,
        ResourceTypes.COUNTABLE, 0, Integer.MAX_VALUE));

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);

    /**
     * Queue structure is:
     *
     * <pre>
     *              root
     *           /  \  \
     *          a    b  c
     * </pre>
     *  A / B / C have 33.3 / 33.3 / 33.4 resources
     *  Total cluster resource have mem=30, cpu=18, GPU=6
     *  A uses mem=6, cpu=3, GPU=3
     *  B uses mem=6, cpu=3, GPU=3
     *  C is asking mem=1,cpu=1,GPU=1
     *
     *  We expect it can preempt from one of the jobs
     */
    String labelsConfig =
        "=30:18:6,true;";
    String nodesConfig =
        "n1= res=30:18:6;"; // n1 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[30:18:6 30:18:6 12:12:6 1:1:1]){priority=1};" + //root
            "-a(=[10:6:2 10:6:2 6:6:3 0:0:0]){priority=1};" + // a
            "-b(=[10:6:2 10:6:2 6:6:3 0:0:0]){priority=1};" + // b
            "-c(=[10:6:2 10:6:2 0:0:0 1:1:1]){priority=2}"; // c
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t" // app1 in a1
            + "(1,2:2:1,n1,,3,false);" +
            "b\t" // app2 in b2
            + "(1,2:2:1,n1,,3,false)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(mDisp, times(1)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  @Test
  public void testPriorityPreemptionForBalanceBetweenSatisfiedQueues()
      throws IOException {
    /**
     * All queues are beyond guarantee, c has higher priority than b.
     * c ask for more resource, and there is no idle left, c should preempt
     * some resource from b but won’t let b under its guarantee.
     *
     * Queue structure is:
     *
     * <pre>
     *        root
     *       / |  \
     *      a  b   c
     * </pre>
     *
     * For priorities
     * - a=1
     * - b=1
     * - c=2
     *
     */
    String labelsConfig = "=100,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100]);" + //root
            "-a(=[30 100 0 0]){priority=1};" + // a
            "-b(=[30 100 40 50]){priority=1};" + // b
            "-c(=[40 100 60 25]){priority=2}";   // c
    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "b\t(1,1,n1,,40,false);" + // app1 in b
            "c\t(1,1,n1,,60,false)"; // app2 in c

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    CapacitySchedulerConfiguration newConf =
        new CapacitySchedulerConfiguration(conf);
    boolean isPreemptionToBalanceRequired = true;
    newConf.setBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_TO_BALANCE_QUEUES_BEYOND_GUARANTEED,
        isPreemptionToBalanceRequired);
    when(cs.getConfiguration()).thenReturn(newConf);
    policy.editSchedule();

    // IdealAssigned b: 30 c: 70. initIdealAssigned: b: 30 c: 40, even though
    // b and c has same relativeAssigned=1.0f(idealAssigned / guaranteed),
    // since c has higher priority, c will be put in mostUnderServedQueue and
    // get all remain 30 capacity.
    verify(mDisp, times(10)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }
}
