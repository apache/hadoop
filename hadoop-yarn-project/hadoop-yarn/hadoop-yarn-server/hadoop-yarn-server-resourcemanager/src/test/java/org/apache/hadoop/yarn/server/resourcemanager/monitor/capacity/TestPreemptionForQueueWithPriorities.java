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

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestPreemptionForQueueWithPriorities
    extends ProportionalCapacityPreemptionPolicyMockFramework {
  @Before
  public void setup() {
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
     *   - b1 (capacity=50), p=2
     * - c (capacity=40), p=2
     * </pre>
     */
    String labelsConfig = "=100,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100]);" + //root
            "-a(=[30 100 40 50]){priority=1};" + // a
            "--a1(=[12 100 20 50]){priority=1};" + // a1
            "--a2(=[18 100 20 50]){priority=1};" + // a2
            "-b(=[30 100 59 50]){priority=1};" + // b
            "--b1(=[15 100 30 50]){priority=1};" + // b1
            "--b2(=[15 100 29 50]){priority=2};" + // b2
            "-c(=[40 100 1 30]){priority=1}";   // c
    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a1\t(1,1,n1,,20,false);" + // app1 in a1
            "a2\t(1,1,n1,,20,false);" + // app2 in a2
            "b1\t(1,1,n1,,30,false);" + // app3 in b1
            "b2\t(1,1,n1,,29,false);" + // app4 in b2
            "c\t(1,1,n1,,29,false)"; // app5 in c


    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // Preemption should first divide capacities between a / b, and b2 should
    // get less preemption than b1 (because b2 has higher priority)
    verify(mDisp, times(5)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
    verify(mDisp, times(15)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
    verify(mDisp, times(9)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
  }

}
