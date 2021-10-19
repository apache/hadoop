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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework.ProportionalCapacityPreemptionPolicyMockFramework;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test class for IntraQueuePreemption scenarios.
 */
public class TestProportionalCapacityPreemptionPolicyIntraQueue
    extends
    ProportionalCapacityPreemptionPolicyMockFramework {
  @Before
  public void setup() {
    super.setup();
    conf.setBoolean(
        CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED, true);
    policy = new ProportionalCapacityPreemptionPolicy(rmContext, cs, mClock);
  }

  @Test
  public void testSimpleIntraQueuePreemption() throws IOException {
    /**
     * The simplest test preemption, Queue structure is:
     *
     * <pre>
     *       root
     *     /  | | \
     *    a  b  c  d
     * </pre>
     *
     * Guaranteed resource of a/b/c/d are 11:40:20:29 Total cluster resource =
     * 100
     * Scenario:
     * Queue B has few running apps and two high priority apps have demand.
     * Apps which are running at low priority (4) will preempt few of its
     * resources to meet the demand.
     */

    conf.set(CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
        "priority_first");

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 79 110 0]);" + // root
            "-a(=[11 100 11 50 0]);" + // a
            "-b(=[40 100 38 50 0]);" + // b
            "-c(=[20 100 10 10 0]);" + // c
            "-d(=[29 100 20 0 0])"; // d

    String appsConfig =
        // queueName\t(priority,resource,host,expression,#repeat,reserved,
        // pending)
        "a\t" // app1 in a
            + "(1,1,n1,,6,false,25);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,5,false,25);" + // app2 a
            "b\t" // app3 in b
            + "(4,1,n1,,34,false,20);" + // app3 b
            "b\t" // app4 in b
            + "(4,1,n1,,2,false,10);" + // app4 b
            "b\t" // app4 in b
            + "(5,1,n1,,1,false,10);" + // app5 b
            "b\t" // app4 in b
            + "(6,1,n1,,1,false,10);" + // app6 in b
            "c\t" // app1 in a
            + "(1,1,n1,,10,false,10);" + "d\t" // app7 in c
            + "(1,1,n1,,20,false,0)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // For queue B, app3 and app4 were of lower priority. Hence take 8
    // containers from them by hitting the intraQueuePreemptionDemand of 20%.
    verify(eventHandler, times(1)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
    verify(eventHandler, times(7)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }

  @Test
  public void testNoIntraQueuePreemptionWithPreemptionDisabledOnQueues()
      throws IOException {
    /**
     * This test has the same configuration as testSimpleIntraQueuePreemption
     * except that preemption is disabled specifically for each queue. The
     * purpose is to test that disabling preemption on a specific queue will
     * avoid intra-queue preemption.
     */
    conf.setPreemptionDisabled("root.a", true);
    conf.setPreemptionDisabled("root.b", true);
    conf.setPreemptionDisabled("root.c", true);
    conf.setPreemptionDisabled("root.d", true);

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 80 110 0]);" + // root
            "-a(=[11 100 11 50 0]);" + // a
            "-b(=[40 100 38 50 0]);" + // b
            "-c(=[20 100 10 10 0]);" + // c
            "-d(=[29 100 20 0 0])"; // d

    String appsConfig =
        // queueName\t(priority,resource,host,expression,#repeat,reserved,
        // pending)
        "a\t" // app1 in a
            + "(1,1,n1,,6,false,25);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,5,false,25);" + // app2 a
            "b\t" // app3 in b
            + "(4,1,n1,,34,false,20);" + // app3 b
            "b\t" // app4 in b
            + "(4,1,n1,,2,false,10);" + // app4 b
            "b\t" // app4 in b
            + "(5,1,n1,,1,false,10);" + // app5 b
            "b\t" // app4 in b
            + "(6,1,n1,,1,false,10);" + // app6 in b
            "c\t" // app1 in a
            + "(1,1,n1,,10,false,10);" + "d\t" // app7 in c
            + "(1,1,n1,,20,false,0)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }

  @Test
  public void testNoPreemptionForSamePriorityApps() throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *     /  | | \
     *    a  b  c  d
     * </pre>
     *
     * Guaranteed resource of a/b/c/d are 10:40:20:30 Total cluster resource =
     * 100
     * Scenario: In queue A/B, all apps are running at same priority. However
     * there are many demands also from these apps. Since all apps are at same
     * priority, preemption should not occur here.
     */
    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 80 120 0]);" + // root
            "-a(=[10 100 10 50 0]);" + // a
            "-b(=[40 100 40 60 0]);" + // b
            "-c(=[20 100 10 10 0]);" + // c
            "-d(=[30 100 20 0 0])"; // d

    String appsConfig =
        // queueName\t(priority,resource,host,expression,#repeat,reserved,
        // pending)
        "a\t" // app1 in a
            + "(1,1,n1,,6,false,25);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,5,false,25);" + // app2 a
            "b\t" // app3 in b
            + "(1,1,n1,,34,false,20);" + // app3 b
            "b\t" // app4 in b
            + "(1,1,n1,,2,false,10);" + // app4 b
            "b\t" // app4 in b
            + "(1,1,n1,,1,false,20);" + // app5 b
            "b\t" // app4 in b
            + "(1,1,n1,,1,false,10);" + // app6 in b
            "c\t" // app1 in a
            + "(1,1,n1,,10,false,10);" + "d\t" // app7 in c
            + "(1,1,n1,,20,false,0)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // For queue B, none of the apps should be preempted.
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(5))));
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(6))));
  }

  @Test
  public void testNoPreemptionWhenQueueIsUnderCapacityLimit()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *      /   \
     *     a     b
     * </pre>
     *
     * Scenario:
     * Guaranteed resource of a/b are 40:60 Total cluster resource = 100 BY
     * default, this limit is 50%. Test to verify that there wont be any
     * preemption since used capacity is under 50% for queue a/b even though
     * there are demands from high priority apps in queue.
     */
    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 35 80 0]);" + // root
            "-a(=[40 100 10 50 0]);" + // a
            "-b(=[60 100 25 30 0])"; // b

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,5,false,25);" + // app1 a
            "a\t" // app2 in a
            + "(2,1,n1,,5,false,25);" + // app2 a
            "b\t" // app3 in b
            + "(4,1,n1,,40,false,20);" + // app3 b
            "b\t" // app1 in a
            + "(6,1,n1,,5,false,20)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // For queue A/B, none of the apps should be preempted as used capacity
    // is under 50%.
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
  }

  @Test
  public void testLimitPreemptionWithMaxIntraQueuePreemptableLimit()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *      /   \
     *     a     b
     * </pre>
     *
     * Guaranteed resource of a/b are 40:60 Total cluster resource = 100
     * maxIntraQueuePreemptableLimit by default is 50%. This test is to verify
     * that the maximum preemption should occur upto 50%, eventhough demand is
     * more.
     */

    // Set max preemption limit as 50%.
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 0.5);
    conf.set(CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
        "priority_first");

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 55 170 0]);" + // root
            "-a(=[40 100 10 50 0]);" + // a
            "-b(=[60 100 45 120 0])"; // b

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,5,false,25);" + // app1 a
            "a\t" // app2 in a
            + "(2,1,n1,,5,false,25);" + // app2 a
            "b\t" // app3 in b
            + "(4,1,n1,,40,false,20);" + // app3 b
            "b\t" // app1 in a
            + "(6,1,n1,,5,false,100)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // For queueB, eventhough app4 needs 100 resources, only 30 resources were
    // preempted. (max is 50% of guaranteed cap of any queue
    // "maxIntraQueuePreemptable")
    verify(eventHandler, times(30)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }

  @Test
  public void testLimitPreemptionWithTotalPreemptedResourceAllowed()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *      /   \
     *     a     b
     * </pre>
     *
     * Scenario:
     * Guaranteed resource of a/b are 40:60 Total cluster resource = 100
     * totalPreemption allowed is 10%. This test is to verify that only
     * 10% is preempted.
     */

    // report "ideal" preempt as 10%. Ensure preemption happens only for 10%
    conf.setFloat(CapacitySchedulerConfiguration.TOTAL_PREEMPTION_PER_ROUND,
        (float) 0.1);
    conf.set(CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
        "priority_first");

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 55 170 0]);" + // root
            "-a(=[40 100 10 50 0]);" + // a
            "-b(=[60 100 45 120 0])"; // b

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,5,false,25);" + // app1 a
            "a\t" // app2 in a
            + "(2,1,n1,,5,false,25);" + // app2 a
            "b\t" // app3 in b
            + "(4,1,n1,,40,false,20);" + // app3 b
            "b\t" // app1 in a
            + "(6,1,n1,,5,false,100)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // For queue B eventhough app4 needs 100 resources, only 10 resources were
    // preempted. This is the 10% limit of TOTAL_PREEMPTION_PER_ROUND.
    verify(eventHandler, times(10)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }

  @Test
  public void testAlreadySelectedContainerFromInterQueuePreemption()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *      /   \
     *     a     b
     * </pre>
     *
     * Scenario:
     * Guaranteed resource of a/b are 40:60 Total cluster resource = 100
     * QueueB is under utilized and QueueA has to release 9 containers here.
     * However within queue A, high priority app has also a demand for 20.
     * So additional 11 more containers will be preempted making a tota of 20.
     */

    // Set max preemption limit as 50%.
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 0.5);
    conf.set(CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
        "priority_first");

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 95 170 0]);" + // root
            "-a(=[60 100 70 35 0]);" + // a
            "-b(=[40 100 25 120 0])"; // b

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "root.a\t" // app1 in a
            + "(1,1,n1,,50,false,15);" + // app1 a
            "root.a\t" // app2 in a
            + "(2,1,n1,,20,false,20);" + // app2 a
            "root.b\t" // app3 in b
            + "(4,1,n1,,20,false,20);" + // app3 b
            "root.b\t" // app1 in a
            + "(4,1,n1,,5,false,100)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // As per intra queue preemption algorithm, 20 more containers were needed
    // for app2 (in queue a). Inter queue pre-emption had already preselected 9
    // containers and hence preempted only 11 more.
    verify(eventHandler, times(20)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(eventHandler, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testSkipAMContainersInInterQueuePreemption() throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *      /   \
     *     a     b
     * </pre>
     *
     * Scenario:
     * Guaranteed resource of a/b are 60:40 Total cluster resource = 100
     * While preempting containers during intra-queue preemption, AM containers
     * will be spared for now. Verify the same.
     */

    // Set max preemption limit as 50%.
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 0.5);
    conf.set(CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
        "priority_first");

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 170 0]);" + // root
            "-a(=[60 100 60 50 0]);" + // a
            "-b(=[40 100 40 120 0])"; // b

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,30,false,10);" + "a\t" // app2 in a
            + "(1,1,n1,,10,false,20);" + "a\t" // app3 in a
            + "(2,1,n1,,20,false,20);" + "b\t" // app4 in b
            + "(4,1,n1,,20,false,20);" + "b\t" // app5 in a
            + "(4,1,n1,,20,false,100)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // Ensure that only 9 containers are preempted from app2 (sparing 1 AM)
    verify(eventHandler, times(11)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(eventHandler, times(9)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testSkipAMContainersInInterQueuePreemptionSingleApp()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *      /   \
     *     a     b
     * </pre>
     *
     * Scenario:
     * Guaranteed resource of a/b are 50:50 Total cluster resource = 100
     * Spare Am container from a lower priority app during its preemption
     * cycle. Eventhough there are more demand and no other low priority
     * apps are present, still AM contaier need to soared.
     */
    conf.set(CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
        "priority_first");

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 170 0]);" + // root
            "-a(=[50 100 50 50 0]);" + // a
            "-b(=[50 100 50 120 0])"; // b

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,10,false,10);" + "a\t" // app1 in a
            + "(2,1,n1,,40,false,10);" + "b\t" // app2 in a
            + "(4,1,n1,,20,false,20);" + "b\t" // app3 in b
            + "(4,1,n1,,30,false,100)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // Make sure that app1's Am container is spared. Only 9/10 is preempted.
    verify(eventHandler, times(9)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(eventHandler, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testNoPreemptionForSingleApp() throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *      /   \
     *     a     b
     * </pre>
     *
     * Scenario:
     * Guaranteed resource of a/b are 60:40 Total cluster resource = 100
     * Only one app is running in queue. And it has more demand but no
     * resource are available in queue. Preemption must not occur here.
     */

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 20 50 0]);" + // root
            "-a(=[60 100 20 50 0]);" + // a
            "-b(=[40 100 0 0 0])"; // b

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(4,1,n1,,20,false,50)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // Ensure there are 0 preemptions since only one app is running in queue.
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  @Test
  public void testOverutilizedQueueResourceWithInterQueuePreemption()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *      /   \
     *     a     b
     * </pre>
     * Scenario:
     * Guaranteed resource of a/b are 20:80 Total cluster resource = 100
     * QueueB is under utilized and 20 resource will be released from queueA.
     * 10 containers will also selected for intra-queue too but it will be
     * pre-selected.
     */

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 70 0]);" + // root
            "-a(=[20 100 100 30 0]);" + // a
            "-b(=[80 100 0 20 0])"; // b

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,50,false,0);" + "a\t" // app1 in a
            + "(3,1,n1,,50,false,30);" + "b\t" // app2 in a
            + "(4,1,n1,,0,false,20)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // Complete demand request from QueueB for 20 resource must be preempted.
    verify(eventHandler, times(20)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }

  @Test
  public void testNodePartitionIntraQueuePreemption() throws IOException {
    /**
     * The simplest test of node label, Queue structure is:
     *
     * <pre>
     *       root
     *       /  \
     *      a    b
     * </pre>
     *
     * Scenario:
     * Both a/b can access x, and guaranteed capacity of them is 50:50. Two
     * nodes, n1 has 100 x, n2 has 100 NO_LABEL 4 applications in the cluster,
     * app1/app2/app3 in a, and app4/app5 in b. app1 uses 50 x, app2 uses 50
     * NO_LABEL, app3 uses 50 x, app4 uses 50 NO_LABEL. a has 20 pending
     * resource for x for app3 of priority 2
     *
     * After preemption, it should preempt 20 from app1
     */

    // Set max preemption limit as 50%.
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 0.5);
    conf.set(CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
        "priority_first");

    String labelsConfig = "=100,true;" + // default partition
        "x=100,true"; // partition=x
    String nodesConfig = "n1=x;" + // n1 has partition=x
        "n2="; // n2 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100],x=[100 100 100 100]);" + // root
            "-a(=[50 100 50 50],x=[50 100 50 50]);" + // a
            "-b(=[50 100 50 50],x=[50 100 50 50])"; // b
    String appsConfig =
        // queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t" // app1 in a
            + "(1,1,n1,x,50,false,10);" + // 50 * x in n1
            "a\t" // app2 in a
            + "(2,1,n1,x,0,false,20);" + // 0 * x in n1
            "a\t" // app2 in a
            + "(1,1,n2,,50,false);" + // 50 default in n2
            "b\t" // app3 in b
            + "(1,1,n1,x,50,false);" + // 50 * x in n1
            "b\t" // app4 in b
            + "(1,1,n2,,50,false)"; // 50 default in n2

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // 20 preempted from app1
    verify(eventHandler, times(20))
        .handle(argThat(new IsPreemptionRequestFor(getAppAttemptId(1))));
    verify(eventHandler, never())
        .handle(argThat(new IsPreemptionRequestFor(getAppAttemptId(2))));
    verify(eventHandler, never())
        .handle(argThat(new IsPreemptionRequestFor(getAppAttemptId(3))));
  }

  @Test
  public void testComplexIntraQueuePreemption() throws IOException {
    /**
     * The complex test preemption, Queue structure is:
     *
     * <pre>
     *       root
     *     /  | | \
     *    a  b  c  d
     * </pre>
     *
     * Scenario:
     * Guaranteed resource of a/b/c/d are 10:40:20:30 Total cluster resource =
     * 100
     * All queues under its capacity, but within each queue there are many
     * under served applications.
     */

    // report "ideal" preempt as 50%. Ensure preemption happens only for 50%
    conf.setFloat(CapacitySchedulerConfiguration.TOTAL_PREEMPTION_PER_ROUND,
        (float) 0.5);
    // Set max preemption limit as 50%.
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 0.5);
    conf.set(CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
        "priority_first");

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 75 130 0]);" + // root
            "-a(=[10 100 5 50 0]);" + // a
            "-b(=[40 100 35 60 0]);" + // b
            "-c(=[20 100 10 10 0]);" + // c
            "-d(=[30 100 25 10 0])"; // d

    String appsConfig =
        // queueName\t(priority,resource,host,expression,#repeat,reserved,
        // pending)
        "a\t" // app1 in a
            + "(1,1,n1,,5,false,25);" + // app1 a
            "a\t"
            + "(4,1,n1,,0,false,25);" + // app2 a
            "a\t"
            + "(5,1,n1,,0,false,2);" + // app3 a
            "b\t"
            + "(3,1,n1,,5,false,20);" + // app4 b
            "b\t"
            + "(4,1,n1,,15,false,10);" + // app5 b
            "b\t"
            + "(4,1,n1,,10,false,10);" + // app6 b
            "b\t"
            + "(5,1,n1,,3,false,5);" + // app7 b
            "b\t"
            + "(5,1,n1,,0,false,2);" + // app8 b
            "b\t"
            + "(6,1,n1,,2,false,10);" + // app9 in b
            "c\t"
            + "(1,1,n1,,8,false,10);" + // app10 in c
            "c\t"
            + "(1,1,n1,,2,false,5);" + // app11 in c
            "c\t"
            + "(2,1,n1,,0,false,3);" + "d\t" // app12 in c
            + "(2,1,n1,,25,false,0);" + "d\t" // app13 in d
            + "(1,1,n1,,0,false,20)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // High priority app in queueA has 30 resource demand. But low priority
    // app has only 5 resource. Hence preempt 4 here sparing AM.
    verify(eventHandler, times(4)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    // Multiple high priority apps has demand  of 17. This will be preempted
    // from another set of low priority apps.
    verify(eventHandler, times(4)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
    verify(eventHandler, times(9)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(6))));
    verify(eventHandler, times(4)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(5))));
    // Only 3 resources will be freed in this round for queue C as we
    // are trying to save AM container.
    verify(eventHandler, times(2)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(10))));
    verify(eventHandler, times(1)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(11))));
  }

  @Test
  public void testIntraQueuePreemptionWithTwoUsers()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *      /   \
     *     a     b
     * </pre>
     *
     * Scenario:
     * Guaranteed resource of a/b are 40:60 Total cluster resource = 100
     * Consider 2 users in a queue, assume minimum user limit factor is 50%.
     * Hence in queueB of 40, each use has a quota of 20. app4 of high priority
     * has a demand of 30 and its already using 5. Adhering to userlimit only
     * 15 more must be preempted. If its same user,20 would have been preempted
     */

    // Set max preemption limit as 50%.
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 0.5);

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 55 170 0]);" + // root
            "-a(=[60 100 10 50 0]);" + // a
            "-b(=[40 100 40 120 0])"; // b

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,5,false,25);" + // app1 a
            "a\t" // app2 in a
            + "(2,1,n1,,5,false,25);" + // app2 a
            "b\t" // app3 in b
            + "(4,1,n1,,35,false,20,user1);" + // app3 b
            "b\t" // app4 in b
            + "(6,1,n1,,5,false,30,user2)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // Considering user-limit of 50% since only 2 users are there, only preempt
    // 14 more (5 is already running) eventhough demand is for 30. Ideally we
    // must preempt 15. But 15th container will bring user1's usage to 20 which
    // is same as user-limit. Hence skip 15th container.
    verify(eventHandler, times(14)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }

  @Test
  public void testComplexNodePartitionIntraQueuePreemption()
      throws IOException {
    /**
     * The simplest test of node label, Queue structure is:
     *
     * <pre>
     *       root
     *       /  \
     *      a    b
     * </pre>
     *
     * Scenario:
     * Both a/b can access x, and guaranteed capacity of them is 50:50. Two
     * nodes, n1 has 100 x, n2 has 100 NO_LABEL 4 applications in the cluster,
     * app1-app4 in a, and app5-app9 in b.
     *
     */

    // Set max preemption limit as 50%.
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 0.5);
    conf.set(CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
        "priority_first");

    String labelsConfig = "=100,true;" + // default partition
        "x=100,true"; // partition=x
    String nodesConfig = "n1=x;" + // n1 has partition=x
        "n2="; // n2 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100],x=[100 100 100 100]);" + // root
            "-a(=[50 100 50 50],x=[50 100 40 50]);" + // a
            "-b(=[50 100 35 50],x=[50 100 50 50])"; // b
    String appsConfig =
        // queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t" // app1 in a
            + "(1,1,n1,x,35,false,10);" + // 20 * x in n1
            "a\t" // app2 in a
            + "(1,1,n1,x,5,false,10);" + // 20 * x in n1
            "a\t" // app3 in a
            + "(2,1,n1,x,0,false,20);" + // 0 * x in n1
            "a\t" // app4 in a
            + "(1,1,n2,,50,false);" + // 50 default in n2
            "b\t" // app5 in b
            + "(1,1,n1,x,50,false);" + // 50 * x in n1
            "b\t" // app6 in b
            + "(1,1,n2,,25,false);" + // 25 * default in n2
            "b\t" // app7 in b
            + "(1,1,n2,,3,false);" + // 3 * default in n2
            "b\t" // app8 in b
            + "(1,1,n2,,2,false);" + // 2 * default in n2
            "b\t" // app9 in b
            + "(5,1,n2,,5,false,30)"; // 50 default in n2

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // Label X: app3 has demand of 20 for label X. Hence app2 will loose
    // 4 (sparing AM) and 16 more from app1 till preemption limit is met.
    verify(eventHandler, times(16))
        .handle(argThat(new IsPreemptionRequestFor(getAppAttemptId(1))));
    verify(eventHandler, times(4))
        .handle(argThat(new IsPreemptionRequestFor(getAppAttemptId(2))));

    // Default Label:For a demand of 30, preempt from all low priority
    // apps of default label. 25 will be preempted as preemption limit is
    // met.
    verify(eventHandler, times(1))
        .handle(argThat(new IsPreemptionRequestFor(getAppAttemptId(8))));
    verify(eventHandler, times(2))
        .handle(argThat(new IsPreemptionRequestFor(getAppAttemptId(7))));
    verify(eventHandler, times(22))
        .handle(argThat(new IsPreemptionRequestFor(getAppAttemptId(6))));
  }

  @Test
  public void testIntraQueuePreemptionAfterQueueDropped()
      throws IOException {
    /**
     * Test intra queue preemption after under-served queue dropped,
     * At first, Queue structure is:
     *
     * <pre>
     *       root
     *     /  | | \
     *    a  b  c  d
     * </pre>
     *
     * After dropped under-served queue "c", Queue structure is:
     *
     * <pre>
     *       root
     *     /  |  \
     *    a   b  d
     * </pre>
     *
     * Verify no exception is thrown and preemption results is correct
     */
    conf.set(CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
        "priority_first");

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 79 110 0]);" + // root
            "-a(=[11 100 11 50 0]);" + // a
            "-b(=[40 100 38 50 0]);" + // b
            "-c(=[20 100 10 10 0]);" + // c
            "-d(=[29 100 20 0 0])"; // d

    String appsConfig =
        // queueName\t(priority,resource,host,expression,#repeat,reserved,
        // pending)
        "a\t" // app1 in a
            + "(1,1,n1,,6,false,25);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,5,false,25);" + // app2 a
            "b\t" // app3 in b
            + "(4,1,n1,,34,false,20);" + // app3 b
            "b\t" // app4 in b
            + "(4,1,n1,,2,false,10);" + // app4 b
            "b\t" // app4 in b
            + "(5,1,n1,,1,false,10);" + // app5 b
            "b\t" // app4 in b
            + "(6,1,n1,,1,false,10);" + // app6 in b
            "c\t" // app1 in a
            + "(1,1,n1,,10,false,10);" + "d\t" // app7 in c
            + "(1,1,n1,,20,false,0)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 69 100 0]);" + // root
            "-a(=[11 100 11 50 0]);" + // a
            "-b(=[40 100 38 50 0]);" + // b
            "-d(=[49 100 20 0 0])"; // d

    updateQueueConfig(queuesConfig);

    // will throw YarnRuntimeException(This shouldn't happen, cannot find
    // TempQueuePerPartition for queueName=c) without patch in YARN-8709
    policy.editSchedule();

    // For queue B, app3 and app4 were of lower priority. Hence take 8
    // containers from them by hitting the intraQueuePreemptionDemand of 20%.
    verify(eventHandler, times(1)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
    verify(eventHandler, times(7)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }
}
