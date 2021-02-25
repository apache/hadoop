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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework.ProportionalCapacityPreemptionPolicyMockFramework;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class for IntraQueuePreemption scenarios.
 */
public class TestProportionalCapacityPreemptionPolicyIntraQueueWithDRF
    extends
    ProportionalCapacityPreemptionPolicyMockFramework {
  @Before
  public void setup() {
    super.setup();
    conf.setBoolean(
        CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED, true);
    resourceCalculator = new DominantResourceCalculator();
    when(cs.getResourceCalculator()).thenReturn(resourceCalculator);
    policy = new ProportionalCapacityPreemptionPolicy(rmContext, cs, mClock);
  }

  @Test
  public void testSimpleIntraQueuePreemptionWithVCoreResource()
      throws IOException {
    /**
     * The simplest test preemption, Queue structure is:
     *
     * <pre>
     *       root
     *     /  | | \
     *    a  b  c  d
     * </pre>
     *
     * Guaranteed resource of a/b/c/d are 10:40:20:30 Total cluster resource =
     * 100 Scenario: Queue B has few running apps and two high priority apps
     * have demand. Apps which are running at low priority (4) will preempt few
     * of its resources to meet the demand.
     */

    conf.set(CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
        "priority_first");

    String labelsConfig = "=100:50,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100:50";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100:50 100:50 80:40 120:60 0]);" + // root
            "-a(=[10:5 100:50 10:5 50:25 0]);" + // a
            "-b(=[40:20 100:50 40:20 60:30 0]);" + // b
            "-c(=[20:10 100:50 10:5 10:5 0]);" + // c
            "-d(=[30:15 100:50 20:10 0 0])"; // d

    String appsConfig =
        // queueName\t(priority,resource,host,expression,#repeat,reserved,
        // pending)
        "a\t" // app1 in a
            + "(1,1:1,n1,,5,false,25:25);" + // app1 a
            "a\t" // app2 in a
            + "(1,1:1,n1,,5,false,25:25);" + // app2 a
            "b\t" // app3 in b
            + "(4,1:1,n1,,36,false,20:20);" + // app3 b
            "b\t" // app4 in b
            + "(4,1:1,n1,,2,false,10:10);" + // app4 b
            "b\t" // app4 in b
            + "(5,1:1,n1,,1,false,10:10);" + // app5 b
            "b\t" // app4 in b
            + "(6,1:1,n1,,1,false,10:10);" + // app6 in b
            "c\t" // app1 in a
            + "(1,1:1,n1,,10,false,10:10);" + "d\t" // app7 in c
            + "(1,1:1,n1,,20,false,0)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // For queue B, app3 and app4 were of lower priority. Hence take 8
    // containers from them by hitting the intraQueuePreemptionDemand of 20%.
    verify(eventHandler, times(1)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
    verify(eventHandler, times(3)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testIntraQueuePreemptionFairOrderingWithStrictAndRelaxedDRF()
      throws IOException {
    /**
     * Continue to allow intra-queue preemption when only one of the user's
     * resources is above the user limit.
     * Queue structure is:
     *
     * <pre>
     *       root
     *     /  |
     *    a   b
     * </pre>
     *
     * Guaranteed resource of a and b are 30720:300 and 30720:300 Total cluster
     * resource = 61440:600.
     * Scenario: Queue B has one running app using 61720:60 resources with no
     * pending resources, and one app with no used resources and 30720:30
     * pending resources.
     *
     * The first part of the test is to show what happens when the conservative
     * DRF property is set. Since the memory is above and the vcores is below
     * the user limit, only the minimum number of containers is allowed.
     * In the second part, since conservative DRF is relaxed, all containers
     * needed are allowed to be preempted (minus the AM size).
     */

    conf.set(CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
        "userlimit_first");
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + "root.b." + CapacitySchedulerConfiguration.ORDERING_POLICY, "fair");
    conf.setBoolean(
        CapacitySchedulerConfiguration.IN_QUEUE_PREEMPTION_CONSERVATIVE_DRF,
        true);

    String labelsConfig = "=61440:600,true;";
    String nodesConfig = // n1 has no label
        "n1= res=61440:600";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[61440:600 61440:600 61440:600 30720:30 0]);" + // root
            "-a(=[30720:300 61440:600 0:0 0:0 0]);" + // a
            "-b(=[30720:300 61440:600 61440:60 30720:30 0]);";  // b

    String appsConfig =
        "b\t" + "(1,1024:1,n1,,60,false,0:0,user1);" + // app1 in b
        "b\t" + "(1,0:0,n1,,0,false,30720:30,user3);"; // app2 in b

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    Resource ul = Resource.newInstance(30720, 300);
    when(((LeafQueue)(cs.getQueue("root.b")))
        .getResourceLimitForAllUsers(any(), any(), any(), any())
    ).thenReturn(ul);
    policy.editSchedule();

    verify(eventHandler, times(6)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    reset(eventHandler);

    conf.setBoolean(
        CapacitySchedulerConfiguration.IN_QUEUE_PREEMPTION_CONSERVATIVE_DRF,
        false);
    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    when(((LeafQueue)(cs.getQueue("root.b")))
        .getResourceLimitForAllUsers(any(), any(), any(), any())
    ).thenReturn(ul);
    policy.editSchedule();
    verify(eventHandler, times(29)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  @Test
  public void testIntraQueuePreemptionWithDominantVCoreResource()
      throws IOException {
    /**
     * The simplest test preemption, Queue structure is:
     *
     * <pre>
     *     root
     *     /  \
     *    a    b
     * </pre>
     *
     * Guaranteed resource of a/b are 40:60 Total cluster resource = 100
     * Scenario: Queue B has few running apps and two high priority apps have
     * demand. Apps which are running at low priority (4) will preempt few of
     * its resources to meet the demand.
     */

    conf.set(CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
        "priority_first");
    // Set max preemption limit as 50%.
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 0.5);

    String labelsConfig = "=100:200,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100:200";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100:50 100:50 50:40 110:60 0]);" + // root
            "-a(=[40:20 100:50 9:9 50:30 0]);" + // a
            "-b(=[60:30 100:50 40:30 60:30 0]);"; // b

    String appsConfig =
        // queueName\t(priority,resource,host,expression,#repeat,reserved,
        // pending)
        "a\t" // app1 in a
            + "(1,2:1,n1,,4,false,25:25);" + // app1 a
            "a\t" // app2 in a
            + "(1,1:3,n1,,2,false,25:25);" + // app2 a
            "b\t" // app3 in b
            + "(4,2:1,n1,,10,false,20:20);" + // app3 b
            "b\t" // app4 in b
            + "(4,1:2,n1,,5,false,10:10);" + // app4 b
            "b\t" // app5 in b
            + "(5,1:1,n1,,5,false,30:20);" + // app5 b
            "b\t" // app6 in b
            + "(6,2:1,n1,,5,false,30:20);";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // For queue B, app3 and app4 were of lower priority. Hence take 4
    // containers.
    verify(eventHandler, times(9)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
    verify(eventHandler, times(4)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
    verify(eventHandler, times(4)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(5))));
  }
}
