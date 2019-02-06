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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
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
    rc = new DominantResourceCalculator();
    when(cs.getResourceCalculator()).thenReturn(rc);
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
    verify(mDisp, times(1)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
    verify(mDisp, times(3)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
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
    verify(mDisp, times(9)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
    verify(mDisp, times(4)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
    verify(mDisp, times(4)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(5))));
  }
}
