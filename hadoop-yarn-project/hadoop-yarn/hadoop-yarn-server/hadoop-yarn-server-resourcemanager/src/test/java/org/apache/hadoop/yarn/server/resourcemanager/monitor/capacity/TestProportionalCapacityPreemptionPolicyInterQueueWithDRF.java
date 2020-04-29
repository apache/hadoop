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

public class TestProportionalCapacityPreemptionPolicyInterQueueWithDRF
    extends ProportionalCapacityPreemptionPolicyMockFramework {

  @Before
  public void setup() {
    super.setup();
    rc = new DominantResourceCalculator();
    when(cs.getResourceCalculator()).thenReturn(rc);
    policy = new ProportionalCapacityPreemptionPolicy(rmContext, cs, mClock);
  }

  @Test
  public void testInterQueuePreemptionWithMultipleResource() throws Exception {
    /**
     * Queue structure is:
     *
     * <pre>
     *           root
     *           /  \
     *          a    b
     * </pre>
     *
     */

    String labelsConfig = "=100:200,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100:200 100:200 100:200 100:200]);" + //root
            "-a(=[50:100 100:200 40:80 30:70]);" + // a
            "-b(=[50:100 100:200 60:120 40:50])";   // b

    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t(1,2:4,n1,,20,false);" + // app1 in a
            "b\t(1,2:4,n1,,30,false)"; // app2 in b

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig, true);
    policy.editSchedule();

    // Preemption should happen in Queue b, preempt <10,20> to Queue a
    verify(mDisp, never()).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(5)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testInterQueuePreemptionWithNaturalTerminationFactor()
      throws Exception {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *      /   \
     *     a     b
     * </pre>
     *
     * Guaranteed resource of a/b are 50:50 Total cluster resource = 100
     * Scenario: All resources are allocated to Queue A.
     * Even though Queue B needs few resources like 1 VCore, some resources
     * must be preempted from the app which is running in Queue A.
     */

    conf.setFloat(
        CapacitySchedulerConfiguration.PREEMPTION_NATURAL_TERMINATION_FACTOR,
        (float) 0.2);

    String labelsConfig = "=100:50,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100:50";
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100:50 100:50 50:50 0:0]);" + // root
            "-a(=[50:25 100:50 50:50 0:0]);" + // a
            "-b(=[50:25 50:25 0:0 2:1]);"; // b

    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t(1,2:1,n1,,50,false);"; // app1 in a

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(mDisp, times(1)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  @Test
  public void test3ResourceTypesInterQueuePreemption() throws IOException {
    // Initialize resource map
    String RESOURCE_1 = "res1";
    riMap.put(RESOURCE_1, ResourceInformation
        .newInstance(RESOURCE_1, "", 0, ResourceTypes.COUNTABLE, 0,
            Integer.MAX_VALUE));

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);

    /*
     *              root
     *           /  \  \
     *          a    b  c
     *
     *  A / B / C have 33.3 / 33.3 / 33.4 resources
     *  Total cluster resource have mem=30, cpu=18, GPU=6
     *  A uses mem=6, cpu=3, GPU=3
     *  B uses mem=6, cpu=3, GPU=3
     *  C is asking mem=1,cpu=1,GPU=1
     *
     *  We expect it can preempt from one of the jobs
     */
    String labelsConfig = "=30:18:6,true;";
    String nodesConfig = "n1= res=30:18:6;"; // n1 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[30:18:6 30:18:6 12:12:6 1:1:1]);" + //root
            "-a(=[10:7:2 10:6:3 6:6:3 0:0:0]);" + // a
            "-b(=[10:6:2 10:6:3 6:6:3 0:0:0]);" + // b
            "-c(=[10:5:2 10:6:2 0:0:0 1:1:1])"; // c
    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t" // app1 in a1
            + "(1,2:2:1,n1,,3,false);" + "b\t" // app2 in b2
            + "(1,2:2:1,n1,,3,false)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(1)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }
}