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

import org.junit.Test;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestProportionalCapacityPreemptionPolicyInterQueueWithDRF
    extends ProportionalCapacityPreemptionPolicyMockFramework {
  @Test
  public void testInterQueuePreemptionWithMultipleResource()
      throws Exception {
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
}
