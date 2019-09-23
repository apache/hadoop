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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestProportionalCapacityPreemptionPolicyPreemptToBalance
    extends ProportionalCapacityPreemptionPolicyMockFramework {

  @Test
  public void testPreemptionToBalanceDisabled() throws IOException {
    String labelsConfig = "=100,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100]);" + //root
            "-a(=[30 100 10 30]);" + // a
            "-b(=[30 100 40 30]);" + // b
            "-c(=[30 100 50 30]);" + // c
            "-d(=[10 100 0 0])";   // d
    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t(1,1,n1,,10,false);" + // app1 in a
            "b\t(1,1,n1,,40,false);" + // app2 in b
            "c\t(1,1,n1,,50,false)"; // app3 in c

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // I_A: A:30 B:35 C:35, preempt 5 from B and 15 from C to A
    verify(mDisp, times(5)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
    verify(mDisp, times(15)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));

    assertEquals(30, policy.getQueuePartitions().get("a")
        .get("").getIdealAssigned().getMemorySize());
    assertEquals(35, policy.getQueuePartitions().get("b")
        .get("").getIdealAssigned().getMemorySize());
    assertEquals(35, policy.getQueuePartitions().get("c")
        .get("").getIdealAssigned().getMemorySize());
  }

  @Test
  public void testPreemptionToBalanceEnabled() throws IOException {
    String labelsConfig = "=100,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100]);" + //root
            "-a(=[30 100 10 30]);" + // a
            "-b(=[30 100 40 30]);" + // b
            "-c(=[30 100 50 30]);" + // c
            "-d(=[10 100 0 0])";   // d
    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t(1,1,n1,,10,false);" + // app1 in a
            "b\t(1,1,n1,,40,false);" + // app2 in b
            "c\t(1,1,n1,,50,false)"; // app3 in c

    // enable preempt to balance and ideal assignment will change.
    boolean isPreemptionToBalanceEnabled = true;
    conf.setBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_TO_BALANCE_QUEUES_BEYOND_GUARANTEED,
        isPreemptionToBalanceEnabled);

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // I_A: A:33 B:33 C:33, preempt 7 from B and 17 from C to A
    verify(mDisp, times(7)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
    verify(mDisp, times(17)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));

    assertEquals(33, policy.getQueuePartitions().get("a")
        .get("").getIdealAssigned().getMemorySize());
    assertEquals(33, policy.getQueuePartitions().get("b")
        .get("").getIdealAssigned().getMemorySize());
    assertEquals(33, policy.getQueuePartitions().get("c")
        .get("").getIdealAssigned().getMemorySize());
  }


  @Test
  public void testPreemptionToBalanceUsedPlusPendingLessThanGuaranteed()
      throws IOException{
    String labelsConfig = "=100,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100]);" + //root
            "-a(=[30 100 10 6]);" + // a
            "-b(=[30 100 40 30]);" + // b
            "-c(=[30 100 50 30]);" + // c
            "-d(=[10 100 0 0])";   // d
    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t(1,1,n1,,10,false);" + // app1 in a
            "b\t(1,1,n1,,40,false);" + // app2 in b
            "c\t(1,1,n1,,50,false)"; // app3 in c

    boolean isPreemptionToBalanceEnabled = true;
    conf.setBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_TO_BALANCE_QUEUES_BEYOND_GUARANTEED,
        isPreemptionToBalanceEnabled);

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // I_A: A:15 B:42 C:43, preempt 7 from B and 17 from C to A
    verify(mDisp, times(8)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));

    assertEquals(16, policy.getQueuePartitions().get("a")
        .get("").getIdealAssigned().getMemorySize());
    assertEquals(42, policy.getQueuePartitions().get("b")
        .get("").getIdealAssigned().getMemorySize());
    assertEquals(42, policy.getQueuePartitions().get("c")
        .get("").getIdealAssigned().getMemorySize());
  }

  @Test
  public void testPreemptionToBalanceWithVcoreResource() throws IOException {
    Logger.getRootLogger().setLevel(Level.DEBUG);
    String labelsConfig = "=100:100,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100:100 100:100 100:100 120:140]);" + //root
            "-a(=[60:60 100:100 40:40 70:40]);" + // a
            "-b(=[40:40 100:100 60:60 50:100])";   // b

    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t(1,1:1,n1,,40,false);" + // app1 in a
            "b\t(1,1:1,n1,,60,false)"; // app2 in b

    boolean isPreemptionToBalanceEnabled = true;
    conf.setBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_TO_BALANCE_QUEUES_BEYOND_GUARANTEED,
        isPreemptionToBalanceEnabled);
    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig, true);
    policy.editSchedule();

    // 21 containers will be preempted here
    verify(mDisp, times(21)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.
            IsPreemptionRequestFor(getAppAttemptId(2))));

    assertEquals(60, policy.getQueuePartitions().get("a")
        .get("").getIdealAssigned().getMemorySize());
    assertEquals(60, policy.getQueuePartitions().get("a")
        .get("").getIdealAssigned().getVirtualCores());
    assertEquals(40, policy.getQueuePartitions().get("b")
        .get("").getIdealAssigned().getMemorySize());
    assertEquals(40, policy.getQueuePartitions().get("b")
        .get("").getIdealAssigned().getVirtualCores());
  }

  @Test
  public void testPreemptionToBalanceWithConfiguredTimeout() throws IOException {
    Logger.getRootLogger().setLevel(Level.DEBUG);
    String labelsConfig = "=100:100,true"; // default partition
    String nodesConfig = "n1="; // only one node
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100:100 100:100 100:100 120:140]);" + //root
            "-a(=[60:60 100:100 40:40 70:40]);" + // a
            "-b(=[40:40 100:100 60:60 50:100])";   // b

    String appsConfig =
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t(1,1:1,n1,,40,false);" + // app1 in a
            "b\t(1,1:1,n1,,60,false)"; // app2 in b

    boolean isPreemptionToBalanceEnabled = true;
    conf.setBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_TO_BALANCE_QUEUES_BEYOND_GUARANTEED,
        isPreemptionToBalanceEnabled);
    final long FB_MAX_BEFORE_KILL = 60 *1000;
    conf.setLong(
        CapacitySchedulerConfiguration.MAX_WAIT_BEFORE_KILL_FOR_QUEUE_BALANCE_PREEMPTION,
        FB_MAX_BEFORE_KILL);

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig, true);
    policy.editSchedule();

    Map<PreemptionCandidatesSelector, Map<ApplicationAttemptId,
        Set<RMContainer>>> pcps= policy.getToPreemptCandidatesPerSelector();

    String FIFO_CANDIDATE_SELECTOR = "FifoCandidatesSelector";
    boolean hasFifoSelector = false;
    for (Map.Entry<PreemptionCandidatesSelector, Map<ApplicationAttemptId,
        Set<RMContainer>>> pc : pcps.entrySet()) {
      if (pc.getKey().getClass().getSimpleName().equals(FIFO_CANDIDATE_SELECTOR)) {
        FifoCandidatesSelector pcs = (FifoCandidatesSelector) pc.getKey();
        if (pcs.getAllowQueuesBalanceAfterAllQueuesSatisfied() == true) {
          hasFifoSelector = true;
          assertThat(pcs.getMaximumKillWaitTimeMs()).
              isEqualTo(FB_MAX_BEFORE_KILL);
        }
      }
    }

    assertEquals(hasFifoSelector, true);

    // 21 containers will be preempted here
    verify(mDisp, times(21)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.
            IsPreemptionRequestFor(getAppAttemptId(2))));

    assertEquals(60, policy.getQueuePartitions().get("a")
        .get("").getIdealAssigned().getMemorySize());
    assertEquals(60, policy.getQueuePartitions().get("a")
        .get("").getIdealAssigned().getVirtualCores());
    assertEquals(40, policy.getQueuePartitions().get("b")
        .get("").getIdealAssigned().getMemorySize());
    assertEquals(40, policy.getQueuePartitions().get("b")
        .get("").getIdealAssigned().getVirtualCores());
  }
}
