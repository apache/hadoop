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

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.Before;
import org.junit.Test;

/*
 * Test class for testing intra-queue preemption when the fair ordering policy
 * is enabled on a capacity queue.
 */
public class TestProportionalCapacityPreemptionPolicyIntraQueueFairOrdering
    extends ProportionalCapacityPreemptionPolicyMockFramework {
  @Before
  public void setup() {
    super.setup();
    conf.setBoolean(
        CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED, true);
    policy = new ProportionalCapacityPreemptionPolicy(rmContext, cs, mClock);
  }

  /*
   * When the capacity scheduler fair ordering policy is enabled, preempt first
   * from the application owned by the user that is the farthest over their
   * user limit.
   */
  @Test
  public void testIntraQueuePreemptionFairOrderingPolicyEnabledOneAppPerUser()
      throws IOException {
    // Enable FairOrderingPolicy for yarn.scheduler.capacity.root.a
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".a.ordering-policy", "fair");
    // Make sure all containers will be preempted in a single round.
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 1.0);

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 1 0]);" + // root
            "-a(=[100 100 100 1 0])"; // a

    // user1/app1 has 60 resources in queue a
    // user2/app2 has 40 resources in queue a
    // user3/app3 is requesting 20 resources in queue a
    // With 3 users, preemptable user limit should be around 35 resources each.
    // With FairOrderingPolicy enabled on queue a, all 20 resources should be
    // preempted from app1
    String appsConfig =
    // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1, user1 in a
            + "(1,1,n1,,60,false,0,user1);" +
            "a\t" // app2, user2 in a
            + "(1,1,n1,,40,false,0,user2);" +
            "a\t" // app3, user3 in a
            + "(1,1,n1,,0,false,20,user3)"
            ;

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(mDisp, times(20)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  /*
   * When the capacity scheduler fifo ordering policy is enabled, preempt first
   * from the youngest application until reduced to user limit, then preempt
   * from next youngest app.
   */
  @Test
  public void testIntraQueuePreemptionFifoOrderingPolicyEnabled()
      throws IOException {
    // Enable FifoOrderingPolicy for yarn.scheduler.capacity.root.a
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".a.ordering-policy", "fifo");
    // Make sure all containers will be preempted in a single round.
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 1.0);

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 1 0]);" + // root
            "-a(=[100 100 100 1 0])"; // a

    // user1/app1 has 60 resources in queue a
    // user2/app2 has 40 resources in queue a
    // user3/app3 is requesting 20 resources in queue a
    // With 3 users, preemptable user limit should be around 35 resources each.
    // With FifoOrderingPolicy enabled on queue a, the first 5 should come from
    // the youngest app, app2, until app2 is reduced to the user limit of 35.
    String appsConfig =
    // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1, user1 in a
            + "(1,1,n1,,60,false,0,user1);" +
            "a\t" // app2, user2 in a
            + "(1,1,n1,,40,false,0,user2);" +
            "a\t" // app3, user3 in a
            + "(1,1,n1,,0,false,5,user3)"
            ;

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(mDisp, times(5)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));

    // user1/app1 has 60 resources in queue a
    // user2/app2 has 35 resources in queue a
    // user3/app3 has 5 resources and is requesting 15 resources in queue a
    // With 3 users, preemptable user limit should be around 35 resources each.
    // The next 15 should come from app1 even though app2 is younger since app2
    // has already been reduced to its user limit.
    appsConfig =
    // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1, user1 in a
            + "(1,1,n1,,60,false,0,user1);" +
            "a\t" // app2, user2 in a
            + "(1,1,n1,,35,false,0,user2);" +
            "a\t" // app3, user3 in a
            + "(1,1,n1,,5,false,15,user3)"
            ;

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(mDisp, times(15)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  /*
   * When the capacity scheduler fair ordering policy is enabled, preempt first
   * from the youngest application from the user that is the farthest over their
   * user limit.
   */
  @Test
  public void testIntraQueuePreemptionFairOrderingPolicyMulitipleAppsPerUser()
      throws IOException {
    // Enable FairOrderingPolicy for yarn.scheduler.capacity.root.a
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".a.ordering-policy", "fair");
    // Make sure all containers will be preempted in a single round.
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 1.0);

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 1 0]);" + // root
            "-a(=[100 100 100 1 0])"; // a

    // user1/app1 has 35 resources in queue a
    // user1/app2 has 25 resources in queue a
    // user2/app3 has 40 resources in queue a
    // user3/app4 is requesting 20 resources in queue a
    // With 3 users, preemptable user limit should be around 35 resources each.
    // With FairOrderingPolicy enabled on queue a, all 20 resources should be
    // preempted from app1 since it's the most over served app from the most
    // over served user
    String appsConfig =
    // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1 and app2, user1 in a
            + "(1,1,n1,,35,false,0,user1);" +
            "a\t"
            + "(1,1,n1,,25,false,0,user1);" +
            "a\t" // app3, user2 in a
            + "(1,1,n1,,40,false,0,user2);" +
            "a\t" // app4, user3 in a
            + "(1,1,n1,,0,false,20,user3)"
            ;

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(mDisp, times(20)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  /*
   * When the capacity scheduler fifo ordering policy is enabled and a user has
   * multiple apps, preempt first from the youngest application.
   */
  @Test
  public void testIntraQueuePreemptionFifoOrderingPolicyMultipleAppsPerUser()
      throws IOException {
    // Enable FifoOrderingPolicy for yarn.scheduler.capacity.root.a
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".a.ordering-policy", "fifo");
    // Make sure all containers will be preempted in a single round.
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 1.0);

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 1 0]);" + // root
            "-a(=[100 100 100 1 0])"; // a

    // user1/app1 has 40 resources in queue a
    // user1/app2 has 20 resources in queue a
    // user3/app3 has 40 resources in queue a
    // user4/app4 is requesting 20 resources in queue a
    // With 3 users, preemptable user limit should be around 35 resources each.
    String appsConfig =
    // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1, user1 in a
            + "(1,1,n1,,40,false,0,user1);" +
        "a\t" // app2, user1 in a
            + "(1,1,n1,,20,false,0,user1);" +
        "a\t" // app3, user3 in a
            + "(1,1,n1,,40,false,0,user3);" +
        "a\t" // app4, user4 in a
            + "(1,1,n1,,0,false,25,user4)"
            ;

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app3 is the younges and also over its user limit. 5 should be preempted
    // from app3 until it comes down to user3's user limit.
    verify(mDisp, times(5)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));

    // User1's app2 is its youngest. 19 should be preempted from app2, leaving
    // only the AM
    verify(mDisp, times(19)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));

    // Preempt the remaining resource from User1's oldest app1.
    verify(mDisp, times(1)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }
}
