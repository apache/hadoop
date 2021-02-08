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

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework.ProportionalCapacityPreemptionPolicyMockFramework;
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

    String labelsConfig = "=100:100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100:100 100:100 100:100 1:1 0]);" + // root
            "-a(=[100:100 100:100 100:100 1:1 0])"; // a

    // user1/app1 has 60 resources in queue a
    // user2/app2 has 40 resources in queue a
    // user3/app3 is requesting 20 resources in queue a
    // With 3 users, preemptable user limit should be around 35 resources each.
    // With FairOrderingPolicy enabled on queue a, all 20 resources should be
    // preempted from app1
    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1, user1 in a
            + "(1,1:1,n1,,60,false,0,user1);" +
            "a\t" // app2, user2 in a
            + "(1,1:1,n1,,40,false,0,user2);" +
            "a\t" // app3, user3 in a
            + "(1,1:1,n1,,0,false,20:20,user3)"
            ;

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(20)).handle(argThat(
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
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1, user1 in a
            + "(1,1,n1,,60,false,0,user1);" +
            "a\t" // app2, user2 in a
            + "(1,1,n1,,40,false,0,user2);" +
            "a\t" // app3, user3 in a
            + "(1,1,n1,,0,false,5,user3)"
            ;

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(5)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));

    // user1/app1 has 60 resources in queue a
    // user2/app2 has 35 resources in queue a
    // user3/app3 has 5 resources and is requesting 15 resources in queue a
    // With 3 users, preemptable user limit should be around 35 resources each.
    // The next 15 should come from app1 even though app2 is younger since app2
    // has already been reduced to its user limit.
    appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1, user1 in a
            + "(1,1,n1,,60,false,0,user1);" +
            "a\t" // app2, user2 in a
            + "(1,1,n1,,35,false,0,user2);" +
            "a\t" // app3, user3 in a
            + "(1,1,n1,,5,false,15,user3)"
            ;

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(15)).handle(argThat(
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

    String labelsConfig = "=100:100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100:100 100:100 100:100 1:1 0]);" + // root
            "-a(=[100:100 100:100 100:100 1:1 0])"; // a

    // user1/app1 has 35 resources in queue a
    // user1/app2 has 25 resources in queue a
    // user2/app3 has 40 resources in queue a
    // user3/app4 is requesting 20 resources in queue a
    // With 3 users, preemptable user limit should be around 35 resources each.
    // With FairOrderingPolicy enabled on queue a,
    // we will preempt around (60 - 35) ~ 25 resources
    // from user1's app1 and app2.
    // However, because app4 has only 20 container demand,
    // 20 containers will be picked from user1.
    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1 and app2, user1 in a
            + "(1,1:1,n1,,35,false,0,user1);" +
            "a\t"
            + "(1,1:1,n1,,25,false,0,user1);" +
            "a\t" // app3, user2 in a
            + "(1,1:1,n1,,40,false,0,user2);" +
            "a\t" // app4, user3 in a
            + "(1,1:1,n1,,0,false,20:20,user3)"
            ;

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(17)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(eventHandler, times(3)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
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
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending,user)
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
    verify(eventHandler, times(5)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));

    // User1's app2 is its youngest. 19 should be preempted from app2, leaving
    // only the AM
    verify(eventHandler, times(19)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));

    // Preempt the remaining resource from User1's oldest app1.
    verify(eventHandler, times(1)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  @Test
  public void testIntraQueuePreemptionFairOrderingPolicyMultiAppsSingleUser()
      throws IOException {
    /**
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 100%
     *   +--------------+------+---------+-----------+
     *   | APP  | USER  | USED | PENDING | FAIRSHARE |
     *   +--------------+------+---------+-----------+
     *   | app1 | user1 | 25   | 0       | 25        |
     *   | app2 | user1 | 35   | 0       | 25        |
     *   | app3 | user1 | 40   | 0       | 25        |
     *   | app4 | user1 | 0    | 20      | 25        |
     *   +--------------+------+---------+-----------+
     * Because all apps are from same user,
     * we expect each app to have 25% fairshare.
     * So app2 can give 9 containers and
     * app3 can loose 13 containers (exclude AM).
     * However, because we are using FiCa scheduling,
     * app3 is least starved
     * and so we will take all its resources above its fairshare.
     * (check: IntraQueuePreemptionPolicy: preemptFromLeastStarvedApp())
     * So we expect app3 to give 13 containers and
     * app2 to give the remaining containers (20 - 13 = 7 containers)
     */
    // Enable FairOrderingPolicy for yarn.scheduler.capacity.root.a
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".a.ordering-policy", "fair");
    // Make sure all containers will be preempted in a single round.
    conf.setFloat(CapacitySchedulerConfiguration.
            INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 1.0);

    String labelsConfig = "=100:100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100:100 100:100 100:100 1:1 0]);" + // root
            "-a(=[100:100 100:100 100:100 1:1 0])"; // a

    String appsConfig =
    // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1, app2, app3, app4 from user1
            + "(1,1:1,n1,,25,false,0,user1);" +
            "a\t"
            + "(1,1:1,n1,,35,false,0,user1);" +
            "a\t"
            + "(1,1:1,n1,,40,false,0,user1);" +
            "a\t"
            + "(1,1:1,n1,,0,false,20:20,user1)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(14)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
    verify(eventHandler, times(6)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testIntraQueuePreemptionFairOPIntraUserPreemption()
      throws IOException {
    /**
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+------+---------+-----------+
     *   | APP  | USER  | USED | PENDING | FAIRSHARE |
     *   +--------------+------+---------+-----------+
     *   | app1 | user1 | 40   | 0       | 25        |
     *   | app2 | user1 | 10   | 0       | 25        |
     *   | app3 | user2 | 50   | 0       | 25        |
     *   | app4 | user2 | 0    | 30      | 25        |
     *   +--------------+------+---------+-----------+
     *
     * Because user1 is at its user limit, for app4 of user2,
     * we will preempt the containers from user2 only, i.e. app3 of user2.
     * Because we want to maintain fairness,
     * we will preempt 24 containers from app3
     * (whose fairshare is 25% -> 1/2 of 50% UL)
     */
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
    String appsConfig =
    // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1 and app2, user1 in a
            + "(1,1,n1,,40,false,0,user1);" +
            "a\t"
            + "(1,1,n1,,10,false,0,user1);" +
            "a\t" // app3 and app4 from user2 in a
            + "(1,1,n1,,50,false,0,user2);" +
            "a\t"
            + "(1,1,n1,,0,false,30,user2)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(24)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }

  @Test
  public void testIntraQueuePreemptionFairOPFairnessAcrossUsers()
      throws IOException {
    /**
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+------+---------+-----------+
     *   | APP  | USER  | USED | PENDING | FAIRSHARE |
     *   +--------------+------+---------+-----------+
     *   | app1 | user1 | 51   | 0       | 25        |
     *   | app2 | user1 | 49   | 0       | 25        |
     *   | app3 | user2 | 0    | 50      | 50        |
     *   +--------------+------+---------+-----------+
     *
     * User1 has 2 apps, Each app will have
     * 25% fairshare (1/2 of 50% of user's fairShare)
     * App3 has fairShare
     *    = UL / num of apps in user(50%) = 50%
     * So app3 asks for 50 resources.
     *  ~25 are given by app1 and app2.
     */
    // Enable FairOrderingPolicy for yarn.scheduler.capacity.root.a
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".a.ordering-policy", "fair");
    // Make sure all containers will be preempted in a single round.
    conf.setFloat(CapacitySchedulerConfiguration.
            INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 1.0);

    String labelsConfig = "=100:100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100:100 100:100 100:100 1:1 0]);" + // root
            "-a(=[100:100 100:100 100:100 1:1 0])"; // a
    String appsConfig =
    // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1 and app2, user1 in a
            + "(1,1:1,n1,,51,false,0,user1);" +
            "a\t"
            + "(1,1:1,n1,,49,false,0,user1);" +
            "a\t" // app3, user2 in a
            + "(1,1:1,n1,,0,false,50:50,user2);";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(25)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(eventHandler, times(23)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testIntraQueuePreemptionFairOPUserLimitPreserved()
      throws IOException {
    /**
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+------------+------+---------+-----------+
     *   | APP  | USER  | USER-LIMIT | USED | PENDING | FAIRSHARE |
     *   +--------------+------------+------+---------+-----------+
     *   | app1 | user1 |  50        | 100  | 0       | 50        |
     *   | app3 | user2 |  50        | 0    | 50      | 50        |
     *   | app4 | user3 |  50        | 0    | 50      | 50        |
     *   +--------------+------------+------+---------+-----------+
     *
     * User1 has 1 app, This app will have 50% fairshare
     * (100% of user1's fairShare)
     * So 50 containers from app1 should be preempted.
     */
    // Enable FairOrderingPolicy for yarn.scheduler.capacity.root.a
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".a.ordering-policy", "fair");
    // Make sure all containers will be preempted in a single round.
    conf.setFloat(CapacitySchedulerConfiguration.
            INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 1.0);

    String labelsConfig = "=100:100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100:100 100:100 100:100 1:1 0]);" + // root
            "-a(=[100:100 100:100 100:100 1:1 0])"; // a
    String appsConfig =
    // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1 and app2, user1 in a
            + "(1,1:1,n1,,100,false,0,user1)" + "\t50;" +
            "a\t" // app3, user2 in a
            + "(1,1:1,n1,,0,false,50,user2)" + "\t50;" +
            "a\t" // app3, user2 in a
            + "(1,1:1,n1,,0,false,50:50,user3)" + "\t50;";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(49)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  @Test
  public void testIntraQueuePreemptionFairOPwithUL100noContPreempted()
      throws IOException {
    /**
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 100%
     *   +--------------+------+---------+-----------+
     *   | APP  | USER  | USED | PENDING | FAIRSHARE |
     *   +--------------+------+---------+-----------+
     *   | app1 | user1 | 10   | 0       | 50        |
     *   | app2 | user1 | 0    | 100     | 50        |
     *   | app3 | user2 | 90   | 0       | 100       |
     *   +--------------+------+---------+-----------+
     *
     * User1 has 2 apps, Each app will have
     * 25% fairshare (1/2 of 100% of queue cap)
     * App3 has fairShare
     *    = 100% of queue cap / num of apps in user = 100%
     * So app1 and app3 don't release any resources as they are below their FS.
     */
    // Enable FairOrderingPolicy for yarn.scheduler.capacity.root.a
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".a.ordering-policy", "fair");
    // Make sure all containers will be preempted in a single round.
    conf.setFloat(CapacitySchedulerConfiguration.
            INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 1.0);

    String labelsConfig = "=100:100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100:100 100:100 100:100 1:1 0]);" + // root
            "-a(=[100:100 100:100 100:100 1:1 0])"; // a
    String appsConfig =
        // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1 and app2, user1 in a
            + "(1,1,n1,,10,false,0,user1)" + "\t100;" +
            "a\t"
            + "(1,1,n1,,0,false,100,user1)" + "\t100;" +
            "a\t" // app3, user2 in a
            + "(1,1,n1,,90,false,0,user2)" + "\t100;";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));

    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }

  @Test
  public void testIntraQueuePreemptionFairOPwithUL100oneUserManyApps()
      throws IOException {
    /**
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 100%
     *   +--------------+------+---------+-----------+
     *   | APP  | USER  | USED | PENDING | FAIRSHARE |
     *   +--------------+------+---------+-----------+
     *   | app1 | user1 | 15   | 10      | 20        |
     *   | app2 | user1 | 17   | 10      | 20        |
     *   | app3 | user1 | 20   | 10      | 20        |
     *   | app4 | user1 | 23   | 10      | 20        |
     *   | app5 | user1 | 25   | 10      | 20        |
     *   | app6 | user2 | 0    | 100     | 100       |
     *   +--------------+------+---------+-----------+
     *
     * User1 has 5 apps, Each app will have
     * 20% fairshare (1/5 of queue cap)
     * App6 has fairShare
     *    = 100% of queue cap / num of apps in user = 100%
     * So app4 to app5 only release resources.
     */
    // Enable FairOrderingPolicy for yarn.scheduler.capacity.root.a
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".a.ordering-policy", "fair");
    // Make sure all containers will be preempted in a single round.
    conf.setFloat(CapacitySchedulerConfiguration.
            INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 1.0);

    String labelsConfig = "=100:100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100:100 100:100 100:100 1:1 0]);" + // root
            "-a(=[100:100 100:100 100:100 1:1 0])"; // a
    String appsConfig =
        // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1 and app2, user1 in a
            + "(1,1,n1,,15,false,10,user1)" + "\t100;" +
            "a\t"
            + "(1,1,n1,,17,false,10,user1)" + "\t100;" +
            "a\t"
            + "(1,1,n1,,20,false,10,user1)" + "\t100;" +
            "a\t"
            + "(1,1,n1,,23,false,10,user1)" + "\t100;" +
            "a\t"
            + "(1,1,n1,,25,false,10,user1)" + "\t100;" +
            "a\t" // app3, user2 in a
            + "(1,1,n1,,0,false,100,user2)" + "\t100;";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(4)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(5))));

    verify(eventHandler, times(2)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
  }

  @Test
  public void testIntraQueuePreemptionFairOPwithUL100MultiUsersSkewedRes()
      throws IOException {
    /**
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 100%
     *   +--------------+------+---------+-----------+
     *   | APP  | USER  | USED | PENDING | FAIRSHARE |
     *   +--------------+------+---------+-----------+
     *   | app1 | user1 | 10   | 100     | 50        |
     *   | app2 | user1 | 70   | 100     | 50        |
     *   | app3 | user2 | 20   | 0       | 50        |
     *   | app4 | user2 | 0    | 100     | 50        |
     *   +--------------+------+---------+-----------+
     *
     * User1 has 2 apps, Each app will have
     * 25% fairshare (1/2 of 100% of queue cap)
     * App3 has fairShare
     *    = 100% of queue cap / num of apps in user = 100%
     * So only app2 releases ~20 resources to be given to app1.
     */
    // Enable FairOrderingPolicy for yarn.scheduler.capacity.root.a
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".a.ordering-policy", "fair");
    // Make sure all containers will be preempted in a single round.
    conf.setFloat(CapacitySchedulerConfiguration.
            INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 1.0);

    String labelsConfig = "=100:100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100:100 100:100 100:100 1:1 0]);" + // root
            "-a(=[100:100 100:100 100:100 1:1 0])"; // a
    String appsConfig =
        // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1 and app2, user1 in a
            + "(1,1,n1,,10,false,100,user1)" + "\t100;" +
            "a\t"
            + "(1,1,n1,,70,false,0,user1)" + "\t100;" +
            "a\t" // app3, user2 in a
            + "(1,1,n1,,20,false,0,user2)" + "\t100;" +
            "a\t" // app3, user2 in a
            + "(1,1,n1,,0,false,100,user2)" + "\t100;";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(19)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testIntraQueuePreemptionFairOPMultiUsersSkewedRes()
      throws IOException {
    /**
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+------+---------+-----------+
     *   | APP  | USER  | USED | PENDING | FAIRSHARE |
     *   +--------------+------+---------+-----------+
     *   | app1 | user1 | 10   | 0       | 25        |
     *   | app2 | user1 | 99   | 0       | 25        |
     *   | app3 | user2 | 0    | 100     | 50        |
     *   | app4 | user3 | 0    | 100     | 50        |
     *   +--------------+------+---------+-----------+
     *
     * User1 has 2 apps, Each app will have
     * 25% fairshare (1/2 of 50% of user's fairShare)
     * App3 and App4 has fairShare
     *    = UL / num of apps in user = 50%
     * So app3 and app4 should ask for 33 resources each from app2.
     * But app2 can only give 50 resources
     * because it will drop below its UL after that.
     */
    // Enable FairOrderingPolicy for yarn.scheduler.capacity.root.a
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".a.ordering-policy", "fair");
    // Make sure all containers will be preempted in a single round.
    conf.setFloat(CapacitySchedulerConfiguration.
            INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 1.0);

    String labelsConfig = "=100:100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100:100 100:100 100:100 1:1 0]);" + // root
            "-a(=[100:100 100:100 100:100 1:1 0])"; // a
    String appsConfig =
        // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1 and app2, user1 in a
            + "(1,1,n1,,1,false,0,user1)" + "\t50;" +
            "a\t"
            + "(1,1,n1,,99,false,0,user1)" + "\t50;" +
            "a\t" // app3, user2 in a
            + "(1,1,n1,,40,false,100,user2)" + "\t50;" +
            "a\t" // app3, user2 in a
            + "(1,1,n1,,0,false,100,user3)" + "\t50;";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(50)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testIntraQueuePreemptionFairNonSchedulablePendingApps()
      throws IOException {
    // Enable FairOrderingPolicy for yarn.scheduler.capacity.root.a
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".a.ordering-policy", "fair");
    // Make sure all containers will be preempted in a single round.
    conf.setFloat(CapacitySchedulerConfiguration.
            INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 1.0);

    String labelsConfig = "=100:100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100:100 100:100 100:100 1:1 0]);" + // root
            "-a(=[100:100 100:100 100:100 1:1 0])"; // a
    String appsConfig =
        // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1 and app2, user1 in a
            + "(1,1,n1,,1,false,0,user1);" +
            "a\t"
            + "(1,1,n1,,99,false,0,user1);" +
            "a\t" // app3, user1 in a
            + "(1,1,n1,,0,false,10,user1)" + "\t100" + "\tfalse;";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testIntraQueuePreemptionFairWithULNonSchedulablePendingApps()
      throws IOException {
    // Enable FairOrderingPolicy for yarn.scheduler.capacity.root.a
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".a.ordering-policy", "fair");
    // Make sure all containers will be preempted in a single round.
    conf.setFloat(CapacitySchedulerConfiguration.
            INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 1.0);

    String labelsConfig = "=100:100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100:100 100:100 100:100 1:1 0]);" + // root
            "-a(=[100:100 100:100 100:100 1:1 0])"; // a
    String appsConfig =
        // queueName\t(prio,resource,host,expression,#repeat,reserved,pending,user)
        "a\t" // app1 and app2, user1 in a
            + "(1,1,n1,,1,false,0,user1);" +
            "a\t"
            + "(1,1,n1,,99,false,0,user2);" +
            "a\t" // app3, user1 in a
            + "(1,1,n1,,0,false,10,user2)" + "\t34" + "\tfalse;";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }
}
