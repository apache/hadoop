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
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test class for IntraQueuePreemption scenarios.
 */
public class TestProportionalCapacityPreemptionPolicyIntraQueueUserLimit
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
  public void testSimpleIntraQueuePreemptionWithTwoUsers()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *        |
     *        a
     * </pre>
     *
     * Scenario:
     * Preconditions:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+----------+------+---------+
     *   | APP  | USER  | PRIORITY | USED | PENDING |
     *   +--------------+----------+------+---------+
     *   | app1 | user1 | 1        | 100  | 0       |
     *   | app2 | user2 | 1        | 0    | 30      |
     *   +--------------+----------+------+---------+
     * Hence in queueA of 100, each user has a quota of 50. app1 of high priority
     * has a demand of 0 and its already using 100. app2 from user2 has a demand
     * of 30, and UL is 50. 30 would be preempted from app1.
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
        "root(=[100 100 100 30 0]);" + // root
            "-a(=[100 100 100 30 0])"; // a

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,100,false,0,user1);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,0,false,30,user2)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2 needs more resource and its well under its user-limit. Hence preempt
    // resources from app1.
    verify(mDisp, times(30)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  @Test
  public void testNoIntraQueuePreemptionWithSingleUser()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *        |
     *        a
     * </pre>
     *
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+----------+------+---------+
     *   | APP  | USER  | PRIORITY | USED | PENDING |
     *   +--------------+----------+------+---------+
     *   | app1 | user1 | 1        | 100  | 0       |
     *   | app2 | user1 | 1        | 0    | 30      |
     *   +--------------+----------+------+---------+
     * Given single user, lower priority/late submitted apps has to
     * wait.
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
        "root(=[100 100 100 30 0]);" + // root
            "-a(=[100 100 100 30 0])"; // a

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,100,false,0,user1);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,0,false,30,user1)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2 needs more resource. Since app1,2 are from same user, there wont be
    // any preemption.
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  @Test
  public void testNoIntraQueuePreemptionWithTwoUserUnderUserLimit()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *        |
     *        a
     * </pre>
     *
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+----------+------+---------+
     *   | APP  | USER  | PRIORITY | USED | PENDING |
     *   +--------------+----------+------+---------+
     *   | app1 | user1 | 1        | 50   | 0       |
     *   | app2 | user2 | 1        | 30   | 30      |
     *   +--------------+----------+------+---------+
     * Hence in queueA of 100, each user has a quota of 50. app1 of high priority
     * has a demand of 0 and its already using 50. app2 from user2 has a demand
     * of 30, and UL is 50. Since app1 is under UL, there should not be any
     * preemption.
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
        "root(=[100 100 80 30 0]);" + // root
            "-a(=[100 100 80 30 0])"; // a

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,50,false,0,user1);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,30,false,30,user2)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2 needs more resource. Since app1,2 are from same user, there wont be
    // any preemption.
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  @Test
  public void testSimpleIntraQueuePreemptionWithTwoUsersWithAppPriority()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *        |
     *        a
     * </pre>
     *
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+----------+------+---------+
     *   | APP  | USER  | PRIORITY | USED | PENDING |
     *   +--------------+----------+------+---------+
     *   | app1 | user1 | 2        | 100  | 0       |
     *   | app2 | user2 | 1        | 0    | 30      |
     *   +--------------+----------+------+---------+
     * Hence in queueA of 100, each user has a quota of 50. app1 of high priority
     * has a demand of 0 and its already using 100. app2 from user2 has a demand
     * of 30, and UL is 50. 30 would be preempted from app1.
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
        "root(=[100 100 100 30 0]);" + // root
            "-a(=[100 100 100 30 0])"; // a

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(2,1,n1,,100,false,0,user1);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,0,false,30,user2)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2 needs more resource and its well under its user-limit. Hence preempt
    // resources from app1 even though its priority is more than app2.
    verify(mDisp, times(30)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  @Test
  public void testIntraQueuePreemptionOfUserLimitWithMultipleApps()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *        |
     *        a
     * </pre>
     *
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+----------+------+---------+
     *   | APP  | USER  | PRIORITY | USED | PENDING |
     *   +--------------+----------+------+---------+
     *   | app1 | user1 | 1        | 30   | 30      |
     *   | app2 | user2 | 1        | 20   | 20      |
     *   | app3 | user1 | 1        | 30   | 30      |
     *   | app4 | user2 | 1        | 0    | 10      |
     *   +--------------+----------+------+---------+
     * Hence in queueA of 100, each user has a quota of 50. Now have multiple
     * apps and check for preemption across apps.
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
        "root(=[100 100 80 90 0]);" + // root
            "-a(=[100 100 80 90 0])"; // a

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,30,false,30,user1);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,20,false,20,user2);" +
            "a\t" // app3 in a
            + "(1,1,n1,,30,false,30,user1);" +
            "a\t" // app4 in a
            + "(1,1,n1,,0,false,10,user2)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2/app4 needs more resource and its well under its user-limit. Hence
    // preempt resources from app3 (compare to app1, app3 has low priority).
    verify(mDisp, times(9)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
  }

  @Test
  public void testNoPreemptionOfUserLimitWithMultipleAppsAndSameUser()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *        |
     *        a
     * </pre>
     *
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+----------+------+---------+
     *   | APP  | USER  | PRIORITY | USED | PENDING |
     *   +--------------+----------+------+---------+
     *   | app1 | user1 | 1        | 30   | 30      |
     *   | app2 | user1 | 1        | 20   | 20      |
     *   | app3 | user1 | 1        | 30   | 30      |
     *   | app4 | user1 | 1        | 0    | 10      |
     *   +--------------+----------+------+---------+
     * Hence in queueA of 100, each user has a quota of 50. Now have multiple
     * apps and check for preemption across apps.
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
        "root(=[100 100 80 90 0]);" + // root
            "-a(=[100 100 80 90 0])"; // a

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,30,false,20,user1);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,20,false,20,user1);" +
            "a\t" // app3 in a
            + "(1,1,n1,,30,false,30,user1);" +
            "a\t" // app4 in a
            + "(1,1,n1,,0,false,10,user1)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2/app4 needs more resource and its well under its user-limit. Hence
    // preempt resources from app3 (compare to app1, app3 has low priority).
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(3))));
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(4))));
  }

  @Test
  public void testIntraQueuePreemptionOfUserLimitWitAppsOfDifferentPriority()
      throws IOException {
    /**
     * Queue structure is:
     * <pre>
     *       root
     *        |
     *        a
     * </pre>
     *
     * Scenario:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+----------+------+---------+
     *   | APP  | USER  | PRIORITY | USED | PENDING |
     *   +--------------+----------+------+---------+
     *   | app1 | user1 | 3        | 30   | 30      |
     *   | app2 | user2 | 1        | 20   | 20      |
     *   | app3 | user1 | 4        | 30   | 0       |
     *   | app4 | user2 | 1        | 0    | 10      |
     *   +--------------+----------+------+---------+
     * Hence in queueA of 100, each user has a quota of 50. Now have multiple
     * apps and check for preemption across apps.
     */

    // Set max preemption limit as 50%.
    conf.setFloat(
        CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 0.5);

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 80 60 0]);" + // root
            "-a(=[100 100 80 60 0])"; // b

    String appsConfig =
        // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(3,1,n1,,30,false,30,user1);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,20,false,20,user2);" + "a\t" // app3 in a
            + "(4,1,n1,,30,false,0,user1);" + "a\t" // app4 in a
            + "(1,1,n1,,0,false,10,user2)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2/app4 needs more resource and its well under its user-limit. Hence
    // preempt resources from app1 (compare to app3, app1 has low priority).
    verify(mDisp, times(9)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  @Test
  public void testIntraQueuePreemptionOfUserLimitInTwoQueues()
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
     * that intra-queue preemption could occur in two queues when user-limit
     * irreuglarity is present in queue.
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
        "root(=[100 100 90 80 0]);" + // root
            "-a(=[60 100 55 60 0]);" + // a
            "-b(=[40 100 35 20 0])"; // b

    String appsConfig =
        // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(3,1,n1,,20,false,30,user1);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,20,false,20,user2);" +
            "a\t" // app3 in a
            + "(4,1,n1,,15,false,0,user1);" +
            "a\t" // app4 in a
            + "(1,1,n1,,0,false,10,user2);" +
            "b\t" // app5 in b
            + "(3,1,n1,,25,false,10,user1);" +
            "b\t" // app6 in b
            + "(1,1,n1,,10,false,10,user2)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2/app4 needs more resource and its well under its user-limit. Hence
    // preempt resources from app1 (compare to app3, app1 has low priority).
    verify(mDisp, times(4)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(4)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(5))));
  }

  @Test
  public void testIntraQueuePreemptionWithTwoRequestingUsers()
      throws IOException {
    /**
    * Queue structure is:
    *
    * <pre>
    *       root
    *        |
    *        a
    * </pre>
    *
    * Scenario:
    *   Queue total resources: 100
    *   Minimum user limit percent: 50%
    *   +--------------+----------+------+---------+
    *   | APP  | USER  | PRIORITY | USED | PENDING |
    *   +--------------+----------+------+---------+
    *   | app1 | user1 | 1        | 60   | 10      |
    *   | app2 | user2 | 1        | 40   | 10      |
    *   +--------------+----------+------+---------+
    * Hence in queueA of 100, each user has a quota of 50. Now have multiple
    * apps and check for preemption across apps.
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
        "root(=[100 100 100 20 0]);" + // root
            "-a(=[100 100 100 20 0])"; // a

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,60,false,10,user1);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,40,false,10,user2)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2 needs more resource and its well under its user-limit. Hence preempt
    // resources from app1.
    verify(mDisp, times(9)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testNoIntraQueuePreemptionIfBelowUserLimitAndLowPriorityExtraUsers()
      throws IOException {
     /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *        |
     *        a
     * </pre>
     *
     * Scenario:
     * Preconditions:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+----------+------+---------+
     *   | APP  | USER  | PRIORITY | USED | PENDING |
     *   +--------------+----------+------+---------+
     *   | app1 | user1 | 1        | 50   | 0       |
     *   | app2 | user2 | 1        | 50   | 0       |
     *   | app3 | user3 | 0        | 0    | 10      |
     *   +--------------+----------+------+---------+
     * This scenario should never preempt from either user1 or user2
     */

    // Set max preemption per round to 50% (this is different from minimum user
    // limit percent).
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 0.7);

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 10 0]);" + // root
            "-a(=[100 100 100 10 0])"; // a

    String appsConfig =
    // queueName\t\
    //     (priority,resource,host,label,#repeat,reserved,pending,user)\tMULP;
        "a\t(1,1,n1,,50,false,0,user1)\t50;" + // app1, user1
        "a\t(1,1,n1,,50,false,0,user2)\t50;" + // app2, user2
        "a\t(0,1,n1,,0,false,10,user3)\t50";   // app3, user3

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2/app4 needs more resource and its well under its user-limit. Hence
    // preempt resources from app1 (compare to app3, app1 has low priority).
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testNoIntraQueuePreemptionIfBelowUserLimitAndSamePriorityExtraUsers()
      throws IOException {
     /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *        |
     *        a
     * </pre>
     *
     * Scenario:
     * Preconditions:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+----------+------+---------+
     *   | APP  | USER  | PRIORITY | USED | PENDING |
     *   +--------------+----------+------+---------+
     *   | app1 | user1 | 1        | 50   | 0       |
     *   | app2 | user2 | 1        | 50   | 0       |
     *   | app3 | user3 | 1        | 0    | 10      |
     *   +--------------+----------+------+---------+
     * This scenario should never preempt from either user1 or user2
     */

    // Set max preemption per round to 50% (this is different from minimum user
    // limit percent).
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 0.7);

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 10 0]);" + // root
            "-a(=[100 100 100 10 0])"; // a

    String appsConfig =
    // queueName\t\
    //     (priority,resource,host,label,#repeat,reserved,pending,user)\tMULP;
        "a\t(1,1,n1,,50,false,0,user1)\t50;" + // app1, user1
        "a\t(1,1,n1,,50,false,0,user2)\t50;" + // app2, user2
        "a\t(1,1,n1,,0,false,10,user3)\t50";   // app3, user3

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2/app4 needs more resource and its well under its user-limit. Hence
    // preempt resources from app1 (compare to app3, app1 has low priority).
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testNoIntraQueuePreemptionIfBelowUserLimitAndHighPriorityExtraUsers()
      throws IOException {
     /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *        |
     *        a
     * </pre>
     *
     * Scenario:
     * Preconditions:
     *   Queue total resources: 100
     *   Minimum user limit percent: 50%
     *   +--------------+----------+------+---------+
     *   | APP  | USER  | PRIORITY | USED | PENDING |
     *   +--------------+----------+------+---------+
     *   | app1 | user1 | 1        | 50   | 0       |
     *   | app2 | user2 | 1        | 50   | 0       |
     *   | app3 | user3 | 5        | 0    | 10      |
     *   +--------------+----------+------+---------+
     * This scenario should never preempt from either user1 or user2
     */

    // Set max preemption per round to 50% (this is different from minimum user
    // limit percent).
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 0.7);

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 10 0]);" + // root
            "-a(=[100 100 100 10 0])"; // a

    String appsConfig =
    // queueName\t\
    //     (priority,resource,host,label,#repeat,reserved,pending,user)\tMULP;
        "a\t(1,1,n1,,50,false,0,user1)\t50;" + // app1, user1
        "a\t(1,1,n1,,50,false,0,user2)\t50;" + // app2, user2
        "a\t(5,1,n1,,0,false,10,user3)\t50";   // app3, user3

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2/app4 needs more resource and its well under its user-limit. Hence
    // preempt resources from app1 (compare to app3, app1 has low priority).
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testNoIntraQueuePreemptionWithUserLimitDeadzone()
      throws IOException {
    /**
    * Queue structure is:
    *
    * <pre>
    *       root
    *        |
    *        a
    * </pre>
    *
    * Scenario:
    *   Queue total resources: 100
    *   Minimum user limit percent: 50%
    *   +--------------+----------+------+---------+
    *   | APP  | USER  | PRIORITY | USED | PENDING |
    *   +--------------+----------+------+---------+
    *   | app1 | user1 | 1        | 60   | 10      |
    *   | app2 | user2 | 1        | 40   | 10      |
    *   +--------------+----------+------+---------+
    * Hence in queueA of 100, each user has a quota of 50. Now have multiple
    * apps and check for preemption across apps but also ensure that user's
    * usage not coming under its user-limit.
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
        "root(=[100 100 100 20 0]);" + // root
            "-a(=[100 100 100 20 0])"; // a

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,3,n1,,20,false,10,user1);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,40,false,10,user2)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2 needs more resource and its well under its user-limit. Hence preempt
    // 3 resources (9GB) from app1. We will not preempt last container as it may
    // pull user's usage under its user-limit.
    verify(mDisp, times(3)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testIntraQueuePreemptionWithUserLimitDeadzoneAndPriority()
      throws IOException {
    /**
    * Queue structure is:
    *
    * <pre>
    *       root
    *        |
    *        a
    * </pre>
    *
    * Scenario:
    *   Queue total resources: 100
    *   Minimum user limit percent: 50%
    *   +--------------+----------+------+---------+
    *   | APP  | USER  | PRIORITY | USED | PENDING |
    *   +--------------+----------+------+---------+
    *   | app1 | user1 | 1        | 60   | 10      |
    *   | app2 | user2 | 1        | 40   | 10      |
    *   +--------------+----------+------+---------+
    * Hence in queueA of 100, each user has a quota of 50. Now have multiple
    * apps and check for preemption across apps but also ensure that user's
    * usage not coming under its user-limit.
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
        "root(=[100 100 100 20 0]);" + // root
            "-a(=[100 100 100 20 0])"; // a

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,3,n1,,20,false,10,user1);" + // app1 a
            "a\t" // app2 in a
            + "(2,1,n1,,0,false,10,user1);" + // app1 a
            "a\t" // app2 in a
            + "(1,1,n1,,40,false,20,user2)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2 needs more resource and its well under its user-limit. Hence preempt
    // 3 resources (9GB) from app1. We will not preempt last container as it may
    // pull user's usage under its user-limit.
    verify(mDisp, times(3)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(mDisp, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));

    // After first round, 3 containers were preempted from app1 and resource
    // distribution will be like below.
    appsConfig =
        // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,3,n1,,17,false,10,user1);" + // app1 a
            "a\t" // app2 in a
            + "(2,1,n1,,0,false,10,user1);" + // app2 a
            "a\t" // app2 in a
            + "(1,1,n1,,49,false,11,user2)";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2 has priority demand within same user 'user1'. However user1's used
    // is alredy under UL. Hence no preemption. We will still get 3 container
    // while asserting as it was aleady selected in earlier round.
    verify(mDisp, times(3)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }

  @Test
  public void testSimpleIntraQueuePreemptionOneUserUnderOneUserAtOneUserAbove()
      throws IOException {
    conf.setFloat(CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        (float) 0.5);

    String labelsConfig = "=100,true;";
    String nodesConfig = // n1 has no label
        "n1= res=100";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 1 0]);" + // root
            "-a(=[100 100 100 1 0])"; // a

    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved,pending)
        "a\t" // app1 in a
            + "(1,1,n1,,65,false,0,user1);" +
            "a\t" // app2 in a
            + "(1,1,n1,,35,false,0,user2);" +
            "a\t" // app3 in a
            + "(1,1,n1,,0,false,1,user3)"
            ;

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // app2 is right at its user limit and app1 needs one resource. Should
    // preempt 1 container.
    verify(mDisp, times(1)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
  }
}
