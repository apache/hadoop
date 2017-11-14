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

import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestProportionalCapacityPreemptionPolicyForNodePartitions
    extends ProportionalCapacityPreemptionPolicyMockFramework {
  @Before
  public void setup() {
    super.setup();
    policy = new ProportionalCapacityPreemptionPolicy(rmContext, cs, mClock);
  }

  @Test
  public void testNodePartitionPreemptionRespectGuaranteedCapacity()
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
     * Both a/b can access x, and guaranteed capacity of them is 50:50. Two
     * nodes, n1 has 100 x, n2 has 100 NO_LABEL 4 applications in the cluster,
     * app1/app2 in a, and app3/app4 in b.
     * app1 uses 80 x, app2 uses 20 NO_LABEL, app3 uses 20 x, app4 uses 80 NO_LABEL.
     * Both a/b have 50 pending resource for x and NO_LABEL
     *
     * After preemption, it should preempt 30 from app1, and 30 from app4.
     */
    String labelsConfig =
        "=100,true;" + // default partition
        "x=100,true"; // partition=x
    String nodesConfig =
        "n1=x;" + // n1 has partition=x
        "n2="; // n2 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100],x=[100 100 100 100]);" + //root
        "-a(=[50 100 20 50],x=[50 100 80 50]);" + // a
        "-b(=[50 100 80 50],x=[50 100 20 50])"; // b
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t" // app1 in a
        + "(1,1,n1,x,80,false);" + // 80 * x in n1
        "a\t" // app2 in a
        + "(1,1,n2,,20,false);" + // 20 default in n2
        "b\t" // app3 in b
        + "(1,1,n1,x,20,false);" + // 80 * x in n1
        "b\t" // app4 in b
        + "(1,1,n2,,80,false)"; // 20 default in n2

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // 30 preempted from app1, 30 preempted from app4, and nothing preempted
    // from app2/app3
    verify(mDisp, times(30)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(1))));
    verify(mDisp, times(30)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(4))));
    verify(mDisp, never()).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(2))));
    verify(mDisp, never()).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(3))));
  }

  @Test
  public void testNodePartitionPreemptionNotHappenBetweenSatisfiedQueues()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *         root
     *       /  |  \
     *      a   b   c
     * </pre>
     *
     * Both a/b/c can access x, and guaranteed_capacity(x) of them is 80:10:10.
     * a/b's max resource is 100, and c's max resource is 30.
     *
     * Two nodes, n1 has 100 x, n2 has 100 NO_LABEL.
     *
     * 2 apps in cluster.
     * app1 in b and app2 in c.
     *
     * app1 uses 90x, and app2 use 10x. We don't expect preemption happen
     * between them because all of them are satisfied
     */
    String labelsConfig =
        "=100,true;" + // default partition
        "x=100,true"; // partition=x
    String nodesConfig =
        "n1=x;" + // n1 has partition=x
        "n2="; // n2 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100],x=[100 100 100 100]);" + //root
        "-a(=[80 80 0 0],x=[80 80 0 0]);" + // a
        "-b(=[10 100 0 0],x=[10 100 90 50]);" + // b
        "-c(=[10 100 0 0],x=[10 30 10 50])"; //c
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "b\t" // app1 in b
        + "(1,1,n1,x,90,false);" + // 80 * x in n1
        "c\t" // app2 in c
        + "(1,1,n1,x,10,false)"; // 20 default in n2

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // No preemption happens
    verify(mDisp, never()).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(1))));
    verify(mDisp, never()).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(2))));
  }

  @Test
  public void testNodePartitionPreemptionOfIgnoreExclusivityAndRespectCapacity()
      throws IOException {
    /**
     * <pre>
     *       root
     *       /  \
     *      a    b
     * </pre>
     *
     * Both a/b can access x, and guaranteed capacity of them is 50:50. Two
     * nodes, n1 has 100 x, n2 has 100 NO_LABEL and 2 applications in the cluster,
     * app1/app2 in a
     * app1 uses 20x (ignoreExclusivity), app2 uses 80x (respectExclusivity).
     *
     * b has 100 pending resource of x
     *
     * After preemption, it should preempt 20 from app1, and 30 from app2.
     */
    String labelsConfig =
        "=100,true;" + // default partition
        "x=100,false"; // partition=x
    String nodesConfig =
        "n1=x;" + // n1 has partition=x
        "n2="; // n2 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100],x=[100 100 100 100]);" + //root
        "-a(=[50 100 0 0],x=[50 100 100 50]);" + // a
        "-b(=[50 100 0 0],x=[50 100 0 100])"; // b
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t" // app1 in a
        + "(1,1,n1,x,1,false)"  // 1 * x in n1 (it's AM container)
        + "(1,1,n1,,20,false);" + // 20 * x in n1 (ignoreExclusivity)
        "a\t" // app2 in a
        + "(1,1,n1,x,79,false)"; // 79 * x

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // 30 preempted from app1, 30 preempted from app4, and nothing preempted
    // from app2/app3
    verify(mDisp, times(20)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(1))));
    verify(mDisp, times(30)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(2))));
  }

  @Test
  public void testNodePartitionPreemptionOfSkippingAMContainer()
      throws IOException {
    /**
     * <pre>
     *       root
     *       /  \
     *      a    b
     * </pre>
     *
     * Both a/b can access x, and guaranteed capacity of them is 20:80. Two
     * nodes, n1 has 100 x, n2 has 100 NO_LABEL and 2 applications in the cluster,
     * app1/app2/app3/app4/app5 in a, both uses 20 resources.
     *
     * b has 100 pending resource of x
     *
     * After preemption, it should preempt 19 from app[5-2] an 4 from app1
     */
    String labelsConfig =
        "=100,true;" + // default partition
        "x=100,true"; // partition=x
    String nodesConfig =
        "n1=x;" + // n1 has partition=x
        "n2="; // n2 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100],x=[100 100 100 100]);" + //root
        "-a(=[50 100 0 0],x=[20 100 100 50]);" + // a
        "-b(=[50 100 0 0],x=[80 100 0 100])"; // b
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t" // app1 in a
        + "(1,1,n1,x,20,false);" + // uses 20 resource
        "a\t" // app2 in a
        + "(1,1,n1,x,20,false);" + // uses 20 resource
        "a\t" // app3 in a
        + "(1,1,n1,x,20,false);" + // uses 20 resource
        "a\t" // app4 in a
        + "(1,1,n1,x,20,false);" + // uses 20 resource
        "a\t" // app5 in a
        + "(1,1,n1,x,20,false);";  // uses 20 resource

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // 4 from app1
    verify(mDisp, times(4)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(1))));
    // 19 from app2-app5
    verify(mDisp, times(19)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(2))));
    verify(mDisp, times(19)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(3))));
    verify(mDisp, times(19)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(4))));
    verify(mDisp, times(19)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(5))));
  }

  @Test
  public void testNodePartitionPreemptionOfAMContainer()
      throws IOException {
    /**
     * <pre>
     *       root
     *       /  \
     *      a    b
     * </pre>
     *
     * Both a/b can access x, and guaranteed capacity of them is 3:97. Two
     * nodes, n1 has 100 x, n2 has 100 NO_LABEL.
     *
     * app1/app2/app3/app4/app5 in a, both uses 20 resources(x)
     *
     * b has 100 pending resource of x
     *
     * After preemption, it should preempt 20 from app4/app5 an 19 from
     * app1-app3. App4/app5's AM container will be preempted
     */
    String labelsConfig =
        "=100,true;" + // default partition
        "x=100,true"; // partition=x
    String nodesConfig =
        "n1=x;" + // n1 has partition=x
        "n2="; // n2 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100],x=[100 100 100 100]);" + //root
        "-a(=[50 100 0 0],x=[3 100 100 50]);" + // a
        "-b(=[50 100 0 0],x=[97 100 0 100])"; // b
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t" // app1 in a
        + "(1,1,n1,x,20,false);" + // uses 20 resource
        "a\t" // app2 in a
        + "(1,1,n1,x,20,false);" + // uses 20 resource
        "a\t" // app3 in a
        + "(1,1,n1,x,20,false);" + // uses 20 resource
        "a\t" // app4 in a
        + "(1,1,n1,x,20,false);" + // uses 20 resource
        "a\t" // app5 in a
        + "(1,1,n1,x,20,false);";  // uses 20 resource

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // 4 from app1
    verify(mDisp, times(19)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(1))));
    // 19 from app2-app5
    verify(mDisp, times(19)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(2))));
    verify(mDisp, times(19)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(3))));
    verify(mDisp, times(20)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(4))));
    verify(mDisp, times(20)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(5))));
  }

  @Test
  public void testNodePartitionDisablePreemptionForSingleLevelQueue()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *         root
     *       /  |  \
     *      a   b   c
     * </pre>
     *
     * Both a/b/c can access x, and guaranteed_capacity(x) of them is 40:20:40.
     * a/b/c's max resource is 100. b is disable-preemption
     *
     * Two nodes, n1 has 100 x, n2 has 100 NO_LABEL.
     *
     * 2 apps in cluster. app1 in a (usage=50), app2 in b(usage=30), app3 in
     * c(usage=20). All of them have 50 pending resource.
     *
     * After preemption, app1 will be preempt 10 containers and app2 will not be
     * preempted
     */
    String labelsConfig =
        "=100,true;" + // default partition
        "x=100,true"; // partition=x
    String nodesConfig =
        "n1=x;" + // n1 has partition=x
        "n2="; // n2 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100],x=[100 100 100 100]);" + //root
        "-a(=[80 80 0 0],x=[40 100 50 50]);" + // a
        "-b(=[10 100 0 0],x=[20 100 30 0]);" + // b
        "-c(=[10 100 0 0],x=[40 100 20 50])"; //c
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t" // app1 in a
        + "(1,1,n1,x,50,false);" + // 50x in n1
        "b\t" // app2 in b
        + "(1,1,n1,x,30,false);" + // 30x in n1
        "c\t" // app3 in c
        + "(1,1,n1,x,20,false)"; // 20x in n1

    conf.setPreemptionDisabled("root.b", true);
    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // 10 preempted from app1, nothing preempted from app2-app3
    verify(mDisp, times(10)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(1))));
    verify(mDisp, never()).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(2))));
    verify(mDisp, never()).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(3))));
  }

  @Test
  public void testNodePartitionNonAccessibleQueuesSharePartitionedResource()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *           root
     *        _________
     *       /  |   |  \
     *      a   b   c   d
     * </pre>
     *
     * a/b can access x, their capacity is 50:50 c/d cannot access x.
     *
     * a uses 0, wants 30
     * b(app1) uses 30, wants 0
     * c(app2)&d(app3) use 35, wants 50
     *
     * After preemption, c/d will be preempted 15 containers, because idle
     * resource = 100 - 30 (which is used by b) - 30 (which is asked by a) = 40
     * will be divided by c/d, so each of c/d get 20.
     */
    String labelsConfig =
        "=100,true;" + // default partition
        "x=100,false"; // partition=x
    String nodesConfig =
        "n1=x;" + // n1 has partition=x
        "n2="; // n2 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 100 100],x=[100 100 100 100]);" + //root
        "-a(=[25 100 0 0],x=[50 100 0 30]);" + // a
        "-b(=[25 100 0 0],x=[50 100 30 0]);" + // b
        "-c(=[25 100 1 0],x=[0 0 35 50]);" + //c
        "-d(=[25 100 1 0],x=[0 0 35 50])"; //d
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "b\t" // app1 in b
        + "(1,1,n1,x,30,false);" + // 50x in n1
        "c\t" // app2 in c
        + "(1,1,n2,,1,false)" // AM container (in n2)
        + "(1,1,n1,,30,false);" + // 30x in n1 (ignore exclusivity)
        "d\t" // app3 in d
        + "(1,1,n2,,1,false)" // AM container (in n2)
        + "(1,1,n1,,30,false)"; // 30x in n1 (ignore exclusivity)

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // 15 will be preempted app2/app3
    verify(mDisp, times(15)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(2))));
    verify(mDisp, times(15)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(3))));
    verify(mDisp, never()).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(1))));
  }

  @Test
  public void testHierarchyPreemptionForMultiplePartitions()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *           root
     *           /  \
     *          a    b
     *        /  \  /  \
     *       a1  a2 b1  b2
     * </pre>
     *
     * Both a/b can access x/y, and in all hierarchy capacity ratio is 50:50.
     * So for a1/a2/b1/b2, all of them can access 25x, 25y
     *
     * a1 uses 35x, 25y
     * a2 uses 25x, 15y
     * b1 uses 15x, 25y
     * b2 uses 25x 35y
     *
     * So as a result, a2 will preempt from b2, and b1 will preempt from a1.
     *
     * After preemption, a1 will be preempted 10x and b2 will be preempted 10y.
     */
    String labelsConfig =
        "=100,true;" + // default partition
        "x=100,true;" + // partition=x
        "y=100,true";   // partition=y
    String nodesConfig =
        "n1=x;" + // n1 has partition=x
        "n2=y;" + // n2 has partition=y
        "n3="; // n3 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 0 0],x=[100 100 100 100],y=[100 100 100 100]);" + //root
        "-a(=[50 100 0 0],x=[50 100 60 40],y=[50 100 40 40]);" + // a
        "--a1(=[25 100 0 0],x=[25 100 35 20],y=[25 100 25 20]);" + // a1
        "--a2(=[25 100 0 0],x=[25 100 25 20],y=[25 100 15 20]);" + // a2
        "-b(=[50 100 0 0],x=[50 100 40 40],y=[50 100 60 40]);" + // b
        "--b1(=[25 100 0 0],x=[25 100 15 20],y=[25 100 25 20]);" + // b1
        "--b2(=[25 100 0 0],x=[25 100 25 20],y=[25 100 35 20])"; // b2
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a1\t" // app1 in a1
        + "(1,1,n1,x,35,false)" // 35 of x
        + "(1,1,n2,y,25,false);" + // 25 of y
        "a2\t" // app2 in a2
        + "(1,1,n1,x,25,false)" // 25 of x
        + "(1,1,n2,y,15,false);" + // 15 of y
        "b1\t" // app3 in b1
        + "(1,1,n1,x,15,false)" // 15 of x
        + "(1,1,n2,y,25,false);" + // 25 of y
        "b2\t" // app4 in b2
        + "(1,1,n1,x,25,false)" // 25 of x
        + "(1,1,n2,y,35,false)"; // 35 of y

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // 10 will be preempted from app1 (a1) /app4 (b2)
    verify(mDisp, times(10)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(1))));
    verify(mDisp, times(10)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(4))));
    verify(mDisp, never()).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(2))));
    verify(mDisp, never()).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(3))));
  }

  @Test
  public void testHierarchyPreemptionForDifferenceAcessibility()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *           root
     *           /  \
     *          a    b
     *        /  \  /  \
     *       a1  a2 b1  b2
     * </pre>
     *
     * a can access x only and b can access y only
     *
     * Capacities of a1/a2, b1/b2 is 50:50
     *
     * a1 uses 100x and b1 uses 80y
     *
     * So as a result, a1 will be preempted 50 containers and b1 will be
     * preempted 30 containers
     */
    String labelsConfig =
        "=100,true;" + // default partition
        "x=100,true;" + // partition=x
        "y=100,true";   // partition=y
    String nodesConfig =
        "n1=x;" + // n1 has partition=x
        "n2=y;" + // n2 has partition=y
        "n3="; // n3 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100 100 0 0],x=[100 100 100 100],y=[100 100 100 100]);" + //root
        "-a(=[50 100 0 0],x=[100 100 100 100]);" + // a
        "--a1(=[25 100 0 0],x=[50 100 100 0]);" + // a1
        "--a2(=[25 100 0 0],x=[50 100 0 100]);" + // a2
        "-b(=[50 100 0 0],y=[100 100 80 100]);" + // b
        "--b1(=[25 100 0 0],y=[50 100 80 0]);" + // b1
        "--b2(=[25 100 0 0],y=[50 100 0 100])"; // b2
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a1\t" // app1 in a1
        + "(1,1,n1,x,100,false);" + // 100 of x
        "b1\t" // app2 in b1
        + "(1,1,n2,y,80,false)"; // 80 of y

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(mDisp, times(50)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(1))));
    verify(mDisp, times(30)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(2))));
  }

  @Test
  public void testNodePartitionPreemptionWithVCoreResource() throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *       /  \
     *      a    b
     * </pre>
     *
     * Both a/b can access x, and guaranteed capacity of them is 50:50. Two
     * nodes, n1 has 100 x, n2 has 100 NO_LABEL 4 applications in the cluster,
     * app1/app2 in a, and app3/app4 in b. app1 uses 80 x, app2 uses 20
     * NO_LABEL, app3 uses 20 x, app4 uses 80 NO_LABEL. Both a/b have 50 pending
     * resource for x and NO_LABEL
     *
     * After preemption, it should preempt 30 from app1, and 30 from app4.
     */
    String labelsConfig = "=100:200,true;" + // default partition
        "x=100:200,true"; // partition=x
    String nodesConfig = "n1=x;" + // n1 has partition=x
        "n2="; // n2 is default partition
    String queuesConfig =
    // guaranteed,max,used,pending
    "root(=[100:200 100:200 100:200 100:200],x=[100:200 100:200 100:200 100:200]);"
        + // root
        "-a(=[50:100 100:200 20:40 50:100],x=[50:100 100:200 80:160 50:100]);" + // a
        "-b(=[50:100 100:200 80:160 50:100],x=[50:100 100:200 20:40 50:100])"; // b
    String appsConfig =
    // queueName\t(priority,resource,host,expression,#repeat,reserved)
    "a\t" // app1 in a
        + "(1,1:2,n1,x,80,false);" + // 80 * x in n1
        "a\t" // app2 in a
        + "(1,1:2,n2,,20,false);" + // 20 default in n2
        "b\t" // app3 in b
        + "(1,1:2,n1,x,20,false);" + // 20 * x in n1
        "b\t" // app4 in b
        + "(1,1:2,n2,,80,false)"; // 80 default in n2

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig, true);
    policy.editSchedule();

    // 30 preempted from app1, 30 preempted from app4, and nothing preempted
    // from app2/app3
    verify(mDisp, times(30)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(1))));
    verify(mDisp, times(30)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(4))));
    verify(mDisp, never()).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(2))));
    verify(mDisp, never()).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(3))));
  }

  @Test
  public void testNormalizeGuaranteeWithMultipleResource() throws IOException {
    // Initialize resource map
    Map<String, ResourceInformation> riMap = new HashMap<>();
    String RESOURCE_1 = "res1";

    // Initialize mandatory resources
    ResourceInformation memory = ResourceInformation.newInstance(
        ResourceInformation.MEMORY_MB.getName(),
        ResourceInformation.MEMORY_MB.getUnits(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    ResourceInformation vcores = ResourceInformation.newInstance(
        ResourceInformation.VCORES.getName(),
        ResourceInformation.VCORES.getUnits(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    riMap.put(ResourceInformation.MEMORY_URI, memory);
    riMap.put(ResourceInformation.VCORES_URI, vcores);
    riMap.put(RESOURCE_1, ResourceInformation.newInstance(RESOURCE_1, "", 0,
        ResourceTypes.COUNTABLE, 0, Integer.MAX_VALUE));

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);

    /**
     * Queue structure is:
     *
     * <pre>
     *           root
     *           /  \
     *          a    b
     *        /  \  /  \
     *       a1  a2 b1  b2
     * </pre>
     *
     * a1 and b2 are using most of resources.
     * a2 and b1 needs more resources. Both are under served.
     * hence demand will consider both queue's need while trying to
     * do preemption.
     */
    String labelsConfig =
        "=100,true;";
    String nodesConfig =
        "n1=;"; // n1 is default partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[100:100:10 100:100:10 100:100:10 100:100:10]);" + //root
        "-a(=[50:80:4 100:100:10 80:90:10 30:20:4]);" + // a
        "--a1(=[25:30:2 100:50:10 80:90:10 0]);" + // a1
        "--a2(=[25:50:2 100:50:10 0 30:20:4]);" + // a2
        "-b(=[50:20:6 100:100:10 20:10 40:50:8]);" + // b
        "--b1(=[25:5:4 100:20:10 0 20:10:4]);" + // b1
        "--b2(=[25:15:2 100:20:10 20:10 20:10:4])"; // b2
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a1\t" // app1 in a1
        + "(1,8:9:1,n1,,10,false);" +
        "b2\t" // app2 in b2
        + "(1,2:1,n1,,10,false)"; // 80 of y

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(mDisp, times(7)).handle(
        argThat(new IsPreemptionRequestFor(getAppAttemptId(1))));

    riMap.remove(RESOURCE_1);
    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);
  }
}
