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

import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.MONITORING_INTERVAL;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.NATURAL_TERMINATION_FACTOR;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.TOTAL_PREEMPTION_PER_ROUND;
import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.WAIT_TIME_BEFORE_KILL;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
public class TestProportionalCapacityPreemptionPolicyForNodePartitions {
  private static final Log LOG =
      LogFactory.getLog(TestProportionalCapacityPreemptionPolicyForNodePartitions.class);
  static final String ROOT = CapacitySchedulerConfiguration.ROOT;

  private Map<String, CSQueue> nameToCSQueues = null;
  private Map<String, Resource> partitionToResource = null;
  private Map<NodeId, SchedulerNode> nodeIdToSchedulerNodes = null;
  private RMNodeLabelsManager nlm = null;
  private RMContext rmContext = null;

  private ResourceCalculator rc = new DefaultResourceCalculator();
  private Clock mClock = null;
  private Configuration conf = null;
  private CapacitySchedulerConfiguration csConf = null;
  private CapacityScheduler cs = null;
  private EventHandler<SchedulerEvent> mDisp = null;
  private ProportionalCapacityPreemptionPolicy policy = null;
  private Resource clusterResource = null;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() {
    org.apache.log4j.Logger.getRootLogger().setLevel(
        org.apache.log4j.Level.DEBUG);

    conf = new Configuration(false);
    conf.setLong(WAIT_TIME_BEFORE_KILL, 10000);
    conf.setLong(MONITORING_INTERVAL, 3000);
    // report "ideal" preempt
    conf.setFloat(TOTAL_PREEMPTION_PER_ROUND, (float) 1.0);
    conf.setFloat(NATURAL_TERMINATION_FACTOR, (float) 1.0);
    conf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        ProportionalCapacityPreemptionPolicy.class.getCanonicalName());
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    // FairScheduler doesn't support this test,
    // Set CapacityScheduler as the scheduler for this test.
    conf.set("yarn.resourcemanager.scheduler.class",
        CapacityScheduler.class.getName());

    mClock = mock(Clock.class);
    cs = mock(CapacityScheduler.class);
    when(cs.getResourceCalculator()).thenReturn(rc);

    nlm = mock(RMNodeLabelsManager.class);
    mDisp = mock(EventHandler.class);

    rmContext = mock(RMContext.class);
    when(rmContext.getNodeLabelManager()).thenReturn(nlm);
    Dispatcher disp = mock(Dispatcher.class);
    when(rmContext.getDispatcher()).thenReturn(disp);
    when(disp.getEventHandler()).thenReturn(mDisp);
    csConf = new CapacitySchedulerConfiguration();
    when(cs.getConfiguration()).thenReturn(csConf);
    when(cs.getRMContext()).thenReturn(rmContext);

    policy = new ProportionalCapacityPreemptionPolicy(conf, rmContext, cs, mClock);
    partitionToResource = new HashMap<>();
    nodeIdToSchedulerNodes = new HashMap<>();
    nameToCSQueues = new HashMap<>();
  }

  @Test
  public void testBuilder() throws Exception {
    /**
     * Test of test, make sure we build expected mock schedulable objects
     */
    String labelsConfig =
        "=200,true;" + // default partition
        "red=100,false;" + // partition=red
        "blue=200,true"; // partition=blue
    String nodesConfig =
        "n1=red;" + // n1 has partition=red
        "n2=blue;" + // n2 has partition=blue
        "n3="; // n3 doesn't have partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[200 200 100 100],red=[100 100 100 100],blue=[200 200 200 200]);" + //root
        "-a(=[100 200 100 100],red=[0 0 0 0],blue=[200 200 200 200]);" + // a
        "--a1(=[50 100 50 100],red=[0 0 0 0],blue=[100 200 200 0]);" + // a1
        "--a2(=[50 200 50 0],red=[0 0 0 0],blue=[100 200 0 200]);" + // a2
        "-b(=[100 200 0 0],red=[100 100 100 100],blue=[0 0 0 0])";
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        // app1 in a1, , 50 in n2 (reserved), 50 in n2 (allocated)
        "a1\t" // app1 in a1
        + "(1,1,n3,red,50,false);" + // 50 * default in n3

        "a1\t" // app2 in a1
        + "(2,1,n2,,50,true)(2,1,n2,,50,false)" // 50 * ignore-exclusivity (reserved),
                                                // 50 * ignore-exclusivity (allocated)
        + "(2,1,n2,blue,50,true)(2,1,n2,blue,50,true);" + // 50 in n2 (reserved),
                                                          // 50 in n2 (allocated)
        "a2\t" // app3 in a2
        + "(1,1,n3,red,50,false);" + // 50 * default in n3

        "b\t" // app4 in b
        + "(1,1,n1,red,100,false);";

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);

    // Check queues:
    // root
    checkAbsCapacities(cs.getQueue("root"), "", 1f, 1f, 0.5f);
    checkPendingResource(cs.getQueue("root"), "", 100);
    checkAbsCapacities(cs.getQueue("root"), "red", 1f, 1f, 1f);
    checkPendingResource(cs.getQueue("root"), "red", 100);
    checkAbsCapacities(cs.getQueue("root"), "blue", 1f, 1f, 1f);
    checkPendingResource(cs.getQueue("root"), "blue", 200);

    // a
    checkAbsCapacities(cs.getQueue("a"), "", 0.5f, 1f, 0.5f);
    checkPendingResource(cs.getQueue("a"), "", 100);
    checkAbsCapacities(cs.getQueue("a"), "red", 0f, 0f, 0f);
    checkPendingResource(cs.getQueue("a"), "red", 0);
    checkAbsCapacities(cs.getQueue("a"), "blue", 1f, 1f, 1f);
    checkPendingResource(cs.getQueue("a"), "blue", 200);

    // a1
    checkAbsCapacities(cs.getQueue("a1"), "", 0.25f, 0.5f, 0.25f);
    checkPendingResource(cs.getQueue("a1"), "", 100);
    checkAbsCapacities(cs.getQueue("a1"), "red", 0f, 0f, 0f);
    checkPendingResource(cs.getQueue("a1"), "red", 0);
    checkAbsCapacities(cs.getQueue("a1"), "blue", 0.5f, 1f, 1f);
    checkPendingResource(cs.getQueue("a1"), "blue", 0);

    // a2
    checkAbsCapacities(cs.getQueue("a2"), "", 0.25f, 1f, 0.25f);
    checkPendingResource(cs.getQueue("a2"), "", 0);
    checkAbsCapacities(cs.getQueue("a2"), "red", 0f, 0f, 0f);
    checkPendingResource(cs.getQueue("a2"), "red", 0);
    checkAbsCapacities(cs.getQueue("a2"), "blue", 0.5f, 1f, 0f);
    checkPendingResource(cs.getQueue("a2"), "blue", 200);

    // b1
    checkAbsCapacities(cs.getQueue("b"), "", 0.5f, 1f, 0f);
    checkPendingResource(cs.getQueue("b"), "", 0);
    checkAbsCapacities(cs.getQueue("b"), "red", 1f, 1f, 1f);
    checkPendingResource(cs.getQueue("b"), "red", 100);
    checkAbsCapacities(cs.getQueue("b"), "blue", 0f, 0f, 0f);
    checkPendingResource(cs.getQueue("b"), "blue", 0);

    // Check ignored partitioned containers in queue
    Assert.assertEquals(100, ((LeafQueue) cs.getQueue("a1"))
        .getIgnoreExclusivityRMContainers().get("blue").size());

    // Check applications
    Assert.assertEquals(2, ((LeafQueue)cs.getQueue("a1")).getApplications().size());
    Assert.assertEquals(1, ((LeafQueue)cs.getQueue("a2")).getApplications().size());
    Assert.assertEquals(1, ((LeafQueue)cs.getQueue("b")).getApplications().size());

    // Check #containers
    FiCaSchedulerApp app1 = getApp("a1", 1);
    FiCaSchedulerApp app2 = getApp("a1", 2);
    FiCaSchedulerApp app3 = getApp("a2", 3);
    FiCaSchedulerApp app4 = getApp("b", 4);

    Assert.assertEquals(50, app1.getLiveContainers().size());
    checkContainerNodesInApp(app1, 50, "n3");

    Assert.assertEquals(50, app2.getLiveContainers().size());
    Assert.assertEquals(150, app2.getReservedContainers().size());
    checkContainerNodesInApp(app2, 200, "n2");

    Assert.assertEquals(50, app3.getLiveContainers().size());
    checkContainerNodesInApp(app3, 50, "n3");

    Assert.assertEquals(100, app4.getLiveContainers().size());
    checkContainerNodesInApp(app4, 100, "n1");
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
  public void testNodePartitionPreemptionRespectMaximumCapacity()
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
     * app1 uses 90x, and app2 use 10x. After preemption, app2 will preempt 10x
     * from app1 because of max capacity.
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

    // 30 preempted from app1, 30 preempted from app4, and nothing preempted
    // from app2/app3
    verify(mDisp, times(20)).handle(
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

    csConf.setPreemptionDisabled("root.b", true);
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

  private ApplicationAttemptId getAppAttemptId(int id) {
    ApplicationId appId = ApplicationId.newInstance(0L, id);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    return appAttemptId;
  }

  private void checkContainerNodesInApp(FiCaSchedulerApp app,
      int expectedContainersNumber, String host) {
    NodeId nodeId = NodeId.newInstance(host, 1);
    int num = 0;
    for (RMContainer c : app.getLiveContainers()) {
      if (c.getAllocatedNode().equals(nodeId)) {
        num++;
      }
    }
    for (RMContainer c : app.getReservedContainers()) {
      if (c.getAllocatedNode().equals(nodeId)) {
        num++;
      }
    }
    Assert.assertEquals(expectedContainersNumber, num);
  }

  private FiCaSchedulerApp getApp(String queueName, int appId) {
    for (FiCaSchedulerApp app : ((LeafQueue) cs.getQueue(queueName))
        .getApplications()) {
      if (app.getApplicationId().getId() == appId) {
        return app;
      }
    }
    return null;
  }

  private void checkAbsCapacities(CSQueue queue, String partition,
      float guaranteed, float max, float used) {
    QueueCapacities qc = queue.getQueueCapacities();
    Assert.assertEquals(guaranteed, qc.getAbsoluteCapacity(partition), 1e-3);
    Assert.assertEquals(max, qc.getAbsoluteMaximumCapacity(partition), 1e-3);
    Assert.assertEquals(used, qc.getAbsoluteUsedCapacity(partition), 1e-3);
  }

  private void checkPendingResource(CSQueue queue, String partition, int pending) {
    ResourceUsage ru = queue.getQueueResourceUsage();
    Assert.assertEquals(pending, ru.getPending(partition).getMemory());
  }

  private void buildEnv(String labelsConfig, String nodesConfig,
      String queuesConfig, String appsConfig) throws IOException {
    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig, false);
  }

  private void buildEnv(String labelsConfig, String nodesConfig,
      String queuesConfig, String appsConfig,
      boolean useDominantResourceCalculator) throws IOException {
    if (useDominantResourceCalculator) {
      when(cs.getResourceCalculator()).thenReturn(
          new DominantResourceCalculator());
    }
    mockNodeLabelsManager(labelsConfig);
    mockSchedulerNodes(nodesConfig);
    for (NodeId nodeId : nodeIdToSchedulerNodes.keySet()) {
      when(cs.getSchedulerNode(nodeId)).thenReturn(
          nodeIdToSchedulerNodes.get(nodeId));
    }
    ParentQueue root = mockQueueHierarchy(queuesConfig);
    when(cs.getRootQueue()).thenReturn(root);
    when(cs.getClusterResource()).thenReturn(clusterResource);
    mockApplications(appsConfig);

    policy = new ProportionalCapacityPreemptionPolicy(conf, rmContext, cs,
        mClock);
  }

  private void mockContainers(String containersConfig, ApplicationAttemptId attemptId,
      String queueName, List<RMContainer> reservedContainers,
      List<RMContainer> liveContainers) {
    int containerId = 1;
    int start = containersConfig.indexOf("=") + 1;
    int end = -1;

    while (start < containersConfig.length()) {
      while (start < containersConfig.length()
          && containersConfig.charAt(start) != '(') {
        start++;
      }
      if (start >= containersConfig.length()) {
        throw new IllegalArgumentException(
            "Error containers specification, line=" + containersConfig);
      }
      end = start + 1;
      while (end < containersConfig.length()
          && containersConfig.charAt(end) != ')') {
        end++;
      }
      if (end >= containersConfig.length()) {
        throw new IllegalArgumentException(
            "Error containers specification, line=" + containersConfig);
      }

      // now we found start/end, get container values
      String[] values = containersConfig.substring(start + 1, end).split(",");
      if (values.length != 6) {
        throw new IllegalArgumentException("Format to define container is:"
            + "(priority,resource,host,expression,repeat,reserved)");
      }
      Priority pri = Priority.newInstance(Integer.valueOf(values[0]));
      Resource res = parseResourceFromString(values[1]);
      NodeId host = NodeId.newInstance(values[2], 1);
      String exp = values[3];
      int repeat = Integer.valueOf(values[4]);
      boolean reserved = Boolean.valueOf(values[5]);

      for (int i = 0; i < repeat; i++) {
        Container c = mock(Container.class);
        when(c.getResource()).thenReturn(res);
        when(c.getPriority()).thenReturn(pri);
        RMContainerImpl rmc = mock(RMContainerImpl.class);
        when(rmc.getAllocatedNode()).thenReturn(host);
        when(rmc.getNodeLabelExpression()).thenReturn(exp);
        when(rmc.getAllocatedResource()).thenReturn(res);
        when(rmc.getContainer()).thenReturn(c);
        when(rmc.getApplicationAttemptId()).thenReturn(attemptId);
        final ContainerId cId = ContainerId.newContainerId(attemptId, containerId);
        when(rmc.getContainerId()).thenReturn(
            cId);
        doAnswer(new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) throws Throwable {
            return cId.compareTo(((RMContainer) invocation.getArguments()[0])
                .getContainerId());
          }
        }).when(rmc).compareTo(any(RMContainer.class));

        if (containerId == 1) {
          when(rmc.isAMContainer()).thenReturn(true);
        }

        if (reserved) {
          reservedContainers.add(rmc);
        } else {
          liveContainers.add(rmc);
        }

        // If this is a non-exclusive allocation
        String partition = null;
        if (exp.isEmpty()
            && !(partition = nodeIdToSchedulerNodes.get(host).getPartition())
                .isEmpty()) {
          LeafQueue queue = (LeafQueue) nameToCSQueues.get(queueName);
          Map<String, TreeSet<RMContainer>> ignoreExclusivityContainers =
              queue.getIgnoreExclusivityRMContainers();
          if (!ignoreExclusivityContainers.containsKey(partition)) {
            ignoreExclusivityContainers.put(partition,
                new TreeSet<RMContainer>());
          }
          ignoreExclusivityContainers.get(partition).add(rmc);
        }
        LOG.debug("add container to app=" + attemptId + " res=" + res
            + " node=" + host + " nodeLabelExpression=" + exp + " partition="
            + partition);

        containerId++;
      }

      start = end + 1;
    }
  }

  /**
   * Format is:
   * <pre>
   * queueName\t  // app1
   * (priority,resource,host,expression,#repeat,reserved)
   * (priority,resource,host,expression,#repeat,reserved);
   * queueName\t  // app2
   * </pre>
   */
  private void mockApplications(String appsConfig) {
    int id = 1;
    for (String a : appsConfig.split(";")) {
      String[] strs = a.split("\t");
      String queueName = strs[0];

      // get containers
      List<RMContainer> liveContainers = new ArrayList<RMContainer>();
      List<RMContainer> reservedContainers = new ArrayList<RMContainer>();
      ApplicationId appId = ApplicationId.newInstance(0L, id);
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);

      mockContainers(strs[1], appAttemptId, queueName, reservedContainers,
          liveContainers);

      FiCaSchedulerApp app = mock(FiCaSchedulerApp.class);
      when(app.getLiveContainers()).thenReturn(liveContainers);
      when(app.getReservedContainers()).thenReturn(reservedContainers);
      when(app.getApplicationAttemptId()).thenReturn(appAttemptId);
      when(app.getApplicationId()).thenReturn(appId);
      when(app.getPriority()).thenReturn(Priority.newInstance(0));

      // add to LeafQueue
      LeafQueue queue = (LeafQueue) nameToCSQueues.get(queueName);
      queue.getApplications().add(app);

      id++;
    }
  }

  /**
   * Format is:
   * host1=partition;
   * host2=partition;
   */
  private void mockSchedulerNodes(String schedulerNodesConfigStr)
      throws IOException {
    String[] nodesConfigStrArray = schedulerNodesConfigStr.split(";");
    for (String p : nodesConfigStrArray) {
      NodeId nodeId = NodeId.newInstance(p.substring(0, p.indexOf("=")), 1);
      String partition = p.substring(p.indexOf("=") + 1, p.length());

      SchedulerNode sn = mock(SchedulerNode.class);
      when(sn.getNodeID()).thenReturn(nodeId);
      when(sn.getPartition()).thenReturn(partition);
      nodeIdToSchedulerNodes.put(nodeId, sn);

      LOG.debug("add scheduler node, id=" + nodeId + ", partition=" + partition);
    }
  }

  /**
   * Format is:
   * <pre>
   * partition0=total_resource,exclusivity;
   * partition1=total_resource,exclusivity;
   * ...
   * </pre>
   */
  private void mockNodeLabelsManager(String nodeLabelsConfigStr) throws IOException {
    String[] partitionConfigArr = nodeLabelsConfigStr.split(";");
    clusterResource = Resources.createResource(0);
    for (String p : partitionConfigArr) {
      String partitionName = p.substring(0, p.indexOf("="));
      Resource res = parseResourceFromString(p.substring(p.indexOf("=") + 1,
          p.indexOf(",")));
     boolean exclusivity =
          Boolean.valueOf(p.substring(p.indexOf(",") + 1, p.length()));
      when(nlm.getResourceByLabel(eq(partitionName), any(Resource.class)))
          .thenReturn(res);
      when(nlm.isExclusiveNodeLabel(eq(partitionName))).thenReturn(exclusivity);

      // add to partition to resource
      partitionToResource.put(partitionName, res);
      LOG.debug("add partition=" + partitionName + " totalRes=" + res
          + " exclusivity=" + exclusivity);
      Resources.addTo(clusterResource, res);
    }

    when(nlm.getClusterNodeLabelNames()).thenReturn(
        partitionToResource.keySet());
  }

  private Resource parseResourceFromString(String p) {
    String[] resource = p.split(":");
    Resource res = Resources.createResource(0);
    if (resource.length == 1) {
      res = Resources.createResource(Integer.valueOf(resource[0]));
    } else {
      res = Resources.createResource(Integer.valueOf(resource[0]),
          Integer.valueOf(resource[1]));
    }
    return res;
  }

  /**
   * Format is:
   * <pre>
   * root (<partition-name-1>=[guaranteed max used pending],<partition-name-2>=..);
   * -A(...);
   * --A1(...);
   * --A2(...);
   * -B...
   * </pre>
   * ";" splits queues, and there should no empty lines, no extra spaces
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private ParentQueue mockQueueHierarchy(String queueExprs) {
    String[] queueExprArray = queueExprs.split(";");
    ParentQueue rootQueue = null;
    for (int idx = 0; idx < queueExprArray.length; idx++) {
      String q = queueExprArray[idx];
      CSQueue queue;

      // Initialize queue
      if (isParent(queueExprArray, idx)) {
        ParentQueue parentQueue = mock(ParentQueue.class);
        queue = parentQueue;
        List<CSQueue> children = new ArrayList<CSQueue>();
        when(parentQueue.getChildQueues()).thenReturn(children);
      } else {
        LeafQueue leafQueue = mock(LeafQueue.class);
        final TreeSet<FiCaSchedulerApp> apps = new TreeSet<>(
            new Comparator<FiCaSchedulerApp>() {
              @Override
              public int compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2) {
                return a1.getApplicationId().compareTo(a2.getApplicationId());
              }
            });
        when(leafQueue.getApplications()).thenReturn(apps);
        OrderingPolicy<FiCaSchedulerApp> so = mock(OrderingPolicy.class);
        when(so.getPreemptionIterator()).thenAnswer(new Answer() {
          public Object answer(InvocationOnMock invocation) {
            return apps.descendingIterator();
          }
        });
        when(leafQueue.getOrderingPolicy()).thenReturn(so);

        Map<String, TreeSet<RMContainer>> ignorePartitionContainers =
            new HashMap<>();
        when(leafQueue.getIgnoreExclusivityRMContainers()).thenReturn(
            ignorePartitionContainers);
        queue = leafQueue;
      }

      setupQueue(queue, q, queueExprArray, idx);
      if (queue.getQueueName().equals(ROOT)) {
        rootQueue = (ParentQueue) queue;
      }
    }
    return rootQueue;
  }

  private void setupQueue(CSQueue queue, String q, String[] queueExprArray,
      int idx) {
    LOG.debug("*** Setup queue, source=" + q);
    String queuePath = null;

    int myLevel = getLevel(q);
    if (0 == myLevel) {
      // It's root
      when(queue.getQueueName()).thenReturn(ROOT);
      queuePath = ROOT;
    }

    String queueName = getQueueName(q);
    when(queue.getQueueName()).thenReturn(queueName);

    // Setup parent queue, and add myself to parentQueue.children-list
    ParentQueue parentQueue = getParentQueue(queueExprArray, idx, myLevel);
    if (null != parentQueue) {
      when(queue.getParent()).thenReturn(parentQueue);
      parentQueue.getChildQueues().add(queue);

      // Setup my path
      queuePath = parentQueue.getQueuePath() + "." + queueName;
    }
    when(queue.getQueuePath()).thenReturn(queuePath);

    QueueCapacities qc = new QueueCapacities(0 == myLevel);
    ResourceUsage ru = new ResourceUsage();

    when(queue.getQueueCapacities()).thenReturn(qc);
    when(queue.getQueueResourceUsage()).thenReturn(ru);

    LOG.debug("Setup queue, name=" + queue.getQueueName() + " path="
        + queue.getQueuePath());
    LOG.debug("Parent=" + (parentQueue == null ? "null" : parentQueue
        .getQueueName()));

    // Setup other fields like used resource, guaranteed resource, etc.
    String capacitySettingStr = q.substring(q.indexOf("(") + 1, q.indexOf(")"));
    for (String s : capacitySettingStr.split(",")) {
      String partitionName = s.substring(0, s.indexOf("="));
      String[] values = s.substring(s.indexOf("[") + 1, s.indexOf("]")).split(" ");
      // Add a small epsilon to capacities to avoid truncate when doing
      // Resources.multiply
      float epsilon = 1e-6f;
      Resource totResoucePerPartition = partitionToResource.get(partitionName);
      float absGuaranteed = Resources.divide(rc, totResoucePerPartition,
          parseResourceFromString(values[0].trim()), totResoucePerPartition)
          + epsilon;
      float absMax = Resources.divide(rc, totResoucePerPartition,
          parseResourceFromString(values[1].trim()), totResoucePerPartition)
          + epsilon;
      float absUsed = Resources.divide(rc, totResoucePerPartition,
          parseResourceFromString(values[2].trim()), totResoucePerPartition)
          + epsilon;
      Resource pending = parseResourceFromString(values[3].trim());
      qc.setAbsoluteCapacity(partitionName, absGuaranteed);
      qc.setAbsoluteMaximumCapacity(partitionName, absMax);
      qc.setAbsoluteUsedCapacity(partitionName, absUsed);
      ru.setPending(partitionName, pending);
      if (!isParent(queueExprArray, idx)) {
        LeafQueue lq = (LeafQueue) queue;
        when(lq.getTotalPendingResourcesConsideringUserLimit(isA(Resource.class),
            isA(String.class))).thenReturn(pending);
      }
      ru.setUsed(partitionName, parseResourceFromString(values[2].trim()));
      LOG.debug("Setup queue=" + queueName + " partition=" + partitionName
          + " [abs_guaranteed=" + absGuaranteed + ",abs_max=" + absMax
          + ",abs_used" + absUsed + ",pending_resource=" + pending + "]");
    }

    // Setup preemption disabled
    when(queue.getPreemptionDisabled()).thenReturn(
        csConf.getPreemptionDisabled(queuePath, false));

    nameToCSQueues.put(queueName, queue);
    when(cs.getQueue(eq(queueName))).thenReturn(queue);
  }

  /**
   * Level of a queue is how many "-" at beginning, root's level is 0
   */
  private int getLevel(String q) {
    int level = 0; // level = how many "-" at beginning
    while (level < q.length() && q.charAt(level) == '-') {
      level++;
    }
    return level;
  }

  private String getQueueName(String q) {
    int idx = 0;
    // find first != '-' char
    while (idx < q.length() && q.charAt(idx) == '-') {
      idx++;
    }
    if (idx == q.length()) {
      throw new IllegalArgumentException("illegal input:" + q);
    }
    // name = after '-' and before '('
    String name = q.substring(idx, q.indexOf('('));
    if (name.isEmpty()) {
      throw new IllegalArgumentException("queue name shouldn't be empty:" + q);
    }
    if (name.contains(".")) {
      throw new IllegalArgumentException("queue name shouldn't contain '.':"
          + name);
    }
    return name;
  }

  private ParentQueue getParentQueue(String[] queueExprArray, int idx, int myLevel) {
    idx--;
    while (idx >= 0) {
      int level = getLevel(queueExprArray[idx]);
      if (level < myLevel) {
        String parentQueuName = getQueueName(queueExprArray[idx]);
        return (ParentQueue) nameToCSQueues.get(parentQueuName);
      }
      idx--;
    }

    return null;
  }

  /**
   * Get if a queue is ParentQueue
   */
  private boolean isParent(String[] queues, int idx) {
    int myLevel = getLevel(queues[idx]);
    idx++;
    while (idx < queues.length && getLevel(queues[idx]) == myLevel) {
      idx++;
    }
    if (idx >= queues.length || getLevel(queues[idx]) < myLevel) {
      // It's a LeafQueue
      return false;
    } else {
      return true;
    }
  }
}
