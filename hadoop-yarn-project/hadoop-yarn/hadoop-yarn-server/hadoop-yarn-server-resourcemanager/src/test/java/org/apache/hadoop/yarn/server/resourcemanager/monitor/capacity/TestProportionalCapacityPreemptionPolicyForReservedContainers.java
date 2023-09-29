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

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework.ProportionalCapacityPreemptionPolicyMockFramework;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestProportionalCapacityPreemptionPolicyForReservedContainers
    extends ProportionalCapacityPreemptionPolicyMockFramework {
  @Before
  public void setup() {
    super.setup();
    conf.setBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_SELECT_CANDIDATES_FOR_RESERVED_CONTAINERS,
        true);
    policy = new ProportionalCapacityPreemptionPolicy(rmContext, cs, mClock);
  }

  @Test
  public void testPreemptionForSimpleReservedContainer() throws IOException {
    /**
     * The simplest test of reserved container, Queue structure is:
     *
     * <pre>
     *       root
     *       /  \
     *      a    b
     * </pre>
     * Guaranteed resource of a/b are 50:50
     * Total cluster resource = 100
     * - A has 90 containers on two node, n1 has 45, n2 has 45, size of each
     * container is 1.
     * - B has am container at n1, and reserves 1 container with size = 9 at n1,
     * so B needs to preempt 9 containers from A at n1 instead of randomly
     * preempt from n1 and n2.
     */
    String labelsConfig =
        "=100,true;";
    String nodesConfig = // n1 / n2 has no label
        "n1= res=50;" +
        "n2= res=50";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 9 9]);" + //root
            "-a(=[50 100 90 0]);" + // a
            "-b(=[50 100 10 9 9])"; // b
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "root.a\t" // app1 in a
            + "(1,1,n1,,45,false)" // 45 in n1
            + "(1,1,n2,,45,false);" + // 45 in n2
        "root.b\t" // app2 in b
            + "(1,1,n1,,1,false)" // AM container in n1
            + "(1,9,n1,,1,true)"; // 1 container with size=9 reserved at n1

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // Total 5 preempted from app1 at n1, don't preempt container from other
    // app/node
    verify(eventHandler, times(5)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(eventHandler, times(5)).handle(
        argThat(new IsPreemptionRequestForQueueAndNode(getAppAttemptId(1),
            "root.a",
            NodeId.newInstance("n1", 1))));
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testUseReservedAndFifoSelectorTogether() throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *       /  \
     *      a    b
     * </pre>
     * Guaranteed resource of a/b are 30:70
     * Total cluster resource = 100
     * - A has 45 containers on two node, n1 has 10, n2 has 35, size of each
     * container is 1.
     * - B has 5 containers at n2, and reserves 1 container with size = 50 at n1,
     *   B also has 20 pending resources.
     * so B needs to preempt:
     * - 10 containers from n1 (for reserved)
     * - 5 containers from n2 for pending resources
     */
    String labelsConfig =
        "=100,true;";
    String nodesConfig = // n1 / n2 has no label
        "n1= res=50;" +
        "n2= res=50";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 70 10]);" + //root
            "-a(=[30 100 45 0]);" + // a
            "-b(=[70 100 55 70 50])"; // b
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t" // app1 in a
            + "(1,1,n2,,35,false)" // 35 in n2
            + "(1,1,n1,,10,false);" + // 10 in n1
            "b\t" // app2 in b
            + "(1,1,n2,,5,false)" // 5 in n2
            + "(1,50,n1,,1,true)"; // 1 container with size=50 reserved at n1

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(15)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(eventHandler, times(10)).handle(
        argThat(new IsPreemptionRequestForQueueAndNode(getAppAttemptId(1), "a",
            NodeId.newInstance("n1", 1))));
    verify(eventHandler, times(5)).handle(
        argThat(new IsPreemptionRequestForQueueAndNode(getAppAttemptId(1), "a",
            NodeId.newInstance("n2", 1))));
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testReservedSelectorSkipsAMContainer() throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *       /  \
     *      a    b
     * </pre>
     * Guaranteed resource of a/b are 30:70
     * Total cluster resource = 100
     * - A has 45 containers on two node, n1 has 10, n2 has 35, size of each
     * container is 1.
     * - B has 5 containers at n2, and reserves 1 container with size = 50 at n1,
     *   B also has 20 pending resources.
     *
     * Ideally B needs to preempt:
     * - 10 containers from n1 (for reserved)
     * - 5 containers from n2 for pending resources
     *
     * However, since one AM container is located at n1 (from queueA), we cannot
     * preempt 10 containers from n1 for reserved container. Instead, we will
     * preempt 15 containers from n2, since containers from queueA launched in n2
     * are later than containers from queueA launched in n1 (FIFO order of containers)
     */
    String labelsConfig =
        "=100,true;";
    String nodesConfig = // n1 / n2 has no label
        "n1= res=50;" +
            "n2= res=50";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 70 10]);" + //root
            "-a(=[30 100 45 0]);" + // a
            "-b(=[70 100 55 70 50])"; // b
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t" // app1 in a
            + "(1,1,n1,,10,false)" // 10 in n1
            + "(1,1,n2,,35,false);" +// 35 in n2
            "b\t" // app2 in b
            + "(1,1,n2,,5,false)" // 5 in n2
            + "(1,50,n1,,1,true)"; // 1 container with size=50 reserved at n1

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(15)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(eventHandler, times(0)).handle(
        argThat(new IsPreemptionRequestForQueueAndNode(getAppAttemptId(1), "a",
            NodeId.newInstance("n1", 1))));
    verify(eventHandler, times(15)).handle(
        argThat(new IsPreemptionRequestForQueueAndNode(getAppAttemptId(1), "a",
            NodeId.newInstance("n2", 1))));
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testPreemptionForReservedContainerRespectGuaranteedResource()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *       /  \
     *      a    b
     * </pre>
     * Guaranteed resource of a/b are 85:15
     * Total cluster resource = 100
     * - A has 90 containers on two node, n1 has 45, n2 has 45, size of each
     * container is 1.
     * - B has am container at n1, and reserves 1 container with size = 9 at n1,
     *
     * If we preempt 9 containers from queue-A, queue-A will be below its
     * guaranteed resource = 90 - 9 = 81 < 85.
     *
     * So no preemption will take place
     */
    String labelsConfig =
        "=100,true;";
    String nodesConfig = // n1 / n2 has no label
        "n1= res=50;" +
            "n2= res=50";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 100 9 9]);" + //root
            "-a(=[85 100 90 0]);" + // a
            "-b(=[15 100 10 9 9])"; // b
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t" // app1 in a
            + "(1,1,n1,,45,false)" // 45 in n1
            + "(1,1,n2,,45,false);" + // 45 in n2
            "b\t" // app2 in b
            + "(1,1,n1,,1,false)" // AM container in n1
            + "(1,9,n1,,1,true)"; // 1 container with size=9 reserved at n1

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(1))));
    verify(eventHandler, times(0)).handle(argThat(
        new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(
            getAppAttemptId(2))));
  }

  @Test
  public void testPreemptionForReservedContainerWhichHasAvailableResource()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *       /  \
     *      a    b
     * </pre>
     *
     * Guaranteed resource of a/b are 50:50
     * Total cluster resource = 100
     * - A has 90 containers on two node, n1 has 45, n2 has 45, size of each
     * container is 1.
     * - B has am container at n1, and reserves 1 container with size = 9 at n1,
     *
     * So we can get 4 containers preempted after preemption.
     * (reserved 5 + preempted 4) = 9
     */
    String labelsConfig =
        "=100,true;";
    String nodesConfig = // n1 / n2 has no label
        "n1= res=50;" +
            "n2= res=50";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 99 9 9]);" + //root
            "-a(=[50 100 90 0]);" + // a
            "-b(=[50 100 9 9 9])"; // b
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "root.a\t" // app1 in a
            + "(1,1,n1,,45,false)" // 45 in n1
            + "(1,1,n2,,45,false);" + // 45 in n2
            "root.b\t" // app2 in b
            + "(1,9,n1,,1,true)"; // 1 container with size=9 reserved at n1

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // Total 4 preempted from app1 at n1, don't preempt container from other
    // app/node
    verify(eventHandler, times(4)).handle(argThat(
        new IsPreemptionRequestForQueueAndNode(getAppAttemptId(1), "root.a",
            NodeId.newInstance("n1", 1))));
    verify(eventHandler, times(0)).handle(argThat(
        new IsPreemptionRequestForQueueAndNode(getAppAttemptId(1), "root.a",
            NodeId.newInstance("n2", 1))));
  }

  @Test
  public void testPreemptionForReservedContainerWhichHasNondivisibleAvailableResource()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *       /  \
     *      a    b
     * </pre>
     *
     * Guaranteed resource of a/b are 50:50
     * Total cluster resource = 100
     * - A has 45 containers on two node, size of each container is 2,
     *   n1 has 23, n2 has 22
     * - B reserves 1 container with size = 9 at n1,
     *
     * So we can get 4 containers (total-resource = 8) preempted after
     * preemption. Actual required is 3.5, but we need to preempt integer
     * number of containers
     */
    String labelsConfig =
        "=100,true;";
    String nodesConfig = // n1 / n2 has no label
        "n1= res=50;" +
            "n2= res=50";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 99 9 9]);" + //root
            "-a(=[50 100 90 0]);" + // a
            "-b(=[50 100 9 9 9])"; // b
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "root.a\t" // app1 in a
            + "(1,2,n1,,24,false)" // 48 in n1
            + "(1,2,n2,,23,false);" + // 46 in n2
            "b\t" // app2 in b
            + "(1,9,n1,,1,true)"; // 1 container with size=9 reserved at n1

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // Total 4 preempted from app1 at n1, don't preempt container from other
    // app/node
    verify(eventHandler, times(4)).handle(argThat(
        new IsPreemptionRequestForQueueAndNode(getAppAttemptId(1), "root.a",
            NodeId.newInstance("n1", 1))));
    verify(eventHandler, times(0)).handle(argThat(
        new IsPreemptionRequestForQueueAndNode(getAppAttemptId(1), "root.a",
            NodeId.newInstance("n2", 1))));
  }

  @Test
  public void testPreemptionForReservedContainerRespectAvailableResources()
      throws IOException {
    /**
     * Queue structure is:
     *
     * <pre>
     *       root
     *       /  \
     *      a    b
     * </pre>
     *
     * Guaranteed resource of a/b are 50:50
     * Total cluster resource = 100, 4 nodes, 25 on each node
     * - A has 10 containers on every node, size of container is 2
     * - B reserves 1 container with size = 9 at n1,
     *
     * So even if we cannot allocate container for B now, no preemption should
     * happen since there're plenty of available resources.
     */
    String labelsConfig =
        "=100,true;";
    String nodesConfig =
        "n1= res=25;" +
            "n2= res=25;" +
            "n3= res=25;" +
            "n4= res=25;";
    String queuesConfig =
        // guaranteed,max,used,pending,reserved
        "root(=[100 100 89 9 9]);" + //root
            "-a(=[50 100 80 0]);" + // a
            "-b(=[50 100 9 9 9])"; // b
    String appsConfig=
        //queueName\t(priority,resource,host,expression,#repeat,reserved)
        "a\t" // app1 in a
            + "(1,2,n1,,10,false)" // 10 in n1
            + "(1,2,n2,,10,false)" // 10 in n2
            + "(1,2,n3,,10,false)" // 10 in n3
            + "(1,2,n4,,10,false);" + // 10 in n4
            "b\t" // app2 in b
            + "(1,9,n1,,1,true)"; // 1 container with size=5 reserved at n1

    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig);
    policy.editSchedule();

    // No preemption should happen
    verify(eventHandler, times(0)).handle(argThat(
        new IsPreemptionRequestForQueueAndNode(getAppAttemptId(1), "a",
            NodeId.newInstance("n1", 1))));
    verify(eventHandler, times(0)).handle(argThat(
        new IsPreemptionRequestForQueueAndNode(getAppAttemptId(1), "a",
            NodeId.newInstance("n2", 1))));
    verify(eventHandler, times(0)).handle(argThat(
        new IsPreemptionRequestForQueueAndNode(getAppAttemptId(1), "a",
            NodeId.newInstance("n3", 1))));
    verify(eventHandler, times(0)).handle(argThat(
        new IsPreemptionRequestForQueueAndNode(getAppAttemptId(1), "a",
            NodeId.newInstance("n4", 1))));
  }
}
