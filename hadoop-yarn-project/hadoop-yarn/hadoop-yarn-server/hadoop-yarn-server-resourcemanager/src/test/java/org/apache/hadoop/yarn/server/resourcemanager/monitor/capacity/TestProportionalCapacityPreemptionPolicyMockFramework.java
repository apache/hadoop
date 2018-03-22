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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.junit.Assert;
import org.junit.Test;

public class TestProportionalCapacityPreemptionPolicyMockFramework
    extends ProportionalCapacityPreemptionPolicyMockFramework {

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
            "--a2(=[50 200 50 0],red=[0 0 0 0],blue=[100 200 0 200]){priority=2};" + // a2
            "-b(=[100 200 0 0],red=[100 100 100 100],blue=[0 0 0 0]){priority=1,disable_preemption=true}";
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
    checkPriority(cs.getQueue("root"), 0); // default

    // a
    checkAbsCapacities(cs.getQueue("a"), "", 0.5f, 1f, 0.5f);
    checkPendingResource(cs.getQueue("a"), "", 100);
    checkAbsCapacities(cs.getQueue("a"), "red", 0f, 0f, 0f);
    checkPendingResource(cs.getQueue("a"), "red", 0);
    checkAbsCapacities(cs.getQueue("a"), "blue", 1f, 1f, 1f);
    checkPendingResource(cs.getQueue("a"), "blue", 200);
    checkPriority(cs.getQueue("a"), 0); // default

    // a1
    checkAbsCapacities(cs.getQueue("a1"), "", 0.25f, 0.5f, 0.25f);
    checkPendingResource(cs.getQueue("a1"), "", 100);
    checkAbsCapacities(cs.getQueue("a1"), "red", 0f, 0f, 0f);
    checkPendingResource(cs.getQueue("a1"), "red", 0);
    checkAbsCapacities(cs.getQueue("a1"), "blue", 0.5f, 1f, 1f);
    checkPendingResource(cs.getQueue("a1"), "blue", 0);
    checkPriority(cs.getQueue("a1"), 0); // default

    // a2
    checkAbsCapacities(cs.getQueue("a2"), "", 0.25f, 1f, 0.25f);
    checkPendingResource(cs.getQueue("a2"), "", 0);
    checkAbsCapacities(cs.getQueue("a2"), "red", 0f, 0f, 0f);
    checkPendingResource(cs.getQueue("a2"), "red", 0);
    checkAbsCapacities(cs.getQueue("a2"), "blue", 0.5f, 1f, 0f);
    checkPendingResource(cs.getQueue("a2"), "blue", 200);
    checkPriority(cs.getQueue("a2"), 2);
    Assert.assertFalse(cs.getQueue("a2").getPreemptionDisabled());

    // b
    checkAbsCapacities(cs.getQueue("b"), "", 0.5f, 1f, 0f);
    checkPendingResource(cs.getQueue("b"), "", 0);
    checkAbsCapacities(cs.getQueue("b"), "red", 1f, 1f, 1f);
    checkPendingResource(cs.getQueue("b"), "red", 100);
    checkAbsCapacities(cs.getQueue("b"), "blue", 0f, 0f, 0f);
    checkPendingResource(cs.getQueue("b"), "blue", 0);
    checkPriority(cs.getQueue("b"), 1);
    Assert.assertTrue(cs.getQueue("b").getPreemptionDisabled());

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
  public void testBuilderWithReservedResource() throws Exception {
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
        "root(=[200 200 100 100 100],red=[100 100 100 100 90],blue=[200 200 200 200 80]);" + //root
            "-a(=[100 200 100 100 50],red=[0 0 0 0 40],blue=[200 200 200 200 30]);" + // a
            "--a1(=[50 100 50 100 40],red=[0 0 0 0 20],blue=[100 200 200 0]);" + // a1
            "--a2(=[50 200 50 0 10],red=[0 0 0 0 20],blue=[100 200 0 200]);" + // a2
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
    checkReservedResource(cs.getQueue("root"), "", 100);
    checkReservedResource(cs.getQueue("root"), "red", 90);

    // a
    checkReservedResource(cs.getQueue("a"), "", 50);
    checkReservedResource(cs.getQueue("a"), "red", 40);

    // a1
    checkReservedResource(cs.getQueue("a1"), "", 40);
    checkReservedResource(cs.getQueue("a1"), "red", 20);

    // b
    checkReservedResource(cs.getQueue("b"), "", 0);
    checkReservedResource(cs.getQueue("b"), "red", 0);
  }

  @Test
  public void testBuilderWithSpecifiedNodeResources() throws Exception {
    String labelsConfig =
        "=200,true;" + // default partition
            "red=100,false;" + // partition=red
            "blue=200,true"; // partition=blue
    String nodesConfig =
        "n1=red res=100;" + // n1 has partition=red
            "n2=blue;" + // n2 has partition=blue
            "n3= res=30"; // n3 doesn't have partition
    String queuesConfig =
        // guaranteed,max,used,pending
        "root(=[200 200 100 100 100],red=[100 100 100 100 90],blue=[200 200 200 200 80]);" + //root
            "-a(=[100 200 100 100 50],red=[0 0 0 0 40],blue=[200 200 200 200 30]);" + // a
            "--a1(=[50 100 50 100 40],red=[0 0 0 0 20],blue=[100 200 200 0]);" + // a1
            "--a2(=[50 200 50 0 10],red=[0 0 0 0 20],blue=[100 200 0 200]);" + // a2
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

    // Check host resources
    Assert.assertEquals(3, this.cs.getAllNodes().size());
    SchedulerNode node1 = cs.getSchedulerNode(NodeId.newInstance("n1", 1));
    Assert.assertEquals(100, node1.getTotalResource().getMemorySize());
    Assert.assertEquals(100, node1.getCopiedListOfRunningContainers().size());
    Assert.assertNull(node1.getReservedContainer());

    SchedulerNode node2 = cs.getSchedulerNode(NodeId.newInstance("n2", 1));
    Assert.assertEquals(0, node2.getTotalResource().getMemorySize());
    Assert.assertEquals(50, node2.getCopiedListOfRunningContainers().size());
    Assert.assertNotNull(node2.getReservedContainer());

    SchedulerNode node3 = cs.getSchedulerNode(NodeId.newInstance("n3", 1));
    Assert.assertEquals(30, node3.getTotalResource().getMemorySize());
    Assert.assertEquals(100, node3.getCopiedListOfRunningContainers().size());
    Assert.assertNull(node3.getReservedContainer());
  }
}
