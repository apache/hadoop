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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.TargetApplicationsNamespace;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentMap;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTag;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTagWithNamespace;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.and;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.cardinality;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetNotIn;

/**
 * Test Container Allocation with SchedulingRequest.
 */
@RunWith(Parameterized.class)
public class TestSchedulingRequestContainerAllocation {
  private static final int GB = 1024;
  private YarnConfiguration conf;
  private String placementConstraintHandler;
  RMNodeLabelsManager mgr;


  @Parameters
  public static Collection<Object[]> placementConstarintHandlers() {
    Object[][] params = new Object[][] {
        {YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER},
        {YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER} };
    return Arrays.asList(params);
  }

  public TestSchedulingRequestContainerAllocation(
      String placementConstraintHandler) {
    this.placementConstraintHandler = placementConstraintHandler;
  }

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        this.placementConstraintHandler);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  private RMApp submitApp(MockRM rm, int memory, Set<String> appTags)
      throws Exception {
    Resource resource = Resource.newInstance(memory, 0);
    ResourceRequest amResourceRequest = ResourceRequest.newInstance(
        Priority.newInstance(0), ResourceRequest.ANY, resource, 1);
    List<ResourceRequest> amResourceRequests =
        Collections.singletonList(amResourceRequest);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithResource(resource, rm)
            .withAmLabel(null)
            .withAmResourceRequests(amResourceRequests)
            .withApplicationTags(appTags)
            .build();
    return MockRMAppSubmitter.submit(rm, data);
  }

  @Test(timeout = 30000L)
  public void testIntraAppAntiAffinity() throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(conf);

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    // app1 asks for 10 anti-affinity containers for the same app. It should
    // only get 4 containers allocated because we only have 4 nodes.
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(10, Resource.newInstance(1024, 1)),
        Priority.newInstance(1), 1L, ImmutableSet.of("mapper"), "mapper");

    List<Container> allocated = waitForAllocation(4, 3000, am1, nms);
    Assert.assertEquals(4, allocated.size());
    Assert.assertEquals(4, getContainerNodesNum(allocated));

    // Similarly, app1 asks 10 anti-affinity containers at different priority,
    // it should be satisfied as well.
    // app1 asks for 10 anti-affinity containers for the same app. It should
    // only get 4 containers allocated because we only have 4 nodes.
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(10, Resource.newInstance(2048, 1)),
        Priority.newInstance(2), 1L, ImmutableSet.of("reducer"), "reducer");

    allocated = waitForAllocation(4, 3000, am1, nms);
    Assert.assertEquals(4, allocated.size());
    Assert.assertEquals(4, getContainerNodesNum(allocated));

    // Test anti-affinity to both of "mapper/reducer", we should only get no
    // container allocated
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(10, Resource.newInstance(2048, 1)),
        Priority.newInstance(3), 1L, ImmutableSet.of("reducer2"), "mapper");

    boolean caughtException = false;
    try {
      allocated = waitForAllocation(1, 3000, am1, nms);
    } catch (Exception e) {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);

    rm1.close();
  }

  @Test(timeout = 30000L)
  public void testIntraAppAntiAffinityWithMultipleTags() throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(conf);

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    // app1 asks for 2 anti-affinity containers for the same app.
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(2, Resource.newInstance(1024, 1)),
        Priority.newInstance(1), 1L, ImmutableSet.of("tag_1_1", "tag_1_2"),
        "tag_1_1", "tag_1_2");

    List<Container> allocated = waitForAllocation(2, 3000, am1, nms);
    Assert.assertEquals(2, allocated.size());
    Assert.assertEquals(2, getContainerNodesNum(allocated));

    // app1 asks for 1 anti-affinity containers for the same app. anti-affinity
    // to tag_1_1/tag_1_2. With allocation_tag = tag_2_1/tag_2_2
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)),
        Priority.newInstance(2), 1L, ImmutableSet.of("tag_2_1", "tag_2_2"),
        "tag_1_1", "tag_1_2");

    List<Container> allocated1 = waitForAllocation(1, 3000, am1, nms);
    Assert.assertEquals(1, allocated1.size());
    allocated.addAll(allocated1);
    Assert.assertEquals(3, getContainerNodesNum(allocated));

    // app1 asks for 1 anti-affinity containers for the same app. anti-affinity
    // to tag_1_1/tag_1_2/tag_2_1/tag_2_2. With allocation_tag = tag_3
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)),
        Priority.newInstance(3), 1L, ImmutableSet.of("tag_3"),
        "tag_1_1", "tag_1_2", "tag_2_1", "tag_2_2");

    allocated1 = waitForAllocation(1, 3000, am1, nms);
    Assert.assertEquals(1, allocated1.size());
    allocated.addAll(allocated1);
    Assert.assertEquals(4, getContainerNodesNum(allocated));

    rm1.close();
  }

  /**
   * This UT covers some basic end-to-end inter-app anti-affinity
   * constraint tests. For comprehensive tests over different namespace
   * types, see more in TestPlacementConstraintsUtil.
   * @throws Exception
   */
  @Test(timeout = 30000L)
  public void testInterAppAntiAffinity() throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(conf);

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data2);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    // app1 asks for 3 anti-affinity containers for the same app. It should
    // only get 3 containers allocated to 3 different nodes..
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(3, Resource.newInstance(1024, 1)),
        Priority.newInstance(1), 1L, ImmutableSet.of("mapper"), "mapper");

    List<Container> allocated = waitForAllocation(3, 3000, am1, nms);
    Assert.assertEquals(3, allocated.size());
    Assert.assertEquals(3, getContainerNodesNum(allocated));

    System.out.println("Mappers on HOST0: "
        + rmNodes[0].getAllocationTagsWithCount().get("mapper"));
    System.out.println("Mappers on HOST1: "
        + rmNodes[1].getAllocationTagsWithCount().get("mapper"));
    System.out.println("Mappers on HOST2: "
        + rmNodes[2].getAllocationTagsWithCount().get("mapper"));

    // app2 -> c
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nms[0]);

    // App2 asks for 3 containers that anti-affinity with any mapper,
    // since 3 out of 4 nodes already have mapper containers, all 3
    // containers will be allocated on the other node.
    TargetApplicationsNamespace.All allNs =
        new TargetApplicationsNamespace.All();
    am2.allocateAppAntiAffinity(
        ResourceSizing.newInstance(3, Resource.newInstance(1024, 1)),
        Priority.newInstance(1), 1L, allNs.toString(),
        ImmutableSet.of("foo"), "mapper");

    List<Container> allocated1 = waitForAllocation(3, 3000, am2, nms);
    Assert.assertEquals(3, allocated1.size());
    Assert.assertEquals(1, getContainerNodesNum(allocated1));
    allocated.addAll(allocated1);
    Assert.assertEquals(4, getContainerNodesNum(allocated));


    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(
        am2.getApplicationAttemptId());

    // The allocated node should not have mapper tag.
    Assert.assertTrue(schedulerApp2.getLiveContainers()
        .stream().allMatch(rmContainer -> {
          // except the nm host
          if (!rmContainer.getContainer().getNodeId().equals(rmNodes[0])) {
            return !rmContainer.getAllocationTags().contains("mapper");
          }
          return true;
        }));

    // app3 -> c
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nms[0]);

    // App3 asks for 3 containers that anti-affinity with any mapper.
    // Unlike the former case, since app3 source tags are also mapper,
    // it will anti-affinity with itself too. So there will be only 1
    // container be allocated.
    am3.allocateAppAntiAffinity(
        ResourceSizing.newInstance(3, Resource.newInstance(1024, 1)),
        Priority.newInstance(1), 1L, allNs.toString(),
        ImmutableSet.of("mapper"), "mapper");


    allocated1 = waitForAllocation(1, 3000, am3, nms);
    Assert.assertEquals(1, allocated1.size());
    allocated.addAll(allocated1);
    Assert.assertEquals(4, getContainerNodesNum(allocated));

    rm1.close();
  }

  @Test
  public void testSchedulingRequestDisabledByDefault() throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(
        new Configuration());

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    // app1 asks for 2 anti-affinity containers for the same app.
    boolean caughtException = false;
    try {
      // Since feature is disabled by default, we should expect exception.
      am1.allocateIntraAppAntiAffinity(
          ResourceSizing.newInstance(2, Resource.newInstance(1024, 1)),
          Priority.newInstance(1), 1L, ImmutableSet.of("tag_1_1", "tag_1_2"),
          "tag_1_1", "tag_1_2");
    } catch (Exception e) {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);
    rm1.close();
  }

  @Test(timeout = 30000L)
  public void testSchedulingRequestWithNullConstraint() throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(conf);

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    PlacementConstraint constraint = targetNotIn("node", allocationTag("t1"))
        .build();
    SchedulingRequest sc = SchedulingRequest
        .newInstance(0, Priority.newInstance(1),
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED),
            ImmutableSet.of("t1"),
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)),
            constraint);
    AllocateRequest request = AllocateRequest.newBuilder()
        .schedulingRequests(ImmutableList.of(sc)).build();
    am1.allocate(request);

    List<Container> allocated = waitForAllocation(1, 3000, am1, nms);
    Assert.assertEquals(1, allocated.size());

    // Send another request with null placement constraint,
    // ensure there is no NPE while handling this request.
    sc = SchedulingRequest
        .newInstance(1, Priority.newInstance(1),
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED),
            ImmutableSet.of("t2"),
            ResourceSizing.newInstance(2, Resource.newInstance(1024, 1)),
            null);
    AllocateRequest request1 = AllocateRequest.newBuilder()
        .schedulingRequests(ImmutableList.of(sc)).build();
    am1.allocate(request1);

    allocated = waitForAllocation(2, 3000, am1, nms);
    Assert.assertEquals(2, allocated.size());

    rm1.close();
  }

  @Test(timeout = 30000L)
  public void testInvalidSchedulingRequest() throws Exception {

    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(conf);
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    MockRMAppSubmissionData submissionData =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm1)
        .withAppName("app")
        .withUser("user")
        .withAcls(null)
        .withQueue("c")
        .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, submissionData);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    // Constraint with Invalid Allocation Tag Namespace
    PlacementConstraint constraint = targetNotIn("node",
        allocationTagWithNamespace("invalid", "t1")).build();
    SchedulingRequest sc = SchedulingRequest
        .newInstance(1, Priority.newInstance(1),
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED),
        ImmutableSet.of("t1"),
        ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)),
        constraint);
    AllocateRequest request = AllocateRequest.newBuilder()
        .schedulingRequests(ImmutableList.of(sc)).build();
    am1.allocate(request);

    try {
      GenericTestUtils.waitFor(() -> {
        try {
          doNodeHeartbeat(nms);
          AllocateResponse response = am1.schedule();
          return response.getRejectedSchedulingRequests().size() == 1;
        } catch (Exception e) {
          return false;
        }
      }, 500, 20000);
    } catch (Exception e) {
      Assert.fail("Failed to reject invalid scheduling request");
    }
  }

  private static void doNodeHeartbeat(MockNM... nms) throws Exception {
    for (MockNM nm : nms) {
      nm.nodeHeartbeat(true);
    }
  }

  public static List<Container> waitForAllocation(int allocNum, int timeout,
      MockAM am, MockNM... nms) throws Exception {
    final List<Container> result = new ArrayList<>();
    GenericTestUtils.waitFor(() -> {
      try {
        AllocateResponse response = am.schedule();
        List<Container> allocated = response.getAllocatedContainers();
        System.out.println("Expecting allocation: " + allocNum
            + ", actual allocation: " + allocated.size());
        for (Container c : allocated) {
          System.out.println("Container " + c.getId().toString()
              + " is allocated on node: " + c.getNodeId().toString()
              + ", allocation tags: "
              + String.join(",", c.getAllocationTags()));
        }
        result.addAll(allocated);
        if (result.size() == allocNum) {
          return true;
        }
        doNodeHeartbeat(nms);
      } catch (Exception e) {
        e.printStackTrace();
      }
      return false;
    }, 500, timeout);
    return result;
  }

  private static SchedulingRequest schedulingRequest(int requestId,
      int containers, int cores, int mem, PlacementConstraint constraint,
      String... tags) {
    return schedulingRequest(1, requestId, containers, cores, mem,
        ExecutionType.GUARANTEED, constraint, tags);
  }

  private static SchedulingRequest schedulingRequest(
      int priority, long allocReqId, int containers, int cores, int mem,
      ExecutionType execType, PlacementConstraint constraint, String... tags) {
    return SchedulingRequest.newBuilder()
        .priority(Priority.newInstance(priority))
        .allocationRequestId(allocReqId)
        .allocationTags(new HashSet<>(Arrays.asList(tags)))
        .executionType(ExecutionTypeRequest.newInstance(execType, true))
        .resourceSizing(
            ResourceSizing.newInstance(containers,
                Resource.newInstance(mem, cores)))
        .placementConstraintExpression(constraint)
        .build();
  }

  public static int getContainerNodesNum(List<Container> containers) {
    Set<NodeId> nodes = new HashSet<>();
    if (containers != null) {
      containers.forEach(c -> nodes.add(c.getNodeId()));
    }
    return nodes.size();
  }

  @Test(timeout = 30000L)
  public void testInterAppCompositeConstraints() throws Exception {
    // This test both intra and inter app constraints.
    // Including simple affinity, anti-affinity, cardinality constraints,
    // and simple AND composite constraints.

    MockRM rm = new MockRM(conf);
    try {
      rm.start();

      MockNM nm1 = rm.registerNode("192.168.0.1:1234", 100*GB, 100);
      MockNM nm2 = rm.registerNode("192.168.0.2:1234", 100*GB, 100);
      MockNM nm3 = rm.registerNode("192.168.0.3:1234", 100*GB, 100);
      MockNM nm4 = rm.registerNode("192.168.0.4:1234", 100*GB, 100);
      MockNM nm5 = rm.registerNode("192.168.0.5:1234", 100*GB, 100);

      RMApp app1 = submitApp(rm, 1*GB, ImmutableSet.of("hbase"));
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      // App1 (hbase)
      // h1: hbase-master(1)
      // h2: hbase-master(1)
      // h3:
      // h4:
      // h5:
      PlacementConstraint pc = targetNotIn("node",
          allocationTag("hbase-master")).build();
      am1.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 2, 1, 2048, pc, "hbase-master")));
      List<Container> allocated = waitForAllocation(2, 3000, am1, nm1, nm2);

      // 2 containers allocated
      Assert.assertEquals(2, allocated.size());
      // containers should be distributed on 2 different nodes
      Assert.assertEquals(2, getContainerNodesNum(allocated));

      // App1 (hbase)
      // h1: hbase-rs(1), hbase-master(1)
      // h2: hbase-rs(1), hbase-master(1)
      // h3: hbase-rs(1)
      // h4: hbase-rs(1)
      // h5:
      pc = targetNotIn("node", allocationTag("hbase-rs")).build();
      am1.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(2, 4, 1, 1024, pc, "hbase-rs")));
      allocated = waitForAllocation(4, 3000, am1, nm1, nm2, nm3, nm4, nm5);

      Assert.assertEquals(4, allocated.size());
      Assert.assertEquals(4, getContainerNodesNum(allocated));

      // App2 (web-server)
      // Web server instance has 2 instance and non of them can be co-allocated
      // with hbase-master.
      RMApp app2 = submitApp(rm, 1*GB, ImmutableSet.of("web-server"));
      MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);

      // App2 (web-server)
      // h1: hbase-rs(1), hbase-master(1)
      // h2: hbase-rs(1), hbase-master(1)
      // h3: hbase-rs(1), ws-inst(1)
      // h4: hbase-rs(1), ws-inst(1)
      // h5:
      pc = and(
          targetIn("node", allocationTagWithNamespace(
              new TargetApplicationsNamespace.All().toString(),
              "hbase-master")),
          targetNotIn("node", allocationTag("ws-inst"))).build();
      am2.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 2, 1, 2048, pc, "ws-inst")));
      allocated = waitForAllocation(2, 3000, am2, nm1, nm2, nm3, nm4, nm5);
      Assert.assertEquals(2, allocated.size());
      Assert.assertEquals(2, getContainerNodesNum(allocated));

      ConcurrentMap<NodeId, RMNode> rmNodes = rm.getRMContext().getRMNodes();
      for (Container c : allocated) {
        RMNode rmNode = rmNodes.get(c.getNodeId());
        Assert.assertNotNull(rmNode);
        Assert.assertTrue("If ws-inst is allocated to a node,"
                + " this node should have inherited the ws-inst tag ",
            rmNode.getAllocationTagsWithCount().get("ws-inst") == 1);
        Assert.assertTrue("ws-inst should be co-allocated to "
                + "hbase-master nodes",
            rmNode.getAllocationTagsWithCount().get("hbase-master") == 1);
      }

      // App3 (ws-servant)
      // App3 has multiple instances that must be co-allocated
      // with app2 server instance, and each node cannot have more than
      // 3 instances.
      RMApp app3 = submitApp(rm, 1*GB, ImmutableSet.of("ws-servants"));
      MockAM am3 = MockRM.launchAndRegisterAM(app3, rm, nm3);


      // App3 (ws-servant)
      // h1: hbase-rs(1), hbase-master(1)
      // h2: hbase-rs(1), hbase-master(1)
      // h3: hbase-rs(1), ws-inst(1), ws-servant(3)
      // h4: hbase-rs(1), ws-inst(1), ws-servant(3)
      // h5:
      pc = and(
          targetIn("node", allocationTagWithNamespace(
              new TargetApplicationsNamespace.AppTag("web-server").toString(),
              "ws-inst")),
          cardinality("node", 0, 2, "ws-servant")).build();
      am3.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 10, 1, 512, pc, "ws-servant")));
      // total 6 containers can be allocated due to cardinality constraint
      // each round, 2 containers can be allocated
      allocated = waitForAllocation(6, 10000, am3, nm1, nm2, nm3, nm4, nm5);
      Assert.assertEquals(6, allocated.size());
      Assert.assertEquals(2, getContainerNodesNum(allocated));

      for (Container c : allocated) {
        RMNode rmNode = rmNodes.get(c.getNodeId());
        Assert.assertNotNull(rmNode);
        Assert.assertTrue("Node has ws-servant allocated must have 3 instances",
            rmNode.getAllocationTagsWithCount().get("ws-servant") == 3);
        Assert.assertTrue("Every ws-servant container should be co-allocated"
                + " with ws-inst",
            rmNode.getAllocationTagsWithCount().get("ws-inst") == 1);
      }
    } finally {
      rm.stop();
    }
  }

  @Test(timeout = 30000L)
  public void testMultiAllocationTagsConstraints() throws Exception {
    // This test simulates to use PC to avoid port conflicts

    MockRM rm = new MockRM(conf);
    try {
      rm.start();

      MockNM nm1 = rm.registerNode("192.168.0.1:1234", 10*GB, 10);
      MockNM nm2 = rm.registerNode("192.168.0.2:1234", 10*GB, 10);
      MockNM nm3 = rm.registerNode("192.168.0.3:1234", 10*GB, 10);
      MockNM nm4 = rm.registerNode("192.168.0.4:1234", 10*GB, 10);
      MockNM nm5 = rm.registerNode("192.168.0.5:1234", 10*GB, 10);

      RMApp app1 = submitApp(rm, 1*GB, ImmutableSet.of("server1"));
      // Allocate AM container on nm1
      doNodeHeartbeat(nm1);
      RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
      MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
      am1.registerAppAttempt();

      // App1 uses ports: 7000, 8000 and 9000
      String[] server1Ports =
          new String[] {"port_6000", "port_7000", "port_8000"};
      PlacementConstraint pc = targetNotIn("node",
          allocationTagWithNamespace(AllocationTagNamespaceType.ALL.toString(),
              server1Ports))
          .build();
      am1.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 2, 1, 1024, pc, server1Ports)));
      List<Container> allocated = waitForAllocation(2, 3000,
          am1, nm1, nm2, nm3, nm4, nm5);

      // 2 containers allocated
      Assert.assertEquals(2, allocated.size());
      // containers should be distributed on 2 different nodes
      Assert.assertEquals(2, getContainerNodesNum(allocated));

      // App1 uses ports: 6000
      String[] server2Ports = new String[] {"port_6000"};
      RMApp app2 = submitApp(rm, 1*GB, ImmutableSet.of("server2"));
      // Allocate AM container on nm1
      doNodeHeartbeat(nm2);
      RMAppAttempt app2attempt1 = app2.getCurrentAppAttempt();
      MockAM am2 = rm.sendAMLaunched(app2attempt1.getAppAttemptId());
      am2.registerAppAttempt();

      pc = targetNotIn("node",
          allocationTagWithNamespace(AllocationTagNamespaceType.ALL.toString(),
              server2Ports))
          .build();
      am2.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 3, 1, 1024, pc, server2Ports)));
      allocated = waitForAllocation(3, 3000, am2, nm1, nm2, nm3, nm4, nm5);
      Assert.assertEquals(3, allocated.size());
      Assert.assertEquals(3, getContainerNodesNum(allocated));

      ConcurrentMap<NodeId, RMNode> rmNodes = rm.getRMContext().getRMNodes();
      for (Container c : allocated) {
        RMNode rmNode = rmNodes.get(c.getNodeId());
        Assert.assertNotNull(rmNode);
        Assert.assertTrue("server2 should not co-allocate to server1 as"
                + " they both need to use port 6000",
            rmNode.getAllocationTagsWithCount().get("port_6000") == 1);
        Assert.assertFalse(rmNode.getAllocationTagsWithCount()
            .containsKey("port_7000"));
        Assert.assertFalse(rmNode.getAllocationTagsWithCount()
            .containsKey("port_8000"));
      }
    } finally {
      rm.stop();
    }
  }

  @Test(timeout = 30000L)
  public void testInterAppConstraintsWithNamespaces() throws Exception {
    // This test verifies inter-app constraints with namespaces
    // not-self/app-id/app-tag
    MockRM rm = new MockRM(conf);
    try {
      rm.start();

      MockNM nm1 = rm.registerNode("192.168.0.1:1234", 100*GB, 100);
      MockNM nm2 = rm.registerNode("192.168.0.2:1234", 100*GB, 100);
      MockNM nm3 = rm.registerNode("192.168.0.3:1234", 100*GB, 100);
      MockNM nm4 = rm.registerNode("192.168.0.4:1234", 100*GB, 100);
      MockNM nm5 = rm.registerNode("192.168.0.5:1234", 100*GB, 100);

      ApplicationId app5Id = null;
      Map<ApplicationId, List<Container>> allocMap = new HashMap<>();
      // 10 apps and all containers are attached with foo tag
      for (int i = 0; i<10; i++) {
        // App1 ~ app5 tag "former5"
        // App6 ~ app10 tag "latter5"
        String applicationTag = i<5 ? "former5" : "latter5";
        RMApp app = submitApp(rm, 1*GB, ImmutableSet.of(applicationTag));
        // Allocate AM container on nm1
        doNodeHeartbeat(nm1, nm2, nm3, nm4, nm5);
        RMAppAttempt attempt = app.getCurrentAppAttempt();
        MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
        am.registerAppAttempt();

        PlacementConstraint pc = targetNotIn("node", allocationTag("foo"))
            .build();
        am.addSchedulingRequest(
            ImmutableList.of(
                schedulingRequest(1, 3, 1, 1024, pc, "foo")));
        List<Container> allocated = waitForAllocation(3, 3000,
            am, nm1, nm2, nm3, nm4, nm5);
        // Memorize containers that has app5 foo
        if (i == 5) {
          app5Id = am.getApplicationAttemptId().getApplicationId();
        }
        allocMap.put(am.getApplicationAttemptId().getApplicationId(),
            allocated);
      }

      Assert.assertNotNull(app5Id);
      Assert.assertEquals(3, getContainerNodesNum(allocMap.get(app5Id)));

      // *** app-id
      // Submit another app, use app-id constraint against app5
      RMApp app1 = submitApp(rm, 1*GB, ImmutableSet.of("xyz"));
      // Allocate AM container on nm1
      doNodeHeartbeat(nm1);
      RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
      MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
      am1.registerAppAttempt();

      PlacementConstraint pc = targetIn("node",
          allocationTagWithNamespace(
              new TargetApplicationsNamespace.AppID(app5Id).toString(),
              "foo"))
          .build();
      am1.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 3, 1, 1024, pc, "foo")));
      List<Container> allocated = waitForAllocation(3, 3000,
          am1, nm1, nm2, nm3, nm4, nm5);

      ConcurrentMap<NodeId, RMNode> rmNodes = rm.getRMContext().getRMNodes();
      List<Container> app5Alloc = allocMap.get(app5Id);
      for (Container c : allocated) {
        RMNode rmNode = rmNodes.get(c.getNodeId());
        Assert.assertNotNull(rmNode);
        Assert.assertTrue("This app is affinity with app-id/app5/foo "
                + "containers",
            app5Alloc.stream().anyMatch(
                c5 -> c5.getNodeId() == c.getNodeId()));
      }

      // *** app-tag
      RMApp app2 = MockRMAppSubmitter.submitWithMemory(1 * GB, rm);
      // Allocate AM container on nm1
      doNodeHeartbeat(nm2);
      RMAppAttempt app2attempt1 = app2.getCurrentAppAttempt();
      MockAM am2 = rm.sendAMLaunched(app2attempt1.getAppAttemptId());
      am2.registerAppAttempt();

      pc = targetNotIn("node",
          allocationTagWithNamespace(
              new TargetApplicationsNamespace.AppTag("xyz").toString(),
              "foo"))
          .build();
      am2.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 2, 1, 1024, pc, "foo")));
      allocated = waitForAllocation(2, 3000, am2, nm1, nm2, nm3, nm4, nm5);
      Assert.assertEquals(2, allocated.size());

      // none of them can be allocated to nodes that has app5 foo containers
      for (Container c : app5Alloc) {
        Assert.assertNotEquals(c.getNodeId(),
            allocated.iterator().next().getNodeId());
      }

      // *** not-self
      RMApp app3 = MockRMAppSubmitter.submitWithMemory(1 * GB, rm);
      // Allocate AM container on nm1
      doNodeHeartbeat(nm3);
      RMAppAttempt app3attempt1 = app3.getCurrentAppAttempt();
      MockAM am3 = rm.sendAMLaunched(app3attempt1.getAppAttemptId());
      am3.registerAppAttempt();

      pc = cardinality("node",
          new TargetApplicationsNamespace.NotSelf().toString(),
          1, 1, "foo").build();
      am3.addSchedulingRequest(
          ImmutableList.of(
              schedulingRequest(1, 1, 1, 1024, pc, "foo")));
      allocated = waitForAllocation(1, 3000, am3, nm1, nm2, nm3, nm4, nm5);
      Assert.assertEquals(1, allocated.size());
      // All 5 containers should be allocated
      Assert.assertTrue(rmNodes.get(allocated.iterator().next().getNodeId())
          .getAllocationTagsWithCount().get("foo") == 2);
    } finally {
      rm.stop();
    }
  }
}
