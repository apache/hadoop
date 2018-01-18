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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
import org.apache.hadoop.yarn.api.records.RejectionReason;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.NODE;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTag;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetCardinality;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetNotIn;

/**
 * This tests end2end workflow of the constraint placement framework.
 */
public class TestPlacementProcessor {

  private static final int GB = 1024;

  private static final Log LOG =
      LogFactory.getLog(TestPlacementProcessor.class);
  private MockRM rm;
  private DrainDispatcher dispatcher;

  @Before
  public void createAndStartRM() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(
        YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_ENABLED, true);
    conf.setInt(
        YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS, 1);
    startRM(conf);
  }

  private void startRM(final YarnConfiguration conf) {
    dispatcher = new DrainDispatcher();
    rm = new MockRM(conf) {
      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    rm.start();
  }

  @After
  public void stopRM() {
    if (rm != null) {
      rm.stop();
    }
  }

  @Test(timeout = 300000)
  public void testAntiAffinityPlacement() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    MockNM nm3 = new MockNM("h3:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm3.getNodeId(), nm3);
    MockNM nm4 = new MockNM("h4:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm4.getNodeId(), nm4);
    nm1.registerNode();
    nm2.registerNode();
    nm3.registerNode();
    nm4.registerNode();

    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "default");
    // Containers with allocationTag 'foo' are restricted to 1 per NODE
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2,
        Collections.singletonMap(Collections.singleton("foo"),
            PlacementConstraints.build(
                PlacementConstraints.targetNotIn(NODE, allocationTag("foo")))));
    am1.addSchedulingRequest(
        Arrays.asList(schedulingRequest(1, 1, 1, 512, "foo"),
            schedulingRequest(1, 2, 1, 512, "foo"),
            schedulingRequest(1, 3, 1, 512, "foo"),
            schedulingRequest(1, 5, 1, 512, "foo")));
    AllocateResponse allocResponse = am1.schedule(); // send the request
    List<Container> allocatedContainers = new ArrayList<>();
    allocatedContainers.addAll(allocResponse.getAllocatedContainers());

    // kick the scheduler
    waitForContainerAllocation(nodes.values(), am1, allocatedContainers, 4);

    Assert.assertEquals(4, allocatedContainers.size());
    Set<NodeId> nodeIds = allocatedContainers.stream().map(x -> x.getNodeId())
        .collect(Collectors.toSet());
    // Ensure unique nodes (antiaffinity)
    Assert.assertEquals(4, nodeIds.size());
  }

  @Test(timeout = 300000)
  public void testCardinalityPlacement() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 8192, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 8192, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    MockNM nm3 = new MockNM("h3:1234", 8192, rm.getResourceTrackerService());
    nodes.put(nm3.getNodeId(), nm3);
    MockNM nm4 = new MockNM("h4:1234", 8192, rm.getResourceTrackerService());
    nodes.put(nm4.getNodeId(), nm4);
    nm1.registerNode();
    nm2.registerNode();
    nm3.registerNode();
    nm4.registerNode();

    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "default");
    // Containers with allocationTag 'foo' should not exceed 4 per NODE
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2,
        Collections.singletonMap(Collections.singleton("foo"),
            PlacementConstraints.build(PlacementConstraints
                .targetCardinality(NODE, 0, 3, allocationTag("foo")))));
    am1.addSchedulingRequest(
        Arrays.asList(schedulingRequest(1, 1, 1, 512, "foo"),
            schedulingRequest(1, 2, 1, 512, "foo"),
            schedulingRequest(1, 3, 1, 512, "foo"),
            schedulingRequest(1, 4, 1, 512, "foo"),
            schedulingRequest(1, 5, 1, 512, "foo"),
            schedulingRequest(1, 6, 1, 512, "foo"),
            schedulingRequest(1, 7, 1, 512, "foo"),
            schedulingRequest(1, 8, 1, 512, "foo")));
    AllocateResponse allocResponse = am1.schedule(); // send the request
    List<Container> allocatedContainers = new ArrayList<>();
    allocatedContainers.addAll(allocResponse.getAllocatedContainers());

    // kick the scheduler
    waitForContainerAllocation(nodes.values(), am1, allocatedContainers, 8);

    Assert.assertEquals(8, allocatedContainers.size());
    Map<NodeId, Long> nodeIdContainerIdMap =
        allocatedContainers.stream().collect(
            Collectors.groupingBy(c -> c.getNodeId(), Collectors.counting()));
    // Ensure no more than 4 containers per node
    for (NodeId n : nodeIdContainerIdMap.keySet()) {
      Assert.assertTrue(nodeIdContainerIdMap.get(n) < 5);
    }
  }

  @Test(timeout = 300000)
  public void testAffinityPlacement() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 8192, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 8192, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    MockNM nm3 = new MockNM("h3:1234", 8192, rm.getResourceTrackerService());
    nodes.put(nm3.getNodeId(), nm3);
    MockNM nm4 = new MockNM("h4:1234", 8192, rm.getResourceTrackerService());
    nodes.put(nm4.getNodeId(), nm4);
    nm1.registerNode();
    nm2.registerNode();
    nm3.registerNode();
    nm4.registerNode();

    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "default");
    // Containers with allocationTag 'foo' should be placed where
    // containers with allocationTag 'bar' are already running
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2,
        Collections.singletonMap(Collections.singleton("foo"),
            PlacementConstraints.build(
                PlacementConstraints.targetIn(NODE, allocationTag("bar")))));
    am1.addSchedulingRequest(
        Arrays.asList(schedulingRequest(1, 1, 1, 512, "bar"),
            schedulingRequest(1, 2, 1, 512, "foo"),
            schedulingRequest(1, 3, 1, 512, "foo"),
            schedulingRequest(1, 4, 1, 512, "foo"),
            schedulingRequest(1, 5, 1, 512, "foo")));
    AllocateResponse allocResponse = am1.schedule(); // send the request
    List<Container> allocatedContainers = new ArrayList<>();
    allocatedContainers.addAll(allocResponse.getAllocatedContainers());

    // kick the scheduler
    waitForContainerAllocation(nodes.values(), am1, allocatedContainers, 5);

    Assert.assertEquals(5, allocatedContainers.size());
    Set<NodeId> nodeIds = allocatedContainers.stream().map(x -> x.getNodeId())
        .collect(Collectors.toSet());
    // Ensure all containers end up on the same node (affinity)
    Assert.assertEquals(1, nodeIds.size());
  }

  @Test(timeout = 300000)
  public void testComplexPlacement() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    MockNM nm3 = new MockNM("h3:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm3.getNodeId(), nm3);
    MockNM nm4 = new MockNM("h4:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm4.getNodeId(), nm4);
    nm1.registerNode();
    nm2.registerNode();
    nm3.registerNode();
    nm4.registerNode();

    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "default");
    Map<Set<String>, PlacementConstraint> constraintMap = new HashMap<>();
    // Containers with allocationTag 'bar' should not exceed 1 per NODE
    constraintMap.put(Collections.singleton("bar"),
        PlacementConstraints.build(targetNotIn(NODE, allocationTag("bar"))));
    // Containers with allocationTag 'foo' should be placed where 'bar' exists
    constraintMap.put(Collections.singleton("foo"),
        PlacementConstraints.build(targetIn(NODE, allocationTag("bar"))));
    // Containers with allocationTag 'foo' should not exceed 2 per NODE
    constraintMap.put(Collections.singleton("foo"), PlacementConstraints
        .build(targetCardinality(NODE, 0, 1, allocationTag("foo"))));
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2, constraintMap);
    am1.addSchedulingRequest(
        Arrays.asList(schedulingRequest(1, 1, 1, 512, "bar"),
            schedulingRequest(1, 2, 1, 512, "bar"),
            schedulingRequest(1, 3, 1, 512, "foo"),
            schedulingRequest(1, 4, 1, 512, "foo"),
            schedulingRequest(1, 5, 1, 512, "foo"),
            schedulingRequest(1, 6, 1, 512, "foo")));
    AllocateResponse allocResponse = am1.schedule(); // send the request
    List<Container> allocatedContainers = new ArrayList<>();
    allocatedContainers.addAll(allocResponse.getAllocatedContainers());

    // kick the scheduler
    waitForContainerAllocation(nodes.values(), am1, allocatedContainers, 6);

    Assert.assertEquals(6, allocatedContainers.size());
    Map<NodeId, Long> nodeIdContainerIdMap =
        allocatedContainers.stream().collect(
            Collectors.groupingBy(c -> c.getNodeId(), Collectors.counting()));
    // Ensure no more than 3 containers per node (1 'bar', 2 'foo')
    for (NodeId n : nodeIdContainerIdMap.keySet()) {
      Assert.assertTrue(nodeIdContainerIdMap.get(n) < 4);
    }
  }

  @Test(timeout = 300000)
  public void testSchedulerRejection() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    MockNM nm3 = new MockNM("h3:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm3.getNodeId(), nm3);
    MockNM nm4 = new MockNM("h4:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm4.getNodeId(), nm4);
    nm1.registerNode();
    nm2.registerNode();
    nm3.registerNode();
    nm4.registerNode();

    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "default");
    // Containers with allocationTag 'foo' are restricted to 1 per NODE
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2,
        Collections.singletonMap(
            Collections.singleton("foo"),
            PlacementConstraints.build(
                PlacementConstraints.targetNotIn(NODE, allocationTag("foo")))
        ));
    am1.addSchedulingRequest(
        Arrays.asList(
            schedulingRequest(1, 1, 1, 512, "foo"),
            schedulingRequest(1, 2, 1, 512, "foo"),
            schedulingRequest(1, 3, 1, 512, "foo"),
            // Ask for a container larger than the node
            schedulingRequest(1, 4, 1, 5120, "foo"))
    );
    AllocateResponse allocResponse = am1.schedule(); // send the request
    List<Container> allocatedContainers = new ArrayList<>();
    List<RejectedSchedulingRequest> rejectedReqs = new ArrayList<>();
    int allocCount = 1;
    allocatedContainers.addAll(allocResponse.getAllocatedContainers());
    rejectedReqs.addAll(allocResponse.getRejectedSchedulingRequests());

    // kick the scheduler
    while (allocCount < 11) {
      nm1.nodeHeartbeat(true);
      nm2.nodeHeartbeat(true);
      nm3.nodeHeartbeat(true);
      nm4.nodeHeartbeat(true);
      LOG.info("Waiting for containers to be created for app 1...");
      sleep(1000);
      allocResponse = am1.schedule();
      allocatedContainers.addAll(allocResponse.getAllocatedContainers());
      rejectedReqs.addAll(allocResponse.getRejectedSchedulingRequests());
      allocCount++;
      if (rejectedReqs.size() > 0 && allocatedContainers.size() > 2) {
        break;
      }
    }

    Assert.assertEquals(3, allocatedContainers.size());
    Set<NodeId> nodeIds = allocatedContainers.stream()
        .map(x -> x.getNodeId()).collect(Collectors.toSet());
    // Ensure unique nodes
    Assert.assertEquals(3, nodeIds.size());
    RejectedSchedulingRequest rej = rejectedReqs.get(0);
    Assert.assertEquals(4, rej.getRequest().getAllocationRequestId());
    Assert.assertEquals(RejectionReason.COULD_NOT_SCHEDULE_ON_NODE,
        rej.getReason());
  }

  @Test(timeout = 300000)
  public void testRePlacementAfterSchedulerRejection() throws Exception {
    stopRM();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(
        YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_ENABLED, true);
    conf.setInt(
        YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS, 2);
    startRM(conf);

    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    MockNM nm3 = new MockNM("h3:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm3.getNodeId(), nm3);
    MockNM nm4 = new MockNM("h4:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm4.getNodeId(), nm4);
    MockNM nm5 = new MockNM("h5:1234", 8192, rm.getResourceTrackerService());
    nodes.put(nm5.getNodeId(), nm5);
    nm1.registerNode();
    nm2.registerNode();
    nm3.registerNode();
    nm4.registerNode();
    // Do not register nm5 yet..

    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "default");
    // Containers with allocationTag 'foo' are restricted to 1 per NODE
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2,
        Collections.singletonMap(
            Collections.singleton("foo"),
            PlacementConstraints.build(
                PlacementConstraints.targetNotIn(NODE, allocationTag("foo")))
        ));
    am1.addSchedulingRequest(
        Arrays.asList(
            schedulingRequest(1, 1, 1, 512, "foo"),
            schedulingRequest(1, 2, 1, 512, "foo"),
            schedulingRequest(1, 3, 1, 512, "foo"),
            // Ask for a container larger than the node
            schedulingRequest(1, 4, 1, 5120, "foo"))
    );
    AllocateResponse allocResponse = am1.schedule(); // send the request
    List<Container> allocatedContainers = new ArrayList<>();
    List<RejectedSchedulingRequest> rejectedReqs = new ArrayList<>();
    int allocCount = 1;
    allocatedContainers.addAll(allocResponse.getAllocatedContainers());
    rejectedReqs.addAll(allocResponse.getRejectedSchedulingRequests());

    // Register node5 only after first allocate - so the initial placement
    // for the large schedReq goes to some other node..
    nm5.registerNode();

    // kick the scheduler
    while (allocCount < 11) {
      nm1.nodeHeartbeat(true);
      nm2.nodeHeartbeat(true);
      nm3.nodeHeartbeat(true);
      nm4.nodeHeartbeat(true);
      nm5.nodeHeartbeat(true);
      LOG.info("Waiting for containers to be created for app 1...");
      sleep(1000);
      allocResponse = am1.schedule();
      allocatedContainers.addAll(allocResponse.getAllocatedContainers());
      rejectedReqs.addAll(allocResponse.getRejectedSchedulingRequests());
      allocCount++;
      if (allocatedContainers.size() > 3) {
        break;
      }
    }

    Assert.assertEquals(4, allocatedContainers.size());
    Set<NodeId> nodeIds = allocatedContainers.stream()
        .map(x -> x.getNodeId()).collect(Collectors.toSet());
    // Ensure unique nodes
    Assert.assertEquals(4, nodeIds.size());
  }

  @Test(timeout = 300000)
  public void testPlacementRejection() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    MockNM nm3 = new MockNM("h3:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm3.getNodeId(), nm3);
    MockNM nm4 = new MockNM("h4:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm4.getNodeId(), nm4);
    nm1.registerNode();
    nm2.registerNode();
    nm3.registerNode();
    nm4.registerNode();

    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "default");
    // Containers with allocationTag 'foo' are restricted to 1 per NODE
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2,
        Collections.singletonMap(
            Collections.singleton("foo"),
            PlacementConstraints.build(
                PlacementConstraints.targetNotIn(NODE, allocationTag("foo")))
        ));
    am1.addSchedulingRequest(
        Arrays.asList(
            schedulingRequest(1, 1, 1, 512, "foo"),
            schedulingRequest(1, 2, 1, 512, "foo"),
            schedulingRequest(1, 3, 1, 512, "foo"),
            schedulingRequest(1, 4, 1, 512, "foo"),
            // Ask for more containers than nodes
            schedulingRequest(1, 5, 1, 512, "foo"))
    );
    AllocateResponse allocResponse = am1.schedule(); // send the request
    List<Container> allocatedContainers = new ArrayList<>();
    List<RejectedSchedulingRequest> rejectedReqs = new ArrayList<>();
    int allocCount = 1;
    allocatedContainers.addAll(allocResponse.getAllocatedContainers());
    rejectedReqs.addAll(allocResponse.getRejectedSchedulingRequests());

    // kick the scheduler
    while (allocCount < 11) {
      nm1.nodeHeartbeat(true);
      nm2.nodeHeartbeat(true);
      nm3.nodeHeartbeat(true);
      nm4.nodeHeartbeat(true);
      LOG.info("Waiting for containers to be created for app 1...");
      sleep(1000);
      allocResponse = am1.schedule();
      allocatedContainers.addAll(allocResponse.getAllocatedContainers());
      rejectedReqs.addAll(allocResponse.getRejectedSchedulingRequests());
      allocCount++;
      if (rejectedReqs.size() > 0 && allocatedContainers.size() > 3) {
        break;
      }
    }

    Assert.assertEquals(4, allocatedContainers.size());
    Set<NodeId> nodeIds = allocatedContainers.stream()
        .map(x -> x.getNodeId()).collect(Collectors.toSet());
    // Ensure unique nodes
    Assert.assertEquals(4, nodeIds.size());
    RejectedSchedulingRequest rej = rejectedReqs.get(0);
    Assert.assertEquals(RejectionReason.COULD_NOT_PLACE_ON_NODE,
        rej.getReason());
  }

  private static void waitForContainerAllocation(Collection<MockNM> nodes,
      MockAM am, List<Container> allocatedContainers, int containerNum)
      throws Exception {
    int attemptCount = 10;
    while (allocatedContainers.size() < containerNum && attemptCount > 0) {
      for (MockNM node : nodes) {
        node.nodeHeartbeat(true);
      }
      LOG.info("Waiting for containers to be created for "
          + am.getApplicationAttemptId().getApplicationId() + "...");
      sleep(1000);
      AllocateResponse allocResponse = am.schedule();
      allocatedContainers.addAll(allocResponse.getAllocatedContainers());
      attemptCount--;
    }
  }

  protected static SchedulingRequest schedulingRequest(
      int priority, long allocReqId, int cores, int mem, String... tags) {
    return schedulingRequest(priority, allocReqId, cores, mem,
        ExecutionType.GUARANTEED, tags);
  }

  protected static SchedulingRequest schedulingRequest(
      int priority, long allocReqId, int cores, int mem,
      ExecutionType execType, String... tags) {
    return SchedulingRequest.newBuilder()
        .priority(Priority.newInstance(priority))
        .allocationRequestId(allocReqId)
        .allocationTags(new HashSet<>(Arrays.asList(tags)))
        .executionType(ExecutionTypeRequest.newInstance(execType, true))
        .resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(mem, cores)))
        .build();
  }
}
