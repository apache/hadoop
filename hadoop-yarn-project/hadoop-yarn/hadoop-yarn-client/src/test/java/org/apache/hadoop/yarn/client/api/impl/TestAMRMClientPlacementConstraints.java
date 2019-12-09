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
package org.apache.hadoop.yarn.client.api.impl;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
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

/**
 * Test Placement Constraints and Scheduling Requests.
 */
public class TestAMRMClientPlacementConstraints extends BaseAMRMClientTest {

  private List<Container> allocatedContainers = null;
  private List<RejectedSchedulingRequest> rejectedSchedulingRequests = null;
  private Map<Set<String>, PlacementConstraint> pcMapping = null;

  @Before
  public void setup() throws Exception {
    conf = new YarnConfiguration();
    allocatedContainers = new ArrayList<>();
    rejectedSchedulingRequests = new ArrayList<>();
    pcMapping = new HashMap<>();
    pcMapping.put(Collections.singleton("foo"),
        PlacementConstraints.build(
            PlacementConstraints.targetNotIn(NODE, allocationTag("foo"))));
    pcMapping.put(Collections.singleton("bar"),
        PlacementConstraints.build(
            PlacementConstraints.targetNotIn(NODE, allocationTag("bar"))));
  }

  @Test(timeout=60000)
  public void testAMRMClientWithPlacementConstraintsByPlacementProcessor()
      throws Exception {
    // we have to create a new instance of MiniYARNCluster to avoid SASL qop
    // mismatches between client and server
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    createClusterAndStartApplication(conf);

    allocatedContainers.clear();
    rejectedSchedulingRequests.clear();
    AMRMClient<AMRMClient.ContainerRequest> amClient =
        AMRMClient.<AMRMClient.ContainerRequest>createAMRMClient();
    amClient.setNMTokenCache(new NMTokenCache());
    //asserting we are not using the singleton instance cache
    Assert.assertNotSame(NMTokenCache.getSingleton(),
        amClient.getNMTokenCache());
    AMRMClientAsync asyncClient = new AMRMClientAsyncImpl<>(amClient,
        1000, new TestCallbackHandler());
    asyncClient.init(conf);
    asyncClient.start();

    asyncClient.registerApplicationMaster("Host", 10000, "", pcMapping);

    // Send two types of requests - 4 with source tag "foo" have numAlloc = 1
    // and 1 with source tag "bar" and has numAlloc = 4. Both should be
    // handled similarly. i.e: Since there are only 3 nodes,
    // 2 schedulingRequests - 1 with source tag "foo" on one with source
    // tag "bar" should get rejected.
    asyncClient.addSchedulingRequests(
        Arrays.asList(
            // 4 reqs with numAlloc = 1
            schedulingRequest(1, 1, 1, 1, 512, "foo"),
            schedulingRequest(1, 1, 2, 1, 512, "foo"),
            schedulingRequest(1, 1, 3, 1, 512, "foo"),
            schedulingRequest(1, 1, 4, 1, 512, "foo"),
            // 1 req with numAlloc = 4
            schedulingRequest(4, 1, 5, 1, 512, "bar")));

    // kick the scheduler
    waitForContainerAllocation(allocatedContainers,
        rejectedSchedulingRequests, 6, 2);

    Assert.assertEquals(6, allocatedContainers.size());
    Map<NodeId, List<Container>> containersPerNode =
        allocatedContainers.stream().collect(
            Collectors.groupingBy(Container::getNodeId));

    Map<Set<String>, List<SchedulingRequest>> outstandingSchedRequests =
        ((AMRMClientImpl)amClient).getOutstandingSchedRequests();
    // Check the outstanding SchedulingRequests
    Assert.assertEquals(2, outstandingSchedRequests.size());
    Assert.assertEquals(1, outstandingSchedRequests.get(
        new HashSet<>(Collections.singletonList("foo"))).size());
    Assert.assertEquals(1, outstandingSchedRequests.get(
        new HashSet<>(Collections.singletonList("bar"))).size());

    // Ensure 2 containers allocated per node.
    // Each node should have a "foo" and a "bar" container.
    Assert.assertEquals(3, containersPerNode.entrySet().size());
    HashSet<String> srcTags = new HashSet<>(Arrays.asList("foo", "bar"));
    containersPerNode.entrySet().forEach(
        x ->
          Assert.assertEquals(
              srcTags,
              x.getValue()
                  .stream()
                  .map(y -> y.getAllocationTags().iterator().next())
                  .collect(Collectors.toSet()))
    );

    // Ensure 2 rejected requests - 1 of "foo" and 1 of "bar"
    Assert.assertEquals(2, rejectedSchedulingRequests.size());
    Assert.assertEquals(srcTags,
        rejectedSchedulingRequests
            .stream()
            .map(x -> x.getRequest().getAllocationTags().iterator().next())
            .collect(Collectors.toSet()));

    asyncClient.stop();
  }

  @Test(timeout=60000)
  public void testAMRMClientWithPlacementConstraintsByScheduler()
      throws Exception {
    // we have to create a new instance of MiniYARNCluster to avoid SASL qop
    // mismatches between client and server
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    createClusterAndStartApplication(conf);

    allocatedContainers.clear();
    rejectedSchedulingRequests.clear();
    AMRMClient<AMRMClient.ContainerRequest> amClient =
        AMRMClient.<AMRMClient.ContainerRequest>createAMRMClient();
    amClient.setNMTokenCache(new NMTokenCache());
    //asserting we are not using the singleton instance cache
    Assert.assertNotSame(NMTokenCache.getSingleton(),
        amClient.getNMTokenCache());
    AMRMClientAsync asyncClient = new AMRMClientAsyncImpl<>(amClient,
        1000, new TestCallbackHandler());
    asyncClient.init(conf);
    asyncClient.start();

    asyncClient.registerApplicationMaster("Host", 10000, "", pcMapping);

    // Send two types of requests - 4 with source tag "foo" have numAlloc = 1
    // and 1 with source tag "bar" and has numAlloc = 4. Both should be
    // handled similarly. i.e: Since there are only 3 nodes,
    // 2 schedulingRequests - 1 with source tag "foo" on one with source
    // tag "bar" should get rejected.
    asyncClient.addSchedulingRequests(
        Arrays.asList(
            // 4 reqs with numAlloc = 1
            schedulingRequest(1, 1, 1, 1, 512, "foo"),
            schedulingRequest(1, 1, 2, 1, 512, "foo"),
            schedulingRequest(1, 1, 3, 1, 512, "foo"),
            schedulingRequest(1, 1, 4, 1, 512, "foo"),
            // 1 req with numAlloc = 4
            schedulingRequest(4, 1, 5, 1, 512, "bar"),
            // 1 empty tag
            schedulingRequest(1, 1, 6, 1, 512, new HashSet<>())));

    // kick the scheduler
    waitForContainerAllocation(allocatedContainers,
        rejectedSchedulingRequests, 7, 0);

    Assert.assertEquals(7, allocatedContainers.size());
    Map<NodeId, List<Container>> containersPerNode =
        allocatedContainers.stream().collect(
            Collectors.groupingBy(Container::getNodeId));

    Map<Set<String>, List<SchedulingRequest>> outstandingSchedRequests =
        ((AMRMClientImpl)amClient).getOutstandingSchedRequests();
    // Check the outstanding SchedulingRequests
    Assert.assertEquals(3, outstandingSchedRequests.size());
    Assert.assertEquals(1, outstandingSchedRequests.get(
        new HashSet<>(Collections.singletonList("foo"))).size());
    Assert.assertEquals(1, outstandingSchedRequests.get(
        new HashSet<>(Collections.singletonList("bar"))).size());
    Assert.assertEquals(0, outstandingSchedRequests.get(
        new HashSet<String>()).size());

    // Each node should have a "foo" and a "bar" container.
    Assert.assertEquals(3, containersPerNode.entrySet().size());
    HashSet<String> srcTags = new HashSet<>(Arrays.asList("foo", "bar"));
    containersPerNode.entrySet().forEach(
        x ->
          Assert.assertEquals(
              srcTags,
              x.getValue()
                  .stream()
                  .filter(y -> !y.getAllocationTags().isEmpty())
                  .map(y -> y.getAllocationTags().iterator().next())
                  .collect(Collectors.toSet()))
    );

    // The rejected requests were not set by scheduler
    Assert.assertEquals(0, rejectedSchedulingRequests.size());

    asyncClient.stop();
  }


  @Test
  /*
   * Three cases of empty HashSet key of outstandingSchedRequests
   * 1. Not set any tags
   * 2. Set a empty set, e.g ImmutableSet.of(), new HashSet<>()
   * 3. Set tag as null
   */
  public void testEmptyKeyOfOutstandingSchedRequests() {
    AMRMClient<AMRMClient.ContainerRequest> amClient =
        AMRMClient.<AMRMClient.ContainerRequest>createAMRMClient();
    HashSet<String> schedRequest = null;
    amClient.addSchedulingRequests(Arrays.asList(
        schedulingRequest(1, 1, 1, 1, 512, ExecutionType.GUARANTEED),
        schedulingRequest(1, 1, 2, 1, 512, new HashSet<>()),
        schedulingRequest(1, 1, 3, 1, 512, schedRequest)));
    Map<Set<String>, List<SchedulingRequest>> outstandingSchedRequests =
        ((AMRMClientImpl)amClient).getOutstandingSchedRequests();
    Assert.assertEquals(1, outstandingSchedRequests.size());
    Assert.assertEquals(3, outstandingSchedRequests
        .get(new HashSet<String>()).size());
  }

  private class TestCallbackHandler extends
      AMRMClientAsync.AbstractCallbackHandler {
    @Override
    public void onContainersAllocated(List<Container> containers) {
      allocatedContainers.addAll(containers);
    }

    @Override
    public void onRequestsRejected(
        List<RejectedSchedulingRequest> rejReqs) {
      rejectedSchedulingRequests.addAll(rejReqs);
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {}
    @Override
    public void onContainersUpdated(List<UpdatedContainer> containers) {}
    @Override
    public void onShutdownRequest() {}
    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}
    @Override
    public void onError(Throwable e) {}

    @Override
    public float getProgress() {
      return 0.1f;
    }
  }

  private static void waitForContainerAllocation(
      List<Container> allocatedContainers,
      List<RejectedSchedulingRequest> rejectedRequests,
      int containerNum, int rejNum) throws Exception {

    int maxCount = 10;
    while (maxCount >= 0 &&
        (allocatedContainers.size() < containerNum ||
            rejectedRequests.size() < rejNum)) {
      maxCount--;
      sleep(1000);
    }
  }

  private static SchedulingRequest schedulingRequest(int numAllocations,
      int priority, long allocReqId, int cores, int mem, String... tags) {
    return schedulingRequest(numAllocations, priority, allocReqId, cores, mem,
        ExecutionType.GUARANTEED, new HashSet<>(Arrays.asList(tags)));
  }

  private static SchedulingRequest schedulingRequest(int numAllocations,
      int priority, long allocReqId, int cores, int mem, Set<String> tags) {
    return schedulingRequest(numAllocations,
        priority, allocReqId, cores, mem, ExecutionType.GUARANTEED, tags);
  }

  private static SchedulingRequest schedulingRequest(int numAllocations,
      int priority, long allocReqId, int cores, int mem,
      ExecutionType execType, Set<String> tags) {
    SchedulingRequest schedRequest = schedulingRequest(numAllocations,
        priority, allocReqId, cores, mem, execType);
    schedRequest.setAllocationTags(tags);
    return schedRequest;
  }

  private static SchedulingRequest schedulingRequest(int numAllocations,
      int priority, long allocReqId, int cores, int mem,
      ExecutionType execType) {
    return SchedulingRequest.newBuilder()
        .priority(Priority.newInstance(priority))
        .allocationRequestId(allocReqId)
        .executionType(ExecutionTypeRequest.newInstance(execType, true))
        .resourceSizing(
            ResourceSizing.newInstance(numAllocations,
                Resource.newInstance(mem, cores)))
        .build();
  }
}
