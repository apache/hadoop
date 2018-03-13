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

  @Test(timeout=60000)
  public void testAMRMClientWithPlacementConstraints()
      throws Exception {
    // we have to create a new instance of MiniYARNCluster to avoid SASL qop
    // mismatches between client and server
    teardown();
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    createClusterAndStartApplication(conf);

    AMRMClient<AMRMClient.ContainerRequest> amClient =
        AMRMClient.<AMRMClient.ContainerRequest>createAMRMClient();
    amClient.setNMTokenCache(new NMTokenCache());
    //asserting we are not using the singleton instance cache
    Assert.assertNotSame(NMTokenCache.getSingleton(),
        amClient.getNMTokenCache());

    final List<Container> allocatedContainers = new ArrayList<>();
    final List<RejectedSchedulingRequest> rejectedSchedulingRequests =
        new ArrayList<>();
    AMRMClientAsync asyncClient = new AMRMClientAsyncImpl<>(amClient, 1000,
        new AMRMClientAsync.AbstractCallbackHandler() {
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
        });

    asyncClient.init(conf);
    asyncClient.start();
    Map<Set<String>, PlacementConstraint> pcMapping = new HashMap<>();
    pcMapping.put(Collections.singleton("foo"),
        PlacementConstraints.build(
            PlacementConstraints.targetNotIn(NODE, allocationTag("foo"))));
    pcMapping.put(Collections.singleton("bar"),
        PlacementConstraints.build(
            PlacementConstraints.targetNotIn(NODE, allocationTag("bar"))));
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
        ExecutionType.GUARANTEED, tags);
  }

  private static SchedulingRequest schedulingRequest(int numAllocations,
      int priority, long allocReqId, int cores, int mem,
      ExecutionType execType, String... tags) {
    return SchedulingRequest.newBuilder()
        .priority(Priority.newInstance(priority))
        .allocationRequestId(allocReqId)
        .allocationTags(new HashSet<>(Arrays.asList(tags)))
        .executionType(ExecutionTypeRequest.newInstance(execType, true))
        .resourceSizing(
            ResourceSizing.newInstance(numAllocations,
                Resource.newInstance(mem, cores)))
        .build();
  }
}
