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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.metrics.OpportunisticSchedulerMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases for Centralized Opportunistic Container Allocator.
 */
public class TestCentralizedOpportunisticContainerAllocator {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestCentralizedOpportunisticContainerAllocator.class);
  private static final int GB = 1024;
  private CentralizedOpportunisticContainerAllocator allocator = null;
  private OpportunisticContainerContext oppCntxt = null;
  private static final Priority PRIORITY_NORMAL = Priority.newInstance(1);
  private static final Resource CAPABILITY_1GB =
      Resources.createResource(GB);
  private static final ResourceBlacklistRequest EMPTY_BLACKLIST_REQUEST =
      ResourceBlacklistRequest.newInstance(
          new ArrayList<>(), new ArrayList<>());

  @Before
  public void setup() {
    // creating a dummy master key to be used for creation of container.
    final MasterKey mKey = new MasterKey() {
      @Override
      public int getKeyId() {
        return 1;
      }
      @Override
      public void setKeyId(int keyId) {}
      @Override
      public ByteBuffer getBytes() {
        return ByteBuffer.allocate(8);
      }
      @Override
      public void setBytes(ByteBuffer bytes) {}
    };

    // creating a dummy tokenSecretManager to be used for creation of
    // container.
    BaseContainerTokenSecretManager secMan =
        new BaseContainerTokenSecretManager(new Configuration()) {
          @Override
          public MasterKey getCurrentKey() {
            return mKey;
          }

          @Override
          public byte[] createPassword(ContainerTokenIdentifier identifier) {
            return new byte[]{1, 2};
          }
        };

    allocator = new CentralizedOpportunisticContainerAllocator(secMan);
    oppCntxt = new OpportunisticContainerContext();
    oppCntxt.getAppParams().setMinResource(Resource.newInstance(1024, 1));
    oppCntxt.getAppParams().setIncrementResource(Resource.newInstance(512, 1));
    oppCntxt.getAppParams().setMaxResource(Resource.newInstance(1024, 10));
  }

  /**
   * Tests allocation of an Opportunistic container from single application.
   * @throws Exception
   */
  @Test
  public void testSimpleAllocation() throws Exception {
    List<ResourceRequest> reqs =
        Collections.singletonList(createResourceRequest(1, "*", 1));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    allocator.setNodeQueueLoadMonitor(createNodeQueueLoadMonitor(1, 2, 100));

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");
    assertEquals(1, containers.size());
    assertEquals(0, oppCntxt.getOutstandingOpReqs().size());
  }

  /**
   * Tests Opportunistic container should not be allocated on blacklisted
   * nodes.
   * @throws Exception
   */
  @Test
  public void testBlacklistRejection() throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            Arrays.asList("h1", "h2"), new ArrayList<>());
    List<ResourceRequest> reqs =
        Collections.singletonList(createResourceRequest(1, "*", 1));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    allocator.setNodeQueueLoadMonitor(createNodeQueueLoadMonitor(2, 2, 100));

    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "user");
    assertEquals(0, containers.size());
    assertEquals(1, oppCntxt.getOutstandingOpReqs().size());
  }

  /**
   * Tests that allocation of Opportunistic containers should be spread out.
   * @throws Exception
   */
  @Test
  public void testRoundRobinSimpleAllocation() throws Exception {
    List<ResourceRequest> reqs =
        Arrays.asList(
            createResourceRequest(1, ResourceRequest.ANY, 1),
            createResourceRequest(2, ResourceRequest.ANY, 1),
            createResourceRequest(3, ResourceRequest.ANY, 1));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    allocator.setNodeQueueLoadMonitor(createNodeQueueLoadMonitor(3, 2, 3));

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");
    LOG.info("Containers: {}", containers);
    Set<String> allocatedNodes = new HashSet<>();
    for (Container c : containers) {
      allocatedNodes.add(c.getNodeId().toString());
    }
    assertTrue(allocatedNodes.contains("h1:1234"));
    assertTrue(allocatedNodes.contains("h2:1234"));
    assertTrue(allocatedNodes.contains("h3:1234"));
    assertEquals(3, containers.size());
  }

  /**
   * Tests allocation of node local Opportunistic container requests.
   * @throws Exception
   */
  @Test
  public void testNodeLocalAllocation() throws Exception {
    List<ResourceRequest> reqs =
        Arrays.asList(
            createResourceRequest(1, ResourceRequest.ANY, 1),
            createResourceRequest(2, "/r1", 1),
            createResourceRequest(2, "h1", 1),
            createResourceRequest(2, ResourceRequest.ANY, 1),
            createResourceRequest(3, "/r1", 1),
            createResourceRequest(3, "h1", 1),
            createResourceRequest(3, ResourceRequest.ANY, 1));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    allocator.setNodeQueueLoadMonitor(createNodeQueueLoadMonitor(3, 2, 5));

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");
    LOG.info("Containers: {}", containers);
    // all 3 containers should be allocated.
    assertEquals(3, containers.size());
    // container with allocation id 2 and 3 should be allocated on node h1
    for (Container c : containers) {
      if (c.getAllocationRequestId() == 2 || c.getAllocationRequestId() == 3) {
        assertEquals("h1:1234", c.getNodeId().toString());
      }
    }
  }

  /**
   * Tests node local allocation of Opportunistic container requests with
   * same allocation request id.
   * @throws Exception
   */
  @Test
  public void testNodeLocalAllocationSameSchedulerKey() throws Exception {
    List<ResourceRequest> reqs =
        Arrays.asList(
            createResourceRequest(2, "/r1", 2),
            createResourceRequest(2, "h1", 2),
            createResourceRequest(2, ResourceRequest.ANY, 2));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    allocator.setNodeQueueLoadMonitor(createNodeQueueLoadMonitor(3, 2, 5));

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");
    LOG.info("Containers: {}", containers);
    Set<String> allocatedHosts = new HashSet<>();
    for (Container c : containers) {
      allocatedHosts.add(c.getNodeId().toString());
    }
    assertEquals(2, containers.size());
    assertTrue(allocatedHosts.contains("h1:1234"));
    assertFalse(allocatedHosts.contains("h2:1234"));
    assertFalse(allocatedHosts.contains("h3:1234"));
  }

  /**
   * Tests rack local allocation of Opportunistic container requests.
   * @throws Exception
   */
  @Test
  public void testSimpleRackLocalAllocation() throws Exception {
    List<ResourceRequest> reqs =
        Arrays.asList(
            createResourceRequest(2, "/r1", 1),
            createResourceRequest(2, "h4", 1),
            createResourceRequest(2, ResourceRequest.ANY, 1));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    NodeQueueLoadMonitor selector = createNodeQueueLoadMonitor(
        Arrays.asList("h1", "h2", "h3"), Arrays.asList("/r2", "/r1", "/r3"),
        Arrays.asList(2, 2, 2), Arrays.asList(5, 5, 5));
    allocator.setNodeQueueLoadMonitor(selector);

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");
    Set<String> allocatedHosts = new HashSet<>();
    for (Container c : containers) {
      allocatedHosts.add(c.getNodeId().toString());
    }
    assertTrue(allocatedHosts.contains("h2:1234"));
    assertFalse(allocatedHosts.contains("h3:1234"));
    assertFalse(allocatedHosts.contains("h4:1234"));
    assertEquals(1, containers.size());
  }

  /**
   * Tests that allocation of rack local Opportunistic container requests
   * should be spread out.
   * @throws Exception
   */
  @Test
  public void testRoundRobinRackLocalAllocation() throws Exception {
    List<ResourceRequest> reqs =
        Arrays.asList(
            createResourceRequest(1, "/r1", 1),
            createResourceRequest(1, "h5", 1),
            createResourceRequest(1, ResourceRequest.ANY, 1),
            createResourceRequest(2, "/r1", 1),
            createResourceRequest(2, "h5", 1),
            createResourceRequest(2, ResourceRequest.ANY, 1));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    NodeQueueLoadMonitor selector = createNodeQueueLoadMonitor(
        Arrays.asList("h1", "h2", "h3", "h4"),
        Arrays.asList("/r2", "/r1", "/r3", "/r1"),
        Arrays.asList(4, 4, 4, 4), Arrays.asList(5, 5, 5, 5));
    allocator.setNodeQueueLoadMonitor(selector);

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");
    Set<String> allocatedHosts = new HashSet<>();
    for (Container c : containers) {
      allocatedHosts.add(c.getNodeId().toString());
    }
    LOG.info("Containers: {}", containers);
    assertTrue(allocatedHosts.contains("h2:1234"));
    assertTrue(allocatedHosts.contains("h4:1234"));
    assertFalse(allocatedHosts.contains("h1:1234"));
    assertFalse(allocatedHosts.contains("h3:1234"));
    assertEquals(2, containers.size());
  }

  /**
   * Tests that allocation of rack local Opportunistic container requests
   * with same allocation request id should be spread out.
   * @throws Exception
   */
  @Test
  public void testRoundRobinRackLocalAllocationSameSchedulerKey()
      throws Exception {
    List<ResourceRequest> reqs =
        Arrays.asList(
            createResourceRequest(2, "/r1", 2),
            createResourceRequest(2, "h5", 2),
            createResourceRequest(2, ResourceRequest.ANY, 2));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    NodeQueueLoadMonitor selector = createNodeQueueLoadMonitor(
        Arrays.asList("h1", "h2", "h3", "h4"),
        Arrays.asList("/r2", "/r1", "/r3", "/r1"),
        Arrays.asList(4, 4, 4, 4), Arrays.asList(5, 5, 5, 5));
    allocator.setNodeQueueLoadMonitor(selector);

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");
    Set<String> allocatedHosts = new HashSet<>();
    for (Container c : containers) {
      allocatedHosts.add(c.getNodeId().toString());
    }
    LOG.info("Containers: {}", containers);
    assertTrue(allocatedHosts.contains("h2:1234"));
    assertTrue(allocatedHosts.contains("h4:1234"));
    assertFalse(allocatedHosts.contains("h1:1234"));
    assertFalse(allocatedHosts.contains("h3:1234"));
    assertEquals(2, containers.size());
  }

  /**
   * Tests off switch allocation of Opportunistic containers.
   * @throws Exception
   */
  @Test
  public void testOffSwitchAllocationWhenNoNodeOrRack() throws Exception {
    List<ResourceRequest> reqs =
        Arrays.asList(
            createResourceRequest(2, "/r3", 2),
            createResourceRequest(2, "h6", 2),
            createResourceRequest(2, ResourceRequest.ANY, 2));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    NodeQueueLoadMonitor selector = createNodeQueueLoadMonitor(
        Arrays.asList("h1", "h2", "h3", "h4"),
        Arrays.asList("/r2", "/r1", "/r2", "/r1"),
        Arrays.asList(4, 4, 4, 4), Arrays.asList(5, 5, 5, 5));
    allocator.setNodeQueueLoadMonitor(selector);

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");
    LOG.info("Containers: {}", containers);
    assertEquals(2, containers.size());
  }

  /**
   * Tests allocation of rack local Opportunistic containers with same
   * scheduler key.
   * @throws Exception
   */
  @Test
  public void testLotsOfContainersRackLocalAllocationSameSchedulerKey()
      throws Exception {
    List<ResourceRequest> reqs =
        Arrays.asList(
            createResourceRequest(2, "/r1", 1000),
            createResourceRequest(2, "h1", 1000),
            createResourceRequest(2, ResourceRequest.ANY, 1000));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    NodeQueueLoadMonitor selector = createNodeQueueLoadMonitor(
        Arrays.asList("h1", "h2", "h3", "h4"),
        Arrays.asList("/r1", "/r1", "/r1", "/r2"),
        Arrays.asList(0, 0, 0, 0), Arrays.asList(500, 500, 500, 300));
    allocator.setNodeQueueLoadMonitor(selector);

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");

    Map<String, Integer> hostsToNumContainerMap = new HashMap<>();
    for (Container c : containers) {
      String host = c.getNodeId().toString();
      int numContainers = 0;
      if (hostsToNumContainerMap.containsKey(host)) {
        numContainers = hostsToNumContainerMap.get(host);
      }
      hostsToNumContainerMap.put(host, numContainers + 1);
    }
    assertEquals(1000, containers.size());
    assertEquals(500, hostsToNumContainerMap.get("h1:1234").intValue());
    assertFalse(hostsToNumContainerMap.containsKey("h4:1234"));
  }

  /**
   * Tests scheduling of many rack local Opportunistic container requests.
   * @throws Exception
   */
  @Test
  public void testLotsOfContainersRackLocalAllocation()
      throws Exception {
    List<ResourceRequest> reqs = new ArrayList<>();
    // add 100 container requests.
    for (int i = 0; i < 100; i++) {
      reqs.add(createResourceRequest(i + 1, ResourceRequest.ANY, 1));
      reqs.add(createResourceRequest(i + 1, "h5", 1));
      reqs.add(createResourceRequest(i + 1, "/r1", 1));
    }
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    NodeQueueLoadMonitor selector = createNodeQueueLoadMonitor(
        Arrays.asList("h1", "h2", "h3", "h4"),
        Arrays.asList("/r1", "/r1", "/r1", "/r2"),
        Arrays.asList(0, 0, 0, 0), Arrays.asList(500, 500, 500, 300));
    allocator.setNodeQueueLoadMonitor(selector);

    List<Container> containers = new ArrayList<>();
    containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");
    assertEquals(100, containers.size());
  }

  /**
   * Tests maximum number of opportunistic containers that can be allocated in
   * AM heartbeat.
   * @throws Exception
   */
  @Test
  public void testMaxAllocationsPerAMHeartbeat() throws Exception {
    allocator.setMaxAllocationsPerAMHeartbeat(2);
    List<ResourceRequest> reqs = Arrays.asList(
        createResourceRequest(2, "/r3", 3),
        createResourceRequest(2, "h6", 3),
        createResourceRequest(2, ResourceRequest.ANY, 3));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    allocator.setNodeQueueLoadMonitor(createNodeQueueLoadMonitor(3, 2, 5));

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");
    LOG.info("Containers: {}", containers);
    // Although capacity is present, but only 2 containers should be allocated
    // as max allocation per AM heartbeat is set to 2.
    assertEquals(2, containers.size());
    containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, new ArrayList<>(), appAttId,
        oppCntxt, 1L, "user");
    LOG.info("Containers: {}", containers);
    // Remaining 1 container should be allocated.
    assertEquals(1, containers.size());
  }

  /**
   * Tests maximum opportunistic container allocation per AM heartbeat for
   * allocation requests with different scheduler key.
   * @throws Exception
   */
  @Test
  public void testMaxAllocationsPerAMHeartbeatDifferentSchedKey()
      throws Exception {
    allocator.setMaxAllocationsPerAMHeartbeat(2);
    List<ResourceRequest> reqs =
        Arrays.asList(
            createResourceRequest(1, ResourceRequest.ANY, 1),
            createResourceRequest(2, "h6", 2),
            createResourceRequest(3, "/r3", 2));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    allocator.setNodeQueueLoadMonitor(createNodeQueueLoadMonitor(3, 2, 5));

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");
    LOG.info("Containers: {}", containers);
    // Although capacity is present, but only 2 containers should be allocated
    // as max allocation per AM heartbeat is set to 2.
    assertEquals(2, containers.size());
    containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, new ArrayList<>(), appAttId,
        oppCntxt, 1L, "user");
    LOG.info("Containers: {}", containers);
    // 2 more containers should be allocated from pending allocation requests.
    assertEquals(2, containers.size());
    containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, new ArrayList<>(), appAttId,
        oppCntxt, 1L, "user");
    LOG.info("Containers: {}", containers);
    // Remaining 1 container should be allocated.
    assertEquals(1, containers.size());
  }

  /**
   * Tests maximum opportunistic container allocation per AM heartbeat when
   * limit is set to -1.
   * @throws Exception
   */
  @Test
  public void testMaxAllocationsPerAMHeartbeatWithNoLimit() throws Exception {
    allocator.setMaxAllocationsPerAMHeartbeat(-1);

    List<ResourceRequest> reqs = new ArrayList<>();
    final int numContainers = 20;
    for (int i = 0; i < numContainers; i++) {
      reqs.add(createResourceRequest(i + 1, "h1", 1));
    }
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    allocator.setNodeQueueLoadMonitor(createNodeQueueLoadMonitor(3, 2, 500));

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");

    // all containers should be allocated in single heartbeat.
    assertEquals(numContainers, containers.size());
  }

  /**
   * Tests maximum opportunistic container allocation per AM heartbeat when
   * limit is set to higher value.
   * @throws Exception
   */
  @Test
  public void testMaxAllocationsPerAMHeartbeatWithHighLimit()
      throws Exception {
    allocator.setMaxAllocationsPerAMHeartbeat(100);
    final int numContainers = 20;
    List<ResourceRequest> reqs = new ArrayList<>();
    for (int i = 0; i < numContainers; i++) {
      reqs.add(createResourceRequest(i + 1, "h1", 1));
    }
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    allocator.setNodeQueueLoadMonitor(createNodeQueueLoadMonitor(3, 2, 500));

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");

    // all containers should be allocated in single heartbeat.
    assertEquals(numContainers, containers.size());
  }

  /**
   * Test opportunistic container allocation latency metrics.
   * @throws Exception
   */
  @Test
  public void testAllocationLatencyMetrics() throws Exception {
    oppCntxt = spy(oppCntxt);
    OpportunisticSchedulerMetrics metrics =
        mock(OpportunisticSchedulerMetrics.class);
    when(oppCntxt.getOppSchedulerMetrics()).thenReturn(metrics);
    List<ResourceRequest> reqs = Arrays.asList(
        createResourceRequest(2, "/r3", 2),
        createResourceRequest(2, "h6", 2),
        createResourceRequest(2, ResourceRequest.ANY, 2));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    allocator.setNodeQueueLoadMonitor(createNodeQueueLoadMonitor(3, 2, 5));

    List<Container> containers = allocator.allocateContainers(
        EMPTY_BLACKLIST_REQUEST, reqs, appAttId, oppCntxt, 1L, "user");
    LOG.info("Containers: {}", containers);
    assertEquals(2, containers.size());
    // for each allocated container, latency should be added.
    verify(metrics, times(2)).addAllocateOLatencyEntry(anyLong());
  }

  private NodeQueueLoadMonitor createNodeQueueLoadMonitor(int numNodes,
      int queueLength, int queueCapacity) {
    NodeQueueLoadMonitor selector = new NodeQueueLoadMonitor(
        NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH);
    for (int i = 1; i <= numNodes; ++i) {
      RMNode node = createRMNode("h" + i, 1234, queueLength, queueCapacity);
      selector.addNode(null, node);
      selector.updateNode(node);
    }
    selector.computeTask.run();
    return selector;
  }

  private NodeQueueLoadMonitor createNodeQueueLoadMonitor(List<String> hosts,
      List<String> racks, List<Integer> queueLengths,
      List<Integer> queueCapacities) {
    NodeQueueLoadMonitor selector = new NodeQueueLoadMonitor(
        NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH);
    for (int i = 0; i < hosts.size(); ++i) {
      RMNode node = createRMNode(hosts.get(i), 1234, racks.get(i),
          queueLengths.get(i), queueCapacities.get(i));
      selector.addNode(null, node);
      selector.updateNode(node);
    }
    selector.computeTask.run();
    return selector;
  }

  private ResourceRequest createResourceRequest(int allocationId,
      String location, int numContainers) {
    return ResourceRequest.newBuilder()
        .allocationRequestId(allocationId)
        .priority(PRIORITY_NORMAL)
        .resourceName(location)
        .capability(CAPABILITY_1GB)
        .relaxLocality(true)
        .numContainers(numContainers)
        .executionType(ExecutionType.OPPORTUNISTIC).build();
  }

  private RMNode createRMNode(String host, int port, int queueLength,
      int queueCapacity) {
    return createRMNode(host, port, "default", queueLength,
        queueCapacity);
  }

  private RMNode createRMNode(String host, int port, String rack,
      int queueLength, int queueCapacity) {
    RMNode node1 = Mockito.mock(RMNode.class);
    NodeId nID1 = new TestNodeQueueLoadMonitor.FakeNodeId(host, port);
    Mockito.when(node1.getHostName()).thenReturn(host);
    Mockito.when(node1.getRackName()).thenReturn(rack);
    Mockito.when(node1.getNodeID()).thenReturn(nID1);
    Mockito.when(node1.getState()).thenReturn(NodeState.RUNNING);
    OpportunisticContainersStatus status1 =
        Mockito.mock(OpportunisticContainersStatus.class);
    Mockito.when(status1.getEstimatedQueueWaitTime())
        .thenReturn(-1);
    Mockito.when(status1.getWaitQueueLength())
        .thenReturn(queueLength);
    Mockito.when(status1.getOpportQueueCapacity())
        .thenReturn(queueCapacity);
    Mockito.when(node1.getOpportunisticContainersStatus()).thenReturn(status1);
    return node1;
  }
}
