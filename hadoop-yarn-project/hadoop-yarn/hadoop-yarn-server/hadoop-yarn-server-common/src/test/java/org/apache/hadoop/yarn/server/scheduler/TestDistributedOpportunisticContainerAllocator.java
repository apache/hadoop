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
package org.apache.hadoop.yarn.server.scheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.metrics.OpportunisticSchedulerMetrics;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases for DistributedOpportunisticContainerAllocator.
 */
public class TestDistributedOpportunisticContainerAllocator {

  private static final Logger LOG =
      LoggerFactory.getLogger(
          TestDistributedOpportunisticContainerAllocator.class);
  private static final int GB = 1024;
  private DistributedOpportunisticContainerAllocator allocator = null;
  private OpportunisticContainerContext oppCntxt = null;
  private static final Priority PRIORITY_NORMAL = Priority.newInstance(1);
  private static final Resource CAPABILITY_1GB =
      Resources.createResource(1 * GB);
  private static final ExecutionTypeRequest OPPORTUNISTIC_REQ =
      ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC, true);

  @Before
  public void setup() {
    SecurityUtil.setTokenServiceUseIp(false);
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
    allocator = new DistributedOpportunisticContainerAllocator(secMan);
    oppCntxt = new OpportunisticContainerContext();
    oppCntxt.getAppParams().setMinResource(Resource.newInstance(1024, 1));
    oppCntxt.getAppParams().setIncrementResource(Resource.newInstance(512, 1));
    oppCntxt.getAppParams().setMaxResource(Resource.newInstance(1024, 10));
  }

  @Test
  public void testSimpleAllocation() throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    List<ResourceRequest> reqs =
        Arrays.asList(ResourceRequest.newInstance(PRIORITY_NORMAL,
        "*", CAPABILITY_1GB, 1, true, null, OPPORTUNISTIC_REQ));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h1", 1234), "h1:1234", "/r1")));
    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser");
    Assert.assertEquals(1, containers.size());
    Assert.assertEquals(0, oppCntxt.getOutstandingOpReqs().size());
  }

  @Test
  public void testBlacklistRejection() throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            Arrays.asList("h1", "h2"), new ArrayList<>());
    List<ResourceRequest> reqs =
        Arrays.asList(ResourceRequest.newInstance(PRIORITY_NORMAL,
            "*", CAPABILITY_1GB, 1, true, null, OPPORTUNISTIC_REQ));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h1", 1234), "h1:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r2")));
    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser");
    Assert.assertEquals(0, containers.size());
    Assert.assertEquals(1, oppCntxt.getOutstandingOpReqs().size());
  }

  @Test
  public void testRoundRobinSimpleAllocation() throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    List<ResourceRequest> reqs =
        Arrays.asList(
            ResourceRequest.newBuilder().allocationRequestId(1)
                .priority(PRIORITY_NORMAL)
                .resourceName(ResourceRequest.ANY)
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(PRIORITY_NORMAL)
                .resourceName(ResourceRequest.ANY)
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(3)
                .priority(PRIORITY_NORMAL)
                .resourceName(ResourceRequest.ANY)
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build());
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h1", 1234), "h1:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h3", 1234), "h3:1234", "/r1")));

    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser");
    LOG.info("Containers: {}", containers);
    Set<String> allocatedHosts = new HashSet<>();
    for (Container c : containers) {
      allocatedHosts.add(c.getNodeHttpAddress());
    }
    Assert.assertTrue(allocatedHosts.contains("h1:1234"));
    Assert.assertTrue(allocatedHosts.contains("h2:1234"));
    Assert.assertTrue(allocatedHosts.contains("h3:1234"));
    Assert.assertEquals(3, containers.size());
  }

  @Test
  public void testNodeLocalAllocation() throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    List<ResourceRequest> reqs =
        Arrays.asList(
            ResourceRequest.newBuilder().allocationRequestId(1)
                .priority(PRIORITY_NORMAL)
                .resourceName(ResourceRequest.ANY)
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(PRIORITY_NORMAL)
                .resourceName("/r1")
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(PRIORITY_NORMAL)
                .resourceName("h1")
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(PRIORITY_NORMAL)
                .resourceName(ResourceRequest.ANY)
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(3)
                .priority(PRIORITY_NORMAL)
                .resourceName("/r1")
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(3)
                .priority(PRIORITY_NORMAL)
                .resourceName("h1")
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(3)
                .priority(PRIORITY_NORMAL)
                .resourceName(ResourceRequest.ANY)
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build());
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h1", 1234), "h1:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h3", 1234), "h3:1234", "/r1")));

    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser");
    LOG.info("Containers: {}", containers);
    // all 3 containers should be allocated.
    Assert.assertEquals(3, containers.size());
    // container with allocation id 2 and 3 should be allocated on node h1
    for (Container c : containers) {
      if (c.getAllocationRequestId() == 2 || c.getAllocationRequestId() == 3) {
        Assert.assertEquals("h1:1234", c.getNodeHttpAddress());
      }
    }
  }

  @Test
  public void testNodeLocalAllocationSameSchedKey() throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    List<ResourceRequest> reqs =
        Arrays.asList(
            ResourceRequest.newBuilder().allocationRequestId(2)
                .numContainers(2)
                .priority(PRIORITY_NORMAL)
                .resourceName("/r1")
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .numContainers(2)
                .priority(PRIORITY_NORMAL)
                .resourceName("h1")
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .numContainers(2)
                .priority(PRIORITY_NORMAL)
                .resourceName(ResourceRequest.ANY)
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build());
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h1", 1234), "h1:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h3", 1234), "h3:1234", "/r1")));

    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser");
    LOG.info("Containers: {}", containers);
    Set<String> allocatedHosts = new HashSet<>();
    for (Container c : containers) {
      allocatedHosts.add(c.getNodeHttpAddress());
    }
    Assert.assertEquals(2, containers.size());
    Assert.assertTrue(allocatedHosts.contains("h1:1234"));
    Assert.assertFalse(allocatedHosts.contains("h2:1234"));
    Assert.assertFalse(allocatedHosts.contains("h3:1234"));
  }

  @Test
  public void testSimpleRackLocalAllocation() throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    List<ResourceRequest> reqs =
        Arrays.asList(
            ResourceRequest.newInstance(PRIORITY_NORMAL, "*",
                CAPABILITY_1GB, 1, true, null, OPPORTUNISTIC_REQ),
            ResourceRequest.newInstance(PRIORITY_NORMAL, "h1",
                CAPABILITY_1GB, 1, true, null, OPPORTUNISTIC_REQ),
            ResourceRequest.newInstance(PRIORITY_NORMAL, "/r1",
                CAPABILITY_1GB, 1, true, null, OPPORTUNISTIC_REQ));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h3", 1234), "h3:1234", "/r2"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h4", 1234), "h4:1234", "/r2")));

    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser");
    Set<String> allocatedHosts = new HashSet<>();
    for (Container c : containers) {
      allocatedHosts.add(c.getNodeHttpAddress());
    }
    Assert.assertTrue(allocatedHosts.contains("h2:1234"));
    Assert.assertFalse(allocatedHosts.contains("h3:1234"));
    Assert.assertFalse(allocatedHosts.contains("h4:1234"));
    Assert.assertEquals(1, containers.size());
  }

  @Test
  public void testRoundRobinRackLocalAllocation() throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    List<ResourceRequest> reqs =
        Arrays.asList(
            ResourceRequest.newBuilder().allocationRequestId(1)
                .priority(PRIORITY_NORMAL)
                .resourceName("/r1")
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(1)
                .priority(PRIORITY_NORMAL)
                .resourceName("h1")
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(1)
                .priority(PRIORITY_NORMAL)
                .resourceName(ResourceRequest.ANY)
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(PRIORITY_NORMAL)
                .resourceName("/r1")
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(PRIORITY_NORMAL)
                .resourceName("h1")
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(PRIORITY_NORMAL)
                .resourceName(ResourceRequest.ANY)
                .capability(CAPABILITY_1GB)
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build());
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h3", 1234), "h3:1234", "/r2"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h5", 1234), "h5:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h4", 1234), "h4:1234", "/r2")));

    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser");
    Set<String> allocatedHosts = new HashSet<>();
    for (Container c : containers) {
      allocatedHosts.add(c.getNodeHttpAddress());
    }
    LOG.info("Containers: {}", containers);
    Assert.assertTrue(allocatedHosts.contains("h2:1234"));
    Assert.assertTrue(allocatedHosts.contains("h5:1234"));
    Assert.assertFalse(allocatedHosts.contains("h3:1234"));
    Assert.assertFalse(allocatedHosts.contains("h4:1234"));
    Assert.assertEquals(2, containers.size());
  }

  @Test
  public void testRoundRobinRackLocalAllocationSameSchedKey() throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    List<ResourceRequest> reqs =
        Arrays.asList(
            ResourceRequest.newInstance(PRIORITY_NORMAL, "*",
                CAPABILITY_1GB, 2, true, null, OPPORTUNISTIC_REQ),
            ResourceRequest.newInstance(PRIORITY_NORMAL, "h1",
                CAPABILITY_1GB, 2, true, null, OPPORTUNISTIC_REQ),
            ResourceRequest.newInstance(PRIORITY_NORMAL, "/r1",
                CAPABILITY_1GB, 2, true, null, OPPORTUNISTIC_REQ));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h3", 1234), "h3:1234", "/r2"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h5", 1234), "h5:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h4", 1234), "h4:1234", "/r2")));

    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser");
    Set<String> allocatedHosts = new HashSet<>();
    for (Container c : containers) {
      allocatedHosts.add(c.getNodeHttpAddress());
    }
    LOG.info("Containers: {}", containers);
    Assert.assertTrue(allocatedHosts.contains("h2:1234"));
    Assert.assertTrue(allocatedHosts.contains("h5:1234"));
    Assert.assertFalse(allocatedHosts.contains("h3:1234"));
    Assert.assertFalse(allocatedHosts.contains("h4:1234"));
    Assert.assertEquals(2, containers.size());
  }

  @Test
  public void testOffSwitchAllocationWhenNoNodeOrRack() throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    List<ResourceRequest> reqs =
        Arrays.asList(
            ResourceRequest.newInstance(PRIORITY_NORMAL, "*",
                CAPABILITY_1GB, 2, true, null, OPPORTUNISTIC_REQ),
            ResourceRequest.newInstance(PRIORITY_NORMAL, "h6",
                CAPABILITY_1GB, 2, true, null, OPPORTUNISTIC_REQ),
            ResourceRequest.newInstance(PRIORITY_NORMAL, "/r3",
                CAPABILITY_1GB, 2, true, null, OPPORTUNISTIC_REQ));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h3", 1234), "h3:1234", "/r2"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h5", 1234), "h5:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h4", 1234), "h4:1234", "/r2")));

    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser");
    LOG.info("Containers: {}", containers);
    Assert.assertEquals(2, containers.size());
  }

  @Test
  public void testLotsOfContainersRackLocalAllocationSameSchedKey()
      throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    List<ResourceRequest> reqs =
        Arrays.asList(
            ResourceRequest.newInstance(PRIORITY_NORMAL, "*",
                CAPABILITY_1GB, 1000, true, null, OPPORTUNISTIC_REQ),
            ResourceRequest.newInstance(PRIORITY_NORMAL, "h1",
                CAPABILITY_1GB, 1000, true, null, OPPORTUNISTIC_REQ),
            ResourceRequest.newInstance(PRIORITY_NORMAL, "/r1",
                CAPABILITY_1GB, 1000, true, null, OPPORTUNISTIC_REQ));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h3", 1234), "h3:1234", "/r2"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h5", 1234), "h5:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h4", 1234), "h4:1234", "/r2")));

    List<Container> containers = new ArrayList<>();
    for (int i = 0; i < 250; i++) {
      containers.addAll(allocator.allocateContainers(
          blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser"));
    }
    Assert.assertEquals(1000, containers.size());
  }

  @Test
  public void testLotsOfContainersRackLocalAllocation()
      throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    List<ResourceRequest> reqs = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      reqs.add(ResourceRequest.newBuilder().allocationRequestId(i + 1)
          .priority(PRIORITY_NORMAL)
          .resourceName("*")
          .capability(CAPABILITY_1GB)
          .relaxLocality(true)
          .executionType(ExecutionType.OPPORTUNISTIC).build());
      reqs.add(ResourceRequest.newBuilder().allocationRequestId(i + 1)
          .priority(PRIORITY_NORMAL)
          .resourceName("h1")
          .capability(CAPABILITY_1GB)
          .relaxLocality(true)
          .executionType(ExecutionType.OPPORTUNISTIC).build());
      reqs.add(ResourceRequest.newBuilder().allocationRequestId(i + 1)
          .priority(PRIORITY_NORMAL)
          .resourceName("/r1")
          .capability(CAPABILITY_1GB)
          .relaxLocality(true)
          .executionType(ExecutionType.OPPORTUNISTIC).build());
    }
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h3", 1234), "h3:1234", "/r2"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h5", 1234), "h5:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h4", 1234), "h4:1234", "/r2")));

    List<Container> containers = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      containers.addAll(allocator.allocateContainers(
          blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser"));
    }
    Assert.assertEquals(100, containers.size());
  }

  @Test
  public void testAllocationWithNodeLabels() throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    List<ResourceRequest> reqs =
        Arrays.asList(ResourceRequest.newInstance(PRIORITY_NORMAL,
            "*", CAPABILITY_1GB, 1, true, "label", OPPORTUNISTIC_REQ));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h1", 1234), "h1:1234", "/r1")));
    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser");
    /* Since there is no node satisfying node label constraints, requests
       won't get fulfilled.
    */
    Assert.assertEquals(0, containers.size());
    Assert.assertEquals(1, oppCntxt.getOutstandingOpReqs().size());

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h1", 1234), "h1:1234", "/r1",
                "label")));

    containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser");
    Assert.assertEquals(1, containers.size());
    Assert.assertEquals(0, oppCntxt.getOutstandingOpReqs().size());
  }

  /**
   * Tests maximum number of opportunistic containers that can be allocated in
   * AM heartbeat.
   * @throws Exception
   */
  @Test
  public void testMaxAllocationsPerAMHeartbeat() throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    allocator.setMaxAllocationsPerAMHeartbeat(2);
    List<ResourceRequest> reqs = Arrays.asList(
        ResourceRequest.newInstance(PRIORITY_NORMAL, "*", CAPABILITY_1GB, 3,
            true, null, OPPORTUNISTIC_REQ),
        ResourceRequest.newInstance(PRIORITY_NORMAL, "h6", CAPABILITY_1GB, 3,
            true, null, OPPORTUNISTIC_REQ),
        ResourceRequest.newInstance(PRIORITY_NORMAL, "/r3", CAPABILITY_1GB, 3,
            true, null, OPPORTUNISTIC_REQ));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h3", 1234), "h3:1234", "/r2"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h5", 1234), "h5:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h4", 1234), "h4:1234", "/r2")));

    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "user1");
    LOG.info("Containers: {}", containers);
    // Although capacity is present, but only 2 containers should be allocated
    // as max allocation per AM heartbeat is set to 2.
    Assert.assertEquals(2, containers.size());
    containers = allocator.allocateContainers(
        blacklistRequest, new ArrayList<>(), appAttId, oppCntxt, 1L, "user1");
    LOG.info("Containers: {}", containers);
    // Remaining 1 container should be allocated.
    Assert.assertEquals(1, containers.size());
  }

  /**
   * Tests maximum opportunistic container allocation per AM heartbeat for
   * allocation requests with different scheduler key.
   * @throws Exception
   */
  @Test
  public void testMaxAllocationsPerAMHeartbeatDifferentSchedKey()
      throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    allocator.setMaxAllocationsPerAMHeartbeat(2);
    final ExecutionTypeRequest oppRequest = ExecutionTypeRequest.newInstance(
        ExecutionType.OPPORTUNISTIC, true);
    List<ResourceRequest> reqs =
        Arrays.asList(
            ResourceRequest.newInstance(Priority.newInstance(1), "*",
                CAPABILITY_1GB, 1, true, null, OPPORTUNISTIC_REQ),
            ResourceRequest.newInstance(Priority.newInstance(2), "h6",
                CAPABILITY_1GB, 2, true, null, OPPORTUNISTIC_REQ),
            ResourceRequest.newInstance(Priority.newInstance(3), "/r3",
                CAPABILITY_1GB, 2, true, null, OPPORTUNISTIC_REQ));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h3", 1234), "h3:1234", "/r2"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h5", 1234), "h5:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h4", 1234), "h4:1234", "/r2")));

    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "user1");
    LOG.info("Containers: {}", containers);
    // Although capacity is present, but only 2 containers should be allocated
    // as max allocation per AM heartbeat is set to 2.
    Assert.assertEquals(2, containers.size());
    containers = allocator.allocateContainers(
        blacklistRequest, new ArrayList<>(), appAttId, oppCntxt, 1L, "user1");
    LOG.info("Containers: {}", containers);
    // 2 more containers should be allocated from pending allocation requests.
    Assert.assertEquals(2, containers.size());
    containers = allocator.allocateContainers(
        blacklistRequest, new ArrayList<>(), appAttId, oppCntxt, 1L, "user1");
    LOG.info("Containers: {}", containers);
    // Remaining 1 container should be allocated.
    Assert.assertEquals(1, containers.size());
  }

  /**
   * Tests maximum opportunistic container allocation per AM heartbeat when
   * limit is set to -1.
   * @throws Exception
   */
  @Test
  public void testMaxAllocationsPerAMHeartbeatWithNoLimit() throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    allocator.setMaxAllocationsPerAMHeartbeat(-1);

    List<ResourceRequest> reqs = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      reqs.add(ResourceRequest.newBuilder().allocationRequestId(i + 1)
          .priority(PRIORITY_NORMAL)
          .resourceName("h1")
          .capability(CAPABILITY_1GB)
          .relaxLocality(true)
          .executionType(ExecutionType.OPPORTUNISTIC).build());
    }
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h1", 1234), "h1:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1")));

    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "user1");

    // all containers should be allocated in single heartbeat.
    Assert.assertEquals(20, containers.size());
  }

  /**
   * Tests maximum opportunistic container allocation per AM heartbeat when
   * limit is set to higher value.
   * @throws Exception
   */
  @Test
  public void testMaxAllocationsPerAMHeartbeatWithHighLimit()
      throws Exception {
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            new ArrayList<>(), new ArrayList<>());
    allocator.setMaxAllocationsPerAMHeartbeat(100);

    List<ResourceRequest> reqs = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      reqs.add(ResourceRequest.newBuilder().allocationRequestId(i + 1)
          .priority(PRIORITY_NORMAL)
          .resourceName("h1")
          .capability(CAPABILITY_1GB)
          .relaxLocality(true)
          .executionType(ExecutionType.OPPORTUNISTIC).build());
    }
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h1", 1234), "h1:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1")));

    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "user1");

    // all containers should be allocated in single heartbeat.
    Assert.assertEquals(20, containers.size());
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
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            Collections.emptyList(), Collections.emptyList());
    List<ResourceRequest> reqs = Arrays.asList(
        ResourceRequest.newInstance(PRIORITY_NORMAL, "*", CAPABILITY_1GB, 2,
            true, null, OPPORTUNISTIC_REQ),
        ResourceRequest.newInstance(PRIORITY_NORMAL, "h6", CAPABILITY_1GB, 2,
            true, null, OPPORTUNISTIC_REQ),
        ResourceRequest.newInstance(PRIORITY_NORMAL, "/r3", CAPABILITY_1GB, 2,
            true, null, OPPORTUNISTIC_REQ));
    ApplicationAttemptId appAttId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0L, 1), 1);

    oppCntxt.updateNodeList(
        Arrays.asList(
            RemoteNode.newInstance(
                NodeId.newInstance("h3", 1234), "h3:1234", "/r2"),
            RemoteNode.newInstance(
                NodeId.newInstance("h2", 1234), "h2:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h5", 1234), "h5:1234", "/r1"),
            RemoteNode.newInstance(
                NodeId.newInstance("h4", 1234), "h4:1234", "/r2")));

    List<Container> containers = allocator.allocateContainers(
        blacklistRequest, reqs, appAttId, oppCntxt, 1L, "luser");
    LOG.info("Containers: {}", containers);
    Assert.assertEquals(2, containers.size());
    // for each allocated container, latency should be added.
    verify(metrics, times(2)).addAllocateOLatencyEntry(anyLong());
  }
}
