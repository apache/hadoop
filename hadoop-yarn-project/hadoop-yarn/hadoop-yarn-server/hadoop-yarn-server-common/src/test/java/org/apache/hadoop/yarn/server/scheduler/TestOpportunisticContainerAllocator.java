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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestOpportunisticContainerAllocator {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOpportunisticContainerAllocator.class);
  private static final int GB = 1024;
  private OpportunisticContainerAllocator allocator = null;
  private OpportunisticContainerContext oppCntxt = null;

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
    allocator = new OpportunisticContainerAllocator(secMan);
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
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
        "*", Resources.createResource(1 * GB), 1, true, null,
        ExecutionTypeRequest.newInstance(
            ExecutionType.OPPORTUNISTIC, true)));
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
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
            "*", Resources.createResource(1 * GB), 1, true, null,
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true)));
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
                .priority(Priority.newInstance(1))
                .resourceName(ResourceRequest.ANY)
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(Priority.newInstance(1))
                .resourceName(ResourceRequest.ANY)
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(3)
                .priority(Priority.newInstance(1))
                .resourceName(ResourceRequest.ANY)
                .capability(Resources.createResource(1 * GB))
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
                .priority(Priority.newInstance(1))
                .resourceName(ResourceRequest.ANY)
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(Priority.newInstance(1))
                .resourceName("/r1")
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(Priority.newInstance(1))
                .resourceName("h1")
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(Priority.newInstance(1))
                .resourceName(ResourceRequest.ANY)
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(3)
                .priority(Priority.newInstance(1))
                .resourceName("/r1")
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(3)
                .priority(Priority.newInstance(1))
                .resourceName("h1")
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(3)
                .priority(Priority.newInstance(1))
                .resourceName(ResourceRequest.ANY)
                .capability(Resources.createResource(1 * GB))
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
                .priority(Priority.newInstance(1))
                .resourceName("/r1")
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .numContainers(2)
                .priority(Priority.newInstance(1))
                .resourceName("h1")
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .numContainers(2)
                .priority(Priority.newInstance(1))
                .resourceName(ResourceRequest.ANY)
                .capability(Resources.createResource(1 * GB))
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
            ResourceRequest.newInstance(Priority.newInstance(1), "*",
                Resources.createResource(1 * GB), 1, true, null,
                ExecutionTypeRequest.newInstance(
                    ExecutionType.OPPORTUNISTIC, true)),
            ResourceRequest.newInstance(Priority.newInstance(1), "h1",
                Resources.createResource(1 * GB), 1, true, null,
                ExecutionTypeRequest.newInstance(
                    ExecutionType.OPPORTUNISTIC, true)),
            ResourceRequest.newInstance(Priority.newInstance(1), "/r1",
                Resources.createResource(1 * GB), 1, true, null,
                ExecutionTypeRequest.newInstance(
                    ExecutionType.OPPORTUNISTIC, true)));
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
                .priority(Priority.newInstance(1))
                .resourceName("/r1")
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(1)
                .priority(Priority.newInstance(1))
                .resourceName("h1")
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(1)
                .priority(Priority.newInstance(1))
                .resourceName(ResourceRequest.ANY)
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(Priority.newInstance(1))
                .resourceName("/r1")
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(Priority.newInstance(1))
                .resourceName("h1")
                .capability(Resources.createResource(1 * GB))
                .relaxLocality(true)
                .executionType(ExecutionType.OPPORTUNISTIC).build(),
            ResourceRequest.newBuilder().allocationRequestId(2)
                .priority(Priority.newInstance(1))
                .resourceName(ResourceRequest.ANY)
                .capability(Resources.createResource(1 * GB))
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
            ResourceRequest.newInstance(Priority.newInstance(1), "*",
                Resources.createResource(1 * GB), 2, true, null,
                ExecutionTypeRequest.newInstance(
                    ExecutionType.OPPORTUNISTIC, true)),
            ResourceRequest.newInstance(Priority.newInstance(1), "h1",
                Resources.createResource(1 * GB), 2, true, null,
                ExecutionTypeRequest.newInstance(
                    ExecutionType.OPPORTUNISTIC, true)),
            ResourceRequest.newInstance(Priority.newInstance(1), "/r1",
                Resources.createResource(1 * GB), 2, true, null,
                ExecutionTypeRequest.newInstance(
                    ExecutionType.OPPORTUNISTIC, true)));
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
            ResourceRequest.newInstance(Priority.newInstance(1), "*",
                Resources.createResource(1 * GB), 2, true, null,
                ExecutionTypeRequest.newInstance(
                    ExecutionType.OPPORTUNISTIC, true)),
            ResourceRequest.newInstance(Priority.newInstance(1), "h6",
                Resources.createResource(1 * GB), 2, true, null,
                ExecutionTypeRequest.newInstance(
                    ExecutionType.OPPORTUNISTIC, true)),
            ResourceRequest.newInstance(Priority.newInstance(1), "/r3",
                Resources.createResource(1 * GB), 2, true, null,
                ExecutionTypeRequest.newInstance(
                    ExecutionType.OPPORTUNISTIC, true)));
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
            ResourceRequest.newInstance(Priority.newInstance(1), "*",
                Resources.createResource(1 * GB), 1000, true, null,
                ExecutionTypeRequest.newInstance(
                    ExecutionType.OPPORTUNISTIC, true)),
            ResourceRequest.newInstance(Priority.newInstance(1), "h1",
                Resources.createResource(1 * GB), 1000, true, null,
                ExecutionTypeRequest.newInstance(
                    ExecutionType.OPPORTUNISTIC, true)),
            ResourceRequest.newInstance(Priority.newInstance(1), "/r1",
                Resources.createResource(1 * GB), 1000, true, null,
                ExecutionTypeRequest.newInstance(
                    ExecutionType.OPPORTUNISTIC, true)));
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
          .priority(Priority.newInstance(1))
          .resourceName("*")
          .capability(Resources.createResource(1 * GB))
          .relaxLocality(true)
          .executionType(ExecutionType.OPPORTUNISTIC).build());
      reqs.add(ResourceRequest.newBuilder().allocationRequestId(i + 1)
          .priority(Priority.newInstance(1))
          .resourceName("h1")
          .capability(Resources.createResource(1 * GB))
          .relaxLocality(true)
          .executionType(ExecutionType.OPPORTUNISTIC).build());
      reqs.add(ResourceRequest.newBuilder().allocationRequestId(i + 1)
          .priority(Priority.newInstance(1))
          .resourceName("/r1")
          .capability(Resources.createResource(1 * GB))
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
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
            "*", Resources.createResource(1 * GB), 1, true, "label",
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true)));
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
}
