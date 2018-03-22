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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings.AssignedResources;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

/**
 * Test class for NumaResourceAllocator.
 */
public class TestNumaResourceAllocator {

  private Configuration conf;
  private NumaResourceAllocator numaResourceAllocator;

  @Before
  public void setUp() throws IOException, YarnException {
    conf = new YarnConfiguration();
    Context mockContext = mock(Context.class);
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<ContainerId, Container> mockContainers = mock(
        ConcurrentHashMap.class);
    Container mockContainer = mock(Container.class);
    when(mockContainer.getResourceMappings())
        .thenReturn(new ResourceMappings());
    when(mockContainers.get(Matchers.any())).thenReturn(mockContainer);
    when(mockContext.getContainers()).thenReturn(mockContainers);
    NMStateStoreService mock = mock(NMStateStoreService.class);
    when(mockContext.getNMStateStore()).thenReturn(mock);
    numaResourceAllocator = new NumaResourceAllocator(mockContext);
    setNumaTopologyConfigs();
    numaResourceAllocator.init(conf);
  }

  @Test
  public void testReadNumaTopologyFromConfigurations() throws Exception {
    Collection<NumaNodeResource> nodesList = numaResourceAllocator
        .getNumaNodesList();
    Collection<NumaNodeResource> expectedNodesList = getExpectedNumaNodesList();
    Assert.assertEquals(expectedNodesList, nodesList);
  }

  @Test
  public void testReadNumaTopologyFromCmdOutput() throws Exception {
    conf.setBoolean(YarnConfiguration.NM_NUMA_AWARENESS_READ_TOPOLOGY, true);
    String cmdOutput = "available: 2 nodes (0-1)\n\t"
        + "node 0 cpus: 0 2 4 6\n\t"
        + "node 0 size: 73717 MB\n\t"
        + "node 0 free: 17272 MB\n\t"
        + "node 1 cpus: 1 3 5 7\n\t"
        + "node 1 size: 73727 MB\n\t"
        + "node 1 free: 10699 MB\n\t"
        + "node distances:\n\t"
        + "node 0 1\n\t"
        + "0: 10 20\n\t"
        + "1: 20 10";
    numaResourceAllocator = new NumaResourceAllocator(mock(Context.class)) {
      @Override
      String executeNGetCmdOutput(Configuration config)
          throws YarnRuntimeException {
        return cmdOutput;
      }
    };
    numaResourceAllocator.init(conf);
    Collection<NumaNodeResource> nodesList = numaResourceAllocator
        .getNumaNodesList();
    Collection<NumaNodeResource> expectedNodesList = getExpectedNumaNodesList();
    Assert.assertEquals(expectedNodesList, nodesList);
  }

  @Test
  public void testAllocateNumaNode() throws Exception {
    NumaResourceAllocation nodeInfo = numaResourceAllocator
        .allocateNumaNodes(getContainer(
            ContainerId.fromString("container_1481156246874_0001_01_000001"),
            Resource.newInstance(2048, 2)));
    Assert.assertEquals("0", String.join(",", nodeInfo.getMemNodes()));
    Assert.assertEquals("0", String.join(",", nodeInfo.getCpuNodes()));
  }

  @Test
  public void testAllocateNumaNodeWithRoundRobinFashionAssignment()
      throws Exception {
    NumaResourceAllocation nodeInfo1 = numaResourceAllocator
        .allocateNumaNodes(getContainer(
            ContainerId.fromString("container_1481156246874_0001_01_000001"),
            Resource.newInstance(2048, 2)));
    Assert.assertEquals("0", String.join(",", nodeInfo1.getMemNodes()));
    Assert.assertEquals("0", String.join(",", nodeInfo1.getCpuNodes()));

    NumaResourceAllocation nodeInfo2 = numaResourceAllocator
        .allocateNumaNodes(getContainer(
            ContainerId.fromString("container_1481156246874_0001_01_000002"),
            Resource.newInstance(2048, 2)));
    Assert.assertEquals("1", String.join(",", nodeInfo2.getMemNodes()));
    Assert.assertEquals("1", String.join(",", nodeInfo2.getCpuNodes()));

    NumaResourceAllocation nodeInfo3 = numaResourceAllocator
        .allocateNumaNodes(getContainer(
            ContainerId.fromString("container_1481156246874_0001_01_000003"),
            Resource.newInstance(2048, 2)));
    Assert.assertEquals("0", String.join(",", nodeInfo3.getMemNodes()));
    Assert.assertEquals("0", String.join(",", nodeInfo3.getCpuNodes()));

    NumaResourceAllocation nodeInfo4 = numaResourceAllocator
        .allocateNumaNodes(getContainer(
            ContainerId.fromString("container_1481156246874_0001_01_000003"),
            Resource.newInstance(2048, 2)));
    Assert.assertEquals("1", String.join(",", nodeInfo4.getMemNodes()));
    Assert.assertEquals("1", String.join(",", nodeInfo4.getCpuNodes()));
  }

  @Test
  public void testAllocateNumaNodeWithMultipleNodesForMemory()
      throws Exception {
    NumaResourceAllocation nodeInfo = numaResourceAllocator
        .allocateNumaNodes(getContainer(
            ContainerId.fromString("container_1481156246874_0001_01_000001"),
            Resource.newInstance(102400, 2)));
    Assert.assertEquals("0,1", String.join(",", nodeInfo.getMemNodes()));
    Assert.assertEquals("0", String.join(",", nodeInfo.getCpuNodes()));
  }

  @Test
  public void testAllocateNumaNodeWithMultipleNodesForCpus() throws Exception {
    NumaResourceAllocation nodeInfo = numaResourceAllocator
        .allocateNumaNodes(getContainer(
            ContainerId.fromString("container_1481156246874_0001_01_000001"),
            Resource.newInstance(2048, 6)));
    Assert.assertEquals("0", String.join(",", nodeInfo.getMemNodes()));
    Assert.assertEquals("0,1", String.join(",", nodeInfo.getCpuNodes()));
  }

  @Test
  public void testAllocateNumaNodeWhenNoNumaMemResourcesAvailable()
      throws Exception {
    NumaResourceAllocation nodeInfo = numaResourceAllocator
        .allocateNumaNodes(getContainer(
            ContainerId.fromString("container_1481156246874_0001_01_000001"),
            Resource.newInstance(2048000, 6)));
    Assert.assertNull("Should not assign numa nodes when there"
        + " are no sufficient memory resources available.", nodeInfo);
  }

  @Test
  public void testAllocateNumaNodeWhenNoNumaCpuResourcesAvailable()
      throws Exception {
    NumaResourceAllocation nodeInfo = numaResourceAllocator
        .allocateNumaNodes(getContainer(
            ContainerId.fromString("container_1481156246874_0001_01_000001"),
            Resource.newInstance(2048, 600)));
    Assert.assertNull("Should not assign numa nodes when there"
        + " are no sufficient cpu resources available.", nodeInfo);
  }

  @Test
  public void testReleaseNumaResourcess() throws Exception {
    NumaResourceAllocation nodeInfo = numaResourceAllocator
        .allocateNumaNodes(getContainer(
            ContainerId.fromString("container_1481156246874_0001_01_000001"),
            Resource.newInstance(2048, 8)));
    Assert.assertEquals("0", String.join(",", nodeInfo.getMemNodes()));
    Assert.assertEquals("0,1", String.join(",", nodeInfo.getCpuNodes()));

    // Request the resource when all cpu nodes occupied
    nodeInfo = numaResourceAllocator.allocateNumaNodes(getContainer(
        ContainerId.fromString("container_1481156246874_0001_01_000002"),
        Resource.newInstance(2048, 4)));
    Assert.assertNull("Should not assign numa nodes when there"
        + " are no sufficient cpu resources available.", nodeInfo);

    // Release the resources
    numaResourceAllocator.releaseNumaResource(
        ContainerId.fromString("container_1481156246874_0001_01_000001"));
    // Request the resources
    nodeInfo = numaResourceAllocator.allocateNumaNodes(getContainer(
        ContainerId.fromString("container_1481156246874_0001_01_000003"),
        Resource.newInstance(1024, 2)));
    Assert.assertEquals("0", String.join(",", nodeInfo.getMemNodes()));
    Assert.assertEquals("0", String.join(",", nodeInfo.getCpuNodes()));
  }

  @Test
  public void testRecoverNumaResource() throws Exception {
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<ContainerId, Container> mockContainers = mock(
        ConcurrentHashMap.class);
    Context mockContext = mock(Context.class);
    Container mockContainer = mock(Container.class);
    ResourceMappings value = new ResourceMappings();
    AssignedResources assignedResources = new AssignedResources();
    assignedResources.updateAssignedResources(
        Arrays.asList(new NumaResourceAllocation("0", 70000, "0", 4)));
    value.addAssignedResources("numa", assignedResources);
    when(mockContainer.getResourceMappings()).thenReturn(value);
    when(mockContainers.get(Matchers.any())).thenReturn(mockContainer);
    when(mockContext.getContainers()).thenReturn(mockContainers);
    NMStateStoreService mock = mock(NMStateStoreService.class);
    when(mockContext.getNMStateStore()).thenReturn(mock);
    numaResourceAllocator = new NumaResourceAllocator(mockContext);
    numaResourceAllocator.init(conf);
    // Recover the resources
    numaResourceAllocator.recoverNumaResource(
        ContainerId.fromString("container_1481156246874_0001_01_000001"));

    // Request resources based on the availability
    NumaResourceAllocation numaNode = numaResourceAllocator
        .allocateNumaNodes(getContainer(
            ContainerId.fromString("container_1481156246874_0001_01_000005"),
            Resource.newInstance(2048, 1)));
    assertEquals("1", String.join(",", numaNode.getMemNodes()));
    assertEquals("1", String.join(",", numaNode.getCpuNodes()));

    // Request resources more than the available
    numaNode = numaResourceAllocator.allocateNumaNodes(getContainer(
        ContainerId.fromString("container_1481156246874_0001_01_000006"),
        Resource.newInstance(2048, 4)));
    assertNull(numaNode);
  }

  private void setNumaTopologyConfigs() {
    conf.set(YarnConfiguration.NM_NUMA_AWARENESS_NODE_IDS, "0,1");
    conf.set("yarn.nodemanager.numa-awareness.0.memory", "73717");
    conf.set("yarn.nodemanager.numa-awareness.0.cpus", "4");
    conf.set("yarn.nodemanager.numa-awareness.1.memory", "73727");
    conf.set("yarn.nodemanager.numa-awareness.1.cpus", "4");
  }

  private Collection<NumaNodeResource> getExpectedNumaNodesList() {
    Collection<NumaNodeResource> expectedNodesList = new ArrayList<>(2);
    expectedNodesList.add(new NumaNodeResource("0", 73717, 4));
    expectedNodesList.add(new NumaNodeResource("1", 73727, 4));
    return expectedNodesList;
  }

  private Container getContainer(ContainerId containerId, Resource resource) {
    Container mockContainer = mock(Container.class);
    when(mockContainer.getContainerId()).thenReturn(containerId);
    when(mockContainer.getResource()).thenReturn(resource);
    return mockContainer;
  }
}
