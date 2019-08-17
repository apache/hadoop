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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings.AssignedResources;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for NumaResourceHandlerImpl.
 *
 */
public class TestNumaResourceHandlerImpl {

  private YarnConfiguration conf;
  private NumaResourceHandlerImpl numaResourceHandler;
  private Container mockContainer;

  @Before
  public void setUp() throws IOException, ResourceHandlerException {
    conf = new YarnConfiguration();
    setNumaTopologyConfigs();
    Context mockContext = createAndGetMockContext();
    NMStateStoreService mock = mock(NMStateStoreService.class);
    when(mockContext.getNMStateStore()).thenReturn(mock);
    numaResourceHandler = new NumaResourceHandlerImpl(conf, mockContext);
    numaResourceHandler.bootstrap(conf);
    mockContainer = mock(Container.class);
  }

  @Test
  public void testAllocateNumaMemoryResource() throws ResourceHandlerException {
    // allocates node 0 for memory and cpu
    testAllocateNumaResource("container_1481156246874_0001_01_000001",
        Resource.newInstance(2048, 2), "0", "0");
    // allocates node 1 for memory and cpu since allocator uses round
    // robin assignment
    testAllocateNumaResource("container_1481156246874_0001_01_000002",
        Resource.newInstance(60000, 2), "1", "1");
    // allocates node 0,1 for memory since there is no sufficient memory in any
    // one node
    testAllocateNumaResource("container_1481156246874_0001_01_000003",
        Resource.newInstance(80000, 2), "0,1", "0");
    // returns null since there are no sufficient resources available for the
    // request
    when(mockContainer.getContainerId()).thenReturn(
        ContainerId.fromString("container_1481156246874_0001_01_000004"));
    when(mockContainer.getResource())
        .thenReturn(Resource.newInstance(80000, 2));
    assertNull(numaResourceHandler.preStart(mockContainer));
    // allocates node 1 for memory and cpu
    testAllocateNumaResource("container_1481156246874_0001_01_000005",
        Resource.newInstance(1024, 2), "1", "1");
  }

  @Test
  public void testAllocateNumaCpusResource() throws ResourceHandlerException {
    // allocates node 0 for memory and cpu
    testAllocateNumaResource("container_1481156246874_0001_01_000001",
        Resource.newInstance(2048, 2), "0", "0");
    // allocates node 1 for memory and cpu since allocator uses round
    // robin assignment
    testAllocateNumaResource("container_1481156246874_0001_01_000002",
        Resource.newInstance(2048, 2), "1", "1");
    // allocates node 0,1 for cpus since there is are no sufficient cpus
    // available in any one node
    testAllocateNumaResource("container_1481156246874_0001_01_000003",
        Resource.newInstance(2048, 3), "0", "0,1");
    // returns null since there are no sufficient resources available for the
    // request
    when(mockContainer.getContainerId()).thenReturn(
        ContainerId.fromString("container_1481156246874_0001_01_000004"));
    when(mockContainer.getResource()).thenReturn(Resource.newInstance(2048, 2));
    assertNull(numaResourceHandler.preStart(mockContainer));
    // allocates node 1 for memory and cpu
    testAllocateNumaResource("container_1481156246874_0001_01_000005",
        Resource.newInstance(2048, 1), "1", "1");
  }

  @Test
  public void testReacquireContainer() throws Exception {
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<ContainerId, Container> mockContainers = mock(
        ConcurrentHashMap.class);
    Context mockContext = mock(Context.class);
    NMStateStoreService mock = mock(NMStateStoreService.class);
    when(mockContext.getNMStateStore()).thenReturn(mock);
    ResourceMappings resourceMappings = new ResourceMappings();
    AssignedResources assignedRscs = new AssignedResources();
    NumaResourceAllocation numaResourceAllocation = new NumaResourceAllocation(
        "0", 70000, "0", 4);
    assignedRscs.updateAssignedResources(Arrays.asList(numaResourceAllocation));
    resourceMappings.addAssignedResources("numa", assignedRscs);
    when(mockContainer.getResourceMappings()).thenReturn(resourceMappings);
    when(mockContainers.get(any())).thenReturn(mockContainer);
    when(mockContext.getContainers()).thenReturn(mockContainers);
    numaResourceHandler = new NumaResourceHandlerImpl(conf, mockContext);
    numaResourceHandler.bootstrap(conf);
    // recovered numa resources should be added to the used resources and
    // remaining will be available for further allocation.
    numaResourceHandler.reacquireContainer(
        ContainerId.fromString("container_1481156246874_0001_01_000001"));

    testAllocateNumaResource("container_1481156246874_0001_01_000005",
        Resource.newInstance(2048, 1), "1", "1");
    when(mockContainer.getContainerId()).thenReturn(
        ContainerId.fromString("container_1481156246874_0001_01_000005"));
    when(mockContainer.getResource()).thenReturn(Resource.newInstance(2048, 4));
    List<PrivilegedOperation> preStart = numaResourceHandler
        .preStart(mockContainer);
    assertNull(preStart);
  }

  private void setNumaTopologyConfigs() {
    conf.set(YarnConfiguration.NM_NUMA_AWARENESS_NODE_IDS, "0,1");
    conf.set("yarn.nodemanager.numa-awareness.0.memory", "73717");
    conf.set("yarn.nodemanager.numa-awareness.0.cpus", "4");
    conf.set("yarn.nodemanager.numa-awareness.1.memory", "73727");
    conf.set("yarn.nodemanager.numa-awareness.1.cpus", "4");
  }

  private Context createAndGetMockContext() {
    Context mockContext = mock(Context.class);
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<ContainerId, Container> mockContainers = mock(
        ConcurrentHashMap.class);
    mockContainer = mock(Container.class);
    when(mockContainer.getResourceMappings())
        .thenReturn(new ResourceMappings());
    when(mockContainers.get(any())).thenReturn(mockContainer);
    when(mockContext.getContainers()).thenReturn(mockContainers);
    return mockContext;
  }

  private void testAllocateNumaResource(String containerId, Resource resource,
      String memNodes, String cpuNodes) throws ResourceHandlerException {
    when(mockContainer.getContainerId())
        .thenReturn(ContainerId.fromString(containerId));
    when(mockContainer.getResource()).thenReturn(resource);
    List<PrivilegedOperation> preStart = numaResourceHandler
        .preStart(mockContainer);
    List<String> arguments = preStart.get(0).getArguments();
    assertEquals(arguments, Arrays.asList("/usr/bin/numactl",
        "--interleave=" + memNodes, "--cpunodebind=" + cpuNodes));
  }
}
