/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu.GpuResourceAllocator.GpuAllocation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDevice;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.util.resource.TestResourceUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;

/**
 * Unit tests for GpuResourceAllocator.
 */
public class TestGpuResourceAllocator {
  private static final int WAIT_PERIOD_FOR_RESOURCE = 100;

  private static class ContainerMatcher extends ArgumentMatcher<Container> {

    private Container container;

    ContainerMatcher(Container container) {
      this.container = container;
    }

    @Override
    public boolean matches(Object o) {
      if (!(o instanceof Container)) {
        return false;
      }

      Container other = (Container) o;

      long expectedId = container.getContainerId().getContainerId();
      long otherId = other.getContainerId().getContainerId();
      return expectedId == otherId;
    }
  }

  @Captor
  private ArgumentCaptor<List<Serializable>> gpuCaptor;

  @Mock
  private NMContext nmContext;

  @Mock
  private NMStateStoreService nmStateStore;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private GpuResourceAllocator testSubject;

  @Before
  public void setup() {
    TestResourceUtils.addNewTypesToResources(ResourceInformation.GPU_URI);
    MockitoAnnotations.initMocks(this);
    testSubject = createTestSubject(WAIT_PERIOD_FOR_RESOURCE);
  }

  private GpuResourceAllocator createTestSubject(int waitPeriodForResource) {
    when(nmContext.getNMStateStore()).thenReturn(nmStateStore);
    when(nmContext.getContainers()).thenReturn(new ConcurrentHashMap<>());
    return new GpuResourceAllocator(nmContext, waitPeriodForResource);
  }

  private Resource createGpuResourceRequest(int gpus) {
    Resource res = Resource.newInstance(1024, 1);

    if (gpus > 0) {
      res.setResourceValue(ResourceInformation.GPU_URI, gpus);
    }
    return res;
  }

  private List<Container> createMockContainers(int gpus,
      int numberOfContainers) {
    final long id = 111L;

    List<Container> containers = Lists.newArrayList();
    for (int i = 0; i < numberOfContainers; i++) {
      containers.add(createMockContainer(gpus, id + i));
    }
    return containers;
  }

  private Container createMockContainer(int gpus, long id) {
    Resource res = createGpuResourceRequest(gpus);
    ContainerId containerId = mock(ContainerId.class);
    when(containerId.getContainerId()).thenReturn(id);

    Container container = mock(Container.class);
    when(container.getResource()).thenReturn(res);
    when(container.getContainerId()).thenReturn(containerId);
    when(container.getContainerState()).thenReturn(ContainerState.RUNNING);
    nmContext.getContainers().put(containerId, container);

    return container;
  }

  private void createAndAddGpus(int numberOfGpus) {
    for (int i = 0; i < numberOfGpus; i++) {
      testSubject.addGpu(new GpuDevice(1, i));
    }

    assertEquals(0, testSubject.getDeviceAllocationMapping().size());
    assertEquals(0, testSubject.getAssignedGpus().size());
    assertEquals(numberOfGpus, testSubject.getAllowedGpus().size());
    assertEquals(numberOfGpus, testSubject.getAvailableGpus());
  }

  private void addGpus(GpuDevice... gpus) {
    for (GpuDevice gpu : gpus) {
      testSubject.addGpu(gpu);
    }
    assertEquals(0, testSubject.getDeviceAllocationMapping().size());
    assertEquals(0, testSubject.getAssignedGpus().size());
    assertEquals(gpus.length, testSubject.getAllowedGpus().size());
    assertEquals(gpus.length, testSubject.getAvailableGpus());
  }

  private void addGpusAndDontVerify(GpuDevice... gpus) {
    for (GpuDevice gpu : gpus) {
      testSubject.addGpu(gpu);
    }
  }

  private void setupContainerAsReleasingGpus(Container... releasingContainers) {
    ContainerState[] finalStates = new ContainerState[] {
        ContainerState.KILLING, ContainerState.DONE,
        ContainerState.LOCALIZATION_FAILED,
        ContainerState.CONTAINER_RESOURCES_CLEANINGUP,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerState.EXITED_WITH_FAILURE,
        ContainerState.EXITED_WITH_SUCCESS
    };

    final Random random = new Random();
    for (Container container : releasingContainers) {
      ContainerState state = finalStates[random.nextInt(finalStates.length)];
      when(container.getContainerState()).thenReturn(state);
      when(container.isContainerInFinalStates()).thenReturn(true);
    }
  }

  private void assertAllocatedGpu(GpuDevice expectedGpu, Container container,
      GpuAllocation allocation) throws IOException {
    assertEquals(1, allocation.getAllowedGPUs().size());
    assertEquals(0, allocation.getDeniedGPUs().size());

    Set<GpuDevice> allowedGPUs = allocation.getAllowedGPUs();

    GpuDevice allocatedGpu = allowedGPUs.iterator().next();
    assertEquals(expectedGpu, allocatedGpu);
    assertAssignmentInStateStore(expectedGpu, container);
  }

  private void assertAllocatedGpus(int gpus, int deniedGpus,
      Container container,
      GpuAllocation allocation) throws IOException {
    assertEquals(gpus, allocation.getAllowedGPUs().size());
    assertEquals(deniedGpus, allocation.getDeniedGPUs().size());
    assertAssignmentInStateStore(gpus, container);
  }

  private void assertNoAllocation(GpuAllocation allocation) {
    assertEquals(1, allocation.getDeniedGPUs().size());
    assertEquals(0, allocation.getAllowedGPUs().size());
    verifyZeroInteractions(nmStateStore);
  }

  private void assertAssignmentInStateStore(GpuDevice expectedGpu,
      Container container) throws IOException {
    verify(nmStateStore).storeAssignedResources(
        argThat(new ContainerMatcher(container)), eq(GPU_URI),
        gpuCaptor.capture());

    List<Serializable> gpuList = gpuCaptor.getValue();
    assertEquals(1, gpuList.size());
    assertEquals(expectedGpu, gpuList.get(0));
  }

  private void assertAssignmentInStateStore(int gpus,
      Container container) throws IOException {
    verify(nmStateStore).storeAssignedResources(
        argThat(new ContainerMatcher(container)), eq(GPU_URI),
        gpuCaptor.capture());

    List<Serializable> gpuList = gpuCaptor.getValue();
    assertEquals(gpus, gpuList.size());
  }

  private static Set<GpuAllocation> findDuplicates(
      List<GpuAllocation> allocations) {
    final Set<GpuAllocation> result = new HashSet<>();
    final Set<GpuAllocation> tmpSet = new HashSet<>();

    for (GpuAllocation allocation : allocations) {
      if (!tmpSet.add(allocation)) {
        result.add(allocation);
      }
    }
    return result;
  }

  @Test
  public void testNewGpuAllocatorHasEmptyCollectionOfDevices() {
    assertEquals(0, testSubject.getDeviceAllocationMapping().size());
    assertEquals(0, testSubject.getAssignedGpus().size());
    assertEquals(0, testSubject.getAllowedGpus().size());
    assertEquals(0, testSubject.getAvailableGpus());
  }

  @Test
  public void testAddOneDevice() {
    addGpus(new GpuDevice(1, 1));
    assertEquals(0, testSubject.getDeviceAllocationMapping().size());
    assertEquals(0, testSubject.getAssignedGpus().size());
  }

  @Test
  public void testAddMoreDevices() {
    addGpus(new GpuDevice(1, 1), new GpuDevice(1, 2), new GpuDevice(1, 3));
    assertEquals(0, testSubject.getDeviceAllocationMapping().size());
    assertEquals(0, testSubject.getAssignedGpus().size());
  }

  @Test
  public void testAddMoreDevicesWithSameData() {
    addGpusAndDontVerify(new GpuDevice(1, 1), new GpuDevice(1, 1));
    assertEquals(0, testSubject.getDeviceAllocationMapping().size());
    assertEquals(0, testSubject.getAssignedGpus().size());
    assertEquals(1, testSubject.getAllowedGpus().size());
    assertEquals(1, testSubject.getAvailableGpus());
  }

  @Test
  public void testRequestZeroGpu() throws ResourceHandlerException {
    addGpus(new GpuDevice(1, 1));

    Container container = createMockContainer(0, 5L);
    GpuAllocation allocation =
        testSubject.assignGpus(container);

    assertNoAllocation(allocation);
  }

  @Test
  public void testRequestOneGpu() throws ResourceHandlerException, IOException {
    GpuDevice gpu = new GpuDevice(1, 1);
    addGpus(gpu);

    Container container = createMockContainer(1, 5L);
    GpuAllocation allocation =
        testSubject.assignGpus(container);

    assertEquals(1, testSubject.getDeviceAllocationMapping().size());
    assertEquals(1, testSubject.getAssignedGpus().size());
    assertEquals(1, testSubject.getAllowedGpus().size());
    assertEquals(0, testSubject.getAvailableGpus());

    assertAllocatedGpu(gpu, container, allocation);
  }

  @Test
  public void testRequestMoreThanAvailableGpu()
      throws ResourceHandlerException {
    addGpus(new GpuDevice(1, 1));
    Container container = createMockContainer(2, 5L);

    exception.expect(ResourceHandlerException.class);
    exception.expectMessage("Failed to find enough GPUs");
    testSubject.assignGpus(container);
  }

  @Test
  public void testRequestMoreThanAvailableGpuAndOneContainerIsReleasingGpus()
      throws ResourceHandlerException, IOException {
    addGpus(new GpuDevice(1, 1), new GpuDevice(1, 2), new GpuDevice(1, 3));
    Container container = createMockContainer(2, 5L);
    GpuAllocation allocation = testSubject.assignGpus(container);
    assertAllocatedGpus(2, 1, container, allocation);

    assertEquals(2, testSubject.getDeviceAllocationMapping().size());
    assertEquals(2, testSubject.getAssignedGpus().size());
    assertEquals(3, testSubject.getAllowedGpus().size());
    assertEquals(1, testSubject.getAvailableGpus());

    setupContainerAsReleasingGpus(container);
    Container container2 = createMockContainer(2, 6L);

    exception.expect(ResourceHandlerException.class);
    exception.expectMessage("as some other containers might not " +
        "releasing GPUs");
    GpuAllocation allocation2 = testSubject.assignGpus(container2);
    assertAllocatedGpus(2, 1, container, allocation2);
  }

  @Test
  public void testThreeContainersJustTwoOfThemSatisfied()
      throws ResourceHandlerException, IOException {
    addGpus(new GpuDevice(1, 1), new GpuDevice(1, 2),
            new GpuDevice(1, 3), new GpuDevice(1, 4),
            new GpuDevice(1, 5), new GpuDevice(1, 6));
    Container container = createMockContainer(3, 5L);
    Container container2 = createMockContainer(2, 6L);
    Container container3 = createMockContainer(2, 6L);

    GpuAllocation allocation = testSubject.assignGpus(container);
    assertAllocatedGpus(3, 3, container, allocation);
    assertEquals(3, testSubject.getDeviceAllocationMapping().size());
    assertEquals(3, testSubject.getAssignedGpus().size());
    assertEquals(6, testSubject.getAllowedGpus().size());
    assertEquals(3, testSubject.getAvailableGpus());

    GpuAllocation allocation2 = testSubject.assignGpus(container2);
    assertAllocatedGpus(2, 4, container2, allocation2);
    assertEquals(5, testSubject.getDeviceAllocationMapping().size());
    assertEquals(5, testSubject.getAssignedGpus().size());
    assertEquals(6, testSubject.getAllowedGpus().size());
    assertEquals(1, testSubject.getAvailableGpus());

    exception.expect(ResourceHandlerException.class);
    exception.expectMessage("Failed to find enough GPUs");
    testSubject.assignGpus(container3);
  }

  @Test
  public void testReleaseAndAssignGpus()
      throws ResourceHandlerException, IOException {
    addGpus(new GpuDevice(1, 1), new GpuDevice(1, 2), new GpuDevice(1, 3));
    Container container = createMockContainer(2, 5L);
    GpuAllocation allocation = testSubject.assignGpus(container);
    assertAllocatedGpus(2, 1, container, allocation);

    assertEquals(2, testSubject.getDeviceAllocationMapping().size());
    assertEquals(2, testSubject.getAssignedGpus().size());
    assertEquals(3, testSubject.getAllowedGpus().size());
    assertEquals(1, testSubject.getAvailableGpus());

    setupContainerAsReleasingGpus(container);
    Container container2 = createMockContainer(2, 6L);
    try {
      testSubject.assignGpus(container2);
    } catch (ResourceHandlerException e) {
      //intended as we have not enough GPUs available
    }

    assertEquals(2, testSubject.getDeviceAllocationMapping().size());
    assertEquals(2, testSubject.getAssignedGpus().size());
    assertEquals(3, testSubject.getAllowedGpus().size());
    assertEquals(1, testSubject.getAvailableGpus());

    testSubject.unassignGpus(container.getContainerId());
    GpuAllocation allocation2 = testSubject.assignGpus(container2);
    assertAllocatedGpus(2, 1, container, allocation2);
  }

  @Test
  public void testCreateLotsOfContainersVerifyGpuAssignmentsAreCorrect()
      throws ResourceHandlerException, IOException {
    createAndAddGpus(100);

    List<Container> containers = createMockContainers(3, 33);
    List<GpuAllocation> allocations = Lists.newArrayList();
    for (Container container : containers) {
      GpuAllocation allocation = testSubject.assignGpus(container);
      allocations.add(allocation);
      assertAllocatedGpus(3, 97, container, allocation);
    }

    assertEquals(99, testSubject.getDeviceAllocationMapping().size());
    assertEquals(99, testSubject.getAssignedGpus().size());
    assertEquals(100, testSubject.getAllowedGpus().size());
    assertEquals(1, testSubject.getAvailableGpus());

    Set<GpuAllocation> duplicateAllocations = findDuplicates(allocations);
    assertEquals(0, duplicateAllocations.size());
  }

  @Test
  public void testGpuGetsUnassignedWhenStateStoreThrowsException()
      throws ResourceHandlerException, IOException {
    doThrow(new IOException("Failed to save container mappings " +
        "to NM state store!"))
        .when(nmStateStore).storeAssignedResources(any(Container.class),
        anyString(), anyList());

    createAndAddGpus(1);

    exception.expect(ResourceHandlerException.class);
    exception.expectMessage("Failed to save container mappings " +
        "to NM state store");
    Container container = createMockContainer(1, 5L);
    testSubject.assignGpus(container);
  }
}