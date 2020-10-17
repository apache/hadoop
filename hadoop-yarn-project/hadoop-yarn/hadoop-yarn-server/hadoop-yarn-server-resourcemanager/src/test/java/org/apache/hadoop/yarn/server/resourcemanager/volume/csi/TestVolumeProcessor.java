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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.VolumeState;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.processor.VolumeAMSProcessor;
import org.apache.hadoop.yarn.server.volume.csi.CsiConstants;
import org.apache.hadoop.yarn.server.volume.csi.VolumeId;
import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;
import org.apache.hadoop.yarn.server.volume.csi.exception.VolumeException;
import org.apache.hadoop.yarn.server.volume.csi.exception.VolumeProvisioningException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

/**
 * Test cases for volume processor.
 */
public class TestVolumeProcessor {

  private static final int GB = 1024;
  private YarnConfiguration conf;
  private RMNodeLabelsManager mgr;
  private MockRM rm;
  private MockNM[] mockNMS;
  private RMNode[] rmNodes;
  private static final int NUM_OF_NMS = 4;
  // resource-types.xml will be created under target/test-classes/ dir,
  // it must be deleted after the test is done, to avoid it from reading
  // by other UT classes.
  private File resourceTypesFile = null;

  private static final String VOLUME_RESOURCE_NAME = "yarn.io/csi-volume";

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    resourceTypesFile = new File(conf.getClassLoader()
        .getResource(".").getPath(), "resource-types.xml");
    writeTmpResourceTypesFile(resourceTypesFile);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    conf.set("yarn.scheduler.capacity.resource-calculator",
        "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator");
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".default.ordering-policy",
        "fair");
    // this is required to enable volume processor
    conf.set(YarnConfiguration.RM_APPLICATION_MASTER_SERVICE_PROCESSORS,
        VolumeAMSProcessor.class.getName());
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    rm = new MockRM(conf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    mockNMS = new MockNM[NUM_OF_NMS];
    rmNodes = new RMNode[NUM_OF_NMS];
    for (int i = 0; i < 4; i++) {
      mockNMS[i] = rm.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm.getRMContext().getRMNodes().get(mockNMS[i].getNodeId());
    }
  }

  @After
  public void tearDown() {
    if (resourceTypesFile != null && resourceTypesFile.exists()) {
      resourceTypesFile.delete();
    }
  }

  private void writeTmpResourceTypesFile(File tmpFile) throws IOException {
    Configuration yarnConf = new YarnConfiguration();
    yarnConf.set(YarnConfiguration.RESOURCE_TYPES, VOLUME_RESOURCE_NAME);
    yarnConf.set("yarn.resource-types."
        + VOLUME_RESOURCE_NAME + ".units", "Mi");
    yarnConf.set("yarn.resource-types."
            + VOLUME_RESOURCE_NAME + ".tags",
        CsiConstants.CSI_VOLUME_RESOURCE_TAG);
    try (FileWriter fw = new FileWriter(tmpFile)) {
      yarnConf.writeXml(fw);
    }
  }

  @Test (timeout = 10000L)
  public void testVolumeProvisioning() throws Exception {
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, mockNMS[0]);
    Resource resource = Resource.newInstance(1024, 1);
    ResourceInformation volumeResource = ResourceInformation
        .newInstance(VOLUME_RESOURCE_NAME, "Mi", 1024,
            ResourceTypes.COUNTABLE, 0, Long.MAX_VALUE,
            ImmutableSet.of(CsiConstants.CSI_VOLUME_RESOURCE_TAG),
            ImmutableMap.of(
                CsiConstants.CSI_VOLUME_ID, "test-vol-000001",
                CsiConstants.CSI_DRIVER_NAME, "hostpath",
                CsiConstants.CSI_VOLUME_MOUNT, "/mnt/data"
            )
        );
    resource.setResourceInformation(VOLUME_RESOURCE_NAME, volumeResource);
    SchedulingRequest sc = SchedulingRequest
        .newBuilder().allocationRequestId(0L)
        .resourceSizing(ResourceSizing.newInstance(1, resource))
        .build();
    AllocateRequest ar = AllocateRequest.newBuilder()
        .schedulingRequests(Arrays.asList(sc))
        .build();

    // inject adaptor client for testing
    CsiAdaptorProtocol mockedClient = Mockito
        .mock(CsiAdaptorProtocol.class);
    rm.getRMContext().getVolumeManager()
        .registerCsiDriverAdaptor("hostpath", mockedClient);

    // simulate validation succeed
    doReturn(ValidateVolumeCapabilitiesResponse.newInstance(true, ""))
        .when(mockedClient)
        .validateVolumeCapacity(any(ValidateVolumeCapabilitiesRequest.class));

    am1.allocate(ar);
    VolumeStates volumeStates =
        rm.getRMContext().getVolumeManager().getVolumeStates();
    Assert.assertNotNull(volumeStates);
    VolumeState volumeState = VolumeState.NEW;
    while (volumeState != VolumeState.NODE_READY) {
      Volume volume = volumeStates
          .getVolume(new VolumeId("test-vol-000001"));
      if (volume != null) {
        volumeState = volume.getVolumeState();
      }
      am1.doHeartbeat();
      mockNMS[0].nodeHeartbeat(true);
      Thread.sleep(500);
    }
    rm.stop();
  }

  @Test (timeout = 30000L)
  public void testInvalidRequest() throws Exception {
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, mockNMS[0]);
    Resource resource = Resource.newInstance(1024, 1);
    ResourceInformation volumeResource = ResourceInformation
        .newInstance(VOLUME_RESOURCE_NAME, "Mi", 1024,
            ResourceTypes.COUNTABLE, 0, Long.MAX_VALUE,
            ImmutableSet.of(CsiConstants.CSI_VOLUME_RESOURCE_TAG),
            ImmutableMap.of(
                // volume ID is missing...
                CsiConstants.CSI_VOLUME_NAME, "test-vol-000001",
                CsiConstants.CSI_DRIVER_NAME, "hostpath",
                CsiConstants.CSI_VOLUME_MOUNT, "/mnt/data"
            )
        );
    resource.setResourceInformation(VOLUME_RESOURCE_NAME, volumeResource);
    SchedulingRequest sc = SchedulingRequest
        .newBuilder().allocationRequestId(0L)
        .resourceSizing(ResourceSizing.newInstance(1, resource))
        .build();
    AllocateRequest ar = AllocateRequest.newBuilder()
        .schedulingRequests(Arrays.asList(sc))
        .build();

    try {
      am1.allocate(ar);
      Assert.fail("allocate should fail because invalid request received");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof InvalidVolumeException);
    }
    rm.stop();
  }

  @Test (timeout = 30000L)
  public void testProvisioningFailures() throws Exception {
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, mockNMS[0]);

    CsiAdaptorProtocol mockedClient = Mockito
        .mock(CsiAdaptorProtocol.class);
    // inject adaptor client
    rm.getRMContext().getVolumeManager()
        .registerCsiDriverAdaptor("hostpath", mockedClient);
    doThrow(new VolumeException("failed"))
        .when(mockedClient)
        .validateVolumeCapacity(any(ValidateVolumeCapabilitiesRequest.class));

    Resource resource = Resource.newInstance(1024, 1);
    ResourceInformation volumeResource = ResourceInformation
        .newInstance(VOLUME_RESOURCE_NAME, "Mi", 1024,
            ResourceTypes.COUNTABLE, 0, Long.MAX_VALUE,
            ImmutableSet.of(CsiConstants.CSI_VOLUME_RESOURCE_TAG),
            ImmutableMap.of(
                CsiConstants.CSI_VOLUME_ID, "test-vol-000001",
                CsiConstants.CSI_DRIVER_NAME, "hostpath",
                CsiConstants.CSI_VOLUME_MOUNT, "/mnt/data"
            )
        );
    resource.setResourceInformation(VOLUME_RESOURCE_NAME, volumeResource);
    SchedulingRequest sc = SchedulingRequest
        .newBuilder().allocationRequestId(0L)
        .resourceSizing(ResourceSizing.newInstance(1, resource))
        .build();
    AllocateRequest ar = AllocateRequest.newBuilder()
        .schedulingRequests(Arrays.asList(sc))
        .build();

    try {
      am1.allocate(ar);
      Assert.fail("allocate should fail");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof VolumeProvisioningException);
    }
    rm.stop();
  }

  @Test (timeout = 10000L)
  public void testVolumeResourceAllocate() throws Exception {
    RMApp app1 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .build());
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, mockNMS[0]);
    Resource resource = Resource.newInstance(1024, 1);
    ResourceInformation volumeResource = ResourceInformation
        .newInstance(VOLUME_RESOURCE_NAME, "Mi", 1024,
            ResourceTypes.COUNTABLE, 0, Long.MAX_VALUE,
            ImmutableSet.of(CsiConstants.CSI_VOLUME_RESOURCE_TAG),
            ImmutableMap.of(
                CsiConstants.CSI_VOLUME_ID, "test-vol-000001",
                CsiConstants.CSI_DRIVER_NAME, "hostpath",
                CsiConstants.CSI_VOLUME_MOUNT, "/mnt/data"
            )
        );
    resource.setResourceInformation(VOLUME_RESOURCE_NAME, volumeResource);
    SchedulingRequest sc = SchedulingRequest
        .newBuilder().allocationRequestId(0L)
        .resourceSizing(ResourceSizing.newInstance(1, resource))
        .build();

    // inject adaptor client for testing
    CsiAdaptorProtocol mockedClient = Mockito
        .mock(CsiAdaptorProtocol.class);
    rm.getRMContext().getVolumeManager()
        .registerCsiDriverAdaptor("hostpath", mockedClient);

    // simulate validation succeed
    doReturn(ValidateVolumeCapabilitiesResponse.newInstance(true, ""))
        .when(mockedClient)
        .validateVolumeCapacity(any(ValidateVolumeCapabilitiesRequest.class));

    am1.addSchedulingRequest(ImmutableList.of(sc));
    List<Container> allocated = new ArrayList<>();
    while (allocated.size() != 1) {
      AllocateResponse response = am1.schedule();
      mockNMS[0].nodeHeartbeat(true);
      allocated.addAll(response.getAllocatedContainers());
      Thread.sleep(500);
    }

    Assert.assertEquals(1, allocated.size());
    Container alloc = allocated.get(0);
    assertThat(alloc.getResource().getMemorySize()).isEqualTo(1024);
    assertThat(alloc.getResource().getVirtualCores()).isEqualTo(1);
    ResourceInformation allocatedVolume =
        alloc.getResource().getResourceInformation(VOLUME_RESOURCE_NAME);
    Assert.assertNotNull(allocatedVolume);
    assertThat(allocatedVolume.getValue()).isEqualTo(1024);
    assertThat(allocatedVolume.getUnits()).isEqualTo("Mi");
    rm.stop();
  }
}