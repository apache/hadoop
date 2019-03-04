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

package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.resource.TestResourceProfiles;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Thread.sleep;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES;
import static org.junit.Assert.fail;

/**
 * Base class for Application Master test classes.
 * Some implementors are for testing CS and FS.
 */
public abstract class ApplicationMasterServiceTestBase {
  private static final Logger LOG = LoggerFactory
      .getLogger(ApplicationMasterServiceTestBase.class);

  static final int GB = 1024;

  static final String CUSTOM_RES = "res_1";
  static final String DEFAULT_HOST = "127.0.0.1";
  static final String DEFAULT_PORT = "1234";

  protected static YarnConfiguration conf;

  protected abstract YarnConfiguration createYarnConfig();

  protected abstract Resource getResourceUsageForQueue(ResourceManager rm,
          String queue);

  protected abstract String getDefaultQueueName();

  Map<String, ResourceInformation> initializeMandatoryResources() {
    Map<String, ResourceInformation> riMap = new HashMap<>();

    ResourceInformation memory = ResourceInformation.newInstance(
        ResourceInformation.MEMORY_MB.getName(),
        ResourceInformation.MEMORY_MB.getUnits(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    ResourceInformation vcores = ResourceInformation.newInstance(
        ResourceInformation.VCORES.getName(),
        ResourceInformation.VCORES.getUnits(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    riMap.put(ResourceInformation.MEMORY_URI, memory);
    riMap.put(ResourceInformation.VCORES_URI, vcores);
    return riMap;
  }

  private void requestResources(MockAM am, long memory, int vCores,
      Map<String, Integer> customResources) throws Exception {
    am.allocate(Collections.singletonList(ResourceRequest.newBuilder()
        .capability(TestUtils.createResource(memory, vCores, customResources))
        .numContainers(1)
        .resourceName("*")
        .build()), null);
  }

  @Before
  public void setup() {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
        ResourceScheduler.class);
  }

  @Test(timeout = 3000000)
  public void testRMIdentifierOnContainerAllocation() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode(DEFAULT_HOST + ":" + DEFAULT_PORT, 6 * GB);

    // Submit an application
    RMApp app1 = rm.submitApp(2048);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    am1.addRequests(new String[] {DEFAULT_HOST}, GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    // kick the scheduler
    nm1.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      sleep(1000);
      alloc1Response = am1.schedule();
    }

    // assert RMIdentifier is set properly in allocated containers
    Container allocatedContainer =
        alloc1Response.getAllocatedContainers().get(0);
    ContainerTokenIdentifier tokenId =
        BuilderUtils.newContainerTokenIdentifier(allocatedContainer
            .getContainerToken());
    Assert.assertEquals(MockRM.getClusterTimeStamp(),
            tokenId.getRMIdentifier());
    rm.stop();
  }

  @Test(timeout = 3000000)
  public void testAllocateResponseIdOverflow() throws Exception {
    MockRM rm = new MockRM(conf);

    try {
      rm.start();

      // Register node1
      MockNM nm1 = rm.registerNode(DEFAULT_HOST + ":" + DEFAULT_PORT, 6 * GB);

      // Submit an application
      RMApp app1 = rm.submitApp(2048);

      // kick off the scheduling
      nm1.nodeHeartbeat(true);
      RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
      MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
      am1.registerAppAttempt();

      // Set the last responseId to be Integer.MAX_VALUE
      Assert.assertTrue(am1.setApplicationLastResponseId(Integer.MAX_VALUE));

      // Both allocate should succeed
      am1.schedule(); // send allocate with responseId = Integer.MAX_VALUE
      Assert.assertEquals(0, am1.getResponseId());

      am1.schedule(); // send allocate with responseId = 0
      Assert.assertEquals(1, am1.getResponseId());

    } finally {
      rm.stop();
    }
  }

  @Test(timeout=600000)
  public void testInvalidContainerReleaseRequest() throws Exception {
    MockRM rm = new MockRM(conf);

    try {
      rm.start();

      // Register node1
      MockNM nm1 = rm.registerNode(DEFAULT_HOST + ":" + DEFAULT_PORT, 6 * GB);

      // Submit an application
      RMApp app1 = rm.submitApp(1024);

      // kick the scheduling
      nm1.nodeHeartbeat(true);
      RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
      MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
      am1.registerAppAttempt();

      am1.addRequests(new String[] {DEFAULT_HOST}, GB, 1, 1);
      AllocateResponse alloc1Response = am1.schedule(); // send the request

      // kick the scheduler
      nm1.nodeHeartbeat(true);
      while (alloc1Response.getAllocatedContainers().size() < 1) {
        LOG.info("Waiting for containers to be created for app 1...");
        sleep(1000);
        alloc1Response = am1.schedule();
      }

      Assert.assertTrue(alloc1Response.getAllocatedContainers().size() > 0);

      RMApp app2 = rm.submitApp(1024);

      nm1.nodeHeartbeat(true);
      RMAppAttempt attempt2 = app2.getCurrentAppAttempt();
      MockAM am2 = rm.sendAMLaunched(attempt2.getAppAttemptId());
      am2.registerAppAttempt();

      // Now trying to release container allocated for app1 -> appAttempt1.
      ContainerId cId = alloc1Response.getAllocatedContainers().get(0).getId();
      am2.addContainerToBeReleased(cId);
      try {
        am2.schedule();
        fail("Exception was expected!!");
      } catch (InvalidContainerReleaseException e) {
        StringBuilder sb = new StringBuilder("Cannot release container : ");
        sb.append(cId.toString());
        sb.append(" not belonging to this application attempt : ");
        sb.append(attempt2.getAppAttemptId().toString());
        Assert.assertTrue(e.getMessage().contains(sb.toString()));
      }
    } finally {
      rm.stop();
    }
  }

  @Test(timeout=1200000)
  public void testProgressFilter() throws Exception{
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode(DEFAULT_HOST + ":" + DEFAULT_PORT, 6 * GB);

    // Submit an application
    RMApp app1 = rm.submitApp(2048);

    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    AllocateRequestPBImpl allocateRequest = new AllocateRequestPBImpl();
    List<ContainerId> release = new ArrayList<>();
    List<ResourceRequest> ask = new ArrayList<>();
    allocateRequest.setReleaseList(release);
    allocateRequest.setAskList(ask);

    allocateRequest.setProgress(Float.POSITIVE_INFINITY);
    am1.allocate(allocateRequest);
    while(attempt1.getProgress()!=1){
      LOG.info("Waiting for allocate event to be handled ...");
      sleep(100);
    }

    allocateRequest.setProgress(Float.NaN);
    am1.allocate(allocateRequest);
    while(attempt1.getProgress()!=0){
      LOG.info("Waiting for allocate event to be handled ...");
      sleep(100);
    }

    allocateRequest.setProgress((float)9);
    am1.allocate(allocateRequest);
    while(attempt1.getProgress()!=1){
      LOG.info("Waiting for allocate event to be handled ...");
      sleep(100);
    }

    allocateRequest.setProgress(Float.NEGATIVE_INFINITY);
    am1.allocate(allocateRequest);
    while(attempt1.getProgress()!=0){
      LOG.info("Waiting for allocate event to be handled ...");
      sleep(100);
    }

    allocateRequest.setProgress((float)0.5);
    am1.allocate(allocateRequest);
    while(attempt1.getProgress()!=0.5){
      LOG.info("Waiting for allocate event to be handled ...");
      sleep(100);
    }

    allocateRequest.setProgress((float)-1);
    am1.allocate(allocateRequest);
    while(attempt1.getProgress()!=0){
      LOG.info("Waiting for allocate event to be handled ...");
      sleep(100);
    }
  }

  @Test(timeout=1200000)
  public void testFinishApplicationMasterBeforeRegistering() throws Exception {
    MockRM rm = new MockRM(conf);

    try {
      rm.start();
      // Register node1
      MockNM nm1 = rm.registerNode(DEFAULT_HOST + ":" + DEFAULT_PORT, 6 * GB);
      // Submit an application
      RMApp app1 = rm.submitApp(2048);
      MockAM am1 = MockRM.launchAM(app1, rm, nm1);
      FinishApplicationMasterRequest req =
          FinishApplicationMasterRequest.newInstance(
              FinalApplicationStatus.FAILED, "", "");
      try {
        am1.unregisterAppAttempt(req, false);
        fail("ApplicationMasterNotRegisteredException should be thrown");
      } catch (ApplicationMasterNotRegisteredException e) {
        Assert.assertNotNull(e);
        Assert.assertNotNull(e.getMessage());
        Assert.assertTrue(e.getMessage().contains(
            "Application Master is trying to unregister before registering for:"
        ));
      } catch (Exception e) {
        fail("ApplicationMasterNotRegisteredException should be thrown");
      }

      am1.registerAppAttempt();

      am1.unregisterAppAttempt(req, false);
      rm.waitForState(am1.getApplicationAttemptId(),
              RMAppAttemptState.FINISHING);
    } finally {
      rm.stop();
    }
  }

  @Test(timeout = 3000000)
  public void testResourceTypes() throws Exception {
    HashMap<YarnConfiguration,
        EnumSet<YarnServiceProtos.SchedulerResourceTypes>> driver =
        new HashMap<>();

    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    csconf.setResourceComparator(DominantResourceCalculator.class);

    YarnConfiguration testCapacityDRConf = new YarnConfiguration(csconf);
    testCapacityDRConf.setClass(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class, ResourceScheduler.class);

    YarnConfiguration testCapacityDefConf = new YarnConfiguration();
    testCapacityDefConf.setClass(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class, ResourceScheduler.class);

    YarnConfiguration testFairDefConf = new YarnConfiguration();
    testFairDefConf.setClass(YarnConfiguration.RM_SCHEDULER,
        FairScheduler.class, ResourceScheduler.class);

    driver.put(conf,
            EnumSet.of(YarnServiceProtos.SchedulerResourceTypes.MEMORY));
    driver.put(testCapacityDRConf,
            EnumSet.of(YarnServiceProtos.SchedulerResourceTypes.CPU,
                    YarnServiceProtos.SchedulerResourceTypes.MEMORY));
    driver.put(testCapacityDefConf,
            EnumSet.of(YarnServiceProtos.SchedulerResourceTypes.MEMORY));
    driver.put(testFairDefConf,
            EnumSet.of(YarnServiceProtos.SchedulerResourceTypes.MEMORY,
                    YarnServiceProtos.SchedulerResourceTypes.CPU));

    for (Map.Entry<YarnConfiguration,
        EnumSet<YarnServiceProtos.SchedulerResourceTypes>> entry :
        driver.entrySet()) {
      EnumSet<YarnServiceProtos.SchedulerResourceTypes> expectedValue =
          entry.getValue();
      MockRM rm = new MockRM(entry.getKey());
      rm.start();
      MockNM nm1 = rm.registerNode(DEFAULT_HOST + ":" + DEFAULT_PORT, 6 * GB);
      RMApp app1 = rm.submitApp(2048);
      //Wait to make sure the attempt has the right state
      //TODO explore a better way than sleeping for a while (YARN-4929)
      Thread.sleep(1000);
      nm1.nodeHeartbeat(true);
      RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
      MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
      RegisterApplicationMasterResponse resp = am1.registerAppAttempt();
      EnumSet<YarnServiceProtos.SchedulerResourceTypes> types =
              resp.getSchedulerResourceTypes();
      LOG.info("types = " + types.toString());
      Assert.assertEquals(expectedValue, types);
      rm.stop();
    }
  }

  @Test(timeout=1200000)
  public void  testAllocateAfterUnregister() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    // Register node1
    MockNM nm1 = rm.registerNode(DEFAULT_HOST + ":" + DEFAULT_PORT, 6 * GB);

    // Submit an application
    RMApp app1 = rm.submitApp(2048);

    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    // unregister app attempt
    FinishApplicationMasterRequest req =
        FinishApplicationMasterRequest.newInstance(
            FinalApplicationStatus.KILLED, "", "");
    am1.unregisterAppAttempt(req, false);
    // request container after unregister
    am1.addRequests(new String[] {DEFAULT_HOST}, GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule();

    nm1.nodeHeartbeat(true);
    rm.drainEvents();
    alloc1Response = am1.schedule();
    Assert.assertEquals(0, alloc1Response.getAllocatedContainers().size());
  }

  @Test(timeout = 300000)
  public void testUpdateTrackingUrl() throws Exception {
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode(DEFAULT_HOST + ":" + DEFAULT_PORT, 6 * GB);

    RMApp app1 = rm.submitApp(2048);

    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    Assert.assertEquals("N/A", rm.getRMContext().getRMApps().get(
        app1.getApplicationId()).getOriginalTrackingUrl());

    AllocateRequestPBImpl allocateRequest = new AllocateRequestPBImpl();
    String newTrackingUrl = "hadoop.apache.org";
    allocateRequest.setTrackingUrl(newTrackingUrl);

    am1.allocate(allocateRequest);
    Assert.assertEquals(newTrackingUrl, rm.getRMContext().getRMApps().get(
        app1.getApplicationId()).getOriginalTrackingUrl());

    // Send it again
    am1.allocate(allocateRequest);
    Assert.assertEquals(newTrackingUrl, rm.getRMContext().getRMApps().get(
        app1.getApplicationId()).getOriginalTrackingUrl());
    rm.stop();
  }

  @Test(timeout = 300000)
  public void testValidateRequestCapacityAgainstMinMaxAllocation()
      throws Exception {
    Map<String, ResourceInformation> riMap =
        initializeMandatoryResources();
    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);

    final YarnConfiguration yarnConf = createYarnConfig();

    // Don't reset resource types since we have already configured resource
    // types
    yarnConf.setBoolean(TestResourceProfiles.TEST_CONF_RESET_RESOURCE_TYPES,
        false);
    yarnConf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, false);

    MockRM rm = new MockRM(yarnConf);
    rm.start();

    MockNM nm1 = rm.registerNode("199.99.99.1:" + DEFAULT_PORT, TestUtils
        .createResource(DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, null));

    RMApp app1 = rm.submitApp(GB, "app", "user", null, getDefaultQueueName());
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

    // Now request resource, memory > allowed
    boolean exception = false;
    try {
      am1.allocate(Collections.singletonList(ResourceRequest.newBuilder()
          .capability(Resource.newInstance(9 * GB, 1))
          .numContainers(1)
          .resourceName("*")
          .build()), null);
    } catch (InvalidResourceRequestException e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    exception = false;
    try {
      // Now request resource, vcores > allowed
      am1.allocate(Collections.singletonList(ResourceRequest.newBuilder()
          .capability(Resource.newInstance(8 * GB, 18))
          .numContainers(1)
          .resourceName("*")
          .build()), null);
    } catch (InvalidResourceRequestException e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    rm.close();
  }

  @Test(timeout = 300000)
  public void testRequestCapacityMinMaxAllocationForResourceTypes()
      throws Exception {
    Map<String, ResourceInformation> riMap = initializeMandatoryResources();
    ResourceInformation res1 = ResourceInformation.newInstance(CUSTOM_RES,
        ResourceInformation.VCORES.getUnits(), 0, 4);
    riMap.put(CUSTOM_RES, res1);

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);

    final YarnConfiguration yarnConf = createYarnConfig();
    // Don't reset resource types since we have already configured resource
    // types
    yarnConf.setBoolean(TestResourceProfiles.TEST_CONF_RESET_RESOURCE_TYPES,
        false);
    yarnConf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, false);

    MockRM rm = new MockRM(yarnConf);
    rm.start();

    MockNM nm1 = rm.registerNode("199.99.99.1:" + DEFAULT_PORT, TestUtils
        .createResource(DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
            ImmutableMap.of(CUSTOM_RES, 4)));

    RMApp app1 = rm.submitApp(GB, "app", "user", null, getDefaultQueueName());
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

    Assert.assertEquals(Resource.newInstance(GB, 1),
        getResourceUsageForQueue(rm, getDefaultQueueName()));

    // Request memory > allowed
    try {
      requestResources(am1, 9 * GB, 1, ImmutableMap.of());
      Assert.fail("Should throw InvalidResourceRequestException");
    } catch (InvalidResourceRequestException ignored) {}

    try {
      // Request vcores > allowed
      requestResources(am1, GB, 18, ImmutableMap.of());
      Assert.fail("Should throw InvalidResourceRequestException");
    } catch (InvalidResourceRequestException ignored) {}

    try {
      // Request custom resource 'res_1' > allowed
      requestResources(am1, GB, 2, ImmutableMap.of(CUSTOM_RES, 100));
      Assert.fail("Should throw InvalidResourceRequestException");
    } catch (InvalidResourceRequestException ignored) {}

    rm.close();
  }
}
