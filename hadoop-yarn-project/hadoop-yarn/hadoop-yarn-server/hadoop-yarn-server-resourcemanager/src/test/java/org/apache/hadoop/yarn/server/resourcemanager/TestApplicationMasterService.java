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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;

import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Thread.sleep;

public class TestApplicationMasterService {
  private static final Log LOG = LogFactory.getLog(TestFifoScheduler.class);

  private final int GB = 1024;
  private static YarnConfiguration conf;

  @BeforeClass
  public static void setup() {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
      ResourceScheduler.class);
  }

  @Test(timeout = 3000000)
  public void testRMIdentifierOnContainerAllocation() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);

    // Submit an application
    RMApp app1 = rm.submitApp(2048);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    am1.addRequests(new String[] { "127.0.0.1" }, GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    // kick the scheduler
    nm1.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      sleep(1000);
      alloc1Response = am1.schedule();
    }

    // assert RMIdentifer is set properly in allocated containers
    Container allocatedContainer =
        alloc1Response.getAllocatedContainers().get(0);
    ContainerTokenIdentifier tokenId =
        BuilderUtils.newContainerTokenIdentifier(allocatedContainer
          .getContainerToken());
    Assert.assertEquals(MockRM.getClusterTimeStamp(), tokenId.getRMIdentifier());
    rm.stop();
  }
  
  @Test(timeout=600000)
  public void testInvalidContainerReleaseRequest() throws Exception {
    MockRM rm = new MockRM(conf);
    
    try {
      rm.start();

      // Register node1
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);

      // Submit an application
      RMApp app1 = rm.submitApp(1024);

      // kick the scheduling
      nm1.nodeHeartbeat(true);
      RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
      MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
      am1.registerAppAttempt();
      
      am1.addRequests(new String[] { "127.0.0.1" }, GB, 1, 1);
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
        Assert.fail("Exception was expected!!");
      } catch (InvalidContainerReleaseException e) {
        StringBuilder sb = new StringBuilder("Cannot release container : ");
        sb.append(cId.toString());
        sb.append(" not belonging to this application attempt : ");
        sb.append(attempt2.getAppAttemptId().toString());
        Assert.assertTrue(e.getMessage().contains(sb.toString()));
      }
    } finally {
      if (rm != null) {
        rm.stop();
      }
    }
  }

  @Test(timeout=1200000)
  public void testProgressFilter() throws Exception{
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);

      // Submit an application
    RMApp app1 = rm.submitApp(2048);

    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    AllocateRequestPBImpl allocateRequest = new AllocateRequestPBImpl();
    List<ContainerId> release = new ArrayList<ContainerId>();
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
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
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);
      // Submit an application
      RMApp app1 = rm.submitApp(2048);
      MockAM am1 = MockRM.launchAM(app1, rm, nm1);
      FinishApplicationMasterRequest req =
          FinishApplicationMasterRequest.newInstance(
              FinalApplicationStatus.FAILED, "", "");
      try {
        am1.unregisterAppAttempt(req, false);
        Assert.fail("ApplicationMasterNotRegisteredException should be thrown");
      } catch (ApplicationMasterNotRegisteredException e) {
        Assert.assertNotNull(e);
        Assert.assertNotNull(e.getMessage());
        Assert.assertTrue(e.getMessage().contains(
            "Application Master is trying to unregister before registering for:"
        ));
      } catch (Exception e) {
        Assert.fail("ApplicationMasterNotRegisteredException should be thrown");
      }

      am1.registerAppAttempt();

      am1.unregisterAppAttempt(req, false);
      am1.waitForState(RMAppAttemptState.FINISHING);
    } finally {
      if (rm != null) {
        rm.stop();
      }
    }
  }

  @Test(timeout = 3000000)
  public void testResourceTypes() throws Exception {
    HashMap<YarnConfiguration, EnumSet<SchedulerResourceTypes>> driver =
        new HashMap<YarnConfiguration, EnumSet<SchedulerResourceTypes>>();

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

    driver.put(conf, EnumSet.of(SchedulerResourceTypes.MEMORY));
    driver.put(testCapacityDRConf,
      EnumSet.of(SchedulerResourceTypes.CPU, SchedulerResourceTypes.MEMORY));
    driver.put(testCapacityDefConf, EnumSet.of(SchedulerResourceTypes.MEMORY));
    driver.put(testFairDefConf,
      EnumSet.of(SchedulerResourceTypes.MEMORY, SchedulerResourceTypes.CPU));

    for (Map.Entry<YarnConfiguration, EnumSet<SchedulerResourceTypes>> entry : driver
      .entrySet()) {
      EnumSet<SchedulerResourceTypes> expectedValue = entry.getValue();
      MockRM rm = new MockRM(entry.getKey());
      rm.start();
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);
      RMApp app1 = rm.submitApp(2048);
      nm1.nodeHeartbeat(true);
      RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
      MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
      RegisterApplicationMasterResponse resp = am1.registerAppAttempt();
      EnumSet<SchedulerResourceTypes> types = resp.getSchedulerResourceTypes();
      LOG.info("types = " + types.toString());
      Assert.assertEquals(expectedValue, types);
      rm.stop();
    }
  }
}
