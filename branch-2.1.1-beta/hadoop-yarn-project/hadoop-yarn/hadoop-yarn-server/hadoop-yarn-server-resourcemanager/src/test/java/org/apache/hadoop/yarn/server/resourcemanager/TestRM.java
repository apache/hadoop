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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestRM {

  private static final Log LOG = LogFactory.getLog(TestRM.class);

  @Test
  public void testGetNewAppId() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    
    GetNewApplicationResponse resp = rm.getNewAppId();
    assert (resp.getApplicationId().getId() != 0);    
    assert (resp.getMaximumResourceCapability().getMemory() > 0);    
    rm.stop();
  }
  
  @Test
  public void testAppWithNoContainers() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    
    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    am.unregisterAppAttempt();
    nm1.nodeHeartbeat(attempt.getAppAttemptId(), 1, ContainerState.COMPLETE);
    am.waitForState(RMAppAttemptState.FINISHED);
    rm.stop();
  }

  @Test
  public void testAppOnMultiNode() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:5678", 10240);
    
    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    
    //request for containers
    int request = 13;
    am.allocate("h1" , 1000, request, new ArrayList<ContainerId>());
    
    //kick the scheduler
    nm1.nodeHeartbeat(true);
    List<Container> conts = am.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    int contReceived = conts.size();
    while (contReceived < 3) {//only 3 containers are available on node1
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      contReceived = conts.size();
      LOG.info("Got " + contReceived + " containers. Waiting to get " + 3);
      Thread.sleep(2000);
    }
    Assert.assertEquals(3, conts.size());

    //send node2 heartbeat
    nm2.nodeHeartbeat(true);
    conts = am.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    contReceived = conts.size();
    while (contReceived < 10) {
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      contReceived = conts.size();
      LOG.info("Got " + contReceived + " containers. Waiting to get " + 10);
      Thread.sleep(2000);
    }
    Assert.assertEquals(10, conts.size());

    am.unregisterAppAttempt();
    nm1.nodeHeartbeat(attempt.getAppAttemptId(), 1, ContainerState.COMPLETE);
    am.waitForState(RMAppAttemptState.FINISHED);

    rm.stop();
  }
  
  @Test
  public void testNMToken() throws Exception {
    MockRM rm = new MockRM();
    try {
      rm.start();
      MockNM nm1 = rm.registerNode("h1:1234", 10000);
      
      NMTokenSecretManagerInRM nmTokenSecretManager =
          rm.getRMContext().getNMTokenSecretManager();
      
      // submitting new application
      RMApp app = rm.submitApp(1000);
      
      // start scheduling.
      nm1.nodeHeartbeat(true);
      
      // Starting application attempt and launching
      // It should get registered with NMTokenSecretManager.
      RMAppAttempt attempt = app.getCurrentAppAttempt();

      MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());

      Assert.assertTrue(nmTokenSecretManager
          .isApplicationAttemptRegistered(attempt.getAppAttemptId()));
      
      // This will register application master.
      am.registerAppAttempt();
      
      ArrayList<Container> containersReceivedForNM1 =
          new ArrayList<Container>();
      List<ContainerId> releaseContainerList =
          new ArrayList<ContainerId>();
      HashMap<String, Token> nmTokens = new HashMap<String, Token>();

      // initially requesting 2 containers.
      AllocateResponse response =
          am.allocate("h1", 1000, 2, releaseContainerList);
      nm1.nodeHeartbeat(true);
      Assert.assertEquals(0, response.getAllocatedContainers().size());
      allocateContainersAndValidateNMTokens(am, containersReceivedForNM1, 2,
          nmTokens);
      Assert.assertEquals(1, nmTokens.size());

      
      // requesting 2 more containers.
      response = am.allocate("h1", 1000, 2, releaseContainerList);
      nm1.nodeHeartbeat(true);
      Assert.assertEquals(0, response.getAllocatedContainers().size());
      allocateContainersAndValidateNMTokens(am, containersReceivedForNM1, 4,
          nmTokens);
      Assert.assertEquals(1, nmTokens.size());
      
      
      // We will be simulating NM restart so restarting newly added h2:1234
      // NM 2 now registers.
      MockNM nm2 = rm.registerNode("h2:1234", 10000);
      nm2.nodeHeartbeat(true);
      ArrayList<Container> containersReceivedForNM2 =
          new ArrayList<Container>();
      
      response = am.allocate("h2", 1000, 2, releaseContainerList);
      nm2.nodeHeartbeat(true);
      Assert.assertEquals(0, response.getAllocatedContainers().size());
      allocateContainersAndValidateNMTokens(am, containersReceivedForNM2, 2,
          nmTokens);
      Assert.assertEquals(2, nmTokens.size());
      
      // Simulating NM-2 restart.
      nm2 = rm.registerNode("h2:1234", 10000);
      nm2.nodeHeartbeat(true);
      
      int interval = 40;
      // Wait for nm Token to be cleared.
      while (nmTokenSecretManager
          .isApplicationAttemptNMTokenPresent(attempt.getAppAttemptId(),
              nm2.getNodeId()) && interval-- > 0) {
        LOG.info("waiting for nmToken to be cleared for : " + nm2.getNodeId());
        Thread.sleep(1000);
      }
      Assert.assertTrue(nmTokenSecretManager
          .isApplicationAttemptRegistered(attempt.getAppAttemptId()));
      
      // removing NMToken for h2:1234
      nmTokens.remove(nm2.getNodeId().toString());
      Assert.assertEquals(1, nmTokens.size());
      
      // We should again receive the NMToken.
      response = am.allocate("h2", 1000, 2, releaseContainerList);
      nm2.nodeHeartbeat(true);
      Assert.assertEquals(0, response.getAllocatedContainers().size());
      allocateContainersAndValidateNMTokens(am, containersReceivedForNM2, 4,
          nmTokens);
      Assert.assertEquals(2, nmTokens.size());

      // Now rolling over NMToken masterKey. it should resend the NMToken in
      // next allocate call.
      Assert.assertTrue(nmTokenSecretManager
          .isApplicationAttemptNMTokenPresent(attempt.getAppAttemptId(),
              nm1.getNodeId()));
      Assert.assertTrue(nmTokenSecretManager
          .isApplicationAttemptNMTokenPresent(attempt.getAppAttemptId(),
              nm2.getNodeId()));
      
      nmTokenSecretManager.rollMasterKey();
      nmTokenSecretManager.activateNextMasterKey();
      
      Assert.assertFalse(nmTokenSecretManager
          .isApplicationAttemptNMTokenPresent(attempt.getAppAttemptId(),
              nm1.getNodeId()));
      Assert.assertFalse(nmTokenSecretManager
          .isApplicationAttemptNMTokenPresent(attempt.getAppAttemptId(),
              nm2.getNodeId()));
      // It should not remove application attempt entry.
      Assert.assertTrue(nmTokenSecretManager
          .isApplicationAttemptRegistered(attempt.getAppAttemptId()));

      nmTokens.clear();
      Assert.assertEquals(0, nmTokens.size());
      // We should again receive the NMToken.
      response = am.allocate("h2", 1000, 1, releaseContainerList);
      nm2.nodeHeartbeat(true);
      Assert.assertEquals(0, response.getAllocatedContainers().size());
      allocateContainersAndValidateNMTokens(am, containersReceivedForNM2, 5,
          nmTokens);
      Assert.assertEquals(1, nmTokens.size());
      Assert.assertTrue(nmTokenSecretManager
          .isApplicationAttemptNMTokenPresent(attempt.getAppAttemptId(),
              nm2.getNodeId()));
      
      
      // After AM is finished making sure that nmtoken entry for app
      Assert.assertTrue(nmTokenSecretManager
          .isApplicationAttemptRegistered(attempt.getAppAttemptId()));
      am.unregisterAppAttempt();
      // marking all the containers as finished.
      for (Container container : containersReceivedForNM1) {
        nm1.nodeHeartbeat(attempt.getAppAttemptId(), container.getId().getId(),
            ContainerState.COMPLETE);
      }
      for (Container container : containersReceivedForNM2) {
        nm2.nodeHeartbeat(attempt.getAppAttemptId(), container.getId().getId(),
            ContainerState.COMPLETE);
      }
      am.waitForState(RMAppAttemptState.FINISHED);
      Assert.assertFalse(nmTokenSecretManager
          .isApplicationAttemptRegistered(attempt.getAppAttemptId()));
    } finally {
      rm.stop();
    }
  }

  protected void allocateContainersAndValidateNMTokens(MockAM am,
      ArrayList<Container> containersReceived, int totalContainerRequested,
      HashMap<String, Token> nmTokens) throws Exception, InterruptedException {
    ArrayList<ContainerId> releaseContainerList = new ArrayList<ContainerId>();
    AllocateResponse response;
    ArrayList<ResourceRequest> resourceRequest =
        new ArrayList<ResourceRequest>();      
    while (containersReceived.size() < totalContainerRequested) {
      LOG.info("requesting containers..");
      response =
          am.allocate(resourceRequest, releaseContainerList);
      containersReceived.addAll(response.getAllocatedContainers());
      if (!response.getNMTokens().isEmpty()) {
        for (NMToken nmToken : response.getNMTokens()) {
          String nodeId = nmToken.getNodeId().toString();
          if (nmTokens.containsKey(nodeId)) {
            Assert.fail("Duplicate NMToken received for : " + nodeId);
          }
          nmTokens.put(nodeId, nmToken.getToken());
        }
      }
      LOG.info("Got " + containersReceived.size()
          + " containers. Waiting to get " + totalContainerRequested);
      Thread.sleep(500);
    }
  }

  @Test (timeout = 300000)
  public void testActivatingApplicationAfterAddingNM() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();

    MockRM rm1 = new MockRM(conf);

    // start like normal because state is empty
    rm1.start();

    // app that gets launched
    RMApp app1 = rm1.submitApp(200);

    // app that does not get launched
    RMApp app2 = rm1.submitApp(200);

    // app1 and app2 should be scheduled, but because no resource is available,
    // they are not activated.
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    ApplicationAttemptId attemptId1 = attempt1.getAppAttemptId();
    rm1.waitForState(attemptId1, RMAppAttemptState.SCHEDULED);
    RMAppAttempt attempt2 = app2.getCurrentAppAttempt();
    ApplicationAttemptId attemptId2 = attempt2.getAppAttemptId();
    rm1.waitForState(attemptId2, RMAppAttemptState.SCHEDULED);

    MockNM nm1 = new MockNM("h1:1234", 15120, rm1.getResourceTrackerService());
    MockNM nm2 = new MockNM("h2:5678", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm2.registerNode();

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    // app1 should be allocated now
    rm1.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
    rm1.waitForState(attemptId2, RMAppAttemptState.SCHEDULED);

    nm2.nodeHeartbeat(true);

    // app2 should be allocated now
    rm1.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
    rm1.waitForState(attemptId2, RMAppAttemptState.ALLOCATED);

    rm1.stop();
  }

  public static void main(String[] args) throws Exception {
    TestRM t = new TestRM();
    t.testGetNewAppId();
    t.testAppWithNoContainers();
    t.testAppOnMultiNode();
  }
}
