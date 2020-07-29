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

import java.util.function.Supplier;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFencedException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;

public class TestRMHA {
  private static final Logger LOG = LoggerFactory.getLogger(TestRMHA.class);
  private Configuration configuration;
  private MockRM rm = null;
  private MockNM nm = null;
  private RMApp app = null;
  private RMAppAttempt attempt = null;
  private static final String STATE_ERR =
      "ResourceManager is in wrong HA state";

  private static final String RM1_ADDRESS = "1.1.1.1:1";
  private static final String RM1_NODE_ID = "rm1";

  private static final String RM2_ADDRESS = "0.0.0.0:0";
  private static final String RM2_NODE_ID = "rm2";

  private static final String RM3_ADDRESS = "2.2.2.2:2";
  private static final String RM3_NODE_ID = "rm3";

  @Before
  public void setUp() throws Exception {
    configuration = new Configuration();
    UserGroupInformation.setConfiguration(configuration);
    configuration.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    configuration.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + ","
        + RM2_NODE_ID);
    for (String confKey : YarnConfiguration
        .getServiceAddressConfKeys(configuration)) {
      configuration.set(HAUtil.addSuffix(confKey, RM1_NODE_ID), RM1_ADDRESS);
      configuration.set(HAUtil.addSuffix(confKey, RM2_NODE_ID), RM2_ADDRESS);
      configuration.set(HAUtil.addSuffix(confKey, RM3_NODE_ID), RM3_ADDRESS);
    }

    // Enable webapp to test web-services also
    configuration.setBoolean(MockRM.ENABLE_WEBAPP, true);
    configuration.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    ClusterMetrics.destroy();
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.shutdown();
  }

  private void checkMonitorHealth() throws IOException {
    try {
      rm.adminService.monitorHealth();
    } catch (HealthCheckFailedException e) {
      fail("The RM is in bad health: it is Active, but the active services " +
          "are not running");
    }
  }

  private void checkStandbyRMFunctionality() throws IOException {
    assertEquals(STATE_ERR, HAServiceState.STANDBY,
        rm.adminService.getServiceStatus().getState());
    assertFalse("Active RM services are started",
        rm.areActiveServicesRunning());
    assertTrue("RM is not ready to become active",
        rm.adminService.getServiceStatus().isReadyToBecomeActive());
  }

  private void checkActiveRMFunctionality() throws Exception {
    assertEquals(STATE_ERR, HAServiceState.ACTIVE,
        rm.adminService.getServiceStatus().getState());
    assertTrue("Active RM services aren't started",
        rm.areActiveServicesRunning());
    assertTrue("RM is not ready to become active",
        rm.adminService.getServiceStatus().isReadyToBecomeActive());

    try {
      rm.getNewAppId();
      nm = rm.registerNode("127.0.0.1:1", 2048);
      app = MockRMAppSubmitter.submitWithMemory(1024, rm);
      attempt = app.getCurrentAppAttempt();
      rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.SCHEDULED);
    } catch (Exception e) {
      fail("Unable to perform Active RM functions");
      LOG.error("ActiveRM check failed", e);
    }

    checkActiveRMWebServices();
  }

  // Do some sanity testing of the web-services after fail-over.
  private void checkActiveRMWebServices() throws JSONException {

    // Validate web-service
    Client webServiceClient = Client.create(new DefaultClientConfig());
    InetSocketAddress rmWebappAddr =
        NetUtils.getConnectAddress(rm.getWebapp().getListenerAddress());
    String webappURL =
        "http://" + rmWebappAddr.getHostName() + ":" + rmWebappAddr.getPort();
    WebResource webResource = webServiceClient.resource(webappURL);
    String path = app.getApplicationId().toString();

    ClientResponse response =
        webResource.path("ws").path("v1").path("cluster").path("apps")
            .path(path).accept(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);

    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject appJson = json.getJSONObject("app");
    assertEquals("ACCEPTED", appJson.getString("state"));
    // Other stuff is verified in the regular web-services related tests
  }

  /**
   * Test to verify the following RM HA transitions to the following states.
   * 1. Standby: Should be a no-op
   * 2. Active: Active services should start
   * 3. Active: Should be a no-op.
   * While active, submit a couple of jobs
   * 4. Standby: Active services should stop
   * 5. Active: Active services should start
   * 6. Stop the RM: All services should stop and RM should not be ready to
   * become Active
   */
  @Test(timeout = 30000)
  public void testFailoverAndTransitions() throws Exception {
    configuration.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    Configuration conf = new YarnConfiguration(configuration);

    rm = new MockRM(conf);
    rm.init(conf);
    StateChangeRequestInfo requestInfo = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    assertEquals(STATE_ERR, HAServiceState.INITIALIZING,
        rm.adminService.getServiceStatus().getState());
    assertFalse("RM is ready to become active before being started",
        rm.adminService.getServiceStatus().isReadyToBecomeActive());
    checkMonitorHealth();

    rm.start();
    checkMonitorHealth();
    checkStandbyRMFunctionality();
    verifyClusterMetrics(0, 0, 0, 0, 0, 0);

    // 1. Transition to Standby - must be a no-op
    rm.adminService.transitionToStandby(requestInfo);
    checkMonitorHealth();
    checkStandbyRMFunctionality();
    verifyClusterMetrics(0, 0, 0, 0, 0, 0);

    // 2. Transition to active
    rm.adminService.transitionToActive(requestInfo);
    checkMonitorHealth();
    checkActiveRMFunctionality();
    verifyClusterMetrics(1, 1, 1, 1, 2048, 1);

    // 3. Transition to active - no-op
    rm.adminService.transitionToActive(requestInfo);
    checkMonitorHealth();
    checkActiveRMFunctionality();
    verifyClusterMetrics(1, 2, 2, 2, 2048, 2);

    // 4. Transition to standby
    rm.adminService.transitionToStandby(requestInfo);
    checkMonitorHealth();
    checkStandbyRMFunctionality();
    verifyClusterMetrics(0, 0, 0, 0, 0, 0);

    // 5. Transition to active to check Active->Standby->Active works
    rm.adminService.transitionToActive(requestInfo);
    checkMonitorHealth();
    checkActiveRMFunctionality();
    verifyClusterMetrics(1, 1, 1, 1, 2048, 1);

    // 6. Stop the RM. All services should stop and RM should not be ready to
    // become active
    rm.stop();
    assertEquals(STATE_ERR, HAServiceState.STOPPING,
        rm.adminService.getServiceStatus().getState());
    assertFalse("RM is ready to become active even after it is stopped",
        rm.adminService.getServiceStatus().isReadyToBecomeActive());
    assertFalse("Active RM services are started",
        rm.areActiveServicesRunning());
    checkMonitorHealth();
  }

  @Test
  public void testTransitionsWhenAutomaticFailoverEnabled() throws Exception {
    final String ERR_UNFORCED_REQUEST = "User request succeeded even when " +
        "automatic failover is enabled";

    Configuration conf = new YarnConfiguration(configuration);

    rm = new MockRM(conf);
    rm.init(conf);
    rm.start();
    StateChangeRequestInfo requestInfo = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    // Transition to standby
    try {
      rm.adminService.transitionToStandby(requestInfo);
      fail(ERR_UNFORCED_REQUEST);
    } catch (AccessControlException e) {
      // expected
    }
    checkMonitorHealth();
    checkStandbyRMFunctionality();

    // Transition to active
    try {
      rm.adminService.transitionToActive(requestInfo);
      fail(ERR_UNFORCED_REQUEST);
    } catch (AccessControlException e) {
      // expected
    }
    checkMonitorHealth();
    checkStandbyRMFunctionality();


    final String ERR_FORCED_REQUEST = "Forced request by user should work " +
        "even if automatic failover is enabled";
    requestInfo = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER_FORCED);

    // Transition to standby
    try {
      rm.adminService.transitionToStandby(requestInfo);
    } catch (AccessControlException e) {
      fail(ERR_FORCED_REQUEST);
    }
    checkMonitorHealth();
    checkStandbyRMFunctionality();

    // Transition to active
    try {
      rm.adminService.transitionToActive(requestInfo);
    } catch (AccessControlException e) {
      fail(ERR_FORCED_REQUEST);
    }
    checkMonitorHealth();
    checkActiveRMFunctionality();
  }

  @Test
  public void testRMDispatcherForHA() throws IOException {
    String errorMessageForEventHandler =
        "Expect to get the same number of handlers";
    String errorMessageForService = "Expect to get the same number of services";
    configuration.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    Configuration conf = new YarnConfiguration(configuration);
    rm = new MockRM(conf) {
      @Override
      protected Dispatcher createDispatcher() {
        return new MyCountingDispatcher();
      }
    };
    rm.init(conf);
    int expectedEventHandlerCount =
        ((MyCountingDispatcher) rm.getRMContext().getDispatcher())
            .getEventHandlerCount();
    int expectedServiceCount = rm.getServices().size();
    assertTrue(expectedEventHandlerCount != 0);

    StateChangeRequestInfo requestInfo = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    assertEquals(STATE_ERR, HAServiceState.INITIALIZING,
        rm.adminService.getServiceStatus().getState());
    assertFalse("RM is ready to become active before being started",
        rm.adminService.getServiceStatus().isReadyToBecomeActive());
    rm.start();

    //call transitions to standby and active a couple of times
    rm.adminService.transitionToStandby(requestInfo);
    rm.adminService.transitionToActive(requestInfo);
    rm.adminService.transitionToStandby(requestInfo);
    rm.adminService.transitionToActive(requestInfo);
    rm.adminService.transitionToStandby(requestInfo);

    MyCountingDispatcher dispatcher =
        (MyCountingDispatcher) rm.getRMContext().getDispatcher();
    assertTrue(!dispatcher.isStopped());

    rm.adminService.transitionToActive(requestInfo);
    assertEquals(errorMessageForEventHandler, expectedEventHandlerCount,
        ((MyCountingDispatcher) rm.getRMContext().getDispatcher())
            .getEventHandlerCount());
    assertEquals(errorMessageForService, expectedServiceCount,
        rm.getServices().size());


    // Keep the dispatcher reference before transitioning to standby
    dispatcher = (MyCountingDispatcher) rm.getRMContext().getDispatcher();


    rm.adminService.transitionToStandby(requestInfo);
    assertEquals(errorMessageForEventHandler, expectedEventHandlerCount,
        ((MyCountingDispatcher) rm.getRMContext().getDispatcher())
            .getEventHandlerCount());
    assertEquals(errorMessageForService, expectedServiceCount,
        rm.getServices().size());

    assertTrue(dispatcher.isStopped());

    rm.stop();
  }

  @Test
  public void testHAIDLookup() {
    //test implicitly lookup HA-ID
    Configuration conf = new YarnConfiguration(configuration);
    rm = new MockRM(conf);
    rm.init(conf);

    assertThat(conf.get(YarnConfiguration.RM_HA_ID)).isEqualTo(RM2_NODE_ID);

    //test explicitly lookup HA-ID
    configuration.set(YarnConfiguration.RM_HA_ID, RM1_NODE_ID);
    conf = new YarnConfiguration(configuration);
    rm = new MockRM(conf);
    rm.init(conf);
    assertThat(conf.get(YarnConfiguration.RM_HA_ID)).isEqualTo(RM1_NODE_ID);

    //test if RM_HA_ID can not be found
    configuration
        .set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM3_NODE_ID);
    configuration.unset(YarnConfiguration.RM_HA_ID);
    conf = new YarnConfiguration(configuration);
    try {
      rm = new MockRM(conf);
      rm.init(conf);
      fail("Should get an exception here.");
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage().contains(
          "Invalid configuration! Can not find valid RM_HA_ID."));
    }
  }

  @Test
  public void testHAWithRMHostName() throws Exception {
    innerTestHAWithRMHostName(false);
    configuration.clear();
    setUp();
    innerTestHAWithRMHostName(true);
  }

  @Test(timeout = 30000)
  public void testFailoverWhenTransitionToActiveThrowException()
      throws Exception {
    configuration.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    Configuration conf = new YarnConfiguration(configuration);

    MemoryRMStateStore memStore = new MockMemoryRMStateStore() {
      int count = 0;

      @Override
      public synchronized void startInternal() throws Exception {
        // first time throw exception
        if (count++ == 0) {
          throw new Exception("Session Expired");
        }
      }
    };
    // start RM
    memStore.init(conf);

    rm = new MockRM(conf, memStore);
    rm.init(conf);
    StateChangeRequestInfo requestInfo =
        new StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    assertEquals(STATE_ERR, HAServiceState.INITIALIZING, rm.adminService
        .getServiceStatus().getState());
    assertFalse("RM is ready to become active before being started",
        rm.adminService.getServiceStatus().isReadyToBecomeActive());
    checkMonitorHealth();

    rm.start();
    checkMonitorHealth();
    checkStandbyRMFunctionality();

    // 2. Try Transition to active, throw exception
    try {
      rm.adminService.transitionToActive(requestInfo);
      Assert.fail("Transitioned to Active should throw exception.");
    } catch (Exception e) {
      assertTrue("Error when transitioning to Active mode".contains(e
          .getMessage()));
    }

    // 3. Transition to active, success
    rm.adminService.transitionToActive(requestInfo);
    checkMonitorHealth();
    checkActiveRMFunctionality();
  }

  @Test
  public void testTransitionedToStandbyShouldNotHang() throws Exception {
    configuration.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    Configuration conf = new YarnConfiguration(configuration);

    MemoryRMStateStore memStore = new MockMemoryRMStateStore() {
      @Override
      public void updateApplicationState(ApplicationStateData appState) {
        notifyStoreOperationFailed(new StoreFencedException());
      }
    };
    memStore.init(conf);
    rm = new MockRM(conf, memStore) {
      @Override
      void stopActiveServices() {
        try {
          Thread.sleep(10000);
        } catch (Exception e) {
          throw new RuntimeException (e);
        }
        super.stopActiveServices();
      }
    };
    rm.init(conf);
    final StateChangeRequestInfo requestInfo =
        new StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    assertEquals(STATE_ERR, HAServiceState.INITIALIZING, rm.adminService
        .getServiceStatus().getState());
    assertFalse("RM is ready to become active before being started",
        rm.adminService.getServiceStatus().isReadyToBecomeActive());
    checkMonitorHealth();

    rm.start();
    checkMonitorHealth();
    checkStandbyRMFunctionality();

    // 2. Transition to Active.
    rm.adminService.transitionToActive(requestInfo);

    // 3. Try Transition to standby
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          rm.transitionToStandby(true);
        } catch (IOException e) {
          e.printStackTrace();
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    });
    t.start();

    rm.getRMContext().getStateStore().updateApplicationState(null);
    t.join(); // wait for thread to finish

    rm.adminService.transitionToStandby(requestInfo);
    checkStandbyRMFunctionality();
    rm.stop();
  }

  @Test
  public void testFailoverClearsRMContext() throws Exception {
    configuration.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    configuration.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    Configuration conf = new YarnConfiguration(configuration);
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());

    // 1. start RM
    rm = new MockRM(conf);
    rm.init(conf);
    rm.start();

    StateChangeRequestInfo requestInfo =
        new StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER);
    checkMonitorHealth();
    checkStandbyRMFunctionality();

    // 2. Transition to active
    rm.adminService.transitionToActive(requestInfo);
    checkMonitorHealth();
    checkActiveRMFunctionality();
    verifyClusterMetrics(1, 1, 1, 1, 2048, 1);
    assertEquals(1, rm.getRMContext().getRMNodes().size());
    assertEquals(1, rm.getRMContext().getRMApps().size());
    Assert.assertNotNull("Node not registered", nm);

    rm.adminService.transitionToStandby(requestInfo);
    checkMonitorHealth();
    checkStandbyRMFunctionality();
    // race condition causes to register/node heartbeat node even after service
    // is stopping/stopped. New RMContext is being created on every transition
    // to standby, so metrics should be 0 which indicates new context reference
    // has taken.
    nm.registerNode();
    verifyClusterMetrics(0, 0, 0, 0, 0, 0);

    // 3. Create new RM
    rm = new MockRM(conf, rm.getRMStateStore()) {
      @Override
      protected ResourceTrackerService createResourceTrackerService() {
        return new ResourceTrackerService(this.rmContext,
            this.nodesListManager, this.nmLivelinessMonitor,
            this.rmContext.getContainerTokenSecretManager(),
            this.rmContext.getNMTokenSecretManager()) {
          @Override
          protected void serviceStart() throws Exception {
            throw new Exception("ResourceTracker service failed");
          }
        };
      }
    };
    rm.init(conf);
    rm.start();
    checkMonitorHealth();
    checkStandbyRMFunctionality();

    // 4. Try Transition to active, throw exception
    try {
      rm.adminService.transitionToActive(requestInfo);
      Assert.fail("Transitioned to Active should throw exception.");
    } catch (Exception e) {
      assertTrue("Error when transitioning to Active mode".contains(e
          .getMessage()));
    }
    // 5. Clears the metrics
    verifyClusterMetrics(0, 0, 0, 0, 0, 0);
    assertEquals(0, rm.getRMContext().getRMNodes().size());
    assertEquals(0, rm.getRMContext().getRMApps().size());
  }

  @Test(timeout = 9000000)
  public void testTransitionedToActiveRefreshFail() throws Exception {
    configuration.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    rm = new MockRM(configuration) {
      @Override
      protected AdminService createAdminService() {
        return new AdminService(this) {
          int counter = 0;
          @Override
          protected void setConfig(Configuration conf) {
            super.setConfig(configuration);
          }

          @Override
          protected void refreshAll() throws ServiceFailedException {
            if (counter == 0) {
              counter++;
              throw new ServiceFailedException("Simulate RefreshFail");
            } else {
              super.refreshAll();
            }
          }
        };
      }

      @Override
      protected Dispatcher createDispatcher() {
        return new FailFastDispatcher();
      }
    };

    rm.init(configuration);
    rm.start();
    final StateChangeRequestInfo requestInfo =
        new StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER);
    FailFastDispatcher dispatcher =
        ((FailFastDispatcher) rm.rmContext.getDispatcher());
    // Verify transition to transitionToStandby
    rm.adminService.transitionToStandby(requestInfo);
    assertEquals("Fatal Event should be 0", 0, dispatcher.getEventCount());
    assertEquals("HA state should be in standBy State", HAServiceState.STANDBY,
        rm.getRMContext().getHAServiceState());
    try {
      // Verify refreshAll call failure and check fail Event is dispatched
      rm.adminService.transitionToActive(requestInfo);
      Assert.fail("Transition to Active should have failed for refreshAll()");
    } catch (Exception e) {
      assertTrue("Service fail Exception expected",
          e instanceof ServiceFailedException);
    }
    // Since refreshAll failed we are expecting fatal event to be send
    // Then fatal event is send RM will shutdown
    dispatcher.await();
    assertEquals("Fatal Event to be received", 1, dispatcher.getEventCount());
    // Check of refreshAll success HA can be active
    rm.adminService.transitionToActive(requestInfo);
    assertEquals(HAServiceState.ACTIVE, rm.getRMContext().getHAServiceState());
    rm.adminService.transitionToStandby(requestInfo);
    assertEquals(HAServiceState.STANDBY, rm.getRMContext().getHAServiceState());
  }

  @Test
  public void testOpportunisticAllocatorAfterFailover() throws Exception {
    configuration.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    configuration.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    Configuration conf = new YarnConfiguration(configuration);
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    conf.setBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED, true);
    // 1. start RM
    rm = new MockRM(conf);
    rm.init(conf);
    rm.start();

    StateChangeRequestInfo requestInfo = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);
    // 2. Transition to active
    rm.adminService.transitionToActive(requestInfo);
    // 3. Transition to standby
    rm.adminService.transitionToStandby(requestInfo);
    // 4. Transition to active
    rm.adminService.transitionToActive(requestInfo);

    MockNM nm1 = rm.registerNode("h1:1234", 8 * 1024);
    RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
    rmNode1.getRMContext().getDispatcher().getEventHandler()
        .handle(new NodeUpdateSchedulerEvent(rmNode1));
    OpportunisticContainerAllocatorAMService appMaster =
        (OpportunisticContainerAllocatorAMService) rm.getRMContext()
            .getApplicationMasterService();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return appMaster.getLeastLoadedNodes().size() == 1;
      }
    }, 100, 3000);
    rm.stop();
    Assert.assertEquals(1, appMaster.getLeastLoadedNodes().size());

  }

  @Test
  public void testResourceProfilesManagerAfterRMWentStandbyThenBackToActive()
      throws Exception {
    configuration.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    configuration.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    Configuration conf = new YarnConfiguration(configuration);
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());

    // 1. start RM
    rm = new MockRM(conf);
    rm.init(conf);
    rm.start();

    StateChangeRequestInfo requestInfo = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);
    checkMonitorHealth();
    checkStandbyRMFunctionality();

    // 2. Transition to active
    rm.adminService.transitionToActive(requestInfo);
    checkMonitorHealth();
    checkActiveRMFunctionality();

    // 3. Transition to standby
    rm.adminService.transitionToStandby(requestInfo);
    checkMonitorHealth();
    checkStandbyRMFunctionality();

    // 4. Transition to active
    rm.adminService.transitionToActive(requestInfo);
    checkMonitorHealth();
    checkActiveRMFunctionality();

    // 5. Check ResourceProfilesManager
    Assert.assertNotNull(
        "ResourceProfilesManager should not be null!",
        rm.getRMContext().getResourceProfilesManager());
  }

  public void innerTestHAWithRMHostName(boolean includeBindHost) {
    //this is run two times, with and without a bind host configured
    if (includeBindHost) {
      configuration.set(YarnConfiguration.RM_BIND_HOST, "9.9.9.9");
    }

    //test if both RM_HOSTBANE_{rm_id} and RM_RPCADDRESS_{rm_id} are set
    //We should only read rpc addresses from RM_RPCADDRESS_{rm_id} configuration
    configuration.set(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME,
        RM1_NODE_ID), "1.1.1.1");
    configuration.set(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME,
        RM2_NODE_ID), "0.0.0.0");
    configuration.set(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME,
        RM3_NODE_ID), "2.2.2.2");
    try {
      Configuration conf = new YarnConfiguration(configuration);
      rm = new MockRM(conf);
      rm.init(conf);
      for (String confKey : YarnConfiguration.getServiceAddressConfKeys(conf)) {
        assertEquals("RPC address not set for " + confKey,
            RM1_ADDRESS, conf.get(HAUtil.addSuffix(confKey, RM1_NODE_ID)));
        assertEquals("RPC address not set for " + confKey,
            RM2_ADDRESS, conf.get(HAUtil.addSuffix(confKey, RM2_NODE_ID)));
        assertEquals("RPC address not set for " + confKey,
            RM3_ADDRESS, conf.get(HAUtil.addSuffix(confKey, RM3_NODE_ID)));
        if (includeBindHost) {
          assertEquals("Web address misconfigured WITH bind-host",
                       rm.webAppAddress.substring(0, 7), "9.9.9.9");
        } else {
          //YarnConfiguration tries to figure out which rm host it's on by binding to it,
          //which doesn't happen for any of these fake addresses, so we end up with 0.0.0.0
          assertEquals("Web address misconfigured WITHOUT bind-host",
                       rm.webAppAddress.substring(0, 7), "0.0.0.0");
        }
      }
    } catch (YarnRuntimeException e) {
      fail("Should not throw any exceptions.");
    }

    //test if only RM_HOSTBANE_{rm_id} is set
    configuration.clear();
    configuration.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    configuration.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + ","
        + RM2_NODE_ID);
    configuration.set(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME,
        RM1_NODE_ID), "1.1.1.1");
    configuration.set(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME,
        RM2_NODE_ID), "0.0.0.0");
    try {
      Configuration conf = new YarnConfiguration(configuration);
      rm = new MockRM(conf);
      rm.init(conf);
      assertEquals("RPC address not set for " + YarnConfiguration.RM_ADDRESS,
          "1.1.1.1:8032",
          conf.get(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS, RM1_NODE_ID)));
      assertEquals("RPC address not set for " + YarnConfiguration.RM_ADDRESS,
          "0.0.0.0:8032",
          conf.get(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS, RM2_NODE_ID)));

    } catch (YarnRuntimeException e) {
      fail("Should not throw any exceptions.");
    }
  }
  
  private void verifyClusterMetrics(int activeNodes, int appsSubmitted,
      int appsPending, int containersPending, long availableMB,
      int activeApplications) throws Exception {
    int timeoutSecs = 0;
    QueueMetrics metrics = rm.getResourceScheduler().getRootQueueMetrics();
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
    boolean isAllMetricAssertionDone = false;
    String message = null;
    while (timeoutSecs++ < 5) {
      try {
        // verify queue metrics
        assertMetric("appsSubmitted", appsSubmitted, metrics.getAppsSubmitted());
        assertMetric("appsPending", appsPending, metrics.getAppsPending());
        assertMetric("containersPending", containersPending,
            metrics.getPendingContainers());
        assertMetric("availableMB", availableMB, metrics.getAvailableMB());
        assertMetric("activeApplications", activeApplications,
            metrics.getActiveApps());
        // verify node metric
        assertMetric("activeNodes", activeNodes,
            clusterMetrics.getNumActiveNMs());
        isAllMetricAssertionDone = true;
        break;
      } catch (AssertionError e) {
        message = e.getMessage();
        System.out.println("Waiting for metrics assertion to complete");
        Thread.sleep(1000);
      }
    }
    assertTrue(message, isAllMetricAssertionDone);
  }

  private void assertMetric(String metricName, long expected, long actual) {
    assertEquals("Incorrect value for metric " + metricName, expected, actual);
  }

  @SuppressWarnings("rawtypes")
  class MyCountingDispatcher extends AbstractService implements Dispatcher {

    private int eventHandlerCount;

    private volatile boolean stopped = false;

    public MyCountingDispatcher() {
      super("MyCountingDispatcher");
      this.eventHandlerCount = 0;
    }

    @Override
    public EventHandler<Event> getEventHandler() {
      return null;
    }

    @Override
    public void register(Class<? extends Enum> eventType, EventHandler handler) {
      this.eventHandlerCount ++;
    }

    public int getEventHandlerCount() {
      return this.eventHandlerCount;
    }

    @Override
    protected void serviceStop() throws Exception {
      this.stopped = true;
      super.serviceStop();
    }

    public boolean isStopped() {
      return this.stopped;
    }
  }

  class FailFastDispatcher extends DrainDispatcher {
    int eventreceived = 0;

    @SuppressWarnings("rawtypes")
    @Override
    protected void dispatch(Event event) {
      if (event.getType() == RMFatalEventType.TRANSITION_TO_ACTIVE_FAILED) {
        eventreceived++;
      } else {
        super.dispatch(event);
      }
    }

    public int getEventCount() {
      return eventreceived;
    }
  }
}
