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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;

public class TestRMHA {
  private Log LOG = LogFactory.getLog(TestRMHA.class);
  private final Configuration configuration = new YarnConfiguration();
  private MockRM rm = null;
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
      rm.registerNode("127.0.0.1:0", 2048);
      app = rm.submitApp(1024);
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
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
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
   *    While active, submit a couple of jobs
   * 4. Standby: Active services should stop
   * 5. Active: Active services should start
   * 6. Stop the RM: All services should stop and RM should not be ready to
   * become Active
   */
  @Test (timeout = 30000)
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

    rm.adminService.transitionToActive(requestInfo);
    assertEquals(errorMessageForEventHandler, expectedEventHandlerCount,
        ((MyCountingDispatcher) rm.getRMContext().getDispatcher())
        .getEventHandlerCount());
    assertEquals(errorMessageForService, expectedServiceCount,
        rm.getServices().size());

    rm.adminService.transitionToStandby(requestInfo);
    assertEquals(errorMessageForEventHandler, expectedEventHandlerCount,
        ((MyCountingDispatcher) rm.getRMContext().getDispatcher())
        .getEventHandlerCount());
    assertEquals(errorMessageForService, expectedServiceCount,
        rm.getServices().size());

    rm.stop();
  }

  @Test
  public void testHAIDLookup() {
    //test implicitly lookup HA-ID
    Configuration conf = new YarnConfiguration(configuration);
    rm = new MockRM(conf);
    rm.init(conf);

    assertEquals(conf.get(YarnConfiguration.RM_HA_ID), RM2_NODE_ID);

    //test explicitly lookup HA-ID
    configuration.set(YarnConfiguration.RM_HA_ID, RM1_NODE_ID);
    conf = new YarnConfiguration(configuration);
    rm = new MockRM(conf);
    rm.init(conf);
    assertEquals(conf.get(YarnConfiguration.RM_HA_ID), RM1_NODE_ID);

    //test if RM_HA_ID can not be found
    configuration.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID+ "," + RM3_NODE_ID);
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
  public void testHAWithRMHostName() {
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
      int appsPending, int containersPending, int availableMB,
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

  private void assertMetric(String metricName, int expected, int actual) {
    assertEquals("Incorrect value for metric " + metricName, expected, actual);
  }

  @SuppressWarnings("rawtypes")
  class MyCountingDispatcher extends AbstractService implements Dispatcher {

    private int eventHandlerCount;

    public MyCountingDispatcher() {
      super("MyCountingDispatcher");
      this.eventHandlerCount = 0;
    }

    @Override
    public EventHandler getEventHandler() {
      return null;
    }

    @Override
    public void register(Class<? extends Enum> eventType, EventHandler handler) {
      this.eventHandlerCount ++;
    }

    public int getEventHandlerCount() {
      return this.eventHandlerCount;
    }
  }
}
