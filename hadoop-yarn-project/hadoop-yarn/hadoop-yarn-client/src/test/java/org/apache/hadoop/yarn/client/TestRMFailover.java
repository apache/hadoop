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

package org.apache.hadoop.yarn.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRMFailover extends ClientBaseWithFixes {
  private static final Log LOG =
      LogFactory.getLog(TestRMFailover.class.getName());
  private static final HAServiceProtocol.StateChangeRequestInfo req =
      new HAServiceProtocol.StateChangeRequestInfo(
          HAServiceProtocol.RequestSource.REQUEST_BY_USER);

  private static final String RM1_NODE_ID = "rm1";
  private static final int RM1_PORT_BASE = 10000;
  private static final String RM2_NODE_ID = "rm2";
  private static final int RM2_PORT_BASE = 20000;

  private Configuration conf;
  private MiniYARNCluster cluster;
  private ApplicationId fakeAppId;


  private void setConfForRM(String rmId, String prefix, String value) {
    conf.set(HAUtil.addSuffix(prefix, rmId), value);
  }

  private void setRpcAddressForRM(String rmId, int base) {
    setConfForRM(rmId, YarnConfiguration.RM_ADDRESS, "0.0.0.0:" +
        (base + YarnConfiguration.DEFAULT_RM_PORT));
    setConfForRM(rmId, YarnConfiguration.RM_SCHEDULER_ADDRESS, "0.0.0.0:" +
        (base + YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT));
    setConfForRM(rmId, YarnConfiguration.RM_ADMIN_ADDRESS, "0.0.0.0:" +
        (base + YarnConfiguration.DEFAULT_RM_ADMIN_PORT));
    setConfForRM(rmId, YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "0.0.0.0:" +
        (base + YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT));
    setConfForRM(rmId, YarnConfiguration.RM_WEBAPP_ADDRESS, "0.0.0.0:" +
        (base + YarnConfiguration.DEFAULT_RM_WEBAPP_PORT));
    setConfForRM(rmId, YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS, "0.0.0.0:" +
        (base + YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT));
  }

  @Before
  public void setup() throws IOException {
    fakeAppId = ApplicationId.newInstance(System.currentTimeMillis(), 0);
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    setRpcAddressForRM(RM1_NODE_ID, RM1_PORT_BASE);
    setRpcAddressForRM(RM2_NODE_ID, RM2_PORT_BASE);

    conf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS, 100L);

    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC, true);

    cluster = new MiniYARNCluster(TestRMFailover.class.getName(), 2, 1, 1, 1);
  }

  @After
  public void teardown() {
    cluster.stop();
  }

  private void verifyClientConnection() {
    int numRetries = 3;
    while(numRetries-- > 0) {
      Configuration conf = new YarnConfiguration(this.conf);
      YarnClient client = YarnClient.createYarnClient();
      client.init(conf);
      client.start();
      try {
        client.getApplications();
        return;
      } catch (Exception e) {
        LOG.error(e);
      } finally {
        client.stop();
      }
    }
    fail("Client couldn't connect to the Active RM");
  }

  private void verifyConnections() throws InterruptedException, YarnException {
    assertTrue("NMs failed to connect to the RM",
        cluster.waitForNodeManagersToConnect(20000));
    verifyClientConnection();
  }

  private AdminService getAdminService(int index) {
    return cluster.getResourceManager(index).getRMContext().getRMAdminService();
  }

  private void explicitFailover() throws IOException {
    int activeRMIndex = cluster.getActiveRMIndex();
    int newActiveRMIndex = (activeRMIndex + 1) % 2;
    getAdminService(activeRMIndex).transitionToStandby(req);
    getAdminService(newActiveRMIndex).transitionToActive(req);
    assertEquals("Failover failed", newActiveRMIndex, cluster.getActiveRMIndex());
  }

  private void failover()
      throws IOException, InterruptedException, YarnException {
    int activeRMIndex = cluster.getActiveRMIndex();
    cluster.stopResourceManager(activeRMIndex);
    assertEquals("Failover failed",
        (activeRMIndex + 1) % 2, cluster.getActiveRMIndex());
    cluster.restartResourceManager(activeRMIndex);
  }

  @Test
  public void testExplicitFailover()
      throws YarnException, InterruptedException, IOException {
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    cluster.init(conf);
    cluster.start();
    getAdminService(0).transitionToActive(req);
    assertFalse("RM never turned active", -1 == cluster.getActiveRMIndex());
    verifyConnections();

    explicitFailover();
    verifyConnections();

    explicitFailover();
    verifyConnections();
  }

  @Test
  public void testAutomaticFailover()
      throws YarnException, InterruptedException, IOException {
    conf.set(YarnConfiguration.RM_CLUSTER_ID, "yarn-test-cluster");
    conf.set(YarnConfiguration.RM_ZK_ADDRESS, hostPort);
    conf.setInt(YarnConfiguration.RM_ZK_TIMEOUT_MS, 2000);

    cluster.init(conf);
    cluster.start();
    assertFalse("RM never turned active", -1 == cluster.getActiveRMIndex());
    verifyConnections();

    failover();
    verifyConnections();

    failover();
    verifyConnections();

    // Make the current Active handle an RMFatalEvent,
    // so it transitions to standby.
    ResourceManager rm = cluster.getResourceManager(
        cluster.getActiveRMIndex());
    rm.handleTransitionToStandBy();
    int maxWaitingAttempts = 2000;
    while (maxWaitingAttempts-- > 0 ) {
      if (rm.getRMContext().getHAServiceState() == HAServiceState.STANDBY) {
        break;
      }
      Thread.sleep(1);
    }
    Assert.assertFalse("RM didn't transition to Standby ",
        maxWaitingAttempts == 0);
    verifyConnections();
  }

  @Test
  public void testWebAppProxyInStandAloneMode() throws YarnException,
      InterruptedException, IOException {
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    WebAppProxyServer webAppProxyServer = new WebAppProxyServer();
    try {
      conf.set(YarnConfiguration.PROXY_ADDRESS, "0.0.0.0:9099");
      cluster.init(conf);
      cluster.start();
      getAdminService(0).transitionToActive(req);
      assertFalse("RM never turned active", -1 == cluster.getActiveRMIndex());
      verifyConnections();
      webAppProxyServer.init(conf);

      // Start webAppProxyServer
      Assert.assertEquals(STATE.INITED, webAppProxyServer.getServiceState());
      webAppProxyServer.start();
      Assert.assertEquals(STATE.STARTED, webAppProxyServer.getServiceState());

      // send httpRequest with fakeApplicationId
      // expect to get "Not Found" response and 404 response code
      URL wrongUrl = new URL("http://0.0.0.0:9099/proxy/" + fakeAppId);
      HttpURLConnection proxyConn = (HttpURLConnection) wrongUrl
          .openConnection();

      proxyConn.connect();
      verifyResponse(proxyConn);

      explicitFailover();
      verifyConnections();
      proxyConn.connect();
      verifyResponse(proxyConn);
    } finally {
      webAppProxyServer.stop();
    }
  }

  @Test
  public void testEmbeddedWebAppProxy() throws YarnException,
      InterruptedException, IOException {
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    cluster.init(conf);
    cluster.start();
    getAdminService(0).transitionToActive(req);
    assertFalse("RM never turned active", -1 == cluster.getActiveRMIndex());
    verifyConnections();

    // send httpRequest with fakeApplicationId
    // expect to get "Not Found" response and 404 response code
    URL wrongUrl = new URL("http://0.0.0.0:18088/proxy/" + fakeAppId);
    HttpURLConnection proxyConn = (HttpURLConnection) wrongUrl
        .openConnection();

    proxyConn.connect();
    verifyResponse(proxyConn);

    explicitFailover();
    verifyConnections();
    proxyConn.connect();
    verifyResponse(proxyConn);
  }

  private void verifyResponse(HttpURLConnection response)
      throws IOException {
    assertEquals("Not Found", response.getResponseMessage());
    assertEquals(404, response.getResponseCode());
  }

  @Test
  public void testRMWebAppRedirect() throws YarnException,
      InterruptedException, IOException {
    cluster = new MiniYARNCluster(TestRMFailover.class.getName(), 2, 0, 1, 1);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);

    cluster.init(conf);
    cluster.start();
    getAdminService(0).transitionToActive(req);
    String rm1Url = "http://0.0.0.0:18088";
    String rm2Url = "http://0.0.0.0:28088";
    String header = getHeader("Refresh", rm2Url);
    assertTrue(header.contains("; url=" + rm1Url));

    header = getHeader("Refresh", rm2Url + "/metrics");
    assertTrue(header.contains("; url=" + rm1Url));

    header = getHeader("Refresh", rm2Url + "/jmx");
    assertTrue(header.contains("; url=" + rm1Url));

    // standby RM links /conf, /stacks, /logLevel, /static, /logs,
    // /cluster/cluster as well as webService
    // /ws/v1/cluster/info should not be redirected to active RM
    header = getHeader("Refresh", rm2Url + "/cluster/cluster");
    assertEquals(null, header);

    header = getHeader("Refresh", rm2Url + "/conf");
    assertEquals(null, header);

    header = getHeader("Refresh", rm2Url + "/stacks");
    assertEquals(null, header);

    header = getHeader("Refresh", rm2Url + "/logLevel");
    assertEquals(null, header);

    header = getHeader("Refresh", rm2Url + "/static");
    assertEquals(null, header);

    header = getHeader("Refresh", rm2Url + "/logs");
    assertEquals(null, header);

    header = getHeader("Refresh", rm2Url + "/ws/v1/cluster/info");
    assertEquals(null, header);

    header = getHeader("Refresh", rm2Url + "/ws/v1/cluster/apps");
    assertTrue(header.contains("; url=" + rm1Url));

    // Due to the limitation of MiniYARNCluster and dispatcher is a singleton,
    // we couldn't add the test case after explicitFailover();
  }

  static String getHeader(String field, String url) {
    String fieldHeader = null;
    try {
      Map<String, List<String>> map =
          new URL(url).openConnection().getHeaderFields();
      fieldHeader = map.get(field).get(0);
    } catch (Exception e) {
      // throw new RuntimeException(e);
    }
    return fieldHeader;
  }

}
