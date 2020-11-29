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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.HATestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMCriticalThreadUncaughtExceptionHandler;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMFatalEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMFatalEventType;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServer;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRMFailover extends ClientBaseWithFixes {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRMFailover.class.getName());
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

  @Before
  public void setup() throws IOException {
    fakeAppId = ApplicationId.newInstance(System.currentTimeMillis(), 0);
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    HATestUtil.setRpcAddressForRM(RM1_NODE_ID, RM1_PORT_BASE, conf);
    HATestUtil.setRpcAddressForRM(RM2_NODE_ID, RM2_PORT_BASE, conf);

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
        LOG.error(e.toString());
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
    assertFalse("RM never turned active", -1 == cluster.getActiveRMIndex());
    verifyConnections();

    explicitFailover();
    verifyConnections();

    explicitFailover();
    verifyConnections();
  }

  private void verifyRMTransitionToStandby(ResourceManager rm)
      throws InterruptedException {
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return rm.getRMContext().getHAServiceState() ==
              HAServiceState.STANDBY;
        }
      }, 100, 20000);
    } catch (TimeoutException e) {
      fail("RM didn't transition to Standby.");
    }
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
    rm.getRMContext().getDispatcher().getEventHandler().handle(
        new RMFatalEvent(RMFatalEventType.STATE_STORE_FENCED, "test"));
    verifyRMTransitionToStandby(rm);
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

    String redirectURL = getRedirectURL(rm2Url);
    // if uri is null, RMWebAppFilter will append a slash at the trail of the redirection url
    assertEquals(redirectURL,rm1Url+"/");

    redirectURL = getRedirectURL(rm2Url + "/metrics");
    assertEquals(redirectURL,rm1Url + "/metrics");


    // standby RM links /conf, /stacks, /logLevel, /static, /logs, /jmx
    // /cluster/cluster as well as webService
    // /ws/v1/cluster/info should not be redirected to active RM
    redirectURL = getRedirectURL(rm2Url + "/cluster/cluster");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/conf");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/stacks");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/logLevel");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/static");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/logs");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/jmx?param1=value1+x&param2=y");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/ws/v1/cluster/info");
    assertNull(redirectURL);

    redirectURL = getRedirectURL(rm2Url + "/ws/v1/cluster/apps");
    assertEquals(redirectURL, rm1Url + "/ws/v1/cluster/apps");

    redirectURL = getRedirectURL(rm2Url + "/proxy/" + fakeAppId);
    assertNull(redirectURL);

    // transit the active RM to standby
    // Both of RMs are in standby mode
    getAdminService(0).transitionToStandby(req);
    // RM2 is expected to send the httpRequest to itself.
    // The Header Field: Refresh is expected to be set.
    redirectURL = getRefreshURL(rm2Url);
    assertTrue(redirectURL != null
        && redirectURL.contains(YarnWebParams.NEXT_REFRESH_INTERVAL)
        && redirectURL.contains(rm2Url));

  }

  // set up http connection with the given url and get the redirection url from the response
  // return null if the url is not redirected
  static String getRedirectURL(String url) {
    String redirectUrl = null;
    try {
      HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
      // do not automatically follow the redirection
      // otherwise we get too many redirections exception
      conn.setInstanceFollowRedirects(false);
      if(conn.getResponseCode() == HttpServletResponse.SC_TEMPORARY_REDIRECT) {
        redirectUrl = conn.getHeaderField("Location");
      }
    } catch (Exception e) {
      // throw new RuntimeException(e);
    }
    return redirectUrl;
  }

  static String getRefreshURL(String url) {
    String redirectUrl = null;
    try {
      HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
      // do not automatically follow the redirection
      // otherwise we get too many redirections exception
      conn.setInstanceFollowRedirects(false);
      redirectUrl = conn.getHeaderField("Refresh");
    } catch (Exception e) {
      // throw new RuntimeException(e);
    }
    return redirectUrl;
  }

  /**
   * Throw {@link RuntimeException} inside a thread of
   * {@link ResourceManager} with HA enabled and check if the
   * {@link ResourceManager} is transited to standby state.
   *
   * @throws InterruptedException if any
   */
  @Test
  public void testUncaughtExceptionHandlerWithHAEnabled()
      throws InterruptedException {
    conf.set(YarnConfiguration.RM_CLUSTER_ID, "yarn-test-cluster");
    conf.set(YarnConfiguration.RM_ZK_ADDRESS, hostPort);
    cluster.init(conf);
    cluster.start();
    assertFalse("RM never turned active", -1 == cluster.getActiveRMIndex());

    ResourceManager resourceManager = cluster.getResourceManager(
        cluster.getActiveRMIndex());

    final RMCriticalThreadUncaughtExceptionHandler exHandler =
        new RMCriticalThreadUncaughtExceptionHandler(
            resourceManager.getRMContext());

    // Create a thread and throw a RTE inside it
    final RuntimeException rte = new RuntimeException("TestRuntimeException");
    final Thread testThread = new Thread(new Runnable() {
      @Override
      public void run() {
        throw rte;
      }
    });
    testThread.setName("TestThread");
    testThread.setUncaughtExceptionHandler(exHandler);
    testThread.start();
    testThread.join();

    verifyRMTransitionToStandby(resourceManager);
  }

  /**
   * Throw {@link RuntimeException} inside a thread of
   * {@link ResourceManager} with HA disabled and check
   * {@link RMCriticalThreadUncaughtExceptionHandler} instance.
   *
   * Used {@link ExitUtil} class to avoid jvm exit through
   * {@code System.exit(-1)}.
   *
   * @throws InterruptedException if any
   */
  @Test
  public void testUncaughtExceptionHandlerWithoutHA()
      throws InterruptedException {
    ExitUtil.disableSystemExit();

    // Create a MockRM and start it
    ResourceManager resourceManager = new MockRM();
    ((AsyncDispatcher) resourceManager.getRMContext().getDispatcher()).start();
    resourceManager.getRMContext().getStateStore().start();
    resourceManager.getRMContext().getContainerTokenSecretManager().
        rollMasterKey();

    final RMCriticalThreadUncaughtExceptionHandler exHandler =
        new RMCriticalThreadUncaughtExceptionHandler(
            resourceManager.getRMContext());
    final RMCriticalThreadUncaughtExceptionHandler spyRTEHandler =
        spy(exHandler);

    // Create a thread and throw a RTE inside it
    final RuntimeException rte = new RuntimeException("TestRuntimeException");
    final Thread testThread = new Thread(new Runnable() {
      @Override public void run() {
        throw rte;
      }
    });
    testThread.setName("TestThread");
    testThread.setUncaughtExceptionHandler(spyRTEHandler);
    assertSame(spyRTEHandler, testThread.getUncaughtExceptionHandler());
    testThread.start();
    testThread.join();

    verify(spyRTEHandler).uncaughtException(testThread, rte);
  }
}
