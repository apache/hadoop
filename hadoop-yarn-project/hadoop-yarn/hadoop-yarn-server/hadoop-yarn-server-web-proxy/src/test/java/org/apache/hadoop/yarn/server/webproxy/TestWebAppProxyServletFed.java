/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.webproxy;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the WebAppProxyServlet and WebAppProxy. For back end use simple web server.
 */
public class TestWebAppProxyServletFed {

  private static final Logger LOG = LoggerFactory.getLogger(TestWebAppProxyServletFed.class);

  public static final String AM_PREFIX = "AM";
  public static final String RM_PREFIX = "RM";
  public static final String AHS_PREFIX = "AHS";

  /*
  * Mocked Server is used for simulating the web of AppMaster, ResourceMamanger or TimelineServer.
  * */
  private static Server mockServer;
  private static int mockServerPort = 0;

  /**
   * Simple http server. Server should send answer with status 200
   */
  @BeforeClass
  public static void setUp() throws Exception {
    mockServer = new Server(0);
    ((QueuedThreadPool) mockServer.getThreadPool()).setMaxThreads(20);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    context.addServlet(new ServletHolder(new MockWebServlet(AM_PREFIX)), "/amweb/*");
    context.addServlet(new ServletHolder(new MockWebServlet(RM_PREFIX)), "/cluster/app/*");
    context.addServlet(new ServletHolder(new MockWebServlet(AHS_PREFIX)),
        "/applicationhistory/app/*");
    mockServer.setHandler(context);

    ((ServerConnector) mockServer.getConnectors()[0]).setHost("localhost");
    mockServer.start();
    mockServerPort = ((ServerConnector) mockServer.getConnectors()[0]).getLocalPort();
    LOG.info("Running embedded servlet container at: http://localhost:" + mockServerPort);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (mockServer != null) {
      mockServer.stop();
      mockServer.destroy();
      mockServer = null;
    }
  }

  @Test
  public void testWebServlet() throws IOException {
    HttpURLConnection conn;
    // 1. Mocked AppMaster web Test
    URL url = new URL("http", "localhost", mockServerPort, "/amweb/apptest");
    conn = (HttpURLConnection) url.openConnection();
    conn.connect();
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    assertEquals(AM_PREFIX + "/apptest", readResponse(conn));
    conn.disconnect();

    // 2. Mocked RM web Test
    url = new URL("http", "localhost", mockServerPort, "/cluster/app/apptest");
    conn = (HttpURLConnection) url.openConnection();
    conn.connect();
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    assertEquals(RM_PREFIX + "/apptest", readResponse(conn));
    conn.disconnect();

    // 3. Mocked AHS web Test
    url = new URL("http", "localhost", mockServerPort, "/applicationhistory/app/apptest");
    conn = (HttpURLConnection) url.openConnection();
    conn.connect();
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    assertEquals(AHS_PREFIX + "/apptest", readResponse(conn));
    conn.disconnect();
  }

  @Test(timeout=5000)
  public void testWebAppProxyServletFed() throws Exception {

    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.PROXY_ADDRESS, "localhost:9090");
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED, true);
    conf.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS, "localhost:" + mockServerPort);
    // overriding num of web server threads, see HttpServer.HTTP_MAXTHREADS
    conf.setInt("hadoop.http.max.threads", 10);

    // Create sub cluster information.
    SubClusterId subClusterId1 = SubClusterId.newInstance("scid1");
    SubClusterId subClusterId2 = SubClusterId.newInstance("scid2");
    SubClusterInfo subClusterInfo1 = SubClusterInfo.newInstance(subClusterId1, "10.0.0.1:1",
        "10.0.0.1:1", "10.0.0.1:1", "localhost:" + mockServerPort, SubClusterState.SC_RUNNING, 0,
        "");
    SubClusterInfo subClusterInfo2 = SubClusterInfo.newInstance(subClusterId2, "10.0.0.2:1",
        "10.0.0.2:1", "10.0.0.2:1", "10.0.0.2:1", SubClusterState.SC_RUNNING, 0, "");

    // App1 and App2 is running applications.
    ApplicationId appId1 = ApplicationId.newInstance(0, 1);
    ApplicationId appId2 = ApplicationId.newInstance(0, 2);
    String appUrl1 = "http://localhost:" + mockServerPort + "/amweb/" + appId1;
    String appUrl2 = "http://localhost:" + mockServerPort + "/amweb/" + appId2;
    // App3 is accepted application, has not registered original url to am.
    ApplicationId appId3 = ApplicationId.newInstance(0, 3);
    // App4 is finished application, has remove from rm, but not remove from timeline server.
    ApplicationId appId4 = ApplicationId.newInstance(0, 4);

    // Mock for application
    ApplicationClientProtocol appManager1 = Mockito.mock(ApplicationClientProtocol.class);
    Mockito.when(appManager1.getApplicationReport(GetApplicationReportRequest.newInstance(appId1)))
        .thenReturn(GetApplicationReportResponse
            .newInstance(newApplicationReport(appId1, YarnApplicationState.RUNNING, appUrl1)));
    Mockito.when(appManager1.getApplicationReport(GetApplicationReportRequest.newInstance(appId3)))
        .thenReturn(GetApplicationReportResponse
            .newInstance(newApplicationReport(appId3, YarnApplicationState.ACCEPTED, null)));

    ApplicationClientProtocol appManager2 = Mockito.mock(ApplicationClientProtocol.class);
    Mockito.when(appManager2.getApplicationReport(GetApplicationReportRequest.newInstance(appId2)))
        .thenReturn(GetApplicationReportResponse
            .newInstance(newApplicationReport(appId2, YarnApplicationState.RUNNING, appUrl2)));
    Mockito.when(appManager2.getApplicationReport(GetApplicationReportRequest.newInstance(appId4)))
        .thenThrow(new ApplicationNotFoundException("APP NOT FOUND"));

    ApplicationHistoryProtocol historyManager = Mockito.mock(ApplicationHistoryProtocol.class);
    Mockito
        .when(historyManager.getApplicationReport(GetApplicationReportRequest.newInstance(appId4)))
        .thenReturn(GetApplicationReportResponse
            .newInstance(newApplicationReport(appId4, YarnApplicationState.FINISHED, null)));

    // Initial federation store.
    FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance();
    facade.getStateStore()
        .registerSubCluster(SubClusterRegisterRequest.newInstance(subClusterInfo1));
    facade.getStateStore()
        .registerSubCluster(SubClusterRegisterRequest.newInstance(subClusterInfo2));
    facade.addApplicationHomeSubCluster(
        ApplicationHomeSubCluster.newInstance(appId1, subClusterId1));
    facade.addApplicationHomeSubCluster(
        ApplicationHomeSubCluster.newInstance(appId2, subClusterId2));
    facade.addApplicationHomeSubCluster(
        ApplicationHomeSubCluster.newInstance(appId3, subClusterId1));
    facade.addApplicationHomeSubCluster(
        ApplicationHomeSubCluster.newInstance(appId4, subClusterId2));

    // Start proxy server
    WebAppProxyServerForTest proxy = new WebAppProxyServerForTest();
    proxy.init(conf);
    proxy.start();

    try {
      // set Mocked rm and timeline
      int proxyPort = proxy.proxy.proxyServer.getConnectorAddress(0).getPort();
      FedAppReportFetcher appReportFetcher = proxy.proxy.appReportFetcher;
      appReportFetcher.registerSubCluster(subClusterInfo1, appManager1);
      appReportFetcher.registerSubCluster(subClusterInfo2, appManager2);
      appReportFetcher.setHistoryManager(historyManager);

      // App1 is running in subcluster1, and original url is registered
      // in rm of subCluster1. So proxy server will get original url from rm by
      // getApplicationReport. Then proxy server will fetch the webapp directly.
      URL url = new URL("http", "localhost", proxyPort, "/proxy/" + appId1.toString());
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      assertEquals(AM_PREFIX + "/" + appId1.toString(), readResponse(conn));
      conn.disconnect();

      // App2 is running in subcluster2, and original url is registered
      // in rm of subCluster2. So proxy server will get original url from rm by
      // getApplicationReport. Then proxy server will fetch the webapp directly.
      url = new URL("http", "localhost", proxyPort, "/proxy/" + appId2.toString());
      conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      assertEquals(AM_PREFIX + "/" + appId2.toString(), readResponse(conn));
      conn.disconnect();

      // App3 is accepted in subcluster1, and original url is not registered
      // yet. So proxy server will fetch the application web from rm.
      url = new URL("http", "localhost", proxyPort, "/proxy/" + appId3.toString());
      conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      assertEquals(RM_PREFIX + "/" + appId3.toString(), readResponse(conn));
      conn.disconnect();

      // App4 is finished in subcluster2, and have removed from rm, but not
      // removed from timeline server. So proxy server will fetch the
      // application web from timeline server.
      url = new URL("http", "localhost", proxyPort, "/proxy/" + appId4.toString());
      conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      assertEquals(AHS_PREFIX + "/" + appId4.toString(), readResponse(conn));
      conn.disconnect();
    } finally {
      proxy.close();
    }
  }

  private ApplicationReport newApplicationReport(ApplicationId appId,
      YarnApplicationState state, String origTrackingUrl) {
    return ApplicationReport.newInstance(appId, null, "testuser", null, null, null, 0, null, state,
        null, null, 0, 0, 0, null, null, origTrackingUrl, 0f, null, null);
  }

  private String readResponse(HttpURLConnection conn) throws IOException {
    InputStream input = conn.getInputStream();
    byte[] bytes = new byte[input.available()];
    input.read(bytes);
    return new String(bytes);
  }

  private class WebAppProxyServerForTest extends CompositeService {

    private WebAppProxyForTest proxy = null;

    WebAppProxyServerForTest() {
      super(WebAppProxyServer.class.getName());
    }

    @Override
    protected synchronized void serviceInit(Configuration conf) throws Exception {
      proxy = new WebAppProxyForTest();
      addService(proxy);
      super.serviceInit(conf);
    }
  }

  /*
   * This servlet is used for simulate the web of AppMaster, ResourceManager,
   * TimelineServer and so on.
   * */
  public static class MockWebServlet extends HttpServlet {

    private String role;

    public MockWebServlet(String role) {
      this.role = role;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws IOException {
      if (req.getPathInfo() != null) {
        resp.getWriter().write(role + req.getPathInfo());
      }
      resp.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws IOException {
      InputStream is = req.getInputStream();
      OutputStream os = resp.getOutputStream();
      int c = is.read();
      while (c > -1) {
        os.write(c);
        c = is.read();
      }
      is.close();
      os.close();
      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }

  private class WebAppProxyForTest extends WebAppProxy {

    private HttpServer2 proxyServer;
    private FedAppReportFetcher appReportFetcher;

    @Override
    protected void serviceStart() throws Exception {
      Configuration conf = getConfig();
      String bindAddress = conf.get(YarnConfiguration.PROXY_ADDRESS);
      bindAddress = StringUtils.split(bindAddress, ':')[0];
      AccessControlList acl = new AccessControlList(conf.get(YarnConfiguration.YARN_ADMIN_ACL,
          YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
      proxyServer = new HttpServer2.Builder()
          .setName("proxy")
          .addEndpoint(URI.create(WebAppUtils.getHttpSchemePrefix(conf) + bindAddress + ":0"))
          .setFindPort(true)
          .setConf(conf)
          .setACL(acl)
          .build();
      proxyServer.addServlet(ProxyUriUtils.PROXY_SERVLET_NAME, ProxyUriUtils.PROXY_PATH_SPEC,
          WebAppProxyServlet.class);

      appReportFetcher = new FedAppReportFetcher(conf);
      proxyServer.setAttribute(FETCHER_ATTRIBUTE, appReportFetcher);
      proxyServer.setAttribute(IS_SECURITY_ENABLED_ATTRIBUTE, Boolean.FALSE);

      String proxy = WebAppUtils.getProxyHostAndPort(conf);
      String[] proxyParts = proxy.split(":");
      String proxyHost = proxyParts[0];

      proxyServer.setAttribute(PROXY_HOST_ATTRIBUTE, proxyHost);
      proxyServer.start();
      LOG.info("Proxy server is started at port {}", proxyServer.getConnectorAddress(0).getPort());
    }
  }
}
