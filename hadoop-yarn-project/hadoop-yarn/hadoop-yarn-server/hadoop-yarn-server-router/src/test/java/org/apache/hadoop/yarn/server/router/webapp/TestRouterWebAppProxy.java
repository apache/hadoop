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

package org.apache.hadoop.yarn.server.router.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
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
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.clientrm.FederationClientInterceptor;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService.RequestInterceptorChainWrapper;
import org.apache.hadoop.yarn.server.webproxy.FedAppReportFetcher;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TestRouterWebAppProxy {

  private static final Logger LOG = LoggerFactory.getLogger(TestRouterWebAppProxy.class);

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

  @Test(timeout=10000)
  public void testRouterWebAppProxyFed() throws Exception {

    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.ROUTER_WEBAPP_ADDRESS, "localhost:9090");
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
    String proxyAppUrl1 = "http://localhost:" + mockServerPort + "/proxy/" + appId1;
    String appUrl2 = "http://localhost:" + mockServerPort + "/amweb/" + appId2;
    String proxyAppUrl2 = "http://localhost:" + mockServerPort + "/proxy/" + appId2;
    // App3 is accepted application, has not registered original url to am.
    ApplicationId appId3 = ApplicationId.newInstance(0, 3);
    String proxyAppUrl3 = "http://localhost:" + mockServerPort + "/proxy/" + appId3;
    // App4 is finished application, has remove from rm, but not remove from timeline server.
    ApplicationId appId4 = ApplicationId.newInstance(0, 4);
    String proxyAppUrl4 = "http://localhost:" + mockServerPort + "/proxy/" + appId4;

    // Mock for application
    ApplicationClientProtocol appManager1 = mock(ApplicationClientProtocol.class);
    Mockito.when(appManager1.getApplicationReport(GetApplicationReportRequest.newInstance(appId1)))
        .thenReturn(GetApplicationReportResponse.newInstance(
            newApplicationReport(appId1, YarnApplicationState.RUNNING, proxyAppUrl1, appUrl1)));
    Mockito.when(appManager1.getApplicationReport(GetApplicationReportRequest.newInstance(appId3)))
        .thenReturn(GetApplicationReportResponse.newInstance(
            newApplicationReport(appId3, YarnApplicationState.ACCEPTED, proxyAppUrl2, null)));

    ApplicationClientProtocol appManager2 = mock(ApplicationClientProtocol.class);
    Mockito.when(appManager2.getApplicationReport(GetApplicationReportRequest.newInstance(appId2)))
        .thenReturn(GetApplicationReportResponse.newInstance(
            newApplicationReport(appId2, YarnApplicationState.RUNNING, proxyAppUrl3, appUrl2)));
    Mockito.when(appManager2.getApplicationReport(GetApplicationReportRequest.newInstance(appId4)))
        .thenThrow(new ApplicationNotFoundException("APP NOT FOUND"));

    ApplicationHistoryProtocol historyManager = mock(ApplicationHistoryProtocol.class);
    Mockito.when(
            historyManager.getApplicationReport(GetApplicationReportRequest.newInstance(appId4)))
        .thenReturn(GetApplicationReportResponse.newInstance(
            newApplicationReport(appId4, YarnApplicationState.FINISHED, proxyAppUrl4, null)));

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

    // Start router for test
    Router router = new Router();
    router.init(conf);
    router.start();
    String user = UserGroupInformation.getCurrentUser().getUserName();
    RequestInterceptorChainWrapper wrapper = mock(RequestInterceptorChainWrapper.class);
    FederationClientInterceptor interceptor = mock(FederationClientInterceptor.class);
    Mockito.when(interceptor.getApplicationReport(GetApplicationReportRequest.newInstance(appId1)))
        .thenReturn(GetApplicationReportResponse.newInstance(
            newApplicationReport(appId1, YarnApplicationState.RUNNING, proxyAppUrl1, appUrl1)));
    Mockito.when(interceptor.getApplicationReport(GetApplicationReportRequest.newInstance(appId2)))
        .thenReturn(GetApplicationReportResponse.newInstance(
            newApplicationReport(appId2, YarnApplicationState.RUNNING, proxyAppUrl2, appUrl2)));
    Mockito.when(interceptor.getApplicationReport(GetApplicationReportRequest.newInstance(appId3)))
        .thenReturn(GetApplicationReportResponse.newInstance(
            newApplicationReport(appId3, YarnApplicationState.ACCEPTED, proxyAppUrl3, null)));
    Mockito.when(interceptor.getApplicationReport(GetApplicationReportRequest.newInstance(appId4)))
        .thenReturn(GetApplicationReportResponse.newInstance(
            newApplicationReport(appId4, YarnApplicationState.FINISHED, proxyAppUrl4, null)));
    Mockito.when(wrapper.getRootInterceptor()).thenReturn(interceptor);
    router.getClientRMProxyService().getUserPipelineMap().put(user, wrapper);
    try {
      // set Mocked rm and timeline
      FedAppReportFetcher appReportFetcher = router.getFetcher();
      appReportFetcher.registerSubCluster(subClusterInfo1, appManager1);
      appReportFetcher.registerSubCluster(subClusterInfo2, appManager2);
      appReportFetcher.setHistoryManager(historyManager);

      // App1 is running in subcluster1, and original url is registered in rm of subCluster1.
      // So router will get original url from rm by getApplicationReport. Then router
      // will fetch the webapp directly.
      GetApplicationReportResponse response = router.getClientRMProxyService()
          .getApplicationReport(GetApplicationReportRequest.newInstance(appId1));
      URL url = new URL(response.getApplicationReport().getTrackingUrl());
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      assertEquals(AM_PREFIX + "/" + appId1, readResponse(conn));
      conn.disconnect();

      // App2 is running in subcluster2, and original url is registered
      // in rm of subCluster2. So router will get original url from rm by
      // getApplicationReport. Then router will fetch the webapp directly.
      response = router.getClientRMProxyService()
          .getApplicationReport(GetApplicationReportRequest.newInstance(appId2));
      url = new URL(response.getApplicationReport().getTrackingUrl());
      conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      assertEquals(AM_PREFIX + "/" + appId2, readResponse(conn));
      conn.disconnect();

      // App3 is accepted in subcluster1, and original url is not registered
      // yet. So router will fetch the application web from rm.
      response = router.getClientRMProxyService()
          .getApplicationReport(GetApplicationReportRequest.newInstance(appId3));
      url = new URL(response.getApplicationReport().getTrackingUrl());
      conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      assertEquals(RM_PREFIX + "/" + appId3, readResponse(conn));
      conn.disconnect();

      // App4 is finished in subcluster2, and have removed from rm, but not
      // removed from timeline server. So rouer will fetch the
      // application web from timeline server.
      response = router.getClientRMProxyService()
          .getApplicationReport(GetApplicationReportRequest.newInstance(appId4));
      url = new URL(response.getApplicationReport().getTrackingUrl());
      conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      assertEquals(AHS_PREFIX + "/" + appId4, readResponse(conn));
      conn.disconnect();
    } finally {
      router.close();
    }
  }

  private ApplicationReport newApplicationReport(ApplicationId appId, YarnApplicationState state,
                                                 String trackingUrl, String origTrackingUrl) {
    return ApplicationReport.newInstance(appId, null, "testuser", null, null, null, 0, null, state,
        null, trackingUrl, 0, 0, 0, null, null, origTrackingUrl, 0f, null, null);
  }

  private String readResponse(HttpURLConnection conn) throws IOException {
    InputStream input = conn.getInputStream();
    byte[] bytes = new byte[input.available()];
    input.read(bytes);
    return new String(bytes);
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
}