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

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.inject.Singleton;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.yarn.server.timeline.reader.ContainerLogsInfoListReader;
import org.apache.hadoop.yarn.server.timeline.reader.TimelineAboutReader;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.ContainerLogFileInfo;
import org.apache.hadoop.yarn.logaggregation.TestContainerLogsUtils;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryClientService;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryManagerOnTimelineStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.TestApplicationHistoryManagerOnTimelineStore;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.server.webapp.LogServlet;
import org.apache.hadoop.yarn.server.webapp.LogWebServiceUtils;
import org.apache.hadoop.yarn.server.webapp.YarnWebServiceParams;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfo;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;

public class TestAHSWebServices extends JerseyTestBase {

  private static ApplicationHistoryClientService historyClientService;
  private static AHSWebServices ahsWebservice;
  private static final String[] USERS = new String[]{"foo", "bar"};
  private static final int MAX_APPS = 6;
  private static Configuration conf;
  private static FileSystem fs;
  private static final String remoteLogRootDir = "target/logs/";
  private static final String rootLogDir = "target/LocalLogs";
  private static final String NM_WEBADDRESS = "test-nm-web-address:9999";
  private static final String NM_ID = "test:1234";
  private static HttpServletRequest request;

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(AHSWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(TestSimpleAuthFilter.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  private static class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      try {
        setupClass();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      bind(conf).to(Configuration.class).named("conf");
      bind(historyClientService).to(ApplicationBaseProtocol.class).named("appBaseProt");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      request = mock(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
      bind(request).to(HttpServletRequest.class);
      bind(ahsWebservice).to(AHSWebServices.class);
    }
  }

  public static void setupClass() throws Exception {
    conf = new YarnConfiguration();
    TimelineStore store =
        TestApplicationHistoryManagerOnTimelineStore.createStore(MAX_APPS);
    TimelineACLsManager aclsManager = new TimelineACLsManager(conf);
    aclsManager.setTimelineStore(store);
    TimelineDataManager dataManager =
        new TimelineDataManager(store, aclsManager);
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "foo");
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteLogRootDir);
    dataManager.init(conf);
    ApplicationACLsManager appAclsManager = new ApplicationACLsManager(conf);
    ApplicationHistoryManagerOnTimelineStore historyManager =
        new ApplicationHistoryManagerOnTimelineStore(dataManager, appAclsManager);
    historyManager.init(conf);
    historyClientService = new ApplicationHistoryClientService(historyManager) {
      @Override
      protected void serviceStart() throws Exception {
        // Do Nothing
      }
    };
    historyClientService.init(conf);
    historyClientService.start();

    ahsWebservice = new AHSWebServices(historyClientService, conf);
    LogServlet logServlet = spy(ahsWebservice.getLogServlet());
    doReturn(null).when(logServlet).getNMWebAddressFromRM(any());
    doReturn(NM_WEBADDRESS).when(logServlet).getNMWebAddressFromRM(NM_ID);
    ahsWebservice.setLogServlet(logServlet);

    fs = FileSystem.get(conf);
  }

  @AfterAll
  public static void tearDownClass() throws Exception {
    if (historyClientService != null) {
      historyClientService.stop();
    }
    fs.delete(new Path(remoteLogRootDir), true);
    fs.delete(new Path(rootLogDir), true);
  }

  public static Collection<Object[]> rounds() {
    return Arrays.asList(new Object[][]{{0}, {1}});
  }

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Singleton
  public static class TestSimpleAuthFilter extends AuthenticationFilter {
    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) throws ServletException {
      Properties properties =
          super.getConfiguration(configPrefix, filterConfig);
      properties.put(AuthenticationFilter.AUTH_TYPE, "simple");
      properties.put(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
      return properties;
    }
  }

  @MethodSource("rounds")
  @ParameterizedTest
  void testInvalidApp(int round) {
    ApplicationId appId = ApplicationId.newInstance(0, MAX_APPS + 1);
    WebTarget r = target();
    Response response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
            .path(appId.toString())
            .queryParam("user.name", USERS[round])
            .request(MediaType.APPLICATION_JSON)
            .get(Response.class);
    assertResponseStatusCode("404 not found expected",
        Response.Status.NOT_FOUND, response.getStatusInfo());
  }

  @MethodSource("rounds")
  @ParameterizedTest
  void testInvalidAttempt(int round) {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, MAX_APPS + 1);
    WebTarget r = target();
    when(request.getRemoteUser()).thenReturn(USERS[round]);
    Response response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
            .path(appId.toString()).path("appattempts")
            .path(appAttemptId.toString())
            .queryParam("user.name", USERS[round])
            .request(MediaType.APPLICATION_JSON)
            .get(Response.class);
    if (round == 1) {
      assertResponseStatusCode(Response.Status.FORBIDDEN, response.getStatusInfo());
      return;
    }
    assertResponseStatusCode("404 not found expected",
        Response.Status.NOT_FOUND, response.getStatusInfo());
  }

  @MethodSource("rounds")
  @ParameterizedTest
  void testInvalidContainer(int round) {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId,
        MAX_APPS + 1);
    WebTarget r = target();
    when(request.getRemoteUser()).thenReturn(USERS[round]);
    Response response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
            .path(appId.toString()).path("appattempts")
            .path(appAttemptId.toString()).path("containers")
            .path(containerId.toString())
            .queryParam("user.name", USERS[round])
            .request(MediaType.APPLICATION_JSON)
            .get(Response.class);
    if (round == 1) {
      assertResponseStatusCode(Response.Status.FORBIDDEN, response.getStatusInfo());
      return;
    }
    assertResponseStatusCode("404 not found expected",
            Response.Status.NOT_FOUND, response.getStatusInfo());
  }

  @MethodSource("rounds")
  @ParameterizedTest
  void testInvalidUri(int round) {
    WebTarget r = target();
    String responseStr = "";
    try {
      responseStr =
          r.path("ws").path("v1").path("applicationhistory").path("bogus")
              .queryParam("user.name", USERS[round])
              .request(MediaType.APPLICATION_JSON).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (NotFoundException ue) {
      Response response = ue.getResponse();
      assertResponseStatusCode(NOT_FOUND, response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @MethodSource("rounds")
  @ParameterizedTest
  public void testInvalidUri2(int round) {
    WebTarget r = target();
    String responseStr = "";
    try {
      responseStr = r.queryParam("user.name", USERS[round])
          .request(MediaType.APPLICATION_JSON).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (NotFoundException ue) {
      Response response = ue.getResponse();
      assertResponseStatusCode(NOT_FOUND, response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @MethodSource("rounds")
  @ParameterizedTest
  public void testInvalidAccept(int round) throws JSONException, Exception {
    WebTarget r = target();
    String responseStr = "";
    try {
      responseStr =
          r.path("ws").path("v1").path("applicationhistory")
              .queryParam("user.name", USERS[round])
              .request(MediaType.TEXT_PLAIN).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (ServiceUnavailableException ue) {
      Response response = ue.getResponse();
      assertResponseStatusCode(SERVICE_UNAVAILABLE,
          response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @MethodSource("rounds")
  @ParameterizedTest
  public void testAbout(int round) throws Exception {
    WebTarget r = target().register(TimelineAboutReader.class);
    Response response = r
        .path("ws").path("v1").path("applicationhistory").path("about")
        .queryParam("user.name", USERS[round])
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    TimelineAbout actualAbout = response.readEntity(TimelineAbout.class);
    TimelineAbout expectedAbout =
        TimelineUtils.createTimelineAbout("Generic History Service API");
    assertNotNull(
        actualAbout, "Timeline service about response is null");
    assertEquals(expectedAbout.getAbout(), actualAbout.getAbout());
    assertEquals(expectedAbout.getTimelineServiceVersion(),
        actualAbout.getTimelineServiceVersion());
    assertEquals(expectedAbout.getTimelineServiceBuildVersion(),
        actualAbout.getTimelineServiceBuildVersion());
    assertEquals(expectedAbout.getTimelineServiceVersionBuiltOn(),
        actualAbout.getTimelineServiceVersionBuiltOn());
    assertEquals(expectedAbout.getHadoopVersion(),
        actualAbout.getHadoopVersion());
    assertEquals(expectedAbout.getHadoopBuildVersion(),
        actualAbout.getHadoopBuildVersion());
    assertEquals(expectedAbout.getHadoopVersionBuiltOn(),
        actualAbout.getHadoopVersionBuiltOn());
  }

  @MethodSource("rounds")
  @ParameterizedTest
  void testAppsQuery(int round) throws Exception {
    WebTarget r = target();
    when(request.getRemoteUser()).thenReturn(USERS[round]);
    Response response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
            .queryParam("state", YarnApplicationState.FINISHED.toString())
            .queryParam("user.name", USERS[round])
            .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject apps = json.getJSONObject("apps");
    assertEquals(1, apps.length(), "incorrect number of elements");
    JSONArray array = apps.getJSONArray("app");
    assertEquals(MAX_APPS, array.length(), "incorrect number of elements");
  }

  @MethodSource("rounds")
  @ParameterizedTest
  public void testQueueQuery(int round) throws Exception {
    WebTarget r = target();
    Response response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
            .queryParam("queue", "test queue")
            .queryParam("user.name", USERS[round])
            .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertResponseStatusCode(OK, response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject apps = json.getJSONObject("apps");
    assertEquals(1, apps.length(), "incorrect number of elements");
    JSONArray array = apps.getJSONArray("app");
    assertEquals(MAX_APPS - 1,
        array.length(),
        "incorrect number of elements");
  }

  @MethodSource("rounds")
  @ParameterizedTest
  void testSingleApp(int round) throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    WebTarget r = target();
    when(request.getRemoteUser()).thenReturn(USERS[round]);
    Response response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
            .path(appId.toString())
            .queryParam("user.name", USERS[round])
            .request(MediaType.APPLICATION_JSON)
            .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject app = json.getJSONObject("app");
    assertEquals(appId.toString(), app.getString("appId"));
    assertEquals("test app", app.get("name"));
    assertEquals(round == 0 ? "test diagnostics info" : "",
        app.get("diagnosticsInfo"));
    assertEquals(Integer.MAX_VALUE + 1L, app.get("submittedTime"));
    assertEquals("test queue", app.get("queue"));
    assertEquals("user1", app.get("user"));
    assertEquals("test app type", app.get("type"));
    assertEquals(FinalApplicationStatus.UNDEFINED.toString(),
        app.get("finalAppStatus"));
    assertEquals(YarnApplicationState.FINISHED.toString(), app.get("appState"));
    assertNotNull(app.get("aggregateResourceAllocation"),
        "Aggregate resource allocation is null");
    assertNotNull(app.get("aggregatePreemptedResourceAllocation"),
        "Aggregate Preempted Resource Allocation is null");
  }

  @MethodSource("rounds")
  @ParameterizedTest
  void testMultipleAttempts(int round) throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    WebTarget r = target();
    when(request.getRemoteUser()).thenReturn(USERS[round]);
    Response response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
            .path(appId.toString()).path("appattempts")
            .queryParam("user.name", USERS[round])
            .request(MediaType.APPLICATION_JSON).get(Response.class);
    if (round == 1) {
      assertResponseStatusCode(FORBIDDEN, response.getStatusInfo());
      return;
    }
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject appAttempts = json.getJSONObject("appAttempts");
    assertEquals(1, appAttempts.length(), "incorrect number of elements");
    JSONArray array = appAttempts.getJSONArray("appAttempt");
    assertEquals(MAX_APPS, array.length(), "incorrect number of elements");
  }

  @MethodSource("rounds")
  @ParameterizedTest
  public void testSingleAttempt(int round) throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    WebTarget r = target();
    when(request.getRemoteUser()).thenReturn(USERS[round]);
    Response response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
            .path(appId.toString()).path("appattempts")
            .path(appAttemptId.toString())
            .queryParam("user.name", USERS[round])
            .request(MediaType.APPLICATION_JSON)
            .get(Response.class);
    if (round == 1) {
      assertResponseStatusCode(FORBIDDEN, response.getStatusInfo());
      return;
    }
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject appAttempt = json.getJSONObject("appAttempt");
    assertEquals(appAttemptId.toString(), appAttempt.getString("appAttemptId"));
    assertEquals("test host", appAttempt.getString("host"));
    assertEquals("test diagnostics info",
        appAttempt.getString("diagnosticsInfo"));
    assertEquals("test tracking url", appAttempt.getString("trackingUrl"));
    assertEquals(YarnApplicationAttemptState.FINISHED.toString(),
        appAttempt.get("appAttemptState"));
  }

  @MethodSource("rounds")
  @ParameterizedTest
  public void testMultipleContainers(int round) throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    WebTarget r = target();
    when(request.getRemoteUser()).thenReturn(USERS[round]);
    Response response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
        .path(appId.toString()).path("appattempts")
        .path(appAttemptId.toString()).path("containers")
        .queryParam("user.name", USERS[round])
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    if (round == 1) {
      assertResponseStatusCode(FORBIDDEN, response.getStatusInfo());
      return;
    }
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject containers = json.getJSONObject("containers");
    assertEquals(1, containers.length(), "incorrect number of elements");
    JSONArray array = containers.getJSONArray("container");
    assertEquals(MAX_APPS, array.length(), "incorrect number of elements");
  }

  @MethodSource("rounds")
  @ParameterizedTest
  void testSingleContainer(int round) throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 1);
    WebTarget r = target();
    when(request.getRemoteUser()).thenReturn(USERS[round]);
    Response response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
            .path(appId.toString()).path("appattempts")
            .path(appAttemptId.toString()).path("containers")
            .path(containerId.toString())
            .queryParam("user.name", USERS[round])
            .request(MediaType.APPLICATION_JSON)
            .get(Response.class);
    if (round == 1) {
      assertResponseStatusCode(FORBIDDEN, response.getStatusInfo());
      return;
    }
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject container = json.getJSONObject("container");
    assertEquals(containerId.toString(), container.getString("containerId"));
    assertEquals("test diagnostics info", container.getString("diagnosticsInfo"));
    assertEquals("-1", container.getString("allocatedMB"));
    assertEquals("-1", container.getString("allocatedVCores"));
    assertEquals(NodeId.newInstance("test host", 100).toString(),
        container.getString("assignedNodeId"));
    assertEquals("-1", container.getString("priority"));
    Configuration conf = new YarnConfiguration();
    assertEquals(WebAppUtils.getHttpSchemePrefix(conf) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(conf) +
        "/applicationhistory/logs/test host:100/container_0_0001_01_000001/" +
        "container_0_0001_01_000001/user1", container.getString("logUrl"));
    assertEquals(ContainerState.COMPLETE.toString(),
        container.getString("containerState"));
  }

  @MethodSource("rounds")
  @ParameterizedTest
  @Timeout(10000)
  void testContainerLogsForFinishedApps(int round) throws Exception {
    String fileName = "syslog";
    String user = "user1";
    NodeId nodeId = NodeId.newInstance("test host", 100);
    NodeId nodeId2 = NodeId.newInstance("host2", 1234);
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 1);
    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 1);
    ContainerId containerId100 = ContainerId.newContainerId(appAttemptId, 100);
    TestContainerLogsUtils.createContainerLogFileInRemoteFS(conf, fs,
        rootLogDir, appId, Collections.singletonMap(containerId1,
            "Hello." + containerId1),
        nodeId, fileName, user, true);
    TestContainerLogsUtils.createContainerLogFileInRemoteFS(conf, fs,
        rootLogDir, appId, Collections.singletonMap(containerId100,
            "Hello." + containerId100),
        nodeId2, fileName, user, false);
    // test whether we can find container log from remote diretory if
    // the containerInfo for this container could be fetched from AHS.
    WebTarget r = target();
    Response response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1.toString()).path(fileName)
        .queryParam("user.name", user)
        .request(MediaType.TEXT_PLAIN)
        .get(Response.class);
    String responseText = response.readEntity(String.class);
    assertTrue(responseText.contains("Hello." + containerId1));
    // Do the same test with new API
    r = target();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containers")
        .path(containerId1.toString()).path("logs").path(fileName)
        .queryParam("user.name", user)
        .request(MediaType.TEXT_PLAIN)
        .get(Response.class);
    responseText = response.readEntity(String.class);
    assertTrue(responseText.contains("Hello." + containerId1));
    // test whether we can find container log from remote directory if
    // the containerInfo for this container could not be fetched from AHS.
    r = target();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId100.toString()).path(fileName)
        .queryParam("user.name", user)
        .request(MediaType.TEXT_PLAIN)
        .get(Response.class);
    responseText = response.readEntity(String.class);
    assertTrue(responseText.contains("Hello." + containerId100));
    // Do the same test with new API
    r = target();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containers")
        .path(containerId100.toString()).path("logs").path(fileName)
        .queryParam("user.name", user)
        .request(MediaType.TEXT_PLAIN)
        .get(Response.class);
    responseText = response.readEntity(String.class);
    assertTrue(responseText.contains("Hello." + containerId100));
    // create an application which can not be found from AHS
    ApplicationId appId100 = ApplicationId.newInstance(0, 100);
    ApplicationAttemptId appAttemptId100 = ApplicationAttemptId.newInstance(
        appId100, 1);
    ContainerId containerId1ForApp100 = ContainerId.newContainerId(
        appAttemptId100, 1);
    TestContainerLogsUtils.createContainerLogFileInRemoteFS(conf, fs,
        rootLogDir, appId100,
        Collections.singletonMap(containerId1ForApp100,
            "Hello." + containerId1ForApp100),
        nodeId, fileName, user, true);
    r = target();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1ForApp100.toString()).path(fileName)
        .queryParam("user.name", user)
        .request(MediaType.TEXT_PLAIN)
        .get(Response.class);
    responseText = response.readEntity(String.class);
    assertTrue(responseText.contains("Hello." + containerId1ForApp100));
    int fullTextSize = responseText.getBytes().length;
    String tailEndSeparator = StringUtils.repeat("*",
        "End of LogType:syslog".length() + 50) + "\n\n";
    int tailTextSize = "\nEnd of LogType:syslog\n".getBytes().length
        + tailEndSeparator.getBytes().length;
    String logMessage = "Hello." + containerId1ForApp100;
    int fileContentSize = logMessage.getBytes().length;
    // specify how many bytes we should get from logs
    // if we specify a position number, it would get the first n bytes from
    // container log
    r = target();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1ForApp100.toString()).path(fileName)
        .queryParam("user.name", user)
        .queryParam("size", "5")
        .request(MediaType.TEXT_PLAIN)
        .get(Response.class);
    responseText = response.readEntity(String.class);
    assertEquals(responseText.getBytes().length,
        (fullTextSize - fileContentSize) + 5);
    assertTrue(fullTextSize >= responseText.getBytes().length);
    assertEquals(new String(responseText.getBytes(),
            (fullTextSize - fileContentSize - tailTextSize), 5),
        new String(logMessage.getBytes(), 0, 5));
    // specify how many bytes we should get from logs
    // if we specify a negative number, it would get the last n bytes from
    // container log
    r = target();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1ForApp100.toString()).path(fileName)
        .queryParam("user.name", user)
        .queryParam("size", "-5")
        .request(MediaType.TEXT_PLAIN)
        .get(Response.class);
    responseText = response.readEntity(String.class);
    assertEquals(responseText.getBytes().length,
        (fullTextSize - fileContentSize) + 5);
    assertTrue(fullTextSize >= responseText.getBytes().length);
    assertEquals(new String(responseText.getBytes(),
            (fullTextSize - fileContentSize - tailTextSize), 5),
        new String(logMessage.getBytes(), fileContentSize - 5, 5));
    // specify the bytes which is larger than the actual file size,
    // we would get the full logs
    r = target();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1ForApp100.toString()).path(fileName)
        .queryParam("user.name", user)
        .queryParam("size", "10000")
        .request(MediaType.TEXT_PLAIN)
        .get(Response.class);
    responseText = response.readEntity(String.class);
    assertThat(responseText.getBytes()).hasSize(fullTextSize);

    r = target();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1ForApp100.toString()).path(fileName)
        .queryParam("user.name", user)
        .queryParam("size", "-10000")
        .request(MediaType.TEXT_PLAIN)
        .get(Response.class);
    responseText = response.readEntity(String.class);
    assertThat(responseText.getBytes()).hasSize(fullTextSize);
  }

  @MethodSource("rounds")
  @ParameterizedTest
  @Timeout(10000)
  public void testContainerLogsForRunningApps(int round) throws Exception {
    String fileName = "syslog";
    String user = "user1";
    ApplicationId appId = ApplicationId.newInstance(
        1234, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 1);
    WebTarget r = target();
    when(request.getRemoteUser()).thenReturn(user);
    URI requestURI = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1.toString()).path(fileName)
        .queryParam("user.name", user).getUri();
    String redirectURL = getRedirectURL(requestURI.toString());
    assertTrue(redirectURL != null);
    assertTrue(redirectURL.contains("test:1234"));
    assertTrue(redirectURL.contains("ws/v1/node/containers"));
    assertTrue(redirectURL.contains(containerId1.toString()));
    assertTrue(redirectURL.contains("/logs/" + fileName));
    assertTrue(redirectURL.contains("user.name=" + user));

    // If we specify NM id, we would re-direct the request
    // to this NM's Web Address.
    requestURI = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1.toString()).path(fileName)
        .queryParam("user.name", user)
        .queryParam(YarnWebServiceParams.NM_ID, NM_ID)
        .getUri();
    redirectURL = getRedirectURL(requestURI.toString());
    assertTrue(redirectURL != null);
    assertTrue(redirectURL.contains(NM_WEBADDRESS));
    assertTrue(redirectURL.contains("ws/v1/node/containers"));
    assertTrue(redirectURL.contains(containerId1.toString()));
    assertTrue(redirectURL.contains("/logs/" + fileName));
    assertTrue(redirectURL.contains("user.name=" + user));

    // Test with new API
    requestURI = r.path("ws").path("v1")
        .path("applicationhistory").path("containers")
        .path(containerId1.toString()).path("logs").path(fileName)
        .queryParam("user.name", user).getUri();
    redirectURL = getRedirectURL(requestURI.toString());
    assertTrue(redirectURL != null);
    assertTrue(redirectURL.contains("test:1234"));
    assertTrue(redirectURL.contains("ws/v1/node/containers"));
    assertTrue(redirectURL.contains(containerId1.toString()));
    assertTrue(redirectURL.contains("/logs/" + fileName));
    assertTrue(redirectURL.contains("user.name=" + user));

    requestURI = r.path("ws").path("v1")
        .path("applicationhistory").path("containers")
        .path(containerId1.toString()).path("logs").path(fileName)
        .queryParam("user.name", user)
        .queryParam(YarnWebServiceParams.NM_ID, NM_ID)
        .getUri();
    redirectURL = getRedirectURL(requestURI.toString());
    assertTrue(redirectURL != null);
    assertTrue(redirectURL.contains(NM_WEBADDRESS));
    assertTrue(redirectURL.contains("ws/v1/node/containers"));
    assertTrue(redirectURL.contains(containerId1.toString()));
    assertTrue(redirectURL.contains("/logs/" + fileName));
    assertTrue(redirectURL.contains("user.name=" + user));

    // If we can not container information from ATS, we would try to
    // get aggregated log from remote FileSystem.
    ContainerId containerId1000 = ContainerId.newContainerId(
        appAttemptId, 1000);
    String content = "Hello." + containerId1000;
    NodeId nodeId = NodeId.newInstance("test host", 100);
    TestContainerLogsUtils.createContainerLogFileInRemoteFS(conf, fs,
        rootLogDir, appId, Collections.singletonMap(containerId1000, content),
        nodeId, fileName, user, true);
    r = target();
    Response response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1000.toString()).path(fileName)
        .queryParam("user.name", user)
        .request(MediaType.TEXT_PLAIN)
        .get(Response.class);
    String responseText = response.readEntity(String.class);
    assertTrue(responseText.contains(content));
    // Also test whether we output the empty local container log, and give
    // the warning message.
    assertTrue(responseText.contains("LogAggregationType: "
        + ContainerLogAggregationType.LOCAL));
    assertTrue(
        responseText.contains(LogWebServiceUtils.getNoRedirectWarning()));

    // If we can not container information from ATS, and we specify the NM id,
    // but we can not get nm web address, we would still try to
    // get aggregated log from remote FileSystem.
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1000.toString()).path(fileName)
        .queryParam(YarnWebServiceParams.NM_ID, "invalid-nm:1234")
        .queryParam("user.name", user)
        .request(MediaType.TEXT_PLAIN)
        .get(Response.class);
    responseText = response.readEntity(String.class);
    assertTrue(responseText.contains(content));
    assertTrue(responseText.contains("LogAggregationType: "
        + ContainerLogAggregationType.LOCAL));
    assertTrue(
        responseText.contains(LogWebServiceUtils.getNoRedirectWarning()));

    // If this is the redirect request, we would not re-direct the request
    // back and get the aggregated logs.
    String content1 = "Hello." + containerId1;
    NodeId nodeId1 = NodeId.fromString(NM_ID);
    TestContainerLogsUtils.createContainerLogFileInRemoteFS(conf, fs,
        rootLogDir, appId, Collections.singletonMap(containerId1, content1),
        nodeId1, fileName, user, true);
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containers")
        .path(containerId1.toString()).path("logs").path(fileName)
        .queryParam("user.name", user)
        .queryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE, "true")
        .request(MediaType.TEXT_PLAIN).get(Response.class);
    responseText = response.readEntity(String.class);
    assertTrue(responseText.contains(content1));
    assertTrue(responseText.contains("LogAggregationType: "
        + ContainerLogAggregationType.AGGREGATED));
  }

  @MethodSource("rounds")
  @ParameterizedTest
  @Timeout(10000)
  public void testContainerLogsMetaForRunningApps(int round) throws Exception {
    String user = "user1";
    ApplicationId appId = ApplicationId.newInstance(
        1234, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 1);
    WebTarget r = target().register(ContainerLogsInfoListReader.class);

    // If we specify the NMID, we re-direct the request by using
    // the NM's web address
    URI requestURI = r.path("ws").path("v1")
        .path("applicationhistory").path("containers")
        .path(containerId1.toString()).path("logs")
        .queryParam("user.name", user)
        .queryParam(YarnWebServiceParams.NM_ID, NM_ID)
        .getUri();
    String redirectURL = getRedirectURL(requestURI.toString());
    assertTrue(redirectURL != null);
    assertTrue(redirectURL.contains(NM_WEBADDRESS));
    assertTrue(redirectURL.contains("ws/v1/node/containers"));
    assertTrue(redirectURL.contains(containerId1.toString()));
    assertTrue(redirectURL.contains("/logs"));

    // If we do not specify the NodeId but can get Container information
    // from ATS, we re-direct the request to the node manager
    // who runs the container.
    requestURI = r.path("ws").path("v1")
        .path("applicationhistory").path("containers")
        .path(containerId1.toString()).path("logs")
        .queryParam("user.name", user).getUri();
    redirectURL = getRedirectURL(requestURI.toString());
    assertTrue(redirectURL != null);
    assertTrue(redirectURL.contains("test:1234"));
    assertTrue(redirectURL.contains("ws/v1/node/containers"));
    assertTrue(redirectURL.contains(containerId1.toString()));
    assertTrue(redirectURL.contains("/logs"));

    // If we can not container information from ATS,
    // and not specify nodeId,
    // we would try to get aggregated log meta from remote FileSystem.
    ContainerId containerId1000 = ContainerId.newContainerId(
        appAttemptId, 1000);
    String fileName = "syslog";
    String content = "Hello." + containerId1000;
    NodeId nodeId = NodeId.newInstance("test host", 100);
    TestContainerLogsUtils.createContainerLogFileInRemoteFS(conf, fs,
        rootLogDir, appId, Collections.singletonMap(containerId1000, content),
        nodeId, fileName, user, true);
    when(request.getRemoteUser()).thenReturn(user);
    Response response = r.path("ws").path("v1")
        .path("applicationhistory").path("containers")
        .path(containerId1000.toString()).path("logs")
        .queryParam("user.name", user)
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);

    List<ContainerLogsInfo> responseText = response.readEntity(new GenericType<
        List<ContainerLogsInfo>>(){
    });
    assertTrue(responseText.size() == 2);

    for (ContainerLogsInfo logInfo : responseText) {
      if (logInfo.getLogType().equals(
          ContainerLogAggregationType.AGGREGATED.toString())) {
        List<ContainerLogFileInfo> logMeta = logInfo
            .getContainerLogsInfo();
        assertTrue(logMeta.size() == 1);
        assertThat(logMeta.get(0).getFileName()).isEqualTo(fileName);
        assertThat(logMeta.get(0).getFileSize()).isEqualTo(String.valueOf(
            content.length()));
      } else {
        assertEquals(logInfo.getLogType(),
            ContainerLogAggregationType.LOCAL.toString());
      }
    }

    // If we can not container information from ATS,
    // and we specify NM id, but can not find NM WebAddress for this nodeId,
    // we would still try to get aggregated log meta from remote FileSystem.
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containers")
        .path(containerId1000.toString()).path("logs")
        .queryParam(YarnWebServiceParams.NM_ID, "invalid-nm:1234")
        .queryParam("user.name", user)
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);

    responseText = response.readEntity(new GenericType<
        List<ContainerLogsInfo>>(){
    });

    assertTrue(responseText.size() == 2);

    for (ContainerLogsInfo logInfo : responseText) {
      if (logInfo.getLogType().equals(
          ContainerLogAggregationType.AGGREGATED.toString())) {
        List<ContainerLogFileInfo> logMeta = logInfo
            .getContainerLogsInfo();
        assertTrue(logMeta.size() == 1);
        assertThat(logMeta.get(0).getFileName()).isEqualTo(fileName);
        assertThat(logMeta.get(0).getFileSize()).isEqualTo(String.valueOf(
            content.length()));
      } else {
        assertThat(logInfo.getLogType()).isEqualTo(
            ContainerLogAggregationType.LOCAL.toString());
      }
    }
  }

  @MethodSource("rounds")
  @ParameterizedTest
  @Timeout(10000)
  void testContainerLogsMetaForFinishedApps(int round) throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 1);
    String fileName = "syslog";
    String user = "user1";
    String content = "Hello." + containerId1;
    NodeId nodeId = NodeId.newInstance("test host", 100);
    TestContainerLogsUtils.createContainerLogFileInRemoteFS(conf, fs,
        rootLogDir, appId, Collections.singletonMap(containerId1, content),
        nodeId, fileName, user, true);

    WebTarget r = target().register(ContainerLogsInfoListReader.class);
    Response response = r.path("ws").path("v1")
        .path("applicationhistory").path("containers")
        .path(containerId1.toString()).path("logs")
        .queryParam("user.name", user)
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    List<ContainerLogsInfo> responseText = response.readEntity(new GenericType<
        List<ContainerLogsInfo>>(){
    });
    assertTrue(responseText.size() == 1);
    assertEquals(responseText.get(0).getLogType(),
        ContainerLogAggregationType.AGGREGATED.toString());
    List<ContainerLogFileInfo> logMeta = responseText.get(0)
        .getContainerLogsInfo();
    assertTrue(logMeta.size() == 1);
    assertThat(logMeta.get(0).getFileName()).isEqualTo(fileName);
    assertThat(logMeta.get(0).getFileSize()).isEqualTo(
        String.valueOf(content.length()));
  }

  private static String getRedirectURL(String url) {
    String redirectUrl = null;
    try {
      HttpURLConnection conn = (HttpURLConnection) new URL(url)
          .openConnection();
      // do not automatically follow the redirection
      // otherwise we get too many redirections exception
      conn.setInstanceFollowRedirects(false);
      if (conn.getResponseCode() == HttpServletResponse.SC_TEMPORARY_REDIRECT) {
        redirectUrl = conn.getHeaderField("Location");
        String queryParams = getQueryParams(url);
        if (queryParams != null && !queryParams.isEmpty()) {
          redirectUrl = appendQueryParams(redirectUrl, queryParams);
        }
      }
    } catch (Exception e) {
      // throw new RuntimeException(e);
    }
    return redirectUrl;
  }

  private static String getQueryParams(String url) {
    try {
      URL u = new URL(url);
      String query = u.getQuery();
      return query != null ? query : "";
    } catch (Exception e) {
      e.printStackTrace();
      return "";
    }
  }

  private static String appendQueryParams(String url, String queryParams) {
    if (url == null || queryParams == null || queryParams.isEmpty()) {
      return url;
    }
    return url + (url.contains("?") ? "&" : "?") + queryParams;
  }
}
