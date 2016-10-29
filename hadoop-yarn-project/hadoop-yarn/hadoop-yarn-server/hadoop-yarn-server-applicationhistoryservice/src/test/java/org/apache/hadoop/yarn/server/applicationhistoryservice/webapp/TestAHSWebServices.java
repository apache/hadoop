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

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryClientService;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryManagerOnTimelineStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.TestApplicationHistoryManagerOnTimelineStore;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.inject.Guice;
import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

@RunWith(Parameterized.class)
public class TestAHSWebServices extends JerseyTestBase {

  private static ApplicationHistoryClientService historyClientService;
  private static AHSWebServices ahsWebservice;
  private static final String[] USERS = new String[] { "foo" , "bar" };
  private static final int MAX_APPS = 5;
  private static Configuration conf;
  private static FileSystem fs;
  private static final String remoteLogRootDir = "target/logs/";
  private static final String rootLogDir = "target/LocalLogs";

  @BeforeClass
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
    fs = FileSystem.get(conf);
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (historyClientService != null) {
      historyClientService.stop();
    }
    fs.delete(new Path(remoteLogRootDir), true);
    fs.delete(new Path(rootLogDir), true);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> rounds() {
    return Arrays.asList(new Object[][] { { 0 }, { 1 } });
  }

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(AHSWebServices.class).toInstance(ahsWebservice);;
      bind(GenericExceptionHandler.class);
      bind(ApplicationBaseProtocol.class).toInstance(historyClientService);
      serve("/*").with(GuiceContainer.class);
      filter("/*").through(TestSimpleAuthFilter.class);
    }
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
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

  private int round;

  public TestAHSWebServices(int round) {
    super(new WebAppDescriptor.Builder(
      "org.apache.hadoop.yarn.server.applicationhistoryservice.webapp")
      .contextListenerClass(GuiceServletConfig.class)
      .filterClass(com.google.inject.servlet.GuiceFilter.class)
      .contextPath("jersey-guice-filter").servletPath("/").build());
    this.round = round;
  }

  @Test
  public void testInvalidApp() {
    ApplicationId appId = ApplicationId.newInstance(0, MAX_APPS + 1);
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
          .path(appId.toString())
          .queryParam("user.name", USERS[round])
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
    assertResponseStatusCode("404 not found expected",
        Status.NOT_FOUND, response.getStatusInfo());
  }

  @Test
  public void testInvalidAttempt() {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, MAX_APPS + 1);
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
          .path(appId.toString()).path("appattempts")
          .path(appAttemptId.toString())
          .queryParam("user.name", USERS[round])
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
    if (round == 1) {
      assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());
      return;
    }
    assertResponseStatusCode("404 not found expected",
        Status.NOT_FOUND, response.getStatusInfo());
  }

  @Test
  public void testInvalidContainer() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId,
        MAX_APPS + 1);
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
          .path(appId.toString()).path("appattempts")
          .path(appAttemptId.toString()).path("containers")
          .path(containerId.toString())
          .queryParam("user.name", USERS[round])
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
    if (round == 1) {
      assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());
      return;
    }
    assertResponseStatusCode("404 not found expected",
        Status.NOT_FOUND, response.getStatusInfo());
  }

  @Test
  public void testInvalidUri() throws JSONException, Exception {
    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr =
          r.path("ws").path("v1").path("applicationhistory").path("bogus")
            .queryParam("user.name", USERS[round])
            .accept(MediaType.APPLICATION_JSON).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());

      WebServicesTestUtils.checkStringMatch(
        "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testInvalidUri2() throws JSONException, Exception {
    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr = r.queryParam("user.name", USERS[round])
          .accept(MediaType.APPLICATION_JSON).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch(
        "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testInvalidAccept() throws JSONException, Exception {
    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr =
          r.path("ws").path("v1").path("applicationhistory")
            .queryParam("user.name", USERS[round])
            .accept(MediaType.TEXT_PLAIN).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.INTERNAL_SERVER_ERROR,
          response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch(
        "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testAbout() throws Exception {
    WebResource r = resource();
    ClientResponse response = r
        .path("ws").path("v1").path("applicationhistory").path("about")
        .queryParam("user.name", USERS[round])
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    TimelineAbout actualAbout = response.getEntity(TimelineAbout.class);
    TimelineAbout expectedAbout =
        TimelineUtils.createTimelineAbout("Generic History Service API");
    Assert.assertNotNull(
        "Timeline service about response is null", actualAbout);
    Assert.assertEquals(expectedAbout.getAbout(), actualAbout.getAbout());
    Assert.assertEquals(expectedAbout.getTimelineServiceVersion(),
        actualAbout.getTimelineServiceVersion());
    Assert.assertEquals(expectedAbout.getTimelineServiceBuildVersion(),
        actualAbout.getTimelineServiceBuildVersion());
    Assert.assertEquals(expectedAbout.getTimelineServiceVersionBuiltOn(),
        actualAbout.getTimelineServiceVersionBuiltOn());
    Assert.assertEquals(expectedAbout.getHadoopVersion(),
        actualAbout.getHadoopVersion());
    Assert.assertEquals(expectedAbout.getHadoopBuildVersion(),
        actualAbout.getHadoopBuildVersion());
    Assert.assertEquals(expectedAbout.getHadoopVersionBuiltOn(),
        actualAbout.getHadoopVersionBuiltOn());
  }

  @Test
  public void testAppsQuery() throws Exception {
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
          .queryParam("state", YarnApplicationState.FINISHED.toString())
          .queryParam("user.name", USERS[round])
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 5, array.length());
  }

  @Test
  public void testSingleApp() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
          .path(appId.toString())
          .queryParam("user.name", USERS[round])
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject app = json.getJSONObject("app");
    assertEquals(appId.toString(), app.getString("appId"));
    assertEquals("test app", app.get("name"));
    assertEquals(round == 0 ? "test diagnostics info" : "",
        app.get("diagnosticsInfo"));
    assertEquals("test queue", app.get("queue"));
    assertEquals("user1", app.get("user"));
    assertEquals("test app type", app.get("type"));
    assertEquals(FinalApplicationStatus.UNDEFINED.toString(),
      app.get("finalAppStatus"));
    assertEquals(YarnApplicationState.FINISHED.toString(), app.get("appState"));
  }

  @Test
  public void testMultipleAttempts() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
          .path(appId.toString()).path("appattempts")
          .queryParam("user.name", USERS[round])
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    if (round == 1) {
      assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());
      return;
    }
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject appAttempts = json.getJSONObject("appAttempts");
    assertEquals("incorrect number of elements", 1, appAttempts.length());
    JSONArray array = appAttempts.getJSONArray("appAttempt");
    assertEquals("incorrect number of elements", 5, array.length());
  }

  @Test
  public void testSingleAttempt() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
          .path(appId.toString()).path("appattempts")
          .path(appAttemptId.toString())
          .queryParam("user.name", USERS[round])
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
    if (round == 1) {
      assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());
      return;
    }
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject appAttempt = json.getJSONObject("appAttempt");
    assertEquals(appAttemptId.toString(), appAttempt.getString("appAttemptId"));
    assertEquals("test host", appAttempt.getString("host"));
    assertEquals("test diagnostics info",
      appAttempt.getString("diagnosticsInfo"));
    assertEquals("test tracking url", appAttempt.getString("trackingUrl"));
    assertEquals(YarnApplicationAttemptState.FINISHED.toString(),
      appAttempt.get("appAttemptState"));
  }

  @Test
  public void testMultipleContainers() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
          .path(appId.toString()).path("appattempts")
          .path(appAttemptId.toString()).path("containers")
          .queryParam("user.name", USERS[round])
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    if (round == 1) {
      assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());
      return;
    }
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject containers = json.getJSONObject("containers");
    assertEquals("incorrect number of elements", 1, containers.length());
    JSONArray array = containers.getJSONArray("container");
    assertEquals("incorrect number of elements", 5, array.length());
  }

  @Test
  public void testSingleContainer() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 1);
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
          .path(appId.toString()).path("appattempts")
          .path(appAttemptId.toString()).path("containers")
          .path(containerId.toString())
          .queryParam("user.name", USERS[round])
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
    if (round == 1) {
      assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());
      return;
    }
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
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

  @Test(timeout = 10000)
  public void testContainerLogsForFinishedApps() throws Exception {
    String fileName = "syslog";
    String user = "user1";
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("user1");
    NodeId nodeId = NodeId.newInstance("test host", 100);
    NodeId nodeId2 = NodeId.newInstance("host2", 1234);
    //prepare the logs for remote directory
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    // create local logs
    List<String> rootLogDirList = new ArrayList<String>();
    rootLogDirList.add(rootLogDir);
    Path rootLogDirPath = new Path(rootLogDir);
    if (fs.exists(rootLogDirPath)) {
      fs.delete(rootLogDirPath, true);
    }
    assertTrue(fs.mkdirs(rootLogDirPath));

    Path appLogsDir = new Path(rootLogDirPath, appId.toString());
    if (fs.exists(appLogsDir)) {
      fs.delete(appLogsDir, true);
    }
    assertTrue(fs.mkdirs(appLogsDir));

    // create container logs in local log file dir
    // create two container log files. We can get containerInfo
    // for container1 from AHS, but can not get such info for
    // container100
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 1);
    ContainerId containerId100 = ContainerId.newContainerId(appAttemptId, 100);
    createContainerLogInLocalDir(appLogsDir, containerId1, fs, fileName,
        ("Hello." + containerId1));
    createContainerLogInLocalDir(appLogsDir, containerId100, fs, fileName,
        ("Hello." + containerId100));

    // upload container logs to remote log dir
    Path path = new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR) +
        user + "/logs/" + appId.toString());
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
    assertTrue(fs.mkdirs(path));
    uploadContainerLogIntoRemoteDir(ugi, conf, rootLogDirList, nodeId,
        containerId1, path, fs);
    uploadContainerLogIntoRemoteDir(ugi, conf, rootLogDirList, nodeId2,
        containerId100, path, fs);

    // test whether we can find container log from remote diretory if
    // the containerInfo for this container could be fetched from AHS.
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1.toString()).path(fileName)
        .queryParam("user.name", user)
        .accept(MediaType.TEXT_PLAIN)
        .get(ClientResponse.class);
    String responseText = response.getEntity(String.class);
    assertTrue(responseText.contains("Hello." + containerId1));

    // test whether we can find container log from remote diretory if
    // the containerInfo for this container could not be fetched from AHS.
    r = resource();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId100.toString()).path(fileName)
        .queryParam("user.name", user)
        .accept(MediaType.TEXT_PLAIN)
        .get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    assertTrue(responseText.contains("Hello." + containerId100));

    // create an application which can not be found from AHS
    ApplicationId appId100 = ApplicationId.newInstance(0, 100);
    appLogsDir = new Path(rootLogDirPath, appId100.toString());
    if (fs.exists(appLogsDir)) {
      fs.delete(appLogsDir, true);
    }
    assertTrue(fs.mkdirs(appLogsDir));
    ApplicationAttemptId appAttemptId100 =
        ApplicationAttemptId.newInstance(appId100, 1);
    ContainerId containerId1ForApp100 = ContainerId
        .newContainerId(appAttemptId100, 1);
    createContainerLogInLocalDir(appLogsDir, containerId1ForApp100, fs,
        fileName, ("Hello." + containerId1ForApp100));
    path = new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR) +
        user + "/logs/" + appId100.toString());
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
    assertTrue(fs.mkdirs(path));
    uploadContainerLogIntoRemoteDir(ugi, conf, rootLogDirList, nodeId2,
        containerId1ForApp100, path, fs);
    r = resource();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1ForApp100.toString()).path(fileName)
        .queryParam("user.name", user)
        .accept(MediaType.TEXT_PLAIN)
        .get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    assertTrue(responseText.contains("Hello." + containerId1ForApp100));
    int fullTextSize = responseText.getBytes().length;
    int tailTextSize = "\nEnd of LogType:syslog\n".getBytes().length;

    String logMessage = "Hello." + containerId1ForApp100;
    int fileContentSize = logMessage.getBytes().length;
    // specify how many bytes we should get from logs
    // if we specify a position number, it would get the first n bytes from
    // container log
    r = resource();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1ForApp100.toString()).path(fileName)
        .queryParam("user.name", user)
        .queryParam("size", "5")
        .accept(MediaType.TEXT_PLAIN)
        .get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    assertEquals(responseText.getBytes().length,
        (fullTextSize - fileContentSize) + 5);
    assertTrue(fullTextSize >= responseText.getBytes().length);
    assertEquals(new String(responseText.getBytes(),
        (fullTextSize - fileContentSize - tailTextSize), 5),
        new String(logMessage.getBytes(), 0, 5));

    // specify how many bytes we should get from logs
    // if we specify a negative number, it would get the last n bytes from
    // container log
    r = resource();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1ForApp100.toString()).path(fileName)
        .queryParam("user.name", user)
        .queryParam("size", "-5")
        .accept(MediaType.TEXT_PLAIN)
        .get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    assertEquals(responseText.getBytes().length,
        (fullTextSize - fileContentSize) + 5);
    assertTrue(fullTextSize >= responseText.getBytes().length);
    assertEquals(new String(responseText.getBytes(),
        (fullTextSize - fileContentSize - tailTextSize), 5),
        new String(logMessage.getBytes(), fileContentSize - 5, 5));

    // specify the bytes which is larger than the actual file size,
    // we would get the full logs
    r = resource();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1ForApp100.toString()).path(fileName)
        .queryParam("user.name", user)
        .queryParam("size", "10000")
        .accept(MediaType.TEXT_PLAIN)
        .get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    assertEquals(responseText.getBytes().length, fullTextSize);

    r = resource();
    response = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1ForApp100.toString()).path(fileName)
        .queryParam("user.name", user)
        .queryParam("size", "-10000")
        .accept(MediaType.TEXT_PLAIN)
        .get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    assertEquals(responseText.getBytes().length, fullTextSize);
  }

  private static void createContainerLogInLocalDir(Path appLogsDir,
      ContainerId containerId, FileSystem fs, String fileName, String content)
      throws Exception {
    Path containerLogsDir = new Path(appLogsDir, containerId.toString());
    if (fs.exists(containerLogsDir)) {
      fs.delete(containerLogsDir, true);
    }
    assertTrue(fs.mkdirs(containerLogsDir));
    Writer writer =
        new FileWriter(new File(containerLogsDir.toString(), fileName));
    writer.write(content);
    writer.close();
  }

  private static void uploadContainerLogIntoRemoteDir(UserGroupInformation ugi,
      Configuration configuration, List<String> rootLogDirs, NodeId nodeId,
      ContainerId containerId, Path appDir, FileSystem fs) throws Exception {
    Path path =
        new Path(appDir, LogAggregationUtils.getNodeString(nodeId));
    AggregatedLogFormat.LogWriter writer =
        new AggregatedLogFormat.LogWriter(configuration, path, ugi);
    writer.writeApplicationOwner(ugi.getUserName());

    writer.append(new AggregatedLogFormat.LogKey(containerId),
        new AggregatedLogFormat.LogValue(rootLogDirs, containerId,
        ugi.getShortUserName()));
    writer.close();
  }

  @Test(timeout = 10000)
  public void testContainerLogsForRunningApps() throws Exception {
    String fileName = "syslog";
    String user = "user1";
    ApplicationId appId = ApplicationId.newInstance(
        1234, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 1);
    WebResource r = resource();
    URI requestURI = r.path("ws").path("v1")
        .path("applicationhistory").path("containerlogs")
        .path(containerId1.toString()).path(fileName)
        .queryParam("user.name", user).getURI();
    String redirectURL = getRedirectURL(requestURI.toString());
    assertTrue(redirectURL != null);
    assertTrue(redirectURL.contains("test:1234"));
    assertTrue(redirectURL.contains("ws/v1/node/containers"));
    assertTrue(redirectURL.contains(containerId1.toString()));
    assertTrue(redirectURL.contains("/logs/" + fileName));
    assertTrue(redirectURL.contains("user.name=" + user));
  }

  private static String getRedirectURL(String url) {
    String redirectUrl = null;
    try {
      HttpURLConnection conn = (HttpURLConnection) new URL(url)
          .openConnection();
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
}
