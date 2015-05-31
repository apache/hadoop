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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryClientService;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryManagerOnTimelineStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.TestApplicationHistoryManagerOnTimelineStore;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.servlet.GuiceServletContextListener;
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
  private static final String[] USERS = new String[] { "foo" , "bar" };
  private static final int MAX_APPS = 5;

  @BeforeClass
  public static void setupClass() throws Exception {
    Configuration conf = new YarnConfiguration();
    TimelineStore store =
        TestApplicationHistoryManagerOnTimelineStore.createStore(MAX_APPS);
    TimelineACLsManager aclsManager = new TimelineACLsManager(conf);
    TimelineDataManager dataManager =
        new TimelineDataManager(store, aclsManager);
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "foo");
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
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (historyClientService != null) {
      historyClientService.stop();
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> rounds() {
    return Arrays.asList(new Object[][] { { 0 }, { 1 } });
  }

  private Injector injector = Guice.createInjector(new ServletModule() {

    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(AHSWebServices.class);
      bind(GenericExceptionHandler.class);
      bind(ApplicationBaseProtocol.class).toInstance(historyClientService);
      serve("/*").with(GuiceContainer.class);
      filter("/*").through(TestSimpleAuthFilter.class);
    }
  });

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

  public class GuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
      return injector;
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
    assertEquals("404 not found expected", Status.NOT_FOUND,
        response.getClientResponseStatus());
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
      assertEquals(Status.FORBIDDEN, response.getClientResponseStatus());
      return;
    }
    assertEquals("404 not found expected", Status.NOT_FOUND,
            response.getClientResponseStatus());
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
      assertEquals(
          Status.FORBIDDEN, response.getClientResponseStatus());
      return;
    }
    assertEquals("404 not found expected", Status.NOT_FOUND,
            response.getClientResponseStatus());
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
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());

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
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
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
      assertEquals(Status.INTERNAL_SERVER_ERROR,
        response.getClientResponseStatus());
      WebServicesTestUtils.checkStringMatch(
        "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testAppsQuery() throws Exception {
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
          .queryParam("state", YarnApplicationState.FINISHED.toString())
          .queryParam("user.name", USERS[round])
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
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
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
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
      assertEquals(
          Status.FORBIDDEN, response.getClientResponseStatus());
      return;
    }
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
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
      assertEquals(
          Status.FORBIDDEN, response.getClientResponseStatus());
      return;
    }
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
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
      assertEquals(
          Status.FORBIDDEN, response.getClientResponseStatus());
      return;
    }
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
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
      assertEquals(
          Status.FORBIDDEN, response.getClientResponseStatus());
      return;
    }
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
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
}
