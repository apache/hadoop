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

import javax.ws.rs.core.MediaType;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ApplicationContext;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryManager;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.MemoryApplicationHistoryStore;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestAHSWebServices extends JerseyTest {

  private static ApplicationHistoryManager ahManager;

  private Injector injector = Guice.createInjector(new ServletModule() {

    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(AHSWebServices.class);
      bind(GenericExceptionHandler.class);
      try {
        ahManager = mockApplicationHistoryManager();
      } catch (Exception e) {
        Assert.fail();
      }
      bind(ApplicationContext.class).toInstance(ahManager);
      serve("/*").with(GuiceContainer.class);
    }
  });

  public class GuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  private ApplicationHistoryManager mockApplicationHistoryManager()
      throws Exception {
    ApplicationHistoryStore store = new MemoryApplicationHistoryStore();
    TestAHSWebApp testAHSWebApp = new TestAHSWebApp();
    testAHSWebApp.setApplicationHistoryStore(store);
    ApplicationHistoryManager ahManager =
        testAHSWebApp.mockApplicationHistoryManager(5, 5, 5);
    return ahManager;
  }

  public TestAHSWebServices() {
    super(new WebAppDescriptor.Builder(
      "org.apache.hadoop.yarn.server.applicationhistoryservice.webapp")
      .contextListenerClass(GuiceServletConfig.class)
      .filterClass(com.google.inject.servlet.GuiceFilter.class)
      .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testInvalidUri() throws JSONException, Exception {
    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr =
          r.path("ws").path("v1").path("applicationhistory").path("bogus")
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
      responseStr = r.accept(MediaType.APPLICATION_JSON).get(String.class);
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
          .path(appId.toString()).accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject app = json.getJSONObject("app");
    assertEquals(appId.toString(), app.getString("appId"));
    assertEquals(appId.toString(), app.get("name"));
    assertEquals(appId.toString(), app.get("diagnosticsInfo"));
    assertEquals("test queue", app.get("queue"));
    assertEquals("test user", app.get("user"));
    assertEquals("test type", app.get("type"));
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
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
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
          .path(appAttemptId.toString()).accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject appAttempt = json.getJSONObject("appAttempt");
    assertEquals(appAttemptId.toString(), appAttempt.getString("appAttemptId"));
    assertEquals(appAttemptId.toString(), appAttempt.getString("host"));
    assertEquals(appAttemptId.toString(),
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
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
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
    ContainerId containerId = ContainerId.newInstance(appAttemptId, 1);
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("applicationhistory").path("apps")
          .path(appId.toString()).path("appattempts")
          .path(appAttemptId.toString()).path("containers")
          .path(containerId.toString()).accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject container = json.getJSONObject("container");
    assertEquals(containerId.toString(), container.getString("containerId"));
    assertEquals(containerId.toString(), container.getString("diagnosticsInfo"));
    assertEquals("0", container.getString("allocatedMB"));
    assertEquals("0", container.getString("allocatedVCores"));
    assertEquals(NodeId.newInstance("localhost", 0).toString(),
      container.getString("assignedNodeId"));
    assertEquals(Priority.newInstance(containerId.getId()).toString(),
      container.getString("priority"));
    Configuration conf = new YarnConfiguration();
    assertEquals(WebAppUtils.getHttpSchemePrefix(conf) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(conf) +
        "/applicationhistory/logs/localhost:0/container_0_0001_01_000001/" +
        "container_0_0001_01_000001/test user",
        container.getString("logUrl"));
    assertEquals(ContainerState.COMPLETE.toString(),
      container.getString("containerState"));
  }

}
