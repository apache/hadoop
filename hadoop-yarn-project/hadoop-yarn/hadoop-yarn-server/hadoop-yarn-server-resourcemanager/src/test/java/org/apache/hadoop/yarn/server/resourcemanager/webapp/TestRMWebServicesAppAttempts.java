/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.Collection;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.checkStringMatch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRMWebServicesAppAttempts extends JerseyTestBase {

  private static MockRM rm;

  private static final int CONTAINER_MB = 1024;

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(RMWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    forceSet(TestProperties.CONTAINER_PORT, JERSEY_RANDOM_PORT);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      Configuration conf = new YarnConfiguration();
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);
      rm = new MockRM(conf);

      final HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getScheme()).thenReturn("http");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestRMWebServicesAppAttempts() {
  }

  @Test
  public void testAppAttempts() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("testwordcount")
            .withUser("user1")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString(), app1,
            MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test (timeout = 20000)
  public void testCompletedAppAttempt() throws Exception {
    Configuration conf = rm.getConfig();
    String logServerUrl = "http://localhost:19888/jobhistory/logs";
    conf.set(YarnConfiguration.YARN_LOG_SERVER_URL, logServerUrl);
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 8192);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
        .withAppName("testwordcount")
        .withUser("user1")
        .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    // fail the AM by sending CONTAINER_FINISHED event without registering.
    amNodeManager.nodeHeartbeat(am.getApplicationAttemptId(), 1,
        ContainerState.COMPLETE);
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    rm.waitForState(app1.getApplicationId(), RMAppState.FAILED);

    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(app1.getApplicationId().toString())
        .path("appattempts").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    JSONObject jsonAppAttempts = json.getJSONObject("appAttempts");
    JSONObject jsonAppAttempt = jsonAppAttempts.getJSONObject("appAttempt");
    JSONArray jsonArray = new JSONArray();
    jsonArray.put(jsonAppAttempt);
    JSONObject info = jsonArray.getJSONObject(0);
    String logsLink = info.getString("logsLink");
    String containerId = app1.getCurrentAppAttempt().getMasterContainer()
        .getId().toString();
    assertThat(logsLink).isEqualTo(logServerUrl
        + "/127.0.0.1:1234/" + containerId + "/" + containerId + "/"
        + "user1");
    rm.stop();
  }

  @Test (timeout = 20000)
  public void testMultipleAppAttempts() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 8192);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("testwordcount")
            .withUser("user1")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    int maxAppAttempts = rm.getConfig().getInt(
            YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    assertTrue(maxAppAttempts > 1);
    int numAttempt = 1;
    while (true) {
      // fail the AM by sending CONTAINER_FINISHED event without registering.
      amNodeManager.nodeHeartbeat(am.getApplicationAttemptId(), 1,
           ContainerState.COMPLETE);
      rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FAILED);
      if (numAttempt == maxAppAttempts) {
        rm.waitForState(app1.getApplicationId(), RMAppState.FAILED);
        break;
      }
      // wait for app to start a new attempt.
      rm.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
      am = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
      numAttempt++;
    }
    assertEquals("incorrect number of attempts", maxAppAttempts,
        app1.getAppAttempts().values().size());
    testAppAttemptsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testAppAttemptsSlash() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1,
            MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testAppAttemptsDefault() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1, "");
    rm.stop();
  }

  @Test
  public void testInvalidAppIdGetAttempts() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster").path("apps")
        .path("application_invalid_12").path("appattempts")
        .request(MediaType.APPLICATION_JSON)
        .get();
    assertResponseStatusCode(Response.Status.BAD_REQUEST, response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject msg = new JSONObject(entity);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    checkStringMatch("exception message",
         "Invalid ApplicationId: application_invalid_12", message);
    checkStringMatch("exception type", "BadRequestException", type);
    checkStringMatch("exception classname",
         "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
    rm.stop();
  }

  @Test
  public void testInvalidAppAttemptId() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster").path("apps")
        .path(app.getApplicationId().toString()).path("appattempts")
        .path("appattempt_invalid_12_000001")
        .request(MediaType.APPLICATION_JSON).get();
    assertResponseStatusCode(Response.Status.BAD_REQUEST,
        response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject msg = new JSONObject(entity);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    checkStringMatch("exception message",
        "Invalid AppAttemptId: appattempt_invalid_12_000001", message);
    checkStringMatch("exception type", "BadRequestException", type);
    checkStringMatch("exception classname",
         "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
    rm.stop();
  }

  @Test
  public void testNonexistAppAttempts() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
        .withAppName("testwordcount")
        .withUser("user1")
        .build();
    MockRMAppSubmitter.submit(rm, data);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster").path("apps")
        .path("application_00000_0099").request(MediaType.APPLICATION_JSON)
        .get();
    assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());

    String entity = response.readEntity(String.class);
    JSONObject msg = new JSONObject(entity);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    checkStringMatch("exception message",
         "app with id: application_00000_0099 not found", message);
    checkStringMatch("exception type", "NotFoundException", type);
    checkStringMatch("exception classname",
         "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    rm.stop();
  }

  private void testAppAttemptsHelper(String path, RMApp app, String media)
      throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(path).path("appattempts").request(media)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jsonAppAttempts = json.getJSONObject("appAttempts");
    assertEquals("incorrect number of elements", 1, jsonAppAttempts.length());
    JSONArray jsonArray = parseJsonAppAttempt(jsonAppAttempts);

    Collection<RMAppAttempt> attempts = app.getAppAttempts().values();
    assertEquals("incorrect number of elements", attempts.size(),
        jsonArray.length());

    // Verify these parallel arrays are the same
    int i = 0;
    for (RMAppAttempt attempt : attempts) {
      verifyAppAttemptsInfo(jsonArray.getJSONObject(i), attempt, app.getUser());
      ++i;
    }
  }

  public JSONArray parseJsonAppAttempt(JSONObject jsonAppAttempts) throws JSONException {
    Object appAttempt = jsonAppAttempts.get("appAttempt");
    if (appAttempt instanceof JSONArray) {
      return jsonAppAttempts.getJSONArray("appAttempt");
    }
    if (appAttempt instanceof JSONObject) {
      JSONObject jsonAppAttempt = jsonAppAttempts.getJSONObject("appAttempt");
      JSONArray jsonArray = new JSONArray();
      jsonArray.put(jsonAppAttempt);
      return jsonArray;
    }
    return null;
  }

  @Test
  public void testAppAttemptsXML() throws Exception {
    rm.start();
    String user = "user1";
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("testwordcount")
            .withUser(user)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
            .path("apps").path(app1.getApplicationId().toString())
            .path("appattempts").request(MediaType.APPLICATION_XML)
            .get(Response.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
    String xml = response.readEntity(String.class);

    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("appAttempts");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    NodeList attempt = dom.getElementsByTagName("appAttempt");
    assertEquals("incorrect number of elements", 1, attempt.getLength());
    verifyAppAttemptsXML(attempt, app1.getCurrentAppAttempt(), user);
    rm.stop();
  }

  private void verifyAppAttemptsXML(NodeList nodes, RMAppAttempt appAttempt,
          String user) {

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyAppAttemptInfoGeneric(appAttempt,
              WebServicesTestUtils.getXmlInt(element, "id"),
              WebServicesTestUtils.getXmlLong(element, "startTime"),
              WebServicesTestUtils.getXmlString(element, "containerId"),
              WebServicesTestUtils.getXmlString(element, "nodeHttpAddress"),
              WebServicesTestUtils.getXmlString(element, "nodeId"),
              WebServicesTestUtils.getXmlString(element, "logsLink"), user,
              WebServicesTestUtils.getXmlString(element, "exportPorts"),
              WebServicesTestUtils.getXmlString(element, "appAttemptState"));
    }
  }

  private void verifyAppAttemptsInfo(JSONObject info, RMAppAttempt appAttempt,
          String user)
          throws Exception {

    assertEquals("incorrect number of elements", 12, info.length());

    verifyAppAttemptInfoGeneric(appAttempt, info.getInt("id"),
            info.getLong("startTime"), info.getString("containerId"),
            info.getString("nodeHttpAddress"), info.getString("nodeId"),
            info.getString("logsLink"), user, info.getString("exportPorts"),
            info.getString("appAttemptState"));
  }

  private void verifyAppAttemptInfoGeneric(RMAppAttempt appAttempt, int id,
          long startTime, String containerId, String nodeHttpAddress, String
          nodeId, String logsLink, String user, String exportPorts,
          String appAttemptState) {

    assertEquals("id doesn't match", appAttempt.getAppAttemptId()
            .getAttemptId(), id);
    assertEquals("startedTime doesn't match", appAttempt.getStartTime(),
            startTime);
    checkStringMatch("containerId", appAttempt
            .getMasterContainer().getId().toString(), containerId);
    checkStringMatch("nodeHttpAddress", appAttempt
            .getMasterContainer().getNodeHttpAddress(), nodeHttpAddress);
    checkStringMatch("nodeId", appAttempt
            .getMasterContainer().getNodeId().toString(), nodeId);
    assertTrue("logsLink doesn't match ", logsLink.startsWith("http://"));
    assertTrue(
            "logsLink doesn't contain user info", logsLink.endsWith("/"
                    + user));
    assertEquals("appAttemptState doesn't match", appAttemptState, appAttempt
            .getAppAttemptState().toString());
  }
}
