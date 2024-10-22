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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
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
import javax.ws.rs.core.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Set;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRMWebServicesApps extends JerseyTestBase {

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
      conf.set(YarnConfiguration.RM_CLUSTER_ID, "subCluster1");
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

  private Set<String> getApplicationIds(JSONArray array) throws JSONException {
    Set<String> ids = Sets.newHashSet();
    for (int i = 0; i < array.length(); i++) {
      JSONObject app = array.getJSONObject(i);
      String appId = (String) app.get("id");
      ids.add(appId);
    }
    return ids;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestRMWebServicesApps() {
  }

  @Test
  public void testApps() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    testAppsHelper("apps", app1, MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testAppsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    testAppsHelper("apps/", app1, MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testAppsDefault() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    testAppsHelper("apps/", app1, "");
    rm.stop();
  }

  @Test
  public void testAppsXML() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("testwordcount")
            .withUser("user1")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").request(MediaType.APPLICATION_XML)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodesApps = dom.getElementsByTagName("apps");
    assertEquals("incorrect number of elements", 1, nodesApps.getLength());
    NodeList nodes = dom.getElementsByTagName("app");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    verifyAppsXML(nodes, app1, false);
    rm.stop();
  }

  @Test
  public void testRunningApp() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("testwordcount")
            .withUser("user1")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    am1.allocate("*", 4096, 1, new ArrayList<>());
    amNodeManager.nodeHeartbeat(true);

    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").request(MediaType.APPLICATION_XML)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodesApps = dom.getElementsByTagName("apps");
    assertEquals("incorrect number of elements", 1, nodesApps.getLength());
    NodeList nodes = dom.getElementsByTagName("app");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    verifyAppsXML(nodes, app1, true);

    testAppsHelper("apps/", app1, MediaType.APPLICATION_JSON, true);
    rm.stop();
  }

  @Test
  public void testAppsXMLMulti() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("testwordcount")
            .withUser("user1")
            .build();
    MockRMAppSubmitter.submit(rm, data1);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(2048, rm)
            .withAppName("testwordcount2")
            .withUser("user1")
            .build();
    MockRMAppSubmitter.submit(rm, data);

    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").request(MediaType.APPLICATION_XML)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodesApps = dom.getElementsByTagName("apps");
    assertEquals("incorrect number of elements", 1, nodesApps.getLength());
    NodeList nodes = dom.getElementsByTagName("app");
    assertEquals("incorrect number of elements", 2, nodes.getLength());
    rm.stop();
  }

  public void testAppsHelper(String path, RMApp app, String media)
      throws JSONException, Exception {
    testAppsHelper(path, app, media, false);
  }

  public void testAppsHelper(String path, RMApp app, String media,
      boolean hasResourceReq) throws JSONException, Exception {
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path(path).request(media).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONObject jSONObjectApp = apps.getJSONObject("app");
    JSONArray array = new JSONArray();
    array.put(jSONObjectApp);
    assertEquals("incorrect number of elements", 1, array.length());
    verifyAppInfo(array.getJSONObject(0), app, hasResourceReq);
  }

  @Test
  public void testAppsQueryState() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps")
        .queryParam("state", YarnApplicationState.ACCEPTED.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONObject app = apps.getJSONObject("app");
    JSONArray array = new JSONArray();
    array.put(app);
    assertEquals("incorrect number of elements", 1, array.length());
    verifyAppInfo(array.getJSONObject(0), app1, false);
    rm.stop();
  }

  @Test
  public void testAppsQueryStates() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    RMApp killedApp = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    rm.killApp(killedApp.getApplicationId());

    amNodeManager.nodeHeartbeat(true);

    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("states", YarnApplicationState.ACCEPTED.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONObject app = apps.getJSONObject("app");
    JSONArray array = new JSONArray();
    array.put(app);
    assertEquals("incorrect number of elements", 1, array.length());
    assertEquals("state not equal to ACCEPTED", "ACCEPTED", array
        .getJSONObject(0).getString("state"));

    r = target();
    response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("states", YarnApplicationState.ACCEPTED.toString())
        .queryParam("states", YarnApplicationState.KILLED.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 2, array.length());
    assertTrue("both app states of ACCEPTED and KILLED are not present",
        (array.getJSONObject(0).getString("state").equals("ACCEPTED") &&
        array.getJSONObject(1).getString("state").equals("KILLED")) ||
        (array.getJSONObject(0).getString("state").equals("KILLED") &&
        array.getJSONObject(1).getString("state").equals("ACCEPTED")));

    rm.stop();
  }

  @Test
  public void testAppsQueryStatesComma() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    RMApp killedApp = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    rm.killApp(killedApp.getApplicationId());

    amNodeManager.nodeHeartbeat(true);

    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("states", YarnApplicationState.ACCEPTED.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONObject app = apps.getJSONObject("app");
    JSONArray array = new JSONArray();
    array.put(app);
    assertEquals("incorrect number of elements", 1, array.length());
    assertEquals("state not equal to ACCEPTED", "ACCEPTED", array
        .getJSONObject(0).getString("state"));

    r = target();
    response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("states", YarnApplicationState.ACCEPTED.toString() + ","
        + YarnApplicationState.KILLED.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 2, array.length());
    assertTrue("both app states of ACCEPTED and KILLED are not present",
        (array.getJSONObject(0).getString("state").equals("ACCEPTED") &&
        array.getJSONObject(1).getString("state").equals("KILLED")) ||
        (array.getJSONObject(0).getString("state").equals("KILLED") &&
        array.getJSONObject(1).getString("state").equals("ACCEPTED")));

    rm.stop();
  }

  @Test
  public void testAppsQueryStatesNone() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps")
        .queryParam("states", YarnApplicationState.RUNNING.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("apps is not empty", "", json.get("apps").toString());
    rm.stop();
  }

  @Test
  public void testAppsQueryStateNone() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps")
        .queryParam("state", YarnApplicationState.RUNNING.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("apps is not empty", "", json.get("apps").toString());
    rm.stop();
  }

  @Test
  public void testAppsQueryStatesInvalid() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("states", "INVALID_test")
        .request(MediaType.APPLICATION_JSON).get();
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
    WebServicesTestUtils.checkStringContains("exception message",
        "Invalid application-state INVALID_test", message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
    rm.stop();
  }

  @Test
  public void testAppsQueryStateInvalid() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("state", "INVALID_test")
        .request(MediaType.APPLICATION_JSON).get();
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
    WebServicesTestUtils.checkStringContains("exception message",
        "Invalid application-state INVALID_test", message);
    WebServicesTestUtils.checkStringMatch("exception type", "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
    rm.stop();
  }

  @Test
  public void testAppsQueryFinalStatus() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("finalStatus",
        FinalApplicationStatus.UNDEFINED.toString())
        .request(MediaType.APPLICATION_JSON).get();
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONObject app = apps.getJSONObject("app");
    JSONArray array = new JSONArray();
    array.put(app);
    assertEquals("incorrect number of elements", 1, array.length());
    verifyAppInfo(array.getJSONObject(0), app1, false);
    rm.stop();
  }

  @Test
  public void testAppsQueryFinalStatusNone() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("finalStatus", FinalApplicationStatus.KILLED.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("apps is not null", "", json.get("apps").toString());
    rm.stop();
  }

  @Test
  public void testAppsQueryFinalStatusInvalid() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("finalStatus", "INVALID_test")
        .request(MediaType.APPLICATION_JSON).get();
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
    WebServicesTestUtils.checkStringContains(
        "exception message",
        "org.apache.hadoop.yarn.api.records.FinalApplicationStatus.INVALID_test", message);
    WebServicesTestUtils.checkStringMatch("exception type", "IllegalArgumentException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "java.lang.IllegalArgumentException", classname);
  }

  @Test
  public void testAppsQueryUser() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);

    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();
    Response response = r
        .path("ws")
        .path("v1")
        .path("cluster")
        .path("apps")
        .queryParam("user",
            UserGroupInformation.getCurrentUser().getShortUserName())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);

    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 2, array.length());
    rm.stop();
  }

  @Test
  public void testAppsQueryQueue() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);

    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("queue", "default")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 2, array.length());
    rm.stop();
  }

  @Test
  public void testAppsQueryQueueAndStateTwoFinishedApps() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    RMApp app2 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);

    finishApp(amNodeManager, app1);
    finishApp(amNodeManager, app2);

    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps")
        .queryParam("queue", "default")
        .queryParam("state", YarnApplicationState.FINISHED.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");

    Set<String> appIds = getApplicationIds(array);
    assertTrue("Finished app1 should be in the result list!",
        appIds.contains(app1.getApplicationId().toString()));
    assertTrue("Finished app2 should be in the result list!",
        appIds.contains(app2.getApplicationId().toString()));
    assertEquals("incorrect number of elements", 2, array.length());

    rm.stop();
  }

  @Test
  public void testAppsQueryQueueAndStateOneFinishedApp() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp finishedApp = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    RMApp runningApp = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);

    finishApp(amNodeManager, finishedApp);

    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps")
        .queryParam("queue", "default")
        .queryParam("state", YarnApplicationState.FINISHED.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());

    JSONObject app = apps.getJSONObject("app");
    JSONArray array = new JSONArray();
    array.put(app);

    Set<String> appIds = getApplicationIds(array);
    assertFalse("Running app should not be in the result list!",
        appIds.contains(runningApp.getApplicationId().toString()));
    assertTrue("Finished app should be in the result list!",
        appIds.contains(finishedApp.getApplicationId().toString()));
    assertEquals("incorrect number of elements", 1, array.length());

    rm.stop();
  }

  @Test
  public void testAppsQueryQueueOneFinishedApp() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp finishedApp = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    RMApp runningApp = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);

    finishApp(amNodeManager, finishedApp);

    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps")
        .queryParam("queue", "default")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());

    JSONArray array = apps.getJSONArray("app");

    Set<String> appIds = getApplicationIds(array);
    assertTrue("Running app should be in the result list!",
        appIds.contains(runningApp.getApplicationId().toString()));
    assertTrue("Finished app should be in the result list!",
        appIds.contains(finishedApp.getApplicationId().toString()));
    assertEquals("incorrect number of elements", 2, array.length());

    rm.stop();
  }

  @Test
  public void testAppsQueryLimit() throws JSONException, Exception {
    rm.start();
    rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("limit", "2")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 2, array.length());
    rm.stop();
  }

  @Test
  public void testAppsQueryStartBegin() throws JSONException, Exception {
    rm.start();
    long start = System.currentTimeMillis();
    Thread.sleep(1);
    rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("startedTimeBegin", String.valueOf(start))
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 3, array.length());
    rm.stop();
  }

  @Test
  public void testAppsQueryStartBeginSome() throws JSONException, Exception {
    rm.start();
    rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    long start = System.currentTimeMillis();
    Thread.sleep(1);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("startedTimeBegin", String.valueOf(start))
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONObject app = apps.getJSONObject("app");
    JSONArray array = new JSONArray();
    array.put(app);
    assertEquals("incorrect number of elements", 1, array.length());
    rm.stop();
  }

  @Test
  public void testAppsQueryStartEnd() throws JSONException, Exception {
    rm.start();
    rm.registerNode("127.0.0.1:1234", 2048);
    long end = System.currentTimeMillis();
    Thread.sleep(1);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("startedTimeEnd", String.valueOf(end))
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("apps is not empty", "", json.get("apps").toString());
    rm.stop();
  }

  @Test
  public void testAppsQueryStartBeginEnd() throws JSONException, Exception {
    rm.start();
    rm.registerNode("127.0.0.1:1234", 2048);
    long start = System.currentTimeMillis();
    Thread.sleep(1);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    long end = System.currentTimeMillis();
    Thread.sleep(1);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("startedTimeBegin", String.valueOf(start))
        .queryParam("startedTimeEnd", String.valueOf(end))
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 2, array.length());
    rm.stop();
  }

  @Test
  public void testAppsQueryFinishBegin() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    long start = System.currentTimeMillis();
    Thread.sleep(1);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    finishApp(amNodeManager, app1);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);

    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("finishedTimeBegin", String.valueOf(start))
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONObject appsJSONObject = apps.getJSONObject("app");
    JSONArray array = new JSONArray();
    array.put(appsJSONObject);
    assertEquals("incorrect number of elements", 1, array.length());
    rm.stop();
  }

  private void finishApp(MockNM amNodeManager, RMApp app) throws Exception {
    MockAM am = rm
        .sendAMLaunched(app.getCurrentAppAttempt().getAppAttemptId());
    am.registerAppAttempt();
    am.unregisterAppAttempt();
    amNodeManager.nodeHeartbeat(app.getCurrentAppAttempt().getAppAttemptId(),
        1, ContainerState.COMPLETE);
    rm.waitForState(app.getApplicationId(), RMAppState.FINISHED);
  }

  @Test
  public void testAppsQueryFinishEnd() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    // finish App
    finishApp(amNodeManager, app1);

    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    long end = System.currentTimeMillis();

    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("finishedTimeEnd", String.valueOf(end))
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 3, array.length());
    rm.stop();
  }

  @Test
  public void testAppsQueryFinishBeginEnd() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    long start = System.currentTimeMillis();
    Thread.sleep(1);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    // finish App
    finishApp(amNodeManager, app1);

    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    long end = System.currentTimeMillis();

    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("finishedTimeBegin", String.valueOf(start))
        .queryParam("finishedTimeEnd", String.valueOf(end))
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONObject appsJSONObject = apps.getJSONObject("app");
    JSONArray array = new JSONArray();
    array.put(appsJSONObject);
    assertEquals("incorrect number of elements", 1, array.length());
    rm.stop();
  }

  @Test
  public void testAppsQueryAppTypes() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    Thread.sleep(1);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    // finish App
    finishApp(amNodeManager, app1);

    MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("")
            .withUser(UserGroupInformation.getCurrentUser()
        .getShortUserName())
            .withAcls(null)
            .withUnmanagedAM(false)
            .withQueue(null)
            .withMaxAppAttempts(2)
            .withCredentials(null)
            .withAppType("MAPREDUCE")
            .build());
    MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("")
            .withUser(UserGroupInformation.getCurrentUser()
        .getShortUserName())
            .withAcls(null)
            .withUnmanagedAM(false)
            .withQueue(null)
            .withMaxAppAttempts(2)
            .withCredentials(null)
            .withAppType("NON-YARN")
            .build());

    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("applicationTypes", "MAPREDUCE")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONObject appsJSONObject = apps.getJSONObject("app");
    JSONArray array = new JSONArray();
    array.put(appsJSONObject);
    assertEquals("incorrect number of elements", 1, array.length());
    assertEquals("MAPREDUCE",
        array.getJSONObject(0).getString("applicationType"));

    r = target();
    response = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("applicationTypes", "YARN")
        .queryParam("applicationTypes", "MAPREDUCE")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 2, array.length());
    assertTrue((array.getJSONObject(0).getString("applicationType")
        .equals("YARN") && array.getJSONObject(1).getString("applicationType")
        .equals("MAPREDUCE")) ||
        (array.getJSONObject(1).getString("applicationType").equals("YARN")
            && array.getJSONObject(0).getString("applicationType")
            .equals("MAPREDUCE")));

    r = target();
    response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .queryParam("applicationTypes", "YARN,NON-YARN")
            .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 2, array.length());
    assertTrue((array.getJSONObject(0).getString("applicationType")
        .equals("YARN") && array.getJSONObject(1).getString("applicationType")
        .equals("NON-YARN")) ||
        (array.getJSONObject(1).getString("applicationType").equals("YARN")
            && array.getJSONObject(0).getString("applicationType")
            .equals("NON-YARN")));

    r = target();
    response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("applicationTypes", "")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 3, array.length());

    r = target();
    response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .queryParam("applicationTypes", "YARN,NON-YARN")
            .queryParam("applicationTypes", "MAPREDUCE")
            .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 3, array.length());

    r = target();
    response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .queryParam("applicationTypes", "YARN")
            .queryParam("applicationTypes", "")
            .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    appsJSONObject = apps.getJSONObject("app");
    array = new JSONArray();
    array.put(appsJSONObject);
    assertEquals("incorrect number of elements", 1, array.length());
    assertEquals("YARN",
        array.getJSONObject(0).getString("applicationType"));

    r = target();
    response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .queryParam("applicationTypes", ",,, ,, YARN ,, ,")
            .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    appsJSONObject = apps.getJSONObject("app");
    array = new JSONArray();
    array.put(appsJSONObject);
    assertEquals("incorrect number of elements", 1, array.length());
    assertEquals("YARN",
        array.getJSONObject(0).getString("applicationType"));

    r = target();
    response = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("applicationTypes", ",,, ,,  ,, ,")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 3, array.length());

    r = target();
    response = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("applicationTypes", "YARN, ,NON-YARN, ,,")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 2, array.length());
    assertTrue((array.getJSONObject(0).getString("applicationType")
        .equals("YARN") && array.getJSONObject(1).getString("applicationType")
        .equals("NON-YARN")) ||
        (array.getJSONObject(1).getString("applicationType").equals("YARN")
            && array.getJSONObject(0).getString("applicationType")
            .equals("NON-YARN")));

    r = target();
    response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .queryParam("applicationTypes", " YARN, ,  ,,,")
            .queryParam("applicationTypes", "MAPREDUCE , ,, ,")
            .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 2, array.length());
    assertTrue((array.getJSONObject(0).getString("applicationType")
        .equals("YARN") && array.getJSONObject(1).getString("applicationType")
        .equals("MAPREDUCE")) ||
        (array.getJSONObject(1).getString("applicationType").equals("YARN")
            && array.getJSONObject(0).getString("applicationType")
            .equals("MAPREDUCE")));

    rm.stop();
  }

  @Test
  public void testAppsQueryWithInvalidDeselects()
      throws JSONException, Exception {
    try {
      rm.start();
      MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
      MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
      amNodeManager.nodeHeartbeat(true);
      WebTarget r = target();
      Response response = r.path("ws").path("v1").path("cluster")
          .path("apps").queryParam("deSelects", "INVALIED_deSelectsParam")
          .request(MediaType.APPLICATION_JSON).get(Response.class);
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
      WebServicesTestUtils.checkStringContains("exception message",
          "Invalid deSelects string"
              + " INVALIED_deSelectsParam " + "specified. It should be one of",
          message);
      WebServicesTestUtils.checkStringEqual("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringEqual("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testAppsQueryWithDeselects() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("deSelects",
        DeSelectFields.DeSelectType.RESOURCE_REQUESTS.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());

    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONObject appsJSONObject = apps.getJSONObject("app");
    JSONArray array = new JSONArray();
    array.put(appsJSONObject);
    assertEquals("incorrect number of elements", 1, array.length());
    JSONObject app = array.getJSONObject(0);
    assertTrue("resource requests shouldn't exist", !app.has("resourceRequests"));

    response = r.path("ws").path("v1").path("cluster").path("apps").queryParam("deSelects",
        DeSelectFields.DeSelectType.AM_NODE_LABEL_EXPRESSION.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());

    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    appsJSONObject = apps.getJSONObject("app");
    array = new JSONArray();
    array.put(appsJSONObject);
    assertEquals("incorrect number of elements", 1, array.length());
    app = array.getJSONObject(0);
    assertTrue("AMNodeLabelExpression shouldn't exist", !app.has("amNodeLabelExpression"));

    response = r.path("ws").path("v1").path("cluster").path("apps").queryParam("deSelects",
        DeSelectFields.DeSelectType.TIMEOUTS.toString()).
        request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());

    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    appsJSONObject = apps.getJSONObject("app");
    array = new JSONArray();
    array.put(appsJSONObject);
    assertEquals("incorrect number of elements", 1, array.length());
    app = array.getJSONObject(0);
    assertTrue("Timeouts shouldn't exist", !app.has("timeouts"));
    rm.stop();

    response =
        r.path("ws").path("v1").path("cluster").path("apps").queryParam("deSelects",
        DeSelectFields.DeSelectType.APP_NODE_LABEL_EXPRESSION.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());

    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    appsJSONObject = apps.getJSONObject("app");
    array = new JSONArray();
    array.put(appsJSONObject);
    assertEquals("incorrect number of elements", 1, array.length());
    app = array.getJSONObject(0);
    assertTrue("AppNodeLabelExpression shouldn't exist", !app.has("appNodeLabelExpression"));
    rm.stop();

    response =
        r.path("ws").path("v1").path("cluster").path("apps").queryParam("deSelects",
        DeSelectFields.DeSelectType.RESOURCE_INFO.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());

    entity = response.readEntity(String.class);
    json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    appsJSONObject = apps.getJSONObject("app");
    array = new JSONArray();
    array.put(appsJSONObject);
    assertEquals("incorrect number of elements", 1, array.length());
    app = array.getJSONObject(0);
    assertTrue("Resource info shouldn't exist", !app.has("resourceInfo"));
    rm.stop();
  }

  @Test
  public void testAppStatistics() throws JSONException, Exception {
    try {
      rm.start();
      MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 4096);
      Thread.sleep(1);
      RMApp app1 = MockRMAppSubmitter.submit(rm,
          MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
              .withAppName("")
              .withUser(UserGroupInformation.getCurrentUser()
                  .getShortUserName())
              .withAcls(null)
              .withUnmanagedAM(false)
              .withQueue(null)
              .withMaxAppAttempts(2)
              .withCredentials(null)
              .withAppType("MAPREDUCE")
              .build());
      amNodeManager.nodeHeartbeat(true);
      finishApp(amNodeManager, app1);

      MockRMAppSubmitter.submit(rm,
          MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
              .withAppName("")
              .withUser(UserGroupInformation.getCurrentUser()
            .getShortUserName())
              .withAcls(null)
              .withUnmanagedAM(false)
              .withQueue(null)
              .withMaxAppAttempts(2)
              .withCredentials(null)
              .withAppType("MAPREDUCE")
              .build());
      MockRMAppSubmitter.submit(rm,
          MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
              .withAppName("")
              .withUser(UserGroupInformation.getCurrentUser()
            .getShortUserName())
              .withAcls(null)
              .withUnmanagedAM(false)
              .withQueue(null)
              .withMaxAppAttempts(2)
              .withCredentials(null)
              .withAppType("OTHER")
              .build());

      // zero type, zero state
      WebTarget r = target();
      Response response = r.path("ws").path("v1").path("cluster")
          .path("appstatistics")
          .request(MediaType.APPLICATION_JSON).get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String entity = response.readEntity(String.class);
      JSONObject json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject appsStatInfo = json.getJSONObject("appStatInfo");
      assertEquals("incorrect number of elements", 1, appsStatInfo.length());
      JSONArray statItems = appsStatInfo.getJSONArray("statItem");
      assertEquals("incorrect number of elements",
          YarnApplicationState.values().length, statItems.length());
      for (int i = 0; i < YarnApplicationState.values().length; ++i) {
        assertEquals("*", statItems.getJSONObject(0).getString("type"));
        if (statItems.getJSONObject(0).getString("state").equals("ACCEPTED")) {
          assertEquals("2", statItems.getJSONObject(0).getString("count"));
        } else if (
            statItems.getJSONObject(0).getString("state").equals("FINISHED")) {
          assertEquals("1", statItems.getJSONObject(0).getString("count"));
        } else {
          assertEquals("0", statItems.getJSONObject(0).getString("count"));
        }
      }

      // zero type, one state
      r = target();
      response = r.path("ws").path("v1").path("cluster")
          .path("appstatistics")
          .queryParam("states", YarnApplicationState.ACCEPTED.toString())
          .request(MediaType.APPLICATION_JSON).get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      entity = response.readEntity(String.class);
      json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      appsStatInfo = json.getJSONObject("appStatInfo");
      assertEquals("incorrect number of elements", 1, appsStatInfo.length());
      JSONObject statItem = appsStatInfo.getJSONObject("statItem");
      statItems = new JSONArray();
      statItems.put(statItem);
      assertEquals("incorrect number of elements", 1, statItems.length());
      assertEquals("ACCEPTED", statItems.getJSONObject(0).getString("state"));
      assertEquals("*", statItems.getJSONObject(0).getString("type"));
      assertEquals("2", statItems.getJSONObject(0).getString("count"));

      // one type, zero state
      r = target();
      response = r.path("ws").path("v1").path("cluster")
          .path("appstatistics")
          .queryParam("applicationTypes", "MAPREDUCE")
          .request(MediaType.APPLICATION_JSON).get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      entity = response.readEntity(String.class);
      json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      appsStatInfo = json.getJSONObject("appStatInfo");
      assertEquals("incorrect number of elements", 1, appsStatInfo.length());
      statItems = appsStatInfo.getJSONArray("statItem");
      assertEquals("incorrect number of elements",
          YarnApplicationState.values().length, statItems.length());
      for (int i = 0; i < YarnApplicationState.values().length; ++i) {
        assertEquals("mapreduce", statItems.getJSONObject(0).getString("type"));
        if (statItems.getJSONObject(0).getString("state").equals("ACCEPTED")) {
          assertEquals("1", statItems.getJSONObject(0).getString("count"));
        } else if (
            statItems.getJSONObject(0).getString("state").equals("FINISHED")) {
          assertEquals("1", statItems.getJSONObject(0).getString("count"));
        } else {
          assertEquals("0", statItems.getJSONObject(0).getString("count"));
        }
      }

      // two types, zero state
      r = target();
      response = r.path("ws").path("v1").path("cluster")
          .path("appstatistics")
          .queryParam("applicationTypes", "MAPREDUCE,OTHER")
          .request(MediaType.APPLICATION_JSON).get(Response.class);
      assertResponseStatusCode(Response.Status.BAD_REQUEST, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      entity = response.readEntity(String.class);
      json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject exception = json.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String className = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringContains("exception message",
          "we temporarily support at most one applicationType", message);
      WebServicesTestUtils.checkStringEqual("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringEqual("exception className",
          "org.apache.hadoop.yarn.webapp.BadRequestException", className);

      // one type, two states
      r = target();
      response = r.path("ws").path("v1").path("cluster")
          .path("appstatistics")
          .queryParam("states", YarnApplicationState.FINISHED.toString()
              + "," + YarnApplicationState.ACCEPTED.toString())
          .queryParam("applicationTypes", "MAPREDUCE")
          .request(MediaType.APPLICATION_JSON).get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      entity = response.readEntity(String.class);
      json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      appsStatInfo = json.getJSONObject("appStatInfo");
      assertEquals("incorrect number of elements", 1, appsStatInfo.length());
      statItems = appsStatInfo.getJSONArray("statItem");
      assertEquals("incorrect number of elements", 2, statItems.length());
      JSONObject statItem1 = statItems.getJSONObject(0);
      JSONObject statItem2 = statItems.getJSONObject(1);
      assertTrue((statItem1.getString("state").equals("ACCEPTED") &&
          statItem2.getString("state").equals("FINISHED")) ||
          (statItem2.getString("state").equals("ACCEPTED") &&
          statItem1.getString("state").equals("FINISHED")));
      assertEquals("mapreduce", statItem1.getString("type"));
      assertEquals("1", statItem1.getString("count"));
      assertEquals("mapreduce", statItem2.getString("type"));
      assertEquals("1", statItem2.getString("count"));

      // invalid state
      r = target();
      response = r.path("ws").path("v1").path("cluster")
          .path("appstatistics").queryParam("states", "wrong_state")
          .request(MediaType.APPLICATION_JSON).get(Response.class);
      assertResponseStatusCode(Response.Status.BAD_REQUEST, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      entity = response.readEntity(String.class);
      json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      exception = json.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      message = exception.getString("message");
      type = exception.getString("exception");
      className = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringContains("exception message",
          "Invalid application-state wrong_state", message);
      WebServicesTestUtils.checkStringEqual("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringEqual("exception className",
          "org.apache.hadoop.yarn.webapp.BadRequestException", className);
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testSingleApp() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("testwordcount")
            .withUser("user1")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    amNodeManager.nodeHeartbeat(true);
    testSingleAppsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testUnmarshalAppInfo() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("testwordcount")
            .withUser("user1")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    amNodeManager.nodeHeartbeat(true);

    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(app1.getApplicationId().toString())
        .request(MediaType.APPLICATION_XML).get(Response.class);

    AppInfo appInfo = response.readEntity(AppInfo.class);

    // Check only a few values; all are validated in testSingleApp.
    assertEquals(app1.getApplicationId().toString(), appInfo.getAppId());
    assertEquals(app1.getName(), appInfo.getName());
    assertEquals(app1.createApplicationState(), appInfo.getState());
    assertEquals(app1.getAMResourceRequests().get(0).getCapability()
            .getMemorySize(), appInfo.getAllocatedMB());

    rm.stop();
  }

  @Test
  public void testSingleAppsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    testSingleAppsHelper(app1.getApplicationId().toString() + "/", app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testSingleAppsDefault() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    testSingleAppsHelper(app1.getApplicationId().toString() + "/", app1, "");
    rm.stop();
  }

  @Test
  public void testInvalidApp() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmitter.submitWithMemory(CONTAINER_MB, rm);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster").path("apps")
        .path("application_invalid_12").request(MediaType.APPLICATION_JSON)
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
    WebServicesTestUtils.checkStringMatch("exception message",
            "Invalid ApplicationId: application_invalid_12",
            message);
    WebServicesTestUtils.checkStringMatch("exception type",
            "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

    rm.stop();
  }

  @Test
  public void testNonexistApp() throws JSONException, Exception {
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
        .path("application_00000_0099").request(MediaType.APPLICATION_JSON).get();
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
    WebServicesTestUtils.checkStringMatch("exception message",
            "app with id: application_00000_0099 not found",
            message);
    WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    rm.stop();
  }

  public void testSingleAppsHelper(String path, RMApp app, String media)
      throws JSONException, Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(path).request(media).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);

    assertEquals("incorrect number of elements", 1, json.length());
    verifyAppInfo(json.getJSONObject("app"), app, false);
  }

  @Test
  public void testSingleAppsXML() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("testwordcount")
            .withUser("user1")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    amNodeManager.nodeHeartbeat(true);
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(app1.getApplicationId().toString())
        .request(MediaType.APPLICATION_XML).get(Response.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);

    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("app");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    verifyAppsXML(nodes, app1, false);
    rm.stop();
  }

  public void verifyAppsXML(NodeList nodes, RMApp app, boolean hasResourceReq)
      throws JSONException, Exception {

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyAppInfoGeneric(app,
          WebServicesTestUtils.getXmlString(element, "id"),
          WebServicesTestUtils.getXmlString(element, "user"),
          WebServicesTestUtils.getXmlString(element, "name"),
          WebServicesTestUtils.getXmlString(element, "applicationType"),
          WebServicesTestUtils.getXmlString(element, "queue"),
          WebServicesTestUtils.getXmlInt(element, "priority"),
          WebServicesTestUtils.getXmlString(element, "state"),
          WebServicesTestUtils.getXmlString(element, "finalStatus"),
          WebServicesTestUtils.getXmlFloat(element, "progress"),
          WebServicesTestUtils.getXmlString(element, "trackingUI"),
          WebServicesTestUtils.getXmlString(element, "diagnostics"),
          WebServicesTestUtils.getXmlLong(element, "clusterId"),
          WebServicesTestUtils.getXmlLong(element, "startedTime"),
          WebServicesTestUtils.getXmlLong(element, "launchTime"),
          WebServicesTestUtils.getXmlLong(element, "finishedTime"),
          WebServicesTestUtils.getXmlLong(element, "elapsedTime"),
          WebServicesTestUtils.getXmlString(element, "amHostHttpAddress"),
          WebServicesTestUtils.getXmlString(element, "amContainerLogs"),
          WebServicesTestUtils.getXmlInt(element, "allocatedMB"),
          WebServicesTestUtils.getXmlInt(element, "allocatedVCores"),
          WebServicesTestUtils.getXmlInt(element, "runningContainers"),
          WebServicesTestUtils.getXmlFloat(element, "queueUsagePercentage"),
          WebServicesTestUtils.getXmlFloat(element, "clusterUsagePercentage"),
          WebServicesTestUtils.getXmlInt(element, "preemptedResourceMB"),
          WebServicesTestUtils.getXmlInt(element, "preemptedResourceVCores"),
          WebServicesTestUtils.getXmlInt(element, "numNonAMContainerPreempted"),
          WebServicesTestUtils.getXmlInt(element, "numAMContainerPreempted"),
          WebServicesTestUtils.getXmlString(element, "logAggregationStatus"),
          WebServicesTestUtils.getXmlBoolean(element, "unmanagedApplication"),
          WebServicesTestUtils.getXmlString(element, "appNodeLabelExpression"),
          WebServicesTestUtils.getXmlString(element, "amNodeLabelExpression"),
          WebServicesTestUtils.getXmlString(element, "amRPCAddress"));

      if (hasResourceReq) {
        assertEquals(element.getElementsByTagName("resourceRequests").getLength(),
            1);
        Element resourceRequests =
            (Element) element.getElementsByTagName("resourceRequests").item(0);
        Element capability =
            (Element) resourceRequests.getElementsByTagName("capability").item(0);
        ResourceRequest rr =
            ((AbstractYarnScheduler)rm.getRMContext().getScheduler())
                .getApplicationAttempt(
                    app.getCurrentAppAttempt().getAppAttemptId())
                .getAppSchedulingInfo().getAllResourceRequests().get(0);
        verifyResourceRequestsGeneric(rr,
            WebServicesTestUtils.getXmlString(resourceRequests,
                "nodeLabelExpression"),
            WebServicesTestUtils.getXmlInt(resourceRequests, "numContainers"),
            WebServicesTestUtils.getXmlBoolean(resourceRequests, "relaxLocality"),
            WebServicesTestUtils.getXmlInt(resourceRequests, "priority"),
            WebServicesTestUtils.getXmlString(resourceRequests, "resourceName"),
            WebServicesTestUtils.getXmlLong(capability, "memory"),
            WebServicesTestUtils.getXmlLong(capability, "vCores"),
            WebServicesTestUtils.getXmlString(resourceRequests, "executionType"),
            WebServicesTestUtils.getXmlBoolean(resourceRequests,
                "enforceExecutionType"));
      }
    }
  }

  public void verifyAppInfo(JSONObject info, RMApp app, boolean hasResourceReqs)
      throws JSONException, Exception {

    int expectedNumberOfElements = 41 + (hasResourceReqs ? 2 : 0);
    String appNodeLabelExpression = null;
    String amNodeLabelExpression = null;
    if (app.getApplicationSubmissionContext()
        .getNodeLabelExpression() != null) {
      expectedNumberOfElements++;
      appNodeLabelExpression = info.getString("appNodeLabelExpression");
    }
    if (app.getAMResourceRequests().get(0).getNodeLabelExpression() != null) {
      expectedNumberOfElements++;
      amNodeLabelExpression = info.getString("amNodeLabelExpression");
    }
    String amRPCAddress = null;
    if (AppInfo.getAmRPCAddressFromRMAppAttempt(app.getCurrentAppAttempt())
        != null) {
      expectedNumberOfElements++;
      amRPCAddress = info.getString("amRPCAddress");
    }
    assertEquals("incorrect number of elements", expectedNumberOfElements,
        info.length());
    assertEquals("rmClusterId is incorrect", "subCluster1", info.getString("rmClusterId"));
    verifyAppInfoGeneric(app, info.getString("id"), info.getString("user"),
        info.getString("name"), info.getString("applicationType"),
        info.getString("queue"), info.getInt("priority"),
        info.getString("state"), info.getString("finalStatus"),
        (float) info.getDouble("progress"), info.getString("trackingUI"),
        info.getString("diagnostics"), info.getLong("clusterId"),
        info.getLong("startedTime"), info.getLong("launchTime"),
        info.getLong("finishedTime"),
        info.getLong("elapsedTime"),
        info.getString("amHostHttpAddress"),
        info.getString("amContainerLogs"), info.getInt("allocatedMB"),
        info.getInt("allocatedVCores"), info.getInt("runningContainers"),
        (float) info.getDouble("queueUsagePercentage"),
        (float) info.getDouble("clusterUsagePercentage"),
        info.getInt("preemptedResourceMB"),
        info.getInt("preemptedResourceVCores"),
        info.getInt("numNonAMContainerPreempted"),
        info.getInt("numAMContainerPreempted"),
        info.getString("logAggregationStatus"),
        info.getBoolean("unmanagedApplication"),
        appNodeLabelExpression,
        amNodeLabelExpression,
        amRPCAddress);

    if (hasResourceReqs) {
      JSONObject resourceRequests = info.getJSONObject("resourceRequests");
      JSONArray array = new JSONArray();
      array.put(resourceRequests);
      verifyResourceRequests(array, app);
    }
  }

  public void verifyAppInfoGeneric(RMApp app, String id, String user,
      String name, String applicationType, String queue, int prioirty,
      String state, String finalStatus, float progress, String trackingUI,
      String diagnostics, long clusterId, long startedTime,
      long launchTime, long finishedTime, long elapsedTime,
      String amHostHttpAddress, String amContainerLogs,
      int allocatedMB, int allocatedVCores, int numContainers,
      float queueUsagePerc, float clusterUsagePerc,
      int preemptedResourceMB, int preemptedResourceVCores,
      int numNonAMContainerPreempted, int numAMContainerPreempted,
      String logAggregationStatus, boolean unmanagedApplication,
      String appNodeLabelExpression, String amNodeLabelExpression,
      String amRPCAddress) throws JSONException, Exception {

    WebServicesTestUtils.checkStringMatch("id", app.getApplicationId()
        .toString(), id);
    WebServicesTestUtils.checkStringMatch("user", app.getUser(), user);
    WebServicesTestUtils.checkStringMatch("name", app.getName(), name);
    WebServicesTestUtils.checkStringMatch("applicationType",
      app.getApplicationType(), applicationType);
    WebServicesTestUtils.checkStringMatch("queue", app.getQueue(), queue);
    assertEquals("priority doesn't match", 0, prioirty);
    WebServicesTestUtils.checkStringMatch("state", app.getState().toString(),
        state);
    WebServicesTestUtils.checkStringMatch("finalStatus", app
        .getFinalApplicationStatus().toString(), finalStatus);
    assertEquals("progress doesn't match", 0, progress, 0.0);
    if ("UNASSIGNED".equals(trackingUI)) {
      WebServicesTestUtils.checkStringMatch("trackingUI", "UNASSIGNED",
          trackingUI);
    }
    WebServicesTestUtils.checkStringEqual("diagnostics",
        app.getDiagnostics().toString(), diagnostics);
    assertEquals("clusterId doesn't match",
        ResourceManager.getClusterTimeStamp(), clusterId);
    assertEquals("startedTime doesn't match", app.getStartTime(), startedTime);
    assertEquals("finishedTime doesn't match", app.getFinishTime(),
        finishedTime);
    assertTrue("elapsed time not greater than 0", elapsedTime > 0);
    WebServicesTestUtils.checkStringMatch("amHostHttpAddress", app
        .getCurrentAppAttempt().getMasterContainer().getNodeHttpAddress(),
        amHostHttpAddress);
    assertTrue("amContainerLogs doesn't match",
        amContainerLogs.startsWith("http://"));
    assertTrue("amContainerLogs doesn't contain user info",
        amContainerLogs.endsWith("/" + app.getUser()));
    assertEquals("allocatedMB doesn't match", 1024, allocatedMB);
    assertEquals("allocatedVCores doesn't match", 1, allocatedVCores);
    assertEquals("queueUsagePerc doesn't match", 50.0f, queueUsagePerc, 0.01f);
    assertEquals("clusterUsagePerc doesn't match", 50.0f, clusterUsagePerc, 0.01f);
    assertEquals("numContainers doesn't match", 1, numContainers);
    assertEquals("preemptedResourceMB doesn't match", app
        .getRMAppMetrics().getResourcePreempted().getMemorySize(),
        preemptedResourceMB);
    assertEquals("preemptedResourceVCores doesn't match", app
        .getRMAppMetrics().getResourcePreempted().getVirtualCores(),
        preemptedResourceVCores);
    assertEquals("numNonAMContainerPreempted doesn't match", app
        .getRMAppMetrics().getNumNonAMContainersPreempted(),
        numNonAMContainerPreempted);
    assertEquals("numAMContainerPreempted doesn't match", app
        .getRMAppMetrics().getNumAMContainersPreempted(),
        numAMContainerPreempted);
    assertEquals("Log aggregation Status doesn't match", app
        .getLogAggregationStatusForAppReport().toString(),
        logAggregationStatus);
    assertEquals("unmanagedApplication doesn't match", app
        .getApplicationSubmissionContext().getUnmanagedAM(),
        unmanagedApplication);
    assertEquals("unmanagedApplication doesn't match",
        app.getApplicationSubmissionContext().getNodeLabelExpression(),
        appNodeLabelExpression);
    assertEquals("unmanagedApplication doesn't match",
        app.getAMResourceRequests().get(0).getNodeLabelExpression(),
        amNodeLabelExpression);
    assertEquals("amRPCAddress",
        AppInfo.getAmRPCAddressFromRMAppAttempt(app.getCurrentAppAttempt()),
        amRPCAddress);
  }

  public void verifyResourceRequests(JSONArray resourceRequest, RMApp app)
      throws JSONException {
    JSONObject requestInfo = resourceRequest.getJSONObject(0);
    ResourceRequest rr =
        ((AbstractYarnScheduler) rm.getRMContext().getScheduler())
            .getApplicationAttempt(
                app.getCurrentAppAttempt().getAppAttemptId())
            .getAppSchedulingInfo().getAllResourceRequests().get(0);
    verifyResourceRequestsGeneric(rr,
        requestInfo.getString("nodeLabelExpression"),
        requestInfo.getInt("numContainers"),
        requestInfo.getBoolean("relaxLocality"), requestInfo.getInt("priority"),
        requestInfo.getString("resourceName"),
        requestInfo.getJSONObject("capability").getLong("memory"),
        requestInfo.getJSONObject("capability").getLong("vCores"),
        requestInfo.getJSONObject("executionTypeRequest")
            .getString("executionType"),
        requestInfo.getJSONObject("executionTypeRequest")
            .getBoolean("enforceExecutionType"));
  }

  public void verifyResourceRequestsGeneric(ResourceRequest request,
      String nodeLabelExpression, int numContainers, boolean relaxLocality,
      int priority, String resourceName, long memory, long vCores,
      String executionType, boolean enforceExecutionType) {
    assertEquals("nodeLabelExpression doesn't match",
        request.getNodeLabelExpression(), nodeLabelExpression);
    assertEquals("numContainers doesn't match", request.getNumContainers(),
        numContainers);
    assertEquals("relaxLocality doesn't match", request.getRelaxLocality(),
        relaxLocality);
    assertEquals("priority does not match", request.getPriority().getPriority(),
        priority);
    assertEquals("resourceName does not match", request.getResourceName(),
        resourceName);
    assertEquals("memory does not match",
        request.getCapability().getMemorySize(), memory);
    assertEquals("vCores does not match",
        request.getCapability().getVirtualCores(), vCores);
    assertEquals("executionType does not match",
        request.getExecutionTypeRequest().getExecutionType().name(),
        executionType);
    assertEquals("enforceExecutionType does not match",
        request.getExecutionTypeRequest().getEnforceExecutionType(),
        enforceExecutionType);
  }

  @Test
  public void testAppsQueryByQueueShortname() throws Exception {

    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    //YARN-11114 - Finished apps can only be queried with exactly the
    // same queue name that the app is submitted to.
    //As the queue is 'root.default'  and the query is 'default' here,
    // this app won't be returned.
    RMApp finishedApp1 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder
            .createWithMemory(CONTAINER_MB, rm)
            .withQueue("default")
            .build());
    RMApp finishedApp2 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder
            .createWithMemory(CONTAINER_MB, rm)
            .withQueue("default")
            .build());

    RMApp runningApp1 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder
            .createWithMemory(CONTAINER_MB, rm)
            .withQueue("root.default")
            .build());
    RMApp runningApp2 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder
            .createWithMemory(CONTAINER_MB, rm)
            .withQueue("root.default")
            .build());
    amNodeManager.nodeHeartbeat(true);
    finishApp(amNodeManager, finishedApp1);
    amNodeManager.nodeHeartbeat(true);
    finishApp(amNodeManager, finishedApp2);

    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps")
        .queryParam("queue", "default")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());

    JSONArray array = apps.getJSONArray("app");

    Set<String> appIds = getApplicationIds(array);
    assertTrue("Running app 1 should be in the result list!",
        appIds.contains(runningApp1.getApplicationId().toString()));
    assertTrue("Running app 2 should be in the result list!",
        appIds.contains(runningApp2.getApplicationId().toString()));
    assertTrue("Running app 1 should be in the result list!",
        appIds.contains(finishedApp1.getApplicationId().toString()));
    assertTrue("Running app 1 should be in the result list!",
        appIds.contains(finishedApp2.getApplicationId().toString()));
    assertEquals("incorrect number of elements", 4, array.length());

    rm.stop();
  }

  @Test
  public void testAppsQueryByQueueFullname() throws Exception {

    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp finishedApp1 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder
            .createWithMemory(CONTAINER_MB, rm)
            .withQueue("root.default")
            .build());
    //YARN-11114 - Finished apps can only be queried with exactly the
    // same queue name that the app is submitted to.
    //As the queue is 'default'  and the query is 'root.default' here,
    // this app won't be returned,
    RMApp finishedApp2 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder
            .createWithMemory(CONTAINER_MB, rm)
            .withQueue("root.default")
            .build());

    RMApp runningApp1 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder
            .createWithMemory(CONTAINER_MB, rm)
            .withQueue("root.default")
            .build());
    RMApp runningApp2 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder
            .createWithMemory(CONTAINER_MB, rm)
            .withQueue("root.default")
            .build());
    amNodeManager.nodeHeartbeat(true);
    finishApp(amNodeManager, finishedApp1);

    amNodeManager.nodeHeartbeat(true);
    finishApp(amNodeManager, finishedApp2);

    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster")
        .path("apps")
        .queryParam("queue", "root.default")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());

    JSONArray array = apps.getJSONArray("app");

    Set<String> appIds = getApplicationIds(array);
    assertTrue("Running app 1 should be in the result list!",
        appIds.contains(runningApp1.getApplicationId().toString()));
    assertTrue("Running app 2 should be in the result list!",
        appIds.contains(runningApp2.getApplicationId().toString()));
    assertTrue("Running app 2 should be in the result list!",
        appIds.contains(finishedApp1.getApplicationId().toString()));
    assertTrue("Running app 2 should be in the result list!",
        appIds.contains(finishedApp2.getApplicationId().toString()));
    assertEquals("incorrect number of elements", 4, array.length());

    rm.stop();
  }

}

