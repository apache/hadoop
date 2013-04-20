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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.Collection;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppFailedAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

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

public class TestRMWebServicesApps extends JerseyTest {

  private static MockRM rm;

  private Injector injector = Guice.createInjector(new ServletModule() {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      Configuration conf = new Configuration();
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);
      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      bind(RMContext.class).toInstance(rm.getRMContext());
      bind(ApplicationACLsManager.class).toInstance(
          rm.getApplicationACLsManager());
      serve("/*").with(GuiceContainer.class);
    }
  });

  public class GuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestRMWebServicesApps() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testApps() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    testAppsHelper("apps", app1, MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testAppsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    testAppsHelper("apps/", app1, MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testAppsDefault() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    testAppsHelper("apps/", app1, "");
    rm.stop();
  }

  @Test
  public void testAppsXML() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodesApps = dom.getElementsByTagName("apps");
    assertEquals("incorrect number of elements", 1, nodesApps.getLength());
    NodeList nodes = dom.getElementsByTagName("app");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    verifyAppsXML(nodes, app1);
    rm.stop();
  }

  @Test
  public void testAppsXMLMulti() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024, "testwordcount", "user1");
    rm.submitApp(2048, "testwordcount2", "user1");

    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
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
    WebResource r = resource();

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path(path).accept(media).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 1, array.length());
    verifyAppInfo(array.getJSONObject(0), app);

  }

  @Test
  public void testAppsQueryState() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("state", RMAppState.ACCEPTED.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 1, array.length());
    verifyAppInfo(array.getJSONObject(0), app1);
    rm.stop();
  }

  @Test
  public void testAppsQueryStateNone() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("state", RMAppState.RUNNING.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("apps is not null", JSONObject.NULL, json.get("apps"));
    rm.stop();
  }

  @Test
  public void testAppsQueryStateInvalid() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .queryParam("state", "INVALID_test")
          .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
      fail("should have thrown exception on invalid state query");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils
          .checkStringContains(
              "exception message",
              "org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState.INVALID_test",
              message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "IllegalArgumentException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "java.lang.IllegalArgumentException", classname);

    } finally {
      rm.stop();
    }
  }

  @Test
  public void testAppsQueryFinalStatus() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("finalStatus", FinalApplicationStatus.UNDEFINED.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    System.out.println(json.toString());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 1, array.length());
    verifyAppInfo(array.getJSONObject(0), app1);
    rm.stop();
  }

  @Test
  public void testAppsQueryFinalStatusNone() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("finalStatus", FinalApplicationStatus.KILLED.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("apps is not null", JSONObject.NULL, json.get("apps"));
    rm.stop();
  }

  @Test
  public void testAppsQueryFinalStatusInvalid() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .queryParam("finalStatus", "INVALID_test")
          .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
      fail("should have thrown exception on invalid state query");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils
          .checkStringContains(
              "exception message",
              "org.apache.hadoop.yarn.api.records.FinalApplicationStatus.INVALID_test",
              message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "IllegalArgumentException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "java.lang.IllegalArgumentException", classname);

    } finally {
      rm.stop();
    }
  }

  @Test
  public void testAppsQueryUser() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024);
    rm.submitApp(1024);

    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();
    ClientResponse response = r
        .path("ws")
        .path("v1")
        .path("cluster")
        .path("apps")
        .queryParam("user",
            UserGroupInformation.getCurrentUser().getShortUserName())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);

    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 2, array.length());
    rm.stop();
  }

  @Test
  public void testAppsQueryQueue() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024);
    rm.submitApp(1024);

    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("queue", "default")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 2, array.length());
    rm.stop();
  }

  @Test
  public void testAppsQueryLimit() throws JSONException, Exception {
    rm.start();
    rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024);
    rm.submitApp(1024);
    rm.submitApp(1024);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("limit", "2")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
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
    rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024);
    rm.submitApp(1024);
    rm.submitApp(1024);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("startedTimeBegin", String.valueOf(start))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
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
    rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024);
    rm.submitApp(1024);
    long start = System.currentTimeMillis();
    Thread.sleep(1);
    rm.submitApp(1024);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("startedTimeBegin", String.valueOf(start))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 1, array.length());
    rm.stop();
  }

  @Test
  public void testAppsQueryStartEnd() throws JSONException, Exception {
    rm.start();
    rm.registerNode("amNM:1234", 2048);
    long end = System.currentTimeMillis();
    Thread.sleep(1);
    rm.submitApp(1024);
    rm.submitApp(1024);
    rm.submitApp(1024);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("startedTimeEnd", String.valueOf(end))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("apps is not null", JSONObject.NULL, json.get("apps"));
    rm.stop();
  }

  @Test
  public void testAppsQueryStartBeginEnd() throws JSONException, Exception {
    rm.start();
    rm.registerNode("amNM:1234", 2048);
    long start = System.currentTimeMillis();
    Thread.sleep(1);
    rm.submitApp(1024);
    rm.submitApp(1024);
    long end = System.currentTimeMillis();
    Thread.sleep(1);
    rm.submitApp(1024);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("startedTimeBegin", String.valueOf(start))
        .queryParam("startedTimeEnd", String.valueOf(end))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
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
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    long start = System.currentTimeMillis();
    Thread.sleep(1);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    // finish App
    MockAM am = rm
        .sendAMLaunched(app1.getCurrentAppAttempt().getAppAttemptId());
    am.registerAppAttempt();
    am.unregisterAppAttempt();
    amNodeManager.nodeHeartbeat(app1.getCurrentAppAttempt().getAppAttemptId(),
        1, ContainerState.COMPLETE);
    rm.submitApp(1024);
    rm.submitApp(1024);

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("finishedTimeBegin", String.valueOf(start))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 1, array.length());
    rm.stop();
  }

  @Test
  public void testAppsQueryFinishEnd() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    // finish App
    MockAM am = rm
        .sendAMLaunched(app1.getCurrentAppAttempt().getAppAttemptId());
    am.registerAppAttempt();
    am.unregisterAppAttempt();
    amNodeManager.nodeHeartbeat(app1.getCurrentAppAttempt().getAppAttemptId(),
        1, ContainerState.COMPLETE);

    rm.submitApp(1024);
    rm.submitApp(1024);
    long end = System.currentTimeMillis();

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("finishedTimeEnd", String.valueOf(end))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
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
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    long start = System.currentTimeMillis();
    Thread.sleep(1);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    // finish App
    MockAM am = rm
        .sendAMLaunched(app1.getCurrentAppAttempt().getAppAttemptId());
    am.registerAppAttempt();
    am.unregisterAppAttempt();
    amNodeManager.nodeHeartbeat(app1.getCurrentAppAttempt().getAppAttemptId(),
        1, ContainerState.COMPLETE);

    rm.submitApp(1024);
    rm.submitApp(1024);
    long end = System.currentTimeMillis();

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("finishedTimeBegin", String.valueOf(start))
        .queryParam("finishedTimeEnd", String.valueOf(end))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 1, array.length());
    rm.stop();
  }

  @Test
  public void testSingleApp() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    testSingleAppsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testSingleAppsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    testSingleAppsHelper(app1.getApplicationId().toString() + "/", app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testSingleAppsDefault() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    testSingleAppsHelper(app1.getApplicationId().toString() + "/", app1, "");
    rm.stop();
  }

  @Test
  public void testInvalidApp() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path("application_invalid_12").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "For input string: \"invalid\"", message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "NumberFormatException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "java.lang.NumberFormatException", classname);

    } finally {
      rm.stop();
    }
  }

  @Test
  public void testNonexistApp() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path("application_00000_0099").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());

      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.Exception: app with id: application_00000_0099 not found",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "NotFoundException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    } finally {
      rm.stop();
    }
  }

  public void testSingleAppsHelper(String path, RMApp app, String media)
      throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(path).accept(media).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);

    assertEquals("incorrect number of elements", 1, json.length());
    verifyAppInfo(json.getJSONObject("app"), app);
  }

  @Test
  public void testSingleAppsXML() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(app1.getApplicationId().toString())
        .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("app");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    verifyAppsXML(nodes, app1);
    rm.stop();
  }

  public void verifyAppsXML(NodeList nodes, RMApp app) throws JSONException,
      Exception {

     for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyAppInfoGeneric(app,
          WebServicesTestUtils.getXmlString(element, "id"),
          WebServicesTestUtils.getXmlString(element, "user"),
          WebServicesTestUtils.getXmlString(element, "name"),
          WebServicesTestUtils.getXmlString(element, "queue"),
          WebServicesTestUtils.getXmlString(element, "state"),
          WebServicesTestUtils.getXmlString(element, "finalStatus"),
          WebServicesTestUtils.getXmlFloat(element, "progress"),
          WebServicesTestUtils.getXmlString(element, "trackingUI"),
          WebServicesTestUtils.getXmlString(element, "diagnostics"),
          WebServicesTestUtils.getXmlLong(element, "clusterId"),
          WebServicesTestUtils.getXmlLong(element, "startedTime"),
          WebServicesTestUtils.getXmlLong(element, "finishedTime"),
          WebServicesTestUtils.getXmlLong(element, "elapsedTime"),
          WebServicesTestUtils.getXmlString(element, "amHostHttpAddress"),
          WebServicesTestUtils.getXmlString(element, "amContainerLogs"));
    }
  }

  public void verifyAppInfo(JSONObject info, RMApp app) throws JSONException,
      Exception {

    // 15 because trackingUrl not assigned yet
    assertEquals("incorrect number of elements", 15, info.length());

    verifyAppInfoGeneric(app, info.getString("id"), info.getString("user"),
        info.getString("name"), info.getString("queue"),
        info.getString("state"), info.getString("finalStatus"),
        (float) info.getDouble("progress"), info.getString("trackingUI"),
        info.getString("diagnostics"), info.getLong("clusterId"),
        info.getLong("startedTime"), info.getLong("finishedTime"),
        info.getLong("elapsedTime"), info.getString("amHostHttpAddress"),
        info.getString("amContainerLogs"));
  }

  public void verifyAppInfoGeneric(RMApp app, String id, String user,
      String name, String queue, String state, String finalStatus,
      float progress, String trackingUI, String diagnostics, long clusterId,
      long startedTime, long finishedTime, long elapsedTime,
      String amHostHttpAddress, String amContainerLogs) throws JSONException,
      Exception {

    WebServicesTestUtils.checkStringMatch("id", app.getApplicationId()
        .toString(), id);
    WebServicesTestUtils.checkStringMatch("user", app.getUser(), user);
    WebServicesTestUtils.checkStringMatch("name", app.getName(), name);
    WebServicesTestUtils.checkStringMatch("queue", app.getQueue(), queue);
    WebServicesTestUtils.checkStringMatch("state", app.getState().toString(),
        state);
    WebServicesTestUtils.checkStringMatch("finalStatus", app
        .getFinalApplicationStatus().toString(), finalStatus);
    assertEquals("progress doesn't match", 0, progress, 0.0);
    WebServicesTestUtils.checkStringMatch("trackingUI", "UNASSIGNED",
        trackingUI);
    WebServicesTestUtils.checkStringMatch("diagnostics", app.getDiagnostics()
        .toString(), diagnostics);
    assertEquals("clusterId doesn't match", ResourceManager.clusterTimeStamp,
        clusterId);
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
  }

  @Test
  public void testAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testMultipleAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    int maxAppAttempts = rm.getConfig().getInt(
        YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    assertTrue(maxAppAttempts > 1);
    int retriesLeft = maxAppAttempts;
    while (--retriesLeft > 0) {
      RMAppEvent event =
          new RMAppFailedAttemptEvent(app1.getApplicationId(),
              RMAppEventType.ATTEMPT_FAILED, "");
      app1.handle(event);
      rm.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
      amNodeManager.nodeHeartbeat(true);
    }
    assertEquals("incorrect number of attempts", maxAppAttempts,
        app1.getAppAttempts().values().size());
    testAppAttemptsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testAppAttemptsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testAppAttemtpsDefault() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1, "");
    rm.stop();
  }

  @Test
  public void testInvalidAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path("application_invalid_12").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "For input string: \"invalid\"", message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "NumberFormatException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "java.lang.NumberFormatException", classname);

    } finally {
      rm.stop();
    }
  }

  @Test
  public void testNonexistAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path("application_00000_0099").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());

      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.Exception: app with id: application_00000_0099 not found",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "NotFoundException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    } finally {
      rm.stop();
    }
  }

  public void testAppAttemptsHelper(String path, RMApp app, String media)
      throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(path).path("appattempts").accept(media)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jsonAppAttempts = json.getJSONObject("appAttempts");
    assertEquals("incorrect number of elements", 1, jsonAppAttempts.length());
    JSONArray jsonArray = jsonAppAttempts.getJSONArray("appAttempt");

    Collection<RMAppAttempt> attempts = app.getAppAttempts().values();
    assertEquals("incorrect number of elements", attempts.size(),
        jsonArray.length());

    // Verify these parallel arrays are the same
    int i = 0;
    for (RMAppAttempt attempt : attempts) {
      verifyAppAttemptsInfo(jsonArray.getJSONObject(i), attempt);
      ++i;
    }
  }

  @Test
  public void testAppAttemptsXML() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(app1.getApplicationId().toString())
        .path("appattempts").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("appAttempts");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    NodeList attempt = dom.getElementsByTagName("appAttempt");
    assertEquals("incorrect number of elements", 1, attempt.getLength());
    verifyAppAttemptsXML(attempt, app1.getCurrentAppAttempt());
    rm.stop();
  }

  public void verifyAppAttemptsXML(NodeList nodes, RMAppAttempt appAttempt)
      throws JSONException, Exception {

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyAppAttemptInfoGeneric(appAttempt,
          WebServicesTestUtils.getXmlInt(element, "id"),
          WebServicesTestUtils.getXmlLong(element, "startTime"),
          WebServicesTestUtils.getXmlString(element, "containerId"),
          WebServicesTestUtils.getXmlString(element, "nodeHttpAddress"),
          WebServicesTestUtils.getXmlString(element, "nodeId"),
          WebServicesTestUtils.getXmlString(element, "logsLink"));
    }
  }

  public void verifyAppAttemptsInfo(JSONObject info, RMAppAttempt appAttempt)
      throws JSONException, Exception {

    assertEquals("incorrect number of elements", 6, info.length());

    verifyAppAttemptInfoGeneric(appAttempt, info.getInt("id"),
        info.getLong("startTime"), info.getString("containerId"),
        info.getString("nodeHttpAddress"), info.getString("nodeId"),
        info.getString("logsLink"));
  }

  public void verifyAppAttemptInfoGeneric(RMAppAttempt appAttempt, int id,
      long startTime, String containerId, String nodeHttpAddress, String nodeId,
      String logsLink)
          throws JSONException, Exception {

    assertEquals("id doesn't match", appAttempt.getAppAttemptId()
        .getAttemptId(), id);
    assertEquals("startedTime doesn't match", appAttempt.getStartTime(),
        startTime);
    WebServicesTestUtils.checkStringMatch("containerId", appAttempt
        .getMasterContainer().getId().toString(), containerId);
    WebServicesTestUtils.checkStringMatch("nodeHttpAddress", appAttempt
        .getMasterContainer().getNodeHttpAddress(), nodeHttpAddress);
    WebServicesTestUtils.checkStringMatch("nodeId", appAttempt
        .getMasterContainer().getNodeId().toString(), nodeId);
    assertTrue("logsLink doesn't match",
        logsLink.startsWith("http://"));
    assertTrue(
        "logsLink doesn't contain user info", logsLink.endsWith("/"
        + appAttempt.getSubmissionContext().getAMContainerSpec().getUser()));
  }

}

