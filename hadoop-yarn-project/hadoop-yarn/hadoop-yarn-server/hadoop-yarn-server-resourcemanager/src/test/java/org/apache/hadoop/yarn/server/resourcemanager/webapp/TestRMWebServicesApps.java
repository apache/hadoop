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
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
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
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestRMWebServicesApps extends JerseyTest {

  private static MockRM rm;
  
  private static final int CONTAINER_MB = 1024;

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
      bind(QueueACLsManager.class).toInstance(rm.getQueueACLsManager());
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
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testAppsHelper("apps", app1, MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testAppsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testAppsHelper("apps/", app1, MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testAppsDefault() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testAppsHelper("apps/", app1, "");
    rm.stop();
  }

  @Test
  public void testAppsXML() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
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
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
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
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps")
        .queryParam("state", YarnApplicationState.ACCEPTED.toString())
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
  public void testAppsQueryStates() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB);
    RMApp killedApp = rm.submitApp(CONTAINER_MB);
    rm.killApp(killedApp.getApplicationId());

    amNodeManager.nodeHeartbeat(true);

    WebResource r = resource();
    MultivaluedMapImpl params = new MultivaluedMapImpl();
    params.add("states", YarnApplicationState.ACCEPTED.toString());
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParams(params)
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 1, array.length());
    assertEquals("state not equal to ACCEPTED", "ACCEPTED", array
        .getJSONObject(0).getString("state"));

    r = resource();
    params = new MultivaluedMapImpl();
    params.add("states", YarnApplicationState.ACCEPTED.toString());
    params.add("states", YarnApplicationState.KILLED.toString());
    response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParams(params)
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
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
    rm.submitApp(CONTAINER_MB);
    RMApp killedApp = rm.submitApp(CONTAINER_MB);
    rm.killApp(killedApp.getApplicationId());

    amNodeManager.nodeHeartbeat(true);

    WebResource r = resource();
    MultivaluedMapImpl params = new MultivaluedMapImpl();
    params.add("states", YarnApplicationState.ACCEPTED.toString());
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParams(params)
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 1, array.length());
    assertEquals("state not equal to ACCEPTED", "ACCEPTED", array
        .getJSONObject(0).getString("state"));

    r = resource();
    params = new MultivaluedMapImpl();
    params.add("states", YarnApplicationState.ACCEPTED.toString() + ","
        + YarnApplicationState.KILLED.toString());
    response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParams(params)
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
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
    rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps")
        .queryParam("states", YarnApplicationState.RUNNING.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("apps is not null", JSONObject.NULL, json.get("apps"));
    rm.stop();
  }

  @Test
  public void testAppsQueryStateNone() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps")
        .queryParam("state", YarnApplicationState.RUNNING.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("apps is not null", JSONObject.NULL, json.get("apps"));
    rm.stop();
  }

  @Test
  public void testAppsQueryStatesInvalid() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .queryParam("states", "INVALID_test")
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
      WebServicesTestUtils.checkStringContains(
          "exception message",
          "Invalid application-state INVALID_test",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

    } finally {
      rm.stop();
    }
  }

  @Test
  public void testAppsQueryStateInvalid() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB);
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
      WebServicesTestUtils.checkStringContains(
          "exception message",
          "Invalid application-state INVALID_test",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

    } finally {
      rm.stop();
    }
  }

  @Test
  public void testAppsQueryFinalStatus() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
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
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB);
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
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB);
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
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB);
    rm.submitApp(CONTAINER_MB);

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
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB);
    rm.submitApp(CONTAINER_MB);

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
    rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB);
    rm.submitApp(CONTAINER_MB);
    rm.submitApp(CONTAINER_MB);
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
    rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB);
    rm.submitApp(CONTAINER_MB);
    rm.submitApp(CONTAINER_MB);
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
    rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB);
    rm.submitApp(CONTAINER_MB);
    long start = System.currentTimeMillis();
    Thread.sleep(1);
    rm.submitApp(CONTAINER_MB);
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
    rm.registerNode("127.0.0.1:1234", 2048);
    long end = System.currentTimeMillis();
    Thread.sleep(1);
    rm.submitApp(CONTAINER_MB);
    rm.submitApp(CONTAINER_MB);
    rm.submitApp(CONTAINER_MB);
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
    rm.registerNode("127.0.0.1:1234", 2048);
    long start = System.currentTimeMillis();
    Thread.sleep(1);
    rm.submitApp(CONTAINER_MB);
    rm.submitApp(CONTAINER_MB);
    long end = System.currentTimeMillis();
    Thread.sleep(1);
    rm.submitApp(CONTAINER_MB);
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
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    long start = System.currentTimeMillis();
    Thread.sleep(1);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    // finish App
    MockAM am = rm
        .sendAMLaunched(app1.getCurrentAppAttempt().getAppAttemptId());
    am.registerAppAttempt();
    am.unregisterAppAttempt();
    amNodeManager.nodeHeartbeat(app1.getCurrentAppAttempt().getAppAttemptId(),
        1, ContainerState.COMPLETE);
    rm.submitApp(CONTAINER_MB);
    rm.submitApp(CONTAINER_MB);

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
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    // finish App
    MockAM am = rm
        .sendAMLaunched(app1.getCurrentAppAttempt().getAppAttemptId());
    am.registerAppAttempt();
    am.unregisterAppAttempt();
    amNodeManager.nodeHeartbeat(app1.getCurrentAppAttempt().getAppAttemptId(),
        1, ContainerState.COMPLETE);

    rm.submitApp(CONTAINER_MB);
    rm.submitApp(CONTAINER_MB);
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
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    long start = System.currentTimeMillis();
    Thread.sleep(1);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    // finish App
    MockAM am = rm
        .sendAMLaunched(app1.getCurrentAppAttempt().getAppAttemptId());
    am.registerAppAttempt();
    am.unregisterAppAttempt();
    amNodeManager.nodeHeartbeat(app1.getCurrentAppAttempt().getAppAttemptId(),
        1, ContainerState.COMPLETE);

    rm.submitApp(CONTAINER_MB);
    rm.submitApp(CONTAINER_MB);
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
  public void testAppsQueryAppTypes() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    Thread.sleep(1);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    // finish App
    MockAM am = rm
        .sendAMLaunched(app1.getCurrentAppAttempt().getAppAttemptId());
    am.registerAppAttempt();
    am.unregisterAppAttempt();
    amNodeManager.nodeHeartbeat(app1.getCurrentAppAttempt().getAppAttemptId(),
        1, ContainerState.COMPLETE);

    rm.submitApp(CONTAINER_MB, "", UserGroupInformation.getCurrentUser()
        .getShortUserName(), null, false, null, 2, null, "MAPREDUCE");
    rm.submitApp(CONTAINER_MB, "", UserGroupInformation.getCurrentUser()
        .getShortUserName(), null, false, null, 2, null, "NON-YARN");

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("applicationTypes", "MAPREDUCE")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 1, array.length());
    assertEquals("MAPREDUCE",
        array.getJSONObject(0).getString("applicationType"));

    r = resource();
    response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .queryParam("applicationTypes", "YARN")
            .queryParam("applicationTypes", "MAPREDUCE")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
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

    r = resource();
    response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .queryParam("applicationTypes", "YARN,NON-YARN")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
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

    r = resource();
    response = r.path("ws").path("v1").path("cluster")
        .path("apps").queryParam("applicationTypes", "")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 3, array.length());

    r = resource();
    response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .queryParam("applicationTypes", "YARN,NON-YARN")
            .queryParam("applicationTypes", "MAPREDUCE")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 3, array.length());

    r = resource();
    response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .queryParam("applicationTypes", "YARN")
            .queryParam("applicationTypes", "")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 1, array.length());
    assertEquals("YARN",
        array.getJSONObject(0).getString("applicationType"));

    r = resource();
    response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .queryParam("applicationTypes", ",,, ,, YARN ,, ,")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 1, array.length());
    assertEquals("YARN",
        array.getJSONObject(0).getString("applicationType"));

    r = resource();
    response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .queryParam("applicationTypes", ",,, ,,  ,, ,")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    array = apps.getJSONArray("app");
    assertEquals("incorrect number of elements", 3, array.length());

    r = resource();
    response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .queryParam("applicationTypes", "YARN, ,NON-YARN, ,,")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
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

    r = resource();
    response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .queryParam("applicationTypes", " YARN, ,  ,,,")
            .queryParam("applicationTypes", "MAPREDUCE , ,, ,")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
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
  public void testAppStatistics() throws JSONException, Exception {
    try {
      rm.start();
      MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 4096);
      Thread.sleep(1);
      RMApp app1 = rm.submitApp(CONTAINER_MB, "", UserGroupInformation.getCurrentUser()
          .getShortUserName(), null, false, null, 2, null, "MAPREDUCE");
      amNodeManager.nodeHeartbeat(true);
      // finish App
      MockAM am = rm
          .sendAMLaunched(app1.getCurrentAppAttempt().getAppAttemptId());
      am.registerAppAttempt();
      am.unregisterAppAttempt();
      amNodeManager.nodeHeartbeat(app1.getCurrentAppAttempt().getAppAttemptId(),
          1, ContainerState.COMPLETE);

      rm.submitApp(CONTAINER_MB, "", UserGroupInformation.getCurrentUser()
          .getShortUserName(), null, false, null, 2, null, "MAPREDUCE");
      rm.submitApp(CONTAINER_MB, "", UserGroupInformation.getCurrentUser()
          .getShortUserName(), null, false, null, 2, null, "OTHER");

      // zero type, zero state
      WebResource r = resource();
      ClientResponse response = r.path("ws").path("v1").path("cluster")
          .path("appstatistics")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
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
      r = resource();
      response = r.path("ws").path("v1").path("cluster")
          .path("appstatistics")
          .queryParam("states", YarnApplicationState.ACCEPTED.toString())
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      appsStatInfo = json.getJSONObject("appStatInfo");
      assertEquals("incorrect number of elements", 1, appsStatInfo.length());
      statItems = appsStatInfo.getJSONArray("statItem");
      assertEquals("incorrect number of elements", 1, statItems.length());
      assertEquals("ACCEPTED", statItems.getJSONObject(0).getString("state"));
      assertEquals("*", statItems.getJSONObject(0).getString("type"));
      assertEquals("2", statItems.getJSONObject(0).getString("count"));

      // one type, zero state
      r = resource();
      response = r.path("ws").path("v1").path("cluster")
          .path("appstatistics")
          .queryParam("applicationTypes", "MAPREDUCE")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      json = response.getEntity(JSONObject.class);
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
      r = resource();
      response = r.path("ws").path("v1").path("cluster")
          .path("appstatistics")
          .queryParam("applicationTypes", "MAPREDUCE,OTHER")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      json = response.getEntity(JSONObject.class);
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
      r = resource();
      response = r.path("ws").path("v1").path("cluster")
          .path("appstatistics")
          .queryParam("states", YarnApplicationState.FINISHED.toString()
              + "," + YarnApplicationState.ACCEPTED.toString())
          .queryParam("applicationTypes", "MAPREDUCE")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      json = response.getEntity(JSONObject.class);
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
      r = resource();
      response = r.path("ws").path("v1").path("cluster")
          .path("appstatistics").queryParam("states", "wrong_state")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      json = response.getEntity(JSONObject.class);
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
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    testSingleAppsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testSingleAppsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testSingleAppsHelper(app1.getApplicationId().toString() + "/", app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testSingleAppsDefault() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testSingleAppsHelper(app1.getApplicationId().toString() + "/", app1, "");
    rm.stop();
  }

  @Test
  public void testInvalidApp() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB);
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
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
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
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
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
          WebServicesTestUtils.getXmlString(element, "applicationType"),
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
          WebServicesTestUtils.getXmlString(element, "amContainerLogs"),
          WebServicesTestUtils.getXmlInt(element, "allocatedMB"),
          WebServicesTestUtils.getXmlInt(element, "allocatedVCores"),
          WebServicesTestUtils.getXmlInt(element, "runningContainers"),
          WebServicesTestUtils.getXmlInt(element, "preemptedResourceMB"),
          WebServicesTestUtils.getXmlInt(element, "preemptedResourceVCores"),
          WebServicesTestUtils.getXmlInt(element, "numNonAMContainerPreempted"),
          WebServicesTestUtils.getXmlInt(element, "numAMContainerPreempted"));
    }
  }

  public void verifyAppInfo(JSONObject info, RMApp app) throws JSONException,
      Exception {

    // 28 because trackingUrl not assigned yet
    assertEquals("incorrect number of elements", 24, info.length());

    verifyAppInfoGeneric(app, info.getString("id"), info.getString("user"),
        info.getString("name"), info.getString("applicationType"),
        info.getString("queue"), info.getString("state"),
        info.getString("finalStatus"), (float) info.getDouble("progress"),
        info.getString("trackingUI"), info.getString("diagnostics"),
        info.getLong("clusterId"), info.getLong("startedTime"),
        info.getLong("finishedTime"), info.getLong("elapsedTime"),
        info.getString("amHostHttpAddress"), info.getString("amContainerLogs"),
        info.getInt("allocatedMB"), info.getInt("allocatedVCores"),
        info.getInt("runningContainers"), 
        info.getInt("preemptedResourceMB"),
        info.getInt("preemptedResourceVCores"),
        info.getInt("numNonAMContainerPreempted"),
        info.getInt("numAMContainerPreempted"));
  }

  public void verifyAppInfoGeneric(RMApp app, String id, String user,
      String name, String applicationType, String queue, String state,
      String finalStatus, float progress, String trackingUI,
      String diagnostics, long clusterId, long startedTime, long finishedTime,
      long elapsedTime, String amHostHttpAddress, String amContainerLogs,
      int allocatedMB, int allocatedVCores, int numContainers,
      int preemptedResourceMB, int preemptedResourceVCores,
      int numNonAMContainerPreempted, int numAMContainerPreempted) throws JSONException,
      Exception {

    WebServicesTestUtils.checkStringMatch("id", app.getApplicationId()
        .toString(), id);
    WebServicesTestUtils.checkStringMatch("user", app.getUser(), user);
    WebServicesTestUtils.checkStringMatch("name", app.getName(), name);
    WebServicesTestUtils.checkStringMatch("applicationType",
      app.getApplicationType(), applicationType);
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
    assertEquals("numContainers doesn't match", 1, numContainers);
    assertEquals("preemptedResourceMB doesn't match", app
        .getRMAppMetrics().getResourcePreempted().getMemory(),
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
  }

  @Test
  public void testAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString(), app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test (timeout = 20000)
  public void testMultipleAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 8192);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    MockAM am = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    int maxAppAttempts = rm.getConfig().getInt(
        YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    assertTrue(maxAppAttempts > 1);
    int numAttempt = 1;
    while (true) {
      // fail the AM by sending CONTAINER_FINISHED event without registering.
      amNodeManager.nodeHeartbeat(am.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
      am.waitForState(RMAppAttemptState.FAILED);
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
  public void testAppAttemptsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1,
        MediaType.APPLICATION_JSON);
    rm.stop();
  }

  @Test
  public void testAppAttemtpsDefault() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB);
    amNodeManager.nodeHeartbeat(true);
    testAppAttemptsHelper(app1.getApplicationId().toString() + "/", app1, "");
    rm.stop();
  }

  @Test
  public void testInvalidAppAttempts() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB);
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
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
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
      verifyAppAttemptsInfo(jsonArray.getJSONObject(i), attempt, app.getUser());
      ++i;
    }
  }

  @Test
  public void testAppAttemptsXML() throws JSONException, Exception {
    rm.start();
    String user = "user1";
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", user);
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
    verifyAppAttemptsXML(attempt, app1.getCurrentAppAttempt(), user);
    rm.stop();
  }

  public void verifyAppAttemptsXML(NodeList nodes, RMAppAttempt appAttempt,
      String user)
      throws JSONException, Exception {

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyAppAttemptInfoGeneric(appAttempt,
          WebServicesTestUtils.getXmlInt(element, "id"),
          WebServicesTestUtils.getXmlLong(element, "startTime"),
          WebServicesTestUtils.getXmlString(element, "containerId"),
          WebServicesTestUtils.getXmlString(element, "nodeHttpAddress"),
          WebServicesTestUtils.getXmlString(element, "nodeId"),
          WebServicesTestUtils.getXmlString(element, "logsLink"), user);
    }
  }

  public void verifyAppAttemptsInfo(JSONObject info, RMAppAttempt appAttempt,
      String user)
      throws JSONException, Exception {

    assertEquals("incorrect number of elements", 6, info.length());

    verifyAppAttemptInfoGeneric(appAttempt, info.getInt("id"),
        info.getLong("startTime"), info.getString("containerId"),
        info.getString("nodeHttpAddress"), info.getString("nodeId"),
        info.getString("logsLink"), user);
  }

  public void verifyAppAttemptInfoGeneric(RMAppAttempt appAttempt, int id,
      long startTime, String containerId, String nodeHttpAddress, String nodeId,
      String logsLink, String user)
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
    assertTrue("logsLink doesn't match", logsLink.startsWith("//"));
    assertTrue(
        "logsLink doesn't contain user info", logsLink.endsWith("/"
        + user));
  }

}

