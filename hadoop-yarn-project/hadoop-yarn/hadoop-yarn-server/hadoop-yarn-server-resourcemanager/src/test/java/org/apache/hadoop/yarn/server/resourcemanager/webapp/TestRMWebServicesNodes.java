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
import java.util.ArrayList;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
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

public class TestRMWebServicesNodes extends JerseyTest {

  private static MockRM rm;

  private Injector injector = Guice.createInjector(new ServletModule() {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      rm = new MockRM(new Configuration());
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

  public TestRMWebServicesNodes() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testNodes() throws JSONException, Exception {
    testNodesHelper("nodes", MediaType.APPLICATION_JSON);
  }

  @Test
  public void testNodesSlash() throws JSONException, Exception {
    testNodesHelper("nodes/", MediaType.APPLICATION_JSON);
  }

  @Test
  public void testNodesDefault() throws JSONException, Exception {
    testNodesHelper("nodes/", "");
  }

  @Test
  public void testNodesDefaultWithUnHealthyNode() throws JSONException,
      Exception {

    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.NMwaitForState(nm1.getNodeId(), RMNodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), RMNodeState.NEW);

    // One unhealthy node which should not appear in the list after
    // MAPREDUCE-3760.
    MockNM nm3 = rm.registerNode("h3:1236", 5122);
    rm.NMwaitForState(nm3.getNodeId(), RMNodeState.NEW);
    rm.sendNodeStarted(nm3);
    rm.NMwaitForState(nm3.getNodeId(), RMNodeState.RUNNING);
    RMNodeImpl node = (RMNodeImpl) rm.getRMContext().getRMNodes()
        .get(nm3.getNodeId());
    NodeHealthStatus nodeHealth = node.getNodeHealthStatus();
    nodeHealth.setHealthReport("test health report");
    nodeHealth.setIsNodeHealthy(false);
    node.handle(new RMNodeStatusEvent(nm3.getNodeId(), nodeHealth,
        new ArrayList<ContainerStatus>(), null, null));
    rm.NMwaitForState(nm3.getNodeId(), RMNodeState.UNHEALTHY);

    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    // Just 2 nodes, leaving behind the unhealthy node.
    assertEquals("incorrect number of elements", 2, nodeArray.length());
  }

  @Test
  public void testNodesQueryState() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.NMwaitForState(nm1.getNodeId(), RMNodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), RMNodeState.NEW);

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("state", RMNodeState.RUNNING.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 1, nodeArray.length());
    JSONObject info = nodeArray.getJSONObject(0);

    verifyNodeInfo(info, nm1);
  }

  @Test
  public void testNodesQueryStateNone() throws JSONException, Exception {
    WebResource r = resource();
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes")
        .queryParam("state", RMNodeState.DECOMMISSIONED.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("nodes is not null", JSONObject.NULL, json.get("nodes"));
  }

  @Test
  public void testNodesQueryStateInvalid() throws JSONException, Exception {
    WebResource r = resource();
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);

    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .queryParam("state", "BOGUSSTATE").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);

      fail("should have thrown exception querying invalid state");
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
              "org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeState.BOGUSSTATE",
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
  public void testNodesQueryStateLost() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1234", 5120);
    rm.sendNodeStarted(nm1);
    rm.sendNodeStarted(nm2);
    rm.NMwaitForState(nm1.getNodeId(), RMNodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), RMNodeState.RUNNING);
    rm.sendNodeLost(nm1);
    rm.sendNodeLost(nm2);

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("state", RMNodeState.LOST.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 2, nodeArray.length());
    for (int i = 0; i < nodeArray.length(); ++i) {
      JSONObject info = nodeArray.getJSONObject(i);
      String host = info.get("id").toString().split(":")[0];
      RMNode rmNode = rm.getRMContext().getInactiveRMNodes().get(host);
      WebServicesTestUtils.checkStringMatch("nodeHTTPAddress", "",
          info.getString("nodeHTTPAddress"));
      WebServicesTestUtils.checkStringMatch("state", rmNode.getState()
          .toString(), info.getString("state"));
    }
  }
  
  @Test
  public void testSingleNodeQueryStateLost() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1234", 5120);
    rm.sendNodeStarted(nm1);
    rm.sendNodeStarted(nm2);
    rm.NMwaitForState(nm1.getNodeId(), RMNodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), RMNodeState.RUNNING);
    rm.sendNodeLost(nm1);
    rm.sendNodeLost(nm2);

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").path("h2:1234").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    JSONObject info = json.getJSONObject("node");
    String id = info.get("id").toString();

    assertEquals("Incorrect Node Information.", "h2:1234", id);

    RMNode rmNode = rm.getRMContext().getInactiveRMNodes().get("h2");
    WebServicesTestUtils.checkStringMatch("nodeHTTPAddress", "",
        info.getString("nodeHTTPAddress"));
    WebServicesTestUtils.checkStringMatch("state",
        rmNode.getState().toString(), info.getString("state"));
  }

  @Test
  public void testNodesQueryHealthy() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.NMwaitForState(nm1.getNodeId(), RMNodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), RMNodeState.NEW);
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("healthy", "true")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 2, nodeArray.length());
  }

  @Test
  public void testNodesQueryHealthyCase() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.NMwaitForState(nm1.getNodeId(), RMNodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), RMNodeState.NEW);
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("healthy", "TRUe")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 2, nodeArray.length());

  }

  @Test
  public void testNodesQueryHealthyAndState() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.NMwaitForState(nm2.getNodeId(), RMNodeState.NEW);
    rm.NMwaitForState(nm1.getNodeId(), RMNodeState.RUNNING);
    RMNodeImpl node = (RMNodeImpl) rm.getRMContext().getRMNodes()
        .get(nm1.getNodeId());
    NodeHealthStatus nodeHealth = node.getNodeHealthStatus();
    nodeHealth.setHealthReport("test health report");
    nodeHealth.setIsNodeHealthy(false);
    node.handle(new RMNodeStatusEvent(nm1.getNodeId(), nodeHealth,
        new ArrayList<ContainerStatus>(), null, null));
    rm.NMwaitForState(nm1.getNodeId(), RMNodeState.UNHEALTHY);

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("healthy", "true")
        .queryParam("state", RMNodeState.RUNNING.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("nodes is not null", JSONObject.NULL, json.get("nodes"));
  }

  @Test
  public void testNodesQueryHealthyFalse() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.NMwaitForState(nm1.getNodeId(), RMNodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), RMNodeState.NEW);
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("healthy", "false")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("nodes is not null", JSONObject.NULL, json.get("nodes"));
  }

  @Test
  public void testNodesQueryHealthyInvalid() throws JSONException, Exception {
    WebResource r = resource();
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);

    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .queryParam("healthy", "tr").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception querying invalid healthy string");
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
          .checkStringMatch(
              "exception message",
              "java.lang.Exception: Error: You must specify either true or false to query on health",
              message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

    } finally {
      rm.stop();
    }
  }

  public void testNodesHelper(String path, String media) throws JSONException,
      Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path(path).accept(media).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 2, nodeArray.length());
    JSONObject info = nodeArray.getJSONObject(0);
    String id = info.get("id").toString();

    if (id.matches("h1:1234")) {
      verifyNodeInfo(info, nm1);
      verifyNodeInfo(nodeArray.getJSONObject(1), nm2);
    } else {
      verifyNodeInfo(info, nm2);
      verifyNodeInfo(nodeArray.getJSONObject(1), nm1);
    }
  }

  @Test
  public void testSingleNode() throws JSONException, Exception {
    rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    testSingleNodeHelper("h2:1235", nm2, MediaType.APPLICATION_JSON);
  }

  @Test
  public void testSingleNodeSlash() throws JSONException, Exception {
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    testSingleNodeHelper("h1:1234/", nm1, MediaType.APPLICATION_JSON);
  }

  @Test
  public void testSingleNodeDefault() throws JSONException, Exception {
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    testSingleNodeHelper("h1:1234/", nm1, "");
  }

  public void testSingleNodeHelper(String nodeid, MockNM nm, String media)
      throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").path(nodeid).accept(media).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject info = json.getJSONObject("node");
    verifyNodeInfo(info, nm);
  }

  @Test
  public void testNonexistNode() throws JSONException, Exception {
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid:99").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
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
      verifyNonexistNodeException(message, type, classname);

    } finally {
      rm.stop();
    }
  }

  // test that the exception output defaults to JSON
  @Test
  public void testNonexistNodeDefault() throws JSONException, Exception {
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid:99").get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
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
      verifyNonexistNodeException(message, type, classname);
    } finally {
      rm.stop();
    }
  }

  // test that the exception output works in XML
  @Test
  public void testNonexistNodeXML() throws JSONException, Exception {
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid:99").accept(MediaType.APPLICATION_XML)
          .get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
      String msg = response.getEntity(String.class);
      System.out.println(msg);
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(msg));
      Document dom = db.parse(is);
      NodeList nodes = dom.getElementsByTagName("RemoteException");
      Element element = (Element) nodes.item(0);
      String message = WebServicesTestUtils.getXmlString(element, "message");
      String type = WebServicesTestUtils.getXmlString(element, "exception");
      String classname = WebServicesTestUtils.getXmlString(element,
          "javaClassName");
      verifyNonexistNodeException(message, type, classname);
    } finally {
      rm.stop();
    }
  }

  private void verifyNonexistNodeException(String message, String type, String classname) {
    assertTrue("exception message incorrect",
        "java.lang.Exception: nodeId, node_invalid:99, is not found"
            .matches(message));
    assertTrue("exception type incorrect", "NotFoundException".matches(type));
    assertTrue("exception className incorrect",
        "org.apache.hadoop.yarn.webapp.NotFoundException".matches(classname));
  }

  @Test
  public void testInvalidNode() throws JSONException, Exception {
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);

    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid_foo").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
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
          "Invalid NodeId \\[node_invalid_foo\\]. Expected host:port", message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "IllegalArgumentException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "java.lang.IllegalArgumentException", classname);
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testNodesXML() throws JSONException, Exception {
    rm.start();
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    // MockNM nm2 = rm.registerNode("h2:1235", 5121);
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodesApps = dom.getElementsByTagName("nodes");
    assertEquals("incorrect number of elements", 1, nodesApps.getLength());
    NodeList nodes = dom.getElementsByTagName("node");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    verifyNodesXML(nodes, nm1);
    rm.stop();
  }

  @Test
  public void testSingleNodesXML() throws JSONException, Exception {
    rm.start();
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    // MockNM nm2 = rm.registerNode("h2:1235", 5121);
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").path("h1:1234").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("node");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    verifyNodesXML(nodes, nm1);
    rm.stop();
  }

  @Test
  public void testNodes2XML() throws JSONException, Exception {
    rm.start();
    WebResource r = resource();
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodesApps = dom.getElementsByTagName("nodes");
    assertEquals("incorrect number of elements", 1, nodesApps.getLength());
    NodeList nodes = dom.getElementsByTagName("node");
    assertEquals("incorrect number of elements", 2, nodes.getLength());
    rm.stop();
  }

  public void verifyNodesXML(NodeList nodes, MockNM nm) throws JSONException,
      Exception {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);
      verifyNodeInfoGeneric(nm,
          WebServicesTestUtils.getXmlString(element, "state"),
          WebServicesTestUtils.getXmlString(element, "rack"),
          WebServicesTestUtils.getXmlString(element, "healthStatus"),
          WebServicesTestUtils.getXmlString(element, "id"),
          WebServicesTestUtils.getXmlString(element, "nodeHostName"),
          WebServicesTestUtils.getXmlString(element, "nodeHTTPAddress"),
          WebServicesTestUtils.getXmlLong(element, "lastHealthUpdate"),
          WebServicesTestUtils.getXmlString(element, "healthReport"),
          WebServicesTestUtils.getXmlInt(element, "numContainers"),
          WebServicesTestUtils.getXmlLong(element, "usedMemoryMB"),
          WebServicesTestUtils.getXmlLong(element, "availMemoryMB"));
    }
  }

  public void verifyNodeInfo(JSONObject nodeInfo, MockNM nm)
      throws JSONException, Exception {
    assertEquals("incorrect number of elements", 11, nodeInfo.length());

    verifyNodeInfoGeneric(nm, nodeInfo.getString("state"),
        nodeInfo.getString("rack"), nodeInfo.getString("healthStatus"),
        nodeInfo.getString("id"), nodeInfo.getString("nodeHostName"),
        nodeInfo.getString("nodeHTTPAddress"),
        nodeInfo.getLong("lastHealthUpdate"),
        nodeInfo.getString("healthReport"), nodeInfo.getInt("numContainers"),
        nodeInfo.getLong("usedMemoryMB"), nodeInfo.getLong("availMemoryMB"));

  }

  public void verifyNodeInfoGeneric(MockNM nm, String state, String rack,
      String healthStatus, String id, String nodeHostName,
      String nodeHTTPAddress, long lastHealthUpdate, String healthReport,
      int numContainers, long usedMemoryMB, long availMemoryMB)
      throws JSONException, Exception {

    RMNode node = rm.getRMContext().getRMNodes().get(nm.getNodeId());
    NodeHealthStatus health = node.getNodeHealthStatus();
    ResourceScheduler sched = rm.getResourceScheduler();
    SchedulerNodeReport report = sched.getNodeReport(nm.getNodeId());

    WebServicesTestUtils.checkStringMatch("state", node.getState().toString(),
        state);
    WebServicesTestUtils.checkStringMatch("rack", node.getRackName(), rack);
    WebServicesTestUtils.checkStringMatch("healthStatus", "Healthy",
        healthStatus);
    WebServicesTestUtils.checkStringMatch("id", nm.getNodeId().toString(), id);
    WebServicesTestUtils.checkStringMatch("nodeHostName", nm.getNodeId()
        .getHost(), nodeHostName);
    WebServicesTestUtils.checkStringMatch("healthReport",
        String.valueOf(health.getHealthReport()), healthReport);
    String expectedHttpAddress = nm.getNodeId().getHost() + ":"
        + nm.getHttpPort();
    WebServicesTestUtils.checkStringMatch("nodeHTTPAddress",
        expectedHttpAddress, nodeHTTPAddress);

    long expectedHealthUpdate = health.getLastHealthReportTime();
    assertEquals("lastHealthUpdate doesn't match, got: " + lastHealthUpdate
        + " expected: " + expectedHealthUpdate, expectedHealthUpdate,
        lastHealthUpdate);

    if (report != null) {
      assertEquals("numContainers doesn't match: " + numContainers,
          report.getNumContainers(), numContainers);
      assertEquals("usedMemoryMB doesn't match: " + usedMemoryMB, report
          .getUsedResource().getMemory(), usedMemoryMB);
      assertEquals("availMemoryMB doesn't match: " + availMemoryMB, report
          .getAvailableResource().getMemory(), availMemoryMB);
    }
  }

}
