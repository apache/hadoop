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

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.EnumSet;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStartedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
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

import com.google.common.base.Joiner;
import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestRMWebServicesNodes extends JerseyTestBase {

  private static MockRM rm;

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      rm = new MockRM(new Configuration());
      rm.getRMContext().getContainerTokenSecretManager().rollMasterKey();
      rm.getRMContext().getNMTokenSecretManager().rollMasterKey();
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
    }
  }

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
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
    getRunningRMNode("h1", 1234, 5120);
    // h2 will be in NEW state
    getNewRMNode("h2", 1235, 5121);

    RMNode node3 = getRunningRMNode("h3", 1236, 5122);
    NodeId nodeId3 = node3.getNodeID();

    RMNode node = rm.getRMContext().getRMNodes().get(nodeId3);
    NodeHealthStatus nodeHealth = NodeHealthStatus.newInstance(false,
        "test health report", System.currentTimeMillis());
    NodeStatus nodeStatus = NodeStatus.newInstance(nodeId3, 1,
      new ArrayList<ContainerStatus>(), null, nodeHealth, null, null, null);
    ((RMNodeImpl) node)
        .handle(new RMNodeStatusEvent(nodeId3, nodeStatus, null));
    rm.waitForState(nodeId3, NodeState.UNHEALTHY);

    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    // 3 nodes, including the unhealthy node and the new node.
    assertEquals("incorrect number of elements", 3, nodeArray.length());
  }

  private RMNode getRunningRMNode(String host, int port, int memory) {
    RMNodeImpl rmnode1 = getNewRMNode(host, port, memory);
    sendStartedEvent(rmnode1);
    return rmnode1;
  }

  private void sendStartedEvent(RMNode node) {
    ((RMNodeImpl) node)
        .handle(new RMNodeStartedEvent(node.getNodeID(), null, null));
  }

  private void sendLostEvent(RMNode node) {
    ((RMNodeImpl) node)
        .handle(new RMNodeEvent(node.getNodeID(), RMNodeEventType.EXPIRE));
  }

  private RMNodeImpl getNewRMNode(String host, int port, int memory) {
    NodeId nodeId = NodeId.newInstance(host, port);
    RMNodeImpl nodeImpl = new RMNodeImpl(nodeId, rm.getRMContext(),
        nodeId.getHost(), nodeId.getPort(), nodeId.getPort() + 1,
        RackResolver.resolve(nodeId.getHost()), Resource.newInstance(memory, 4),
        YarnVersionInfo.getVersion());
    rm.getRMContext().getRMNodes().put(nodeId, nodeImpl);
    return nodeImpl;
  }

  @Test
  public void testNodesQueryNew() throws JSONException, Exception {
    WebResource r = resource();
    getRunningRMNode("h1", 1234, 5120);
    // h2 will be in NEW state
    RMNode rmnode2 = getNewRMNode("h2", 1235, 5121);

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("states", NodeState.NEW.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 1, nodeArray.length());
    JSONObject info = nodeArray.getJSONObject(0);

    verifyNodeInfo(info, rmnode2);
  }

  @Test
  public void testNodesQueryStateNone() throws JSONException, Exception {
    WebResource r = resource();
    getNewRMNode("h1", 1234, 5120);
    getNewRMNode("h2", 1235, 5121);

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes")
        .queryParam("states", NodeState.DECOMMISSIONED.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("nodes is not empty",
        new JSONObject().toString(), json.get("nodes").toString());
  }

  @Test
  public void testNodesQueryStateInvalid() throws JSONException, Exception {
    WebResource r = resource();
    getNewRMNode("h1", 1234, 5120);
    getNewRMNode("h2", 1235, 5121);

    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .queryParam("states", "BOGUSSTATE").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);

      fail("should have thrown exception querying invalid state");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertResponseStatusCode(Status.BAD_REQUEST, response.getStatusInfo());
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
              "org.apache.hadoop.yarn.api.records.NodeState.BOGUSSTATE",
              message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "IllegalArgumentException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "java.lang.IllegalArgumentException", classname);

    }
  }
  
  @Test
  public void testNodesQueryStateLost() throws JSONException, Exception {
    WebResource r = resource();
    RMNode rmnode1 = getRunningRMNode("h1", 1234, 5120);
    sendLostEvent(rmnode1);
    RMNode rmnode2 = getRunningRMNode("h2", 1235, 5121);
    sendLostEvent(rmnode2);

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("states", NodeState.LOST.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 2, nodeArray.length());
    for (int i = 0; i < nodeArray.length(); ++i) {
      JSONObject info = nodeArray.getJSONObject(i);
      String[] node = info.get("id").toString().split(":");
      NodeId nodeId = NodeId.newInstance(node[0], Integer.parseInt(node[1]));
      RMNode rmNode = rm.getRMContext().getInactiveRMNodes().get(nodeId);
      WebServicesTestUtils.checkStringMatch("nodeHTTPAddress", "",
          info.getString("nodeHTTPAddress"));
      if (rmNode != null) {
        WebServicesTestUtils.checkStringMatch("state",
            rmNode.getState().toString(), info.getString("state"));
      }
    }
  }
  
  @Test
  public void testSingleNodeQueryStateLost() throws JSONException, Exception {
    WebResource r = resource();
    getRunningRMNode("h1", 1234, 5120);
    RMNode rmnode2 = getRunningRMNode("h2", 1234, 5121);
    sendLostEvent(rmnode2);

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").path("h2:1234").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    JSONObject info = json.getJSONObject("node");
    String id = info.get("id").toString();

    assertEquals("Incorrect Node Information.", "h2:1234", id);

    RMNode rmNode =
        rm.getRMContext().getInactiveRMNodes().get(rmnode2.getNodeID());
    WebServicesTestUtils.checkStringMatch("nodeHTTPAddress", "",
        info.getString("nodeHTTPAddress"));
    if (rmNode != null) {
      WebServicesTestUtils.checkStringMatch("state",
          rmNode.getState().toString(), info.getString("state"));
    }
  }

  @Test
  public void testNodesQueryRunning() throws JSONException, Exception {
    WebResource r = resource();
    getRunningRMNode("h1", 1234, 5120);
    // h2 will be in NEW state
    getNewRMNode("h2", 1235, 5121);
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("states", "running")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 1, nodeArray.length());
  }

  @Test
  public void testNodesQueryHealthyFalse() throws JSONException, Exception {
    WebResource r = resource();
    getRunningRMNode("h1", 1234, 5120);
    // h2 will be in NEW state
    getNewRMNode("h2", 1235, 5121);
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("states", "UNHEALTHY")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("nodes is not empty",
        new JSONObject().toString(), json.get("nodes").toString());
  }

  public void testNodesHelper(String path, String media) throws JSONException,
      Exception {
    WebResource r = resource();
    RMNode rmnode1 = getRunningRMNode("h1", 1234, 5120);
    RMNode rmnode2 = getRunningRMNode("h2", 1235, 5121);

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
      verifyNodeInfo(info, rmnode1);
      verifyNodeInfo(nodeArray.getJSONObject(1), rmnode2);
    } else {
      verifyNodeInfo(info, rmnode2);
      verifyNodeInfo(nodeArray.getJSONObject(1), rmnode1);
    }
  }

  @Test
  public void testSingleNode() throws JSONException, Exception {
    getRunningRMNode("h1", 1234, 5120);
    RMNode rmnode2 = getRunningRMNode("h2", 1235, 5121);
    testSingleNodeHelper("h2:1235", rmnode2, MediaType.APPLICATION_JSON);
  }

  @Test
  public void testSingleNodeSlash() throws JSONException, Exception {
    RMNode rmnode1 = getRunningRMNode("h1", 1234, 5120);
    getRunningRMNode("h2", 1235, 5121);
    testSingleNodeHelper("h1:1234/", rmnode1, MediaType.APPLICATION_JSON);
  }

  @Test
  public void testSingleNodeDefault() throws JSONException, Exception {
    RMNode rmnode1 = getRunningRMNode("h1", 1234, 5120);
    getRunningRMNode("h2", 1235, 5121);
    testSingleNodeHelper("h1:1234/", rmnode1, "");
  }

  public void testSingleNodeHelper(String nodeid, RMNode nm, String media)
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
    // add h1 node in NEW state
    getNewRMNode("h1", 1234, 5120);
    // add h2 node in NEW state
    getNewRMNode("h2", 1235, 5121);
    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid:99").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      verifyNonexistNodeException(message, type, classname);

    }
  }

  // test that the exception output defaults to JSON
  @Test
  public void testNonexistNodeDefault() throws JSONException, Exception {
    getNewRMNode("h1", 1234, 5120);
    getNewRMNode("h2", 1235, 5121);
    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid:99").get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      verifyNonexistNodeException(message, type, classname);
    }
  }

  // test that the exception output works in XML
  @Test
  public void testNonexistNodeXML() throws JSONException, Exception {
    getNewRMNode("h1", 1234, 5120);
    getNewRMNode("h2", 1235, 5121);
    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid:99").accept(MediaType.APPLICATION_XML)
          .get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
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
    getNewRMNode("h1", 1234, 5120);
    getNewRMNode("h2", 1235, 5121);

    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid_foo").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertResponseStatusCode(Status.BAD_REQUEST, response.getStatusInfo());
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
    }
  }

  @Test
  public void testNodesXML() throws JSONException, Exception {
    WebResource r = resource();
    RMNodeImpl rmnode1 = getNewRMNode("h1", 1234, 5120);
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
    verifyNodesXML(nodes, rmnode1);
  }

  @Test
  public void testSingleNodesXML() throws JSONException, Exception {
    WebResource r = resource();
    // add h2 node in NEW state
    RMNodeImpl rmnode1 = getNewRMNode("h1", 1234, 5120);
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
    verifyNodesXML(nodes, rmnode1);
  }

  @Test
  public void testNodes2XML() throws JSONException, Exception {
    WebResource r = resource();
    getNewRMNode("h1", 1234, 5120);
    getNewRMNode("h2", 1235, 5121);
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
  }
  
  @Test
  public void testQueryAll() throws Exception {
    WebResource r = resource();
    getRunningRMNode("h1", 1234, 5120);
    // add h2 node in NEW state
    getNewRMNode("h2", 1235, 5121);
    // add lost node
    RMNode nm3 = getRunningRMNode("h3", 1236, 5122);
    sendLostEvent(nm3);

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes")
        .queryParam("states", Joiner.on(',').join(EnumSet.allOf(NodeState.class)))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 3, nodeArray.length());
  }

  @Test
  public void testNodesResourceUtilization() throws JSONException, Exception {
    WebResource r = resource();
    RMNode rmnode1 = getRunningRMNode("h1", 1234, 5120);
    NodeId nodeId1 = rmnode1.getNodeID();

    RMNodeImpl node = (RMNodeImpl) rm.getRMContext().getRMNodes().get(nodeId1);
    NodeHealthStatus nodeHealth = NodeHealthStatus.newInstance(true,
        "test health report", System.currentTimeMillis());
    ResourceUtilization nodeResource = ResourceUtilization.newInstance(4096, 0,
        (float) 10.5);
    ResourceUtilization containerResource = ResourceUtilization.newInstance(
        2048, 0, (float) 5.05);
    NodeStatus nodeStatus =
        NodeStatus.newInstance(nodeId1, 0,
        new ArrayList<ContainerStatus>(), null, nodeHealth, containerResource,
        nodeResource, null);
    node.handle(new RMNodeStatusEvent(nodeId1, nodeStatus, null));
    rm.waitForState(nodeId1, NodeState.RUNNING);

    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("nodes").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 1, nodeArray.length());
    JSONObject info = nodeArray.getJSONObject(0);

    // verify the resource utilization
    verifyNodeInfo(info, rmnode1);
  }

  public void verifyNodesXML(NodeList nodes, RMNode nm)
      throws JSONException,
      Exception {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);
      verifyNodeInfoGeneric(nm,
          WebServicesTestUtils.getXmlString(element, "state"),
          WebServicesTestUtils.getXmlString(element, "rack"),
          WebServicesTestUtils.getXmlString(element, "id"),
          WebServicesTestUtils.getXmlString(element, "nodeHostName"),
          WebServicesTestUtils.getXmlString(element, "nodeHTTPAddress"),
          WebServicesTestUtils.getXmlLong(element, "lastHealthUpdate"),
          WebServicesTestUtils.getXmlString(element, "healthReport"),
          WebServicesTestUtils.getXmlInt(element, "numContainers"),
          WebServicesTestUtils.getXmlLong(element, "usedMemoryMB"),
          WebServicesTestUtils.getXmlLong(element, "availMemoryMB"),
          WebServicesTestUtils.getXmlLong(element, "usedVirtualCores"),
          WebServicesTestUtils.getXmlLong(element,  "availableVirtualCores"),
          WebServicesTestUtils.getXmlString(element, "version"),
          WebServicesTestUtils.getXmlInt(element, "nodePhysicalMemoryMB"),
          WebServicesTestUtils.getXmlInt(element, "nodeVirtualMemoryMB"),
          WebServicesTestUtils.getXmlFloat(element, "nodeCPUUsage"),
          WebServicesTestUtils.getXmlInt(element,
              "aggregatedContainersPhysicalMemoryMB"),
          WebServicesTestUtils.getXmlInt(element,
              "aggregatedContainersVirtualMemoryMB"),
          WebServicesTestUtils.getXmlFloat(element, "containersCPUUsage"));
    }
  }

  public void verifyNodeInfo(JSONObject nodeInfo, RMNode nm)
      throws JSONException, Exception {
    assertEquals("incorrect number of elements", 14, nodeInfo.length());

    JSONObject resourceInfo = nodeInfo.getJSONObject("resourceUtilization");
    verifyNodeInfoGeneric(nm, nodeInfo.getString("state"),
        nodeInfo.getString("rack"),
        nodeInfo.getString("id"), nodeInfo.getString("nodeHostName"),
        nodeInfo.getString("nodeHTTPAddress"),
        nodeInfo.getLong("lastHealthUpdate"),
        nodeInfo.getString("healthReport"), nodeInfo.getInt("numContainers"),
        nodeInfo.getLong("usedMemoryMB"), nodeInfo.getLong("availMemoryMB"),
        nodeInfo.getLong("usedVirtualCores"), nodeInfo.getLong("availableVirtualCores"),
        nodeInfo.getString("version"),
        resourceInfo.getInt("nodePhysicalMemoryMB"),
        resourceInfo.getInt("nodeVirtualMemoryMB"),
        resourceInfo.getDouble("nodeCPUUsage"),
        resourceInfo.getInt("aggregatedContainersPhysicalMemoryMB"),
        resourceInfo.getInt("aggregatedContainersVirtualMemoryMB"),
        resourceInfo.getDouble("containersCPUUsage"));
  }

  public void verifyNodeInfoGeneric(RMNode node, String state, String rack,
      String id, String nodeHostName,
      String nodeHTTPAddress, long lastHealthUpdate, String healthReport,
      int numContainers, long usedMemoryMB, long availMemoryMB, long usedVirtualCores, 
      long availVirtualCores, String version, int nodePhysicalMemoryMB,
      int nodeVirtualMemoryMB, double nodeCPUUsage,
      int containersPhysicalMemoryMB, int containersVirtualMemoryMB,
      double containersCPUUsage)
      throws JSONException, Exception {

    ResourceScheduler sched = rm.getResourceScheduler();
    SchedulerNodeReport report = sched.getNodeReport(node.getNodeID());

    WebServicesTestUtils.checkStringMatch("state", node.getState().toString(),
        state);
    WebServicesTestUtils.checkStringMatch("rack", node.getRackName(), rack);
    WebServicesTestUtils.checkStringMatch("id", node.getNodeID().toString(),
        id);
    WebServicesTestUtils.checkStringMatch("nodeHostName",
        node.getNodeID().getHost(), nodeHostName);
    WebServicesTestUtils.checkStringMatch("healthReport",
        String.valueOf(node.getHealthReport()), healthReport);
    String expectedHttpAddress =
        node.getNodeID().getHost() + ":" + node.getHttpPort();
    WebServicesTestUtils.checkStringMatch("nodeHTTPAddress",
        expectedHttpAddress, nodeHTTPAddress);
    WebServicesTestUtils.checkStringMatch("version",
        node.getNodeManagerVersion(), version);
    if (node.getNodeUtilization() != null) {
      ResourceUtilization nodeResource = ResourceUtilization.newInstance(
          nodePhysicalMemoryMB, nodeVirtualMemoryMB, (float) nodeCPUUsage);
      assertEquals("nodeResourceUtilization doesn't match",
          node.getNodeUtilization(), nodeResource);
    }
    if (node.getAggregatedContainersUtilization() != null) {
      ResourceUtilization containerResource = ResourceUtilization.newInstance(
          containersPhysicalMemoryMB, containersVirtualMemoryMB,
          (float) containersCPUUsage);
      assertEquals("containerResourceUtilization doesn't match",
          node.getAggregatedContainersUtilization(), containerResource);
    }

    long expectedHealthUpdate = node.getLastHealthReportTime();
    assertEquals("lastHealthUpdate doesn't match, got: " + lastHealthUpdate
        + " expected: " + expectedHealthUpdate, expectedHealthUpdate,
        lastHealthUpdate);

    if (report != null) {
      assertEquals("numContainers doesn't match: " + numContainers,
          report.getNumContainers(), numContainers);
      assertEquals("usedMemoryMB doesn't match: " + usedMemoryMB, report
          .getUsedResource().getMemorySize(), usedMemoryMB);
      assertEquals("availMemoryMB doesn't match: " + availMemoryMB, report
          .getAvailableResource().getMemorySize(), availMemoryMB);
      assertEquals("usedVirtualCores doesn't match: " + usedVirtualCores, report
          .getUsedResource().getVirtualCores(), usedVirtualCores);
      assertEquals("availVirtualCores doesn't match: " + availVirtualCores, report
          .getAvailableResource().getVirtualCores(), availVirtualCores);
    }
  }

}
