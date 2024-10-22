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

import static org.apache.hadoop.yarn.server.resourcemanager.MockNM.createMockNodeStatus;
import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.Iterator;

import javax.inject.Singleton;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStartedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceOptionInfo;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
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

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

public class TestRMWebServicesNodes extends JerseyTestBase {

  private static MockRM rm;
  private static YarnConfiguration conf;

  private static String userName;

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(RMWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(TestRMCustomAuthFilter.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    forceSet(TestProperties.CONTAINER_PORT, JERSEY_RANDOM_PORT);
    return config;
  }

  private static class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {

      try {
        userName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException ioe) {
        throw new RuntimeException("Unable to get current user name "
            + ioe.getMessage(), ioe);
      }

      conf = new YarnConfiguration();
      conf.set(YarnConfiguration.YARN_ADMIN_ACL, userName);

      rm = new MockRM(conf);
      rm.getRMContext().getContainerTokenSecretManager().rollMasterKey();
      rm.getRMContext().getNMTokenSecretManager().rollMasterKey();
      rm.disableDrainEventsImplicitly();

      final HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getScheme()).thenReturn("http");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(userName);
      bind(response).to(HttpServletResponse.class);
    }
  }

  /**
   * Custom filter to be able to test auth methods and let the other ones go.
   */
  @Singleton
  public static class TestRMCustomAuthFilter extends AuthenticationFilter {

    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) throws ServletException {
      Properties props = new Properties();
      Enumeration<?> names = filterConfig.getInitParameterNames();
      while (names.hasMoreElements()) {
        String name = (String) names.nextElement();
        if (name.startsWith(configPrefix)) {
          String value = filterConfig.getInitParameter(name);
          props.put(name.substring(configPrefix.length()), value);
        }
      }
      props.put(AuthenticationFilter.AUTH_TYPE, "simple");
      props.put(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "true");
      return props;
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestRMWebServicesNodes() {
  }

  @Test
  public void testNodes() throws Exception {
    testNodesHelper("nodes", MediaType.APPLICATION_JSON);
  }

  @Test
  public void testNodesSlash() throws Exception {
    testNodesHelper("nodes/", MediaType.APPLICATION_JSON);
  }

  @Test
  public void testNodesDefault() throws Exception {
    testNodesHelper("nodes/", "");
  }

  @Test
  public void testNodesDefaultWithUnHealthyNode() throws JSONException,
      Exception {

    WebTarget r = target();
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

    Response response =
        r.path("ws").path("v1").path("cluster").path("nodes")
          .request(MediaType.APPLICATION_JSON).get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
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
    NodeStatus mockNodeStatus = createMockNodeStatus();
    ((RMNodeImpl) node)
        .handle(new RMNodeStartedEvent(node.getNodeID(), null, null,
        mockNodeStatus));
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
  public void testNodesQueryNew() throws Exception {
    WebTarget r = target();
    getRunningRMNode("h1", 1234, 5120);
    // h2 will be in NEW state
    RMNode rmnode2 = getNewRMNode("h2", 1235, 5121);

    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("states", NodeState.NEW.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONObject node = nodes.getJSONObject("node");
    JSONArray nodeArray = new JSONArray();
    nodeArray.put(node);

    assertEquals("incorrect number of elements", 1, nodeArray.length());
    JSONObject info = nodeArray.getJSONObject(0);

    verifyNodeInfo(info, rmnode2);
  }

  @Test
  public void testNodesQueryStateNone() throws JSONException, Exception {
    WebTarget r = target();
    getNewRMNode("h1", 1234, 5120);
    getNewRMNode("h2", 1235, 5121);

    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes")
        .queryParam("states", NodeState.DECOMMISSIONED.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("nodes is not empty", "", json.get("nodes").toString());
  }

  @Test
  public void testNodesQueryStateInvalid() throws JSONException, Exception {
    WebTarget r = target();
    getNewRMNode("h1", 1234, 5120);
    getNewRMNode("h2", 1235, 5121);

    Response response = r.path("ws").path("v1").path("cluster").path("nodes")
        .queryParam("states", "BOGUSSTATE").request(MediaType.APPLICATION_JSON)
        .get();

    assertResponseStatusCode(Response.Status.BAD_REQUEST, response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());

    String json = response.readEntity(String.class);
    JSONObject msg = new JSONObject(json);
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
  
  @Test
  public void testNodesQueryStateLost() throws JSONException, Exception {
    WebTarget r = target();
    RMNode rmnode1 = getRunningRMNode("h1", 1234, 5120);
    sendLostEvent(rmnode1);
    RMNode rmnode2 = getRunningRMNode("h2", 1235, 5121);
    sendLostEvent(rmnode2);

    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("states", NodeState.LOST.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
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
    WebTarget r = target();
    getRunningRMNode("h1", 1234, 5120);
    RMNode rmnode2 = getRunningRMNode("h2", 1234, 5121);
    sendLostEvent(rmnode2);

    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes").path("h2:1234").request(MediaType.APPLICATION_JSON)
        .get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
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
    WebTarget r = target();
    getRunningRMNode("h1", 1234, 5120);
    // h2 will be in NEW state
    getNewRMNode("h2", 1235, 5121);
    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("states", "running")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONObject node = nodes.getJSONObject("node");
    JSONArray nodeArray = new JSONArray();
    nodeArray.put(node);
    assertEquals("incorrect number of elements", 1, nodeArray.length());
  }

  @Test
  public void testNodesQueryHealthyFalse() throws JSONException, Exception {
    WebTarget r = target();
    getRunningRMNode("h1", 1234, 5120);
    // h2 will be in NEW state
    getNewRMNode("h2", 1235, 5121);
    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes").queryParam("states", "UNHEALTHY")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("nodes is not empty", "", json.get("nodes").toString());
  }

  public void testNodesHelper(String path, String media) throws JSONException,
      Exception {
    WebTarget r = target();
    RMNode rmnode1 = getRunningRMNode("h1", 1234, 5120);
    RMNode rmnode2 = getRunningRMNode("h2", 1235, 5121);

    Response response = r.path("ws").path("v1").path("cluster")
        .path(path).request(media).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
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
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes").path(nodeid).request(media).get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject info = json.getJSONObject("node");
    verifyNodeInfo(info, nm);
  }

  @Test
  public void testNonexistNode() throws Exception {
    // add h1 node in NEW state
    getNewRMNode("h1", 1234, 5120);
    // add h2 node in NEW state
    getNewRMNode("h2", 1235, 5121);
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster").path("nodes")
        .path("node_invalid:99").request(MediaType.APPLICATION_JSON)
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
    verifyNonexistNodeException(message, type, classname);
  }

  // test that the exception output defaults to JSON
  @Test
  public void testNonexistNodeDefault() throws Exception {
    getNewRMNode("h1", 1234, 5120);
    getNewRMNode("h2", 1235, 5121);
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster").path("nodes")
        .path("node_invalid:99").request().get();

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
    verifyNonexistNodeException(message, type, classname);
  }

  // test that the exception output works in XML
  @Test
  public void testNonexistNodeXML() throws JSONException, Exception {
    getNewRMNode("h1", 1234, 5120);
    getNewRMNode("h2", 1235, 5121);
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster").path("nodes")
        .path("node_invalid:99").request(MediaType.APPLICATION_XML)
        .get();
    assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String msg = response.readEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource(new StringReader(msg));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("RemoteException");
    Element element = (Element) nodes.item(0);
    String message = WebServicesTestUtils.getXmlString(element, "message");
    String type = WebServicesTestUtils.getXmlString(element, "exception");
    String classname = WebServicesTestUtils.getXmlString(element, "javaClassName");
    verifyNonexistNodeException(message, type, classname);
  }

  private void verifyNonexistNodeException(String message, String type, String classname) {
    assertTrue("exception message incorrect: " + message,
        "nodeId, node_invalid:99, is not found".matches(message));
    assertTrue("exception type incorrect", "NotFoundException".matches(type));
    assertTrue("exception className incorrect",
        "org.apache.hadoop.yarn.webapp.NotFoundException".matches(classname));
  }

  @Test
  public void testInvalidNode() throws Exception {
    getNewRMNode("h1", 1234, 5120);
    getNewRMNode("h2", 1235, 5121);

    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster").path("nodes")
        .path("node_invalid_foo").request(MediaType.APPLICATION_JSON).get();
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
        "Invalid NodeId \\[node_invalid_foo\\]. Expected host:port", message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "IllegalArgumentException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "java.lang.IllegalArgumentException", classname);
  }

  @Test
  public void testNodesXML() throws Exception {
    WebTarget r = target();
    RMNodeImpl rmnode1 = getNewRMNode("h1", 1234, 5120);
    // MockNM nm2 = rm.registerNode("h2:1235", 5121);
    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes").request(MediaType.APPLICATION_XML)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodesApps = dom.getElementsByTagName("nodes");
    assertEquals("incorrect number of elements", 1, nodesApps.getLength());
    NodeList nodes = dom.getElementsByTagName("node");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    verifyNodesXML(nodes, rmnode1);
  }

  @Test
  public void testSingleNodesXML() throws Exception {
    WebTarget r = target();
    // add h2 node in NEW state
    RMNodeImpl rmnode1 = getNewRMNode("h1", 1234, 5120);
    // MockNM nm2 = rm.registerNode("h2:1235", 5121);
    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes").path("h1:1234").request(MediaType.APPLICATION_XML)
        .get(Response.class);

    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);

    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("node");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    verifyNodesXML(nodes, rmnode1);
  }

  @Test
  public void testNodes2XML() throws Exception {
    WebTarget r = target();
    getNewRMNode("h1", 1234, 5120);
    getNewRMNode("h2", 1235, 5121);
    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes").request(MediaType.APPLICATION_XML)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);

    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
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
    WebTarget r = target();
    getRunningRMNode("h1", 1234, 5120);
    // add h2 node in NEW state
    getNewRMNode("h2", 1235, 5121);
    // add lost node
    RMNode nm3 = getRunningRMNode("h3", 1236, 5122);
    sendLostEvent(nm3);

    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes")
        .queryParam("states", Joiner.on(',').join(EnumSet.allOf(NodeState.class)))
        .request(MediaType.APPLICATION_JSON).get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 3, nodeArray.length());
  }

  @Test
  public void testNodesResourceUtilization() throws JSONException, Exception {
    WebTarget r = target();
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

    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes").request(MediaType.APPLICATION_JSON)
        .get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONObject jsonNode = nodes.getJSONObject("node");
    JSONArray nodeArray = new JSONArray();
    nodeArray.put(jsonNode);
    assertEquals("incorrect number of elements", 1, nodeArray.length());
    JSONObject info = nodeArray.getJSONObject(0);

    // verify the resource utilization
    verifyNodeInfo(info, rmnode1);
  }

  @Test
  public void testUpdateNodeResource() throws Exception {
    WebTarget r = target().path(RMWSConsts.RM_WEB_SERVICE_PATH);

    r = r.queryParam("user.name", userName);
    RMNode rmnode = getRunningRMNode("h1", 1234, 5120);
    String rmnodeId = rmnode.getNodeID().toString();
    assertEquals("h1:1234", rmnodeId);

    // assert memory and default vcores
    Response response = r.path(RMWSConsts.NODES).path(rmnodeId)
        .request(MediaType.APPLICATION_XML)
        .get(Response.class);
    NodeInfo nodeInfo0 = response.readEntity(NodeInfo.class);
    ResourceInfo nodeResource0 = nodeInfo0.getTotalResource();
    assertEquals(5120, nodeResource0.getMemorySize());
    assertEquals(4, nodeResource0.getvCores());

    // the RM needs to be running to process the resource update
    rm.start();

    // update memory to 8192MB and 5 cores
    Resource resource = Resource.newInstance(8192, 5);
    ResourceOptionInfo resourceOption = new ResourceOptionInfo(
        ResourceOption.newInstance(resource, 1000));
    response = r.path(RMWSConsts.NODES).path(rmnodeId).path("resource")
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(resourceOption), Response.class);
    assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
    ResourceInfo updatedResource = response.readEntity(ResourceInfo.class);
    assertEquals(8192, updatedResource.getMemorySize());
    assertEquals(5, updatedResource.getvCores());

    // assert updated memory and cores
    response = r.path(RMWSConsts.NODES).path(rmnodeId)
        .request(MediaType.APPLICATION_XML)
        .get(Response.class);
    NodeInfo nodeInfo1 = response.readEntity(NodeInfo.class);
    ResourceInfo nodeResource1 = nodeInfo1.getTotalResource();
    assertEquals(8192, nodeResource1.getMemorySize());
    assertEquals(5, nodeResource1.getvCores());

    // test non existing node
    response = r.path(RMWSConsts.NODES).path("badnode").path("resource")
        .request(MediaType.APPLICATION_XML_TYPE)
        .post(Entity.json(resourceOption), Response.class);
    assertResponseStatusCode(Response.Status.BAD_REQUEST, response.getStatusInfo());
    JSONObject json = response.readEntity(JSONObject.class);
    JSONObject exception = json.getJSONObject("RemoteException");
    assertEquals("IllegalArgumentException", exception.getString("exception"));
    String msg = exception.getString("message");
    assertTrue("Wrong message: " + msg, msg.startsWith("Invalid NodeId"));

    rm.stop();
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
          WebServicesTestUtils.getXmlFloat(element, "containersCPUUsage"),
          WebServicesTestUtils.getXmlInt(element, "numRunningOpportContainers"),
          WebServicesTestUtils.getXmlLong(element, "usedMemoryOpportGB"),
          WebServicesTestUtils.getXmlInt(element, "usedVirtualCoresOpport"),
          WebServicesTestUtils.getXmlInt(element, "numQueuedContainers"));
    }
  }

  public void verifyNodeInfo(JSONObject nodeInfo, RMNode nm)
      throws JSONException, Exception {
    assertEquals("incorrect number of elements", 23, nodeInfo.length());

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
        resourceInfo.getDouble("containersCPUUsage"),
        nodeInfo.getInt("numRunningOpportContainers"),
        nodeInfo.getLong("usedMemoryOpportGB"),
        nodeInfo.getInt("usedVirtualCoresOpport"),
        nodeInfo.getInt("numQueuedContainers"));
  }

  public void verifyNodeInfoGeneric(RMNode node, String state, String rack,
      String id, String nodeHostName,
      String nodeHTTPAddress, long lastHealthUpdate, String healthReport,
      int numContainers, long usedMemoryMB, long availMemoryMB,
      long usedVirtualCores, long availVirtualCores, String version,
      int nodePhysicalMemoryMB, int nodeVirtualMemoryMB, double nodeCPUUsage,
      int containersPhysicalMemoryMB, int containersVirtualMemoryMB,
      double containersCPUUsage, int numRunningOpportContainers,
      long usedMemoryOpportGB, int usedVirtualCoresOpport,
      int numQueuedContainers)
      throws JSONException, Exception {

    ResourceScheduler sched = rm.getResourceScheduler();
    SchedulerNodeReport report = sched.getNodeReport(node.getNodeID());
    OpportunisticContainersStatus opportunisticStatus =
        node.getOpportunisticContainersStatus();

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

    if (opportunisticStatus != null) {
      assertEquals("numRunningOpportContainers doesn't match: " +
              numRunningOpportContainers,
          opportunisticStatus.getRunningOpportContainers(),
          numRunningOpportContainers);
      assertEquals("usedMemoryOpportGB doesn't match: " + usedMemoryOpportGB,
          opportunisticStatus.getOpportMemoryUsed(), usedMemoryOpportGB);
      assertEquals(
          "usedVirtualCoresOpport doesn't match: " + usedVirtualCoresOpport,
          opportunisticStatus.getOpportCoresUsed(), usedVirtualCoresOpport);
      assertEquals("numQueuedContainers doesn't match: " + numQueuedContainers,
          opportunisticStatus.getQueuedOpportContainers(), numQueuedContainers);
    }
  }

  @Test
  public void testNodesAllocationTags() throws Exception {
    NodeId nm1 = NodeId.newInstance("host1", 1234);
    NodeId nm2 = NodeId.newInstance("host2", 2345);
    AllocationTagsManager atm = mock(AllocationTagsManager.class);

    Map<String, Map<String, Long>> expectedAllocationTags = new TreeMap<>();
    Map<String, Long> nm1Tags = new TreeMap<>();
    nm1Tags.put("A", 1L);
    nm1Tags.put("B", 2L);
    Map<String, Long> nm2Tags = new TreeMap<>();
    nm2Tags.put("C", 1L);
    nm2Tags.put("D", 2L);
    expectedAllocationTags.put(nm1.toString(), nm1Tags);
    expectedAllocationTags.put(nm2.toString(), nm2Tags);

    when(atm.getAllocationTagsWithCount(nm1)).thenReturn(nm1Tags);
    when(atm.getAllocationTagsWithCount(nm2)).thenReturn(nm2Tags);
    rm.getRMContext().setAllocationTagsManager(atm);

    rm.start();

    rm.registerNode(nm1.toString(), 1024);
    rm.registerNode(nm2.toString(), 1024);

    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes").request("application/json").get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject nodesInfoJson = new JSONObject(entity);
    verifyNodeAllocationTag(nodesInfoJson, expectedAllocationTags);

    rm.stop();
  }

  @Test
  public void testNodeAttributesInfo() throws Exception {
    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest registerReq =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host1", 1234);
    Resource capability = Resources.createResource(1024);
    registerReq.setResource(capability);
    registerReq.setNodeId(nodeId);
    registerReq.setHttpPort(1234);
    registerReq.setNMVersion(YarnVersionInfo.getVersion());
    RegisterNodeManagerResponse registerResponse =
        resourceTrackerService.registerNodeManager(registerReq);

    Set<NodeAttribute> nodeAttributes = new HashSet<>();
    nodeAttributes.add(NodeAttribute.newInstance(
        NodeAttribute.PREFIX_DISTRIBUTED, "host",
        NodeAttributeType.STRING, "host1"));
    nodeAttributes.add(NodeAttribute.newInstance(
        NodeAttribute.PREFIX_DISTRIBUTED, "rack",
        NodeAttributeType.STRING, "rack1"));

    NodeHeartbeatRequest heartbeatReq =
        Records.newRecord(NodeHeartbeatRequest.class);
    NodeStatus nodeStatus =
        NodeStatus.newInstance(nodeId, 0, new ArrayList<ContainerStatus>(),
        null, null, null, null, null);
    heartbeatReq.setNodeStatus(nodeStatus);
    heartbeatReq.setLastKnownNMTokenMasterKey(registerResponse
        .getNMTokenMasterKey());
    heartbeatReq.setLastKnownContainerTokenMasterKey(registerResponse
        .getContainerTokenMasterKey());
    heartbeatReq.setNodeAttributes(nodeAttributes);
    resourceTrackerService.nodeHeartbeat(heartbeatReq);

    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("nodes").request("application/json").get(Response.class);

    String entity =  response.readEntity(String.class);
    JSONObject nodesInfoJson = new JSONObject(entity);
    JSONObject jsonNodes = nodesInfoJson.getJSONObject("nodes");
    JSONObject jsonNode = jsonNodes.getJSONObject("node");
    JSONArray nodes = new JSONArray();
    nodes.put(jsonNode);
    JSONObject nodeJson = nodes.getJSONObject(0);
    JSONArray nodeAttributesInfo = nodeJson.getJSONObject("nodeAttributesInfo")
        .getJSONArray("nodeAttributeInfo");
    assertEquals(nodeAttributes.size(), nodeAttributesInfo.length());

    Iterator<NodeAttribute> it = nodeAttributes.iterator();
    for (int j=0; j<nodeAttributesInfo.length(); j++) {
      JSONObject nodeAttributeInfo = nodeAttributesInfo.getJSONObject(j);
      NodeAttribute expectedNodeAttribute = it.next();
      String expectedPrefix = expectedNodeAttribute.getAttributeKey()
          .getAttributePrefix();
      String expectedName = expectedNodeAttribute.getAttributeKey()
          .getAttributeName();
      String expectedType = expectedNodeAttribute.getAttributeType()
          .toString();
      String expectedValue = expectedNodeAttribute.getAttributeValue();
      assertEquals(expectedPrefix, nodeAttributeInfo.getString("prefix"));
      assertEquals(expectedName, nodeAttributeInfo.getString("name"));
      assertEquals(expectedType, nodeAttributeInfo.getString("type"));
      assertEquals(expectedValue, nodeAttributeInfo.getString("value"));
    }
  }

  private void verifyNodeAllocationTag(JSONObject json,
      Map<String, Map<String, Long>> expectedAllocationTags)
      throws JSONException {
    JSONArray nodes = json.getJSONObject("nodes").getJSONArray("node");
    assertEquals(expectedAllocationTags.size(), nodes.length());
    for (int i=0; i<nodes.length(); i++) {
      JSONObject nodeJson = nodes.getJSONObject(i);
      String nodeId = nodeJson.getString("id");

      // Ensure the response contains all nodes info
      assertTrue("Nodes info should have expected node IDs",
          expectedAllocationTags.containsKey(nodeId));

      Map<String, Long> expectedTags = expectedAllocationTags.get(nodeId);
      JSONArray tagsInfo = nodeJson.getJSONObject("allocationTags")
          .getJSONArray("allocationTagInfo");

      // Ensure number of tags are expected.
      assertEquals(expectedTags.size(), tagsInfo.length());

      // Iterate expected tags and make sure the actual
      // tags/counts are matched.
      Iterator<String> it = expectedTags.keySet().iterator();
      for (int j=0; j<tagsInfo.length(); j++) {
        JSONObject tagInfo = tagsInfo.getJSONObject(j);
        String expectedTag = it.next();
        assertThat(tagInfo.getString("allocationTag")).isEqualTo(expectedTag);
        assertThat(tagInfo.getLong("allocationsCount")).isEqualTo(
            expectedTags.get(expectedTag).longValue());
      }
    }
  }

}
