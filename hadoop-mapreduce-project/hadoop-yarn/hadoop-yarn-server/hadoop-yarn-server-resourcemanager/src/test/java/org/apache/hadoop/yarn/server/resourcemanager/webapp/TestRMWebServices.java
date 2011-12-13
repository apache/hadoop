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

import java.util.ArrayList;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
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

public class TestRMWebServices extends JerseyTest {

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

  public TestRMWebServices() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testCluster() throws JSONException, Exception {
    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("cluster")
        .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testClusterSlash() throws JSONException, Exception {
    WebResource r = resource();
    // test with trailing "/" to make sure acts same as without slash
    JSONObject json = r.path("ws").path("v1").path("cluster/")
        .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testInfo() throws JSONException, Exception {
    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("cluster").path("info")
        .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testInfoSlash() throws JSONException, Exception {
    WebResource r = resource();
    // test with trailing "/" to make sure acts same as without slash
    JSONObject json = r.path("ws").path("v1").path("cluster").path("info/")
        .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
    verifyClusterInfo(json);
  }

  public void verifyClusterInfo(JSONObject json) throws JSONException,
      Exception {
    assertEquals("correct number of elements", 1, json.length());
    JSONObject clusterinfo = json.getJSONObject("clusterInfo");
    assertEquals("correct number of elements", 9, clusterinfo.length());
    String clusterid = clusterinfo.get("id").toString();
    assertTrue("clusterId doesn't match: " + clusterid, clusterid.toString()
        .matches("^\\d+"));
    String startedon = clusterinfo.get("startedOn").toString();
    assertTrue("startedOn doesn't match: " + startedon,
        startedon.matches("^\\d+"));
    String state = clusterinfo.get("state").toString();
    assertTrue("stated doesn't match: " + state, state.matches("INITED"));
    String rmVersion = clusterinfo.get("resourceManagerVersion").toString();
    assertTrue("rm version doesn't match: " + rmVersion,
        rmVersion.matches(".*"));
    String rmBuildVersion = clusterinfo.get("resourceManagerBuildVersion")
        .toString();
    assertTrue("rm Build version doesn't match: " + rmBuildVersion,
        rmBuildVersion.matches(".*"));
    String rmVersionBuiltOn = clusterinfo.get("resourceManagerVersionBuiltOn")
        .toString();
    assertTrue(
        "rm version built on doesn't match: " + rmVersionBuiltOn,
        rmVersionBuiltOn
            .matches("^\\w+\\s+\\w+\\s+\\d+\\s+\\d\\d:\\d\\d:\\d\\d\\s+\\w+\\s+\\d\\d\\d\\d"));
    String hadoopVersion = clusterinfo.get("hadoopVersion").toString();
    assertTrue("hadoop version doesn't match: " + hadoopVersion,
        hadoopVersion.matches(".*"));
    String hadoopBuildVersion = clusterinfo.get("hadoopBuildVersion")
        .toString();
    assertTrue("hadoop Build version doesn't match: " + hadoopBuildVersion,
        hadoopBuildVersion.matches(".*"));
    String hadoopVersionBuiltOn = clusterinfo.get("hadoopVersionBuiltOn")
        .toString();
    assertTrue(
        "hadoop version built on doesn't match: " + hadoopVersionBuiltOn,
        hadoopVersionBuiltOn
            .matches("^\\w+\\s+\\w+\\s+\\d+\\s+\\d\\d:\\d\\d:\\d\\d\\s+\\w+\\s+\\d\\d\\d\\d"));
  }

  @Test
  public void testClusterMetrics() throws JSONException, Exception {
    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("cluster").path("metrics")
        .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
    verifyClusterMetrics(json);
  }

  @Test
  public void testClusterMetricsSlash() throws JSONException, Exception {
    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("cluster").path("metrics/")
        .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
    verifyClusterMetrics(json);
  }

  public void verifyClusterMetrics(JSONObject json) throws JSONException,
      Exception {
    assertEquals("correct number of elements", 1, json.length());
    JSONObject clusterinfo = json.getJSONObject("clusterMetrics");
    assertEquals("correct number of elements", 11, clusterinfo.length());
    assertEquals("appsSubmitted doesn't match", 0,
        clusterinfo.getInt("appsSubmitted"));
    assertEquals("reservedMB doesn't match", 0,
        clusterinfo.getInt("reservedMB"));
    assertEquals("availableMB doesn't match", 0,
        clusterinfo.getInt("availableMB"));
    assertEquals("allocatedMB doesn't match", 0,
        clusterinfo.getInt("allocatedMB"));
    assertEquals("containersAllocated doesn't match", 0,
        clusterinfo.getInt("containersAllocated"));
    assertEquals("totalMB doesn't match", 0, clusterinfo.getInt("totalMB"));
    assertEquals("totalNodes doesn't match", 0,
        clusterinfo.getInt("totalNodes"));
    assertEquals("lostNodes doesn't match", 0, clusterinfo.getInt("lostNodes"));
    assertEquals("unhealthyNodes doesn't match", 0,
        clusterinfo.getInt("unhealthyNodes"));
    assertEquals("decommissionedNodes doesn't match", 0,
        clusterinfo.getInt("decommissionedNodes"));
    assertEquals("rebootedNodes doesn't match", 0,
        clusterinfo.getInt("rebootedNodes"));
  }

  @Test
  public void testClusterSchedulerFifo() throws JSONException, Exception {
    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("cluster").path("scheduler")
        .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
    verifyClusterSchedulerFifo(json);
  }

  @Test
  public void testClusterSchedulerFifoSlash() throws JSONException, Exception {
    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("cluster")
        .path("scheduler/").accept(MediaType.APPLICATION_JSON)
        .get(JSONObject.class);
    verifyClusterSchedulerFifo(json);
  }

  public void verifyClusterSchedulerFifo(JSONObject json) throws JSONException,
      Exception {
    assertEquals("correct number of elements", 1, json.length());
    JSONObject info = json.getJSONObject("scheduler");
    assertEquals("correct number of elements", 1, info.length());
    info = info.getJSONObject("schedulerInfo");
    assertEquals("correct number of elements", 11, info.length());
    assertEquals("type doesn't match", "fifoScheduler", info.getString("type"));
    assertEquals("qstate doesn't match", QueueState.RUNNING.toString(),
        info.getString("qstate"));
    assertEquals("capacity doesn't match", 1.0, info.getDouble("capacity"), 0.0);
    assertEquals("usedCapacity doesn't match", Float.NaN,
        info.getDouble("usedCapacity"), 0.0);
    assertEquals("minQueueMemoryCapacity doesn't match", 1024,
        info.getInt("minQueueMemoryCapacity"));
    assertEquals("maxQueueMemoryCapacity doesn't match", 10240,
        info.getInt("maxQueueMemoryCapacity"));
    assertEquals("maxQueueMemoryCapacity doesn't match", 10240,
        info.getInt("maxQueueMemoryCapacity"));

  }

  @Test
  public void testNodes() throws JSONException, Exception {
    testNodesHelper("nodes");
  }

  @Test
  public void testNodesSlash() throws JSONException, Exception {
    testNodesHelper("nodes/");
  }

  @Test
  public void testNodesQueryState() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.NMwaitForState(nm1.getNodeId(), RMNodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), RMNodeState.NEW);

    JSONObject json = r.path("ws").path("v1").path("cluster").path("nodes")
        .queryParam("state", RMNodeState.RUNNING.toString())
        .accept("application/json").get(JSONObject.class);

    assertEquals("correct number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("correct number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("correct number of elements", 1, nodeArray.length());
    JSONObject info = nodeArray.getJSONObject(0);

    verifyNodeInfo(info, nm1, RMNodeState.RUNNING);
  }

  @Test
  public void testNodesQueryStateNone() throws JSONException, Exception {
    WebResource r = resource();
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);

    JSONObject json = r.path("ws").path("v1").path("cluster").path("nodes")
        .queryParam("state", RMNodeState.DECOMMISSIONED.toString())
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    assertEquals("nodes is not null", JSONObject.NULL, json.get("nodes"));
  }

  @Test
  public void testNodesQueryStateInvalid() throws JSONException, Exception {
    WebResource r = resource();
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);

    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .queryParam("state", "BOGUSSTATE").accept("application/json")
          .get(JSONObject.class);

      fail("should have thrown exception querying invalid state");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("correct number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      checkStringMatch(
          "exception message",
          "No enum const class org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeState.BOGUSSTATE",
          message);
      checkStringMatch("exception type", "IllegalArgumentException", type);
      checkStringMatch("exception classname",
          "java.lang.IllegalArgumentException", classname);

    } finally {
      rm.stop();
    }
  }

  @Test
  public void testNodesQueryHealthy() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.NMwaitForState(nm1.getNodeId(), RMNodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), RMNodeState.NEW);
    JSONObject json = r.path("ws").path("v1").path("cluster").path("nodes")
        .queryParam("healthy", "true").accept("application/json")
        .get(JSONObject.class);

    assertEquals("correct number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("correct number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("correct number of elements", 2, nodeArray.length());
  }

  @Test
  public void testNodesQueryHealthyCase() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.NMwaitForState(nm1.getNodeId(), RMNodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), RMNodeState.NEW);
    JSONObject json = r.path("ws").path("v1").path("cluster").path("nodes")
        .queryParam("healthy", "TRUe").accept("application/json")
        .get(JSONObject.class);

    assertEquals("correct number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("correct number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("correct number of elements", 2, nodeArray.length());

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
        new ArrayList<ContainerStatus>(), null));
    rm.NMwaitForState(nm1.getNodeId(), RMNodeState.UNHEALTHY);

    JSONObject json = r.path("ws").path("v1").path("cluster").path("nodes")
        .queryParam("healthy", "true")
        .queryParam("state", RMNodeState.RUNNING.toString())
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
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
    JSONObject json = r.path("ws").path("v1").path("cluster").path("nodes")
        .queryParam("healthy", "false").accept("application/json")
        .get(JSONObject.class);

    assertEquals("correct number of elements", 1, json.length());
    assertEquals("nodes is not null", JSONObject.NULL, json.get("nodes"));
  }

  @Test
  public void testNodesQueryHealthyInvalid() throws JSONException, Exception {
    WebResource r = resource();
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);

    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .queryParam("healthy", "tr").accept("application/json")
          .get(JSONObject.class);
      fail("should have thrown exception querying invalid healthy string");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("correct number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      checkStringMatch(
          "exception message",
          "java.lang.Exception: Error: You must specify either true or false to query on health",
          message);
      checkStringMatch("exception type", "BadRequestException", type);
      checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

    } finally {
      rm.stop();
    }
  }

  public void testNodesHelper(String path) throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    JSONObject json = r.path("ws").path("v1").path("cluster").path(path)
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("correct number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("correct number of elements", 2, nodeArray.length());
    JSONObject info = nodeArray.getJSONObject(0);
    String id = info.get("id").toString();

    if (id.matches("h1:1234")) {
      verifyNodeInfo(info, nm1, RMNodeState.NEW);
      verifyNodeInfo(nodeArray.getJSONObject(1), nm2, RMNodeState.NEW);
    } else {
      verifyNodeInfo(info, nm2, RMNodeState.NEW);
      verifyNodeInfo(nodeArray.getJSONObject(1), nm1, RMNodeState.NEW);
    }
  }

  @Test
  public void testSingleNode() throws JSONException, Exception {
    rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    testSingleNodeHelper("h2:1235", nm2);
  }

  @Test
  public void testSingleNodeSlash() throws JSONException, Exception {
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    testSingleNodeHelper("h1:1234/", nm1);
  }

  public void testSingleNodeHelper(String nodeid, MockNM nm)
      throws JSONException, Exception {
    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("cluster").path("nodes")
        .path(nodeid).accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    JSONObject info = json.getJSONObject("node");
    verifyNodeInfo(info, nm, RMNodeState.NEW);
  }

  @Test
  public void testNonexistNode() throws JSONException, Exception {
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid:99").accept("application/json")
          .get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());

      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("correct number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      checkStringMatch("exception message",
          "java.lang.Exception: nodeId, node_invalid:99, is not found", message);
      checkStringMatch("exception type", "NotFoundException", type);
      checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.NotFoundException", classname);

    } finally {
      rm.stop();
    }
  }

  @Test
  public void testInvalidNode() throws JSONException, Exception {
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);

    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid_foo").accept("application/json")
          .get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("correct number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      checkStringMatch("exception message",
          "Invalid NodeId \\[node_invalid_foo\\]. Expected host:port", message);
      checkStringMatch("exception type", "IllegalArgumentException", type);
      checkStringMatch("exception classname",
          "java.lang.IllegalArgumentException", classname);
    } finally {
      rm.stop();
    }
  }

  public void verifyNodeInfo(JSONObject nodeInfo, MockNM nm,
      RMNodeState expectedState) throws JSONException, Exception {
    assertEquals("correct number of elements", 11, nodeInfo.length());
    String state = nodeInfo.get("state").toString();
    assertTrue("stated doesn't match: " + state,
        state.matches(expectedState.toString()));
    String rack = nodeInfo.get("rack").toString();
    assertTrue("rack doesn't match: " + rack, rack.matches("/default-rack"));
    String healthStatus = nodeInfo.get("healthStatus").toString();
    assertTrue("healthStatus doesn't match: " + healthStatus,
        healthStatus.matches("Healthy"));
    String id = nodeInfo.get("id").toString();
    assertTrue("id doesn't match, got: " + id + " expected: "
        + nm.getNodeId().toString(), id.matches(nm.getNodeId().toString()));
    String nodeHostName = nodeInfo.get("nodeHostName").toString();
    assertTrue("hostname doesn't match, got: " + nodeHostName + " expected: "
        + nm.getNodeId().getHost(),
        nodeHostName.matches(nm.getNodeId().getHost()));

    String nodeHTTPAddress = nodeInfo.get("nodeHTTPAddress").toString();
    String expectedHttpAddress = nm.getNodeId().getHost() + ":"
        + nm.getHttpPort();
    assertTrue("nodeHTTPAddress doesn't match, got: " + nodeHTTPAddress
        + " expected: " + expectedHttpAddress,
        nodeHTTPAddress.matches(expectedHttpAddress));
    // could use this for other checks
    RMNode node = rm.getRMContext().getRMNodes().get(nm.getNodeId());
    long lastHealthUpdate = nodeInfo.getLong("lastHealthUpdate");
    long expectedHealthUpdate = node.getNodeHealthStatus()
        .getLastHealthReportTime();
    assertEquals("lastHealthUpdate doesn't match, got: " + lastHealthUpdate
        + " expected: " + expectedHealthUpdate, expectedHealthUpdate,
        lastHealthUpdate);
    String healthReport = nodeInfo.get("healthReport").toString();
    assertTrue("healthReport doesn't match: " + healthReport,
        healthReport.matches("Healthy"));

    int numContainers = nodeInfo.getInt("numContainers");
    assertEquals("numContainers doesn't match: " + numContainers, 0,
        numContainers);

    long usedMemoryMB = nodeInfo.getLong("usedMemoryMB");
    assertEquals("usedMemoryMB doesn't match: " + usedMemoryMB, 0, usedMemoryMB);

    long availMemoryMB = nodeInfo.getLong("availMemoryMB");
    assertEquals("availMemoryMB doesn't match: " + availMemoryMB, 0,
        availMemoryMB);
  }

  @Test
  public void testApps() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    testAppsHelper("apps", app1);
    rm.stop();

  }

  @Test
  public void testAppsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    testAppsHelper("apps/", app1);
    rm.stop();

  }

  public void testAppsHelper(String path, RMApp app) throws JSONException,
      Exception {
    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("cluster").path(path)
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("correct number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("correct number of elements", 1, array.length());
    verifyAppInfo(array.getJSONObject(0), app);

  }

  @Test
  public void testAppsQueryState() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("state", RMAppState.ACCEPTED.toString())
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("correct number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("correct number of elements", 1, array.length());
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
    JSONObject json = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("state", RMAppState.RUNNING.toString())
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
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
          .queryParam("state", "INVALID_test").accept("application/json")
          .get(JSONObject.class);
      fail("should have thrown exception on invalid state query");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("correct number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      checkStringMatch(
          "exception message",
          "No enum const class org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState.INVALID_test",
          message);
      checkStringMatch("exception type", "IllegalArgumentException", type);
      checkStringMatch("exception classname",
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
    JSONObject json = r
        .path("ws")
        .path("v1")
        .path("cluster")
        .path("apps")
        .queryParam("user",
            UserGroupInformation.getCurrentUser().getShortUserName())
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("correct number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("correct number of elements", 2, array.length());
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
    JSONObject json = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("queue", "default").accept("application/json")
        .get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("correct number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("correct number of elements", 2, array.length());
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
    JSONObject json = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("limit", "2").accept("application/json")
        .get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("correct number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("correct number of elements", 2, array.length());
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
    JSONObject json = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("startedTimeBegin", String.valueOf(start))
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("correct number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("correct number of elements", 3, array.length());
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
    JSONObject json = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("startedTimeBegin", String.valueOf(start))
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("correct number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("correct number of elements", 1, array.length());
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
    JSONObject json = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("startedTimeEnd", String.valueOf(end))
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
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
    JSONObject json = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("startedTimeBegin", String.valueOf(start))
        .queryParam("startedTimeEnd", String.valueOf(end))
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("correct number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("correct number of elements", 2, array.length());
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
    rm.submitApp(1024);
    rm.submitApp(1024);

    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("finishedTimeBegin", String.valueOf(start))
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("correct number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("correct number of elements", 1, array.length());
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

    rm.submitApp(1024);
    rm.submitApp(1024);
    long end = System.currentTimeMillis();

    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("finishedTimeEnd", String.valueOf(end))
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("correct number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("correct number of elements", 3, array.length());
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

    rm.submitApp(1024);
    rm.submitApp(1024);
    long end = System.currentTimeMillis();

    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("cluster").path("apps")
        .queryParam("finishedTimeBegin", String.valueOf(start))
        .queryParam("finishedTimeEnd", String.valueOf(end))
        .accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("correct number of elements", 1, apps.length());
    JSONArray array = apps.getJSONArray("app");
    assertEquals("correct number of elements", 1, array.length());
    rm.stop();
  }

  @Test
  public void testSingleApp() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    testSingleAppsHelper(app1.getApplicationId().toString(), app1);
    rm.stop();
  }

  @Test
  public void testSingleAppsSlash() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    RMApp app1 = rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    testSingleAppsHelper(app1.getApplicationId().toString() + "/", app1);
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
          .path("application_invalid_12").accept("application/json")
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("correct number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      checkStringMatch("exception message", "For input string: \"invalid\"",
          message);
      checkStringMatch("exception type", "NumberFormatException", type);
      checkStringMatch("exception classname",
          "java.lang.NumberFormatException", classname);

    } finally {
      rm.stop();
    }
  }

  @Test
  public void testNonexistApp() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    rm.submitApp(1024);
    amNodeManager.nodeHeartbeat(true);
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("cluster").path("apps")
          .path("application_00000_0099").accept("application/json")
          .get(JSONObject.class);
      fail("should have thrown exception on invalid appid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("correct number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      checkStringMatch("exception message",
          "java.lang.Exception: app with id: application_00000_0099 not found",
          message);
      checkStringMatch("exception type", "NotFoundException", type);
      checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    } finally {
      rm.stop();
    }
  }

  public void testSingleAppsHelper(String path, RMApp app)
      throws JSONException, Exception {
    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("cluster").path("apps")
        .path(path).accept("application/json").get(JSONObject.class);
    assertEquals("correct number of elements", 1, json.length());
    verifyAppInfo(json.getJSONObject("app"), app);
  }

  public void verifyAppInfo(JSONObject info, RMApp app) throws JSONException,
      Exception {

    // 15 because trackingUrl not assigned yet
    assertEquals("correct number of elements", 15, info.length());
    String id = info.getString("id");
    String expectedId = app.getApplicationId().toString();
    checkStringMatch("id", expectedId, id);

    String user = info.getString("user");
    String expectedUser = app.getUser();
    checkStringMatch("user", expectedUser, user);

    checkStringMatch("name", "", info.getString("name"));
    checkStringMatch("queue", "default", info.getString("queue"));
    checkStringMatch("state", RMAppState.ACCEPTED.toString(),
        info.getString("state"));
    checkStringMatch("finalStatus",
        FinalApplicationStatus.UNDEFINED.toString(),
        info.getString("finalStatus"));
    assertEquals("progress doesn't match", 0, info.getDouble("progress"), 0.0);
    checkStringMatch("trackingUI", "UNASSIGNED", info.getString("trackingUI"));
    checkStringMatch("diagnostics", "", info.getString("diagnostics"));
    assertEquals("clusterId doesn't match", ResourceManager.clusterTimeStamp,
        info.getLong("clusterId"));
    assertEquals("startedTime doesn't match", app.getStartTime(),
        info.getLong("startedTime"));
    assertEquals("finishedTime doesn't match", app.getFinishTime(),
        info.getLong("finishedTime"));
    assertTrue("elapsed time not greater than 0",
        info.getLong("elapsedTime") > 0);
    checkStringMatch("amHostHttpAddress", app.getCurrentAppAttempt()
        .getMasterContainer().getNodeHttpAddress(),
        info.getString("amHostHttpAddress"));
    assertTrue("amContainerLogs doesn't match",
        info.getString("amContainerLogs").startsWith("http://"));
  }

  private void checkStringMatch(String print, String expected, String got) {
    assertTrue(
        print + " doesn't match, got: " + got + " expected: " + expected,
        got.matches(expected));
  }

}
