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

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.BeforeClass;
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

public class TestRMWebServices extends JerseyTest {

  private static MockRM rm;

  private Injector injector = Guice.createInjector(new ServletModule() {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      Configuration conf = new Configuration();
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

  public TestRMWebServices() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @BeforeClass
  public static void initClusterMetrics() {
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
    clusterMetrics.incrDecommisionedNMs();
    clusterMetrics.incrNumActiveNodes();
    clusterMetrics.incrNumLostNMs();
    clusterMetrics.incrNumRebootedNMs();
    clusterMetrics.incrNumUnhealthyNMs();
  }

  @Test
  public void testInfoXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("info").accept("application/xml").get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    verifyClusterInfoXML(xml);
  }

  @Test
  public void testInvalidUri() throws JSONException, Exception {
    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr = r.path("ws").path("v1").path("cluster").path("bogus")
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
      responseStr = r.path("ws").path("v1").path("cluster")
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
  public void testCluster() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testClusterSlash() throws JSONException, Exception {
    WebResource r = resource();
    // test with trailing "/" to make sure acts same as without slash
    ClientResponse response = r.path("ws").path("v1").path("cluster/")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testClusterDefault() throws JSONException, Exception {
    WebResource r = resource();
    // test with trailing "/" to make sure acts same as without slash
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testInfo() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("info").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testInfoSlash() throws JSONException, Exception {
    // test with trailing "/" to make sure acts same as without slash
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("info/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testInfoDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("info").get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  public void verifyClusterInfoXML(String xml) throws JSONException, Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("clusterInfo");
    assertEquals("incorrect number of elements", 1, nodes.getLength());

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyClusterGeneric(WebServicesTestUtils.getXmlLong(element, "id"),
          WebServicesTestUtils.getXmlLong(element, "startedOn"),
          WebServicesTestUtils.getXmlString(element, "state"),
          WebServicesTestUtils.getXmlString(element, "haState"),
          WebServicesTestUtils.getXmlString(element, "hadoopVersionBuiltOn"),
          WebServicesTestUtils.getXmlString(element, "hadoopBuildVersion"),
          WebServicesTestUtils.getXmlString(element, "hadoopVersion"),
          WebServicesTestUtils.getXmlString(element,
              "resourceManagerVersionBuiltOn"),
          WebServicesTestUtils.getXmlString(element,
              "resourceManagerBuildVersion"),
          WebServicesTestUtils.getXmlString(element, "resourceManagerVersion"));
    }
  }

  public void verifyClusterInfo(JSONObject json) throws JSONException,
      Exception {
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject info = json.getJSONObject("clusterInfo");
    assertEquals("incorrect number of elements", 10, info.length());
    verifyClusterGeneric(info.getLong("id"), info.getLong("startedOn"),
        info.getString("state"), info.getString("haState"),
        info.getString("hadoopVersionBuiltOn"),
        info.getString("hadoopBuildVersion"), info.getString("hadoopVersion"),
        info.getString("resourceManagerVersionBuiltOn"),
        info.getString("resourceManagerBuildVersion"),
        info.getString("resourceManagerVersion"));

  }

  public void verifyClusterGeneric(long clusterid, long startedon,
      String state, String haState, String hadoopVersionBuiltOn,
      String hadoopBuildVersion, String hadoopVersion,
      String resourceManagerVersionBuiltOn, String resourceManagerBuildVersion,
      String resourceManagerVersion) {

    assertEquals("clusterId doesn't match: ",
        ResourceManager.getClusterTimeStamp(), clusterid);
    assertEquals("startedOn doesn't match: ",
        ResourceManager.getClusterTimeStamp(), startedon);
    assertTrue("stated doesn't match: " + state,
        state.matches(STATE.INITED.toString()));
    assertTrue("HA state doesn't match: " + haState,
        haState.matches("INITIALIZING"));

    WebServicesTestUtils.checkStringMatch("hadoopVersionBuiltOn",
        VersionInfo.getDate(), hadoopVersionBuiltOn);
    WebServicesTestUtils.checkStringEqual("hadoopBuildVersion",
        VersionInfo.getBuildVersion(), hadoopBuildVersion);
    WebServicesTestUtils.checkStringMatch("hadoopVersion",
        VersionInfo.getVersion(), hadoopVersion);

    WebServicesTestUtils.checkStringMatch("resourceManagerVersionBuiltOn",
        YarnVersionInfo.getDate(), resourceManagerVersionBuiltOn);
    WebServicesTestUtils.checkStringEqual("resourceManagerBuildVersion",
        YarnVersionInfo.getBuildVersion(), resourceManagerBuildVersion);
    WebServicesTestUtils.checkStringMatch("resourceManagerVersion",
        YarnVersionInfo.getVersion(), resourceManagerVersion);
  }

  @Test
  public void testClusterMetrics() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("metrics").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterMetricsJSON(json);
  }

  @Test
  public void testClusterMetricsSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("metrics/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterMetricsJSON(json);
  }

  @Test
  public void testClusterMetricsDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("metrics").get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterMetricsJSON(json);
  }

  @Test
  public void testClusterMetricsXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("metrics").accept("application/xml").get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    verifyClusterMetricsXML(xml);
  }

  public void verifyClusterMetricsXML(String xml) throws JSONException,
      Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("clusterMetrics");
    assertEquals("incorrect number of elements", 1, nodes.getLength());

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyClusterMetrics(
          WebServicesTestUtils.getXmlInt(element, "appsSubmitted"),
          WebServicesTestUtils.getXmlInt(element, "appsCompleted"),
          WebServicesTestUtils.getXmlInt(element, "reservedMB"),
          WebServicesTestUtils.getXmlInt(element, "availableMB"),
          WebServicesTestUtils.getXmlInt(element, "allocatedMB"),
          WebServicesTestUtils.getXmlInt(element, "reservedVirtualCores"),
          WebServicesTestUtils.getXmlInt(element, "availableVirtualCores"),
          WebServicesTestUtils.getXmlInt(element, "allocatedVirtualCores"),
          WebServicesTestUtils.getXmlInt(element, "totalVirtualCores"),
          WebServicesTestUtils.getXmlInt(element, "containersAllocated"),
          WebServicesTestUtils.getXmlInt(element, "totalMB"),
          WebServicesTestUtils.getXmlInt(element, "totalNodes"),
          WebServicesTestUtils.getXmlInt(element, "lostNodes"),
          WebServicesTestUtils.getXmlInt(element, "unhealthyNodes"),
          WebServicesTestUtils.getXmlInt(element, "decommissionedNodes"),
          WebServicesTestUtils.getXmlInt(element, "rebootedNodes"),
          WebServicesTestUtils.getXmlInt(element, "activeNodes"));
    }
  }

  public void verifyClusterMetricsJSON(JSONObject json) throws JSONException,
      Exception {
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject clusterinfo = json.getJSONObject("clusterMetrics");
    assertEquals("incorrect number of elements", 23, clusterinfo.length());
    verifyClusterMetrics(
        clusterinfo.getInt("appsSubmitted"), clusterinfo.getInt("appsCompleted"),
        clusterinfo.getInt("reservedMB"), clusterinfo.getInt("availableMB"),
        clusterinfo.getInt("allocatedMB"),
        clusterinfo.getInt("reservedVirtualCores"), clusterinfo.getInt("availableVirtualCores"),
        clusterinfo.getInt("allocatedVirtualCores"), clusterinfo.getInt("totalVirtualCores"),
        clusterinfo.getInt("containersAllocated"),
        clusterinfo.getInt("totalMB"), clusterinfo.getInt("totalNodes"),
        clusterinfo.getInt("lostNodes"), clusterinfo.getInt("unhealthyNodes"),
        clusterinfo.getInt("decommissionedNodes"),
        clusterinfo.getInt("rebootedNodes"),clusterinfo.getInt("activeNodes"));
  }

  public void verifyClusterMetrics(int submittedApps, int completedApps,
      int reservedMB, int availableMB,
      int allocMB, int reservedVirtualCores, int availableVirtualCores, 
      int allocVirtualCores, int totalVirtualCores,
      int containersAlloc, int totalMB, int totalNodes,
      int lostNodes, int unhealthyNodes, int decommissionedNodes,
      int rebootedNodes, int activeNodes) throws JSONException, Exception {

    ResourceScheduler rs = rm.getResourceScheduler();
    QueueMetrics metrics = rs.getRootQueueMetrics();
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();

    long totalMBExpect = 
        metrics.getAvailableMB() + metrics.getAllocatedMB();
    long totalVirtualCoresExpect = 
        metrics.getAvailableVirtualCores() + metrics.getAllocatedVirtualCores();
    assertEquals("appsSubmitted doesn't match", 
        metrics.getAppsSubmitted(), submittedApps);
    assertEquals("appsCompleted doesn't match", 
        metrics.getAppsCompleted(), completedApps);
    assertEquals("reservedMB doesn't match",
        metrics.getReservedMB(), reservedMB);
    assertEquals("availableMB doesn't match", 
        metrics.getAvailableMB(), availableMB);
    assertEquals("allocatedMB doesn't match", 
        metrics.getAllocatedMB(), allocMB);
    assertEquals("reservedVirtualCores doesn't match",
        metrics.getReservedVirtualCores(), reservedVirtualCores);
    assertEquals("availableVirtualCores doesn't match",
        metrics.getAvailableVirtualCores(), availableVirtualCores);
    assertEquals("allocatedVirtualCores doesn't match",
        totalVirtualCoresExpect, allocVirtualCores);
    assertEquals("containersAllocated doesn't match", 0, containersAlloc);
    assertEquals("totalMB doesn't match", totalMBExpect, totalMB);
    assertEquals(
        "totalNodes doesn't match",
        clusterMetrics.getNumActiveNMs() + clusterMetrics.getNumLostNMs()
            + clusterMetrics.getNumDecommisionedNMs()
            + clusterMetrics.getNumRebootedNMs()
            + clusterMetrics.getUnhealthyNMs(), totalNodes);
    assertEquals("lostNodes doesn't match", clusterMetrics.getNumLostNMs(),
        lostNodes);
    assertEquals("unhealthyNodes doesn't match",
        clusterMetrics.getUnhealthyNMs(), unhealthyNodes);
    assertEquals("decommissionedNodes doesn't match",
        clusterMetrics.getNumDecommisionedNMs(), decommissionedNodes);
    assertEquals("rebootedNodes doesn't match",
        clusterMetrics.getNumRebootedNMs(), rebootedNodes);
    assertEquals("activeNodes doesn't match", clusterMetrics.getNumActiveNMs(),
        activeNodes);
  }

  @Test
  public void testClusterSchedulerFifo() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterSchedulerFifo(json);
  }

  @Test
  public void testClusterSchedulerFifoSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterSchedulerFifo(json);
  }

  @Test
  public void testClusterSchedulerFifoDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterSchedulerFifo(json);
  }

  @Test
  public void testClusterSchedulerFifoXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    verifySchedulerFifoXML(xml);
  }

  public void verifySchedulerFifoXML(String xml) throws JSONException,
      Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodesSched = dom.getElementsByTagName("scheduler");
    assertEquals("incorrect number of elements", 1, nodesSched.getLength());
    NodeList nodes = dom.getElementsByTagName("schedulerInfo");
    assertEquals("incorrect number of elements", 1, nodes.getLength());

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyClusterSchedulerFifoGeneric(
          WebServicesTestUtils.getXmlAttrString(element, "xsi:type"),
          WebServicesTestUtils.getXmlString(element, "qstate"),
          WebServicesTestUtils.getXmlFloat(element, "capacity"),
          WebServicesTestUtils.getXmlFloat(element, "usedCapacity"),
          WebServicesTestUtils.getXmlInt(element, "minQueueMemoryCapacity"),
          WebServicesTestUtils.getXmlInt(element, "maxQueueMemoryCapacity"),
          WebServicesTestUtils.getXmlInt(element, "numNodes"),
          WebServicesTestUtils.getXmlInt(element, "usedNodeCapacity"),
          WebServicesTestUtils.getXmlInt(element, "availNodeCapacity"),
          WebServicesTestUtils.getXmlInt(element, "totalNodeCapacity"),
          WebServicesTestUtils.getXmlInt(element, "numContainers"));
    }
  }

  public void verifyClusterSchedulerFifo(JSONObject json) throws JSONException,
      Exception {
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject info = json.getJSONObject("scheduler");
    assertEquals("incorrect number of elements", 1, info.length());
    info = info.getJSONObject("schedulerInfo");
    assertEquals("incorrect number of elements", 11, info.length());

    verifyClusterSchedulerFifoGeneric(info.getString("type"),
        info.getString("qstate"), (float) info.getDouble("capacity"),
        (float) info.getDouble("usedCapacity"),
        info.getInt("minQueueMemoryCapacity"),
        info.getInt("maxQueueMemoryCapacity"), info.getInt("numNodes"),
        info.getInt("usedNodeCapacity"), info.getInt("availNodeCapacity"),
        info.getInt("totalNodeCapacity"), info.getInt("numContainers"));

  }

  public void verifyClusterSchedulerFifoGeneric(String type, String state,
      float capacity, float usedCapacity, int minQueueCapacity,
      int maxQueueCapacity, int numNodes, int usedNodeCapacity,
      int availNodeCapacity, int totalNodeCapacity, int numContainers)
      throws JSONException, Exception {

    assertEquals("type doesn't match", "fifoScheduler", type);
    assertEquals("qstate doesn't match", QueueState.RUNNING.toString(), state);
    assertEquals("capacity doesn't match", 1.0, capacity, 0.0);
    assertEquals("usedCapacity doesn't match", 0.0, usedCapacity, 0.0);
    assertEquals(
        "minQueueMemoryCapacity doesn't match",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        minQueueCapacity);
    assertEquals("maxQueueMemoryCapacity doesn't match",
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        maxQueueCapacity);
    assertEquals("numNodes doesn't match", 0, numNodes);
    assertEquals("usedNodeCapacity doesn't match", 0, usedNodeCapacity);
    assertEquals("availNodeCapacity doesn't match", 0, availNodeCapacity);
    assertEquals("totalNodeCapacity doesn't match", 0, totalNodeCapacity);
    assertEquals("numContainers doesn't match", 0, numContainers);

  }

}
