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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.security.Principal;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.*;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerConfigValidator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.MutableCSConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterUserInfo;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.AdHocLogDumper;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestRMWebServices extends JerseyTestBase {
  private static final Logger LOG =
          LoggerFactory.getLogger(TestRMWebServices.class);

  private static MockRM rm;

  private static class WebServletModule extends ServletModule {
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
    assertEquals(MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());

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
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
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
      assertResponseStatusCode(Status.INTERNAL_SERVER_ERROR,
          response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testCluster() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testClusterSlash() throws JSONException, Exception {
    WebResource r = resource();
    // test with trailing "/" to make sure acts same as without slash
    ClientResponse response = r.path("ws").path("v1").path("cluster/")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testClusterDefault() throws JSONException, Exception {
    WebResource r = resource();
    // test with trailing "/" to make sure acts same as without slash
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testInfo() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("info").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testInfoDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("info").get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
          WebServicesTestUtils.getXmlString(
              element, "haZooKeeperConnectionState"),
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
    assertEquals("incorrect number of elements", 12, info.length());
    verifyClusterGeneric(info.getLong("id"), info.getLong("startedOn"),
        info.getString("state"), info.getString("haState"),
        info.getString("haZooKeeperConnectionState"),
        info.getString("hadoopVersionBuiltOn"),
        info.getString("hadoopBuildVersion"), info.getString("hadoopVersion"),
        info.getString("resourceManagerVersionBuiltOn"),
        info.getString("resourceManagerBuildVersion"),
        info.getString("resourceManagerVersion"));

  }

  public void verifyClusterGeneric(long clusterid, long startedon,
      String state, String haState, String haZooKeeperConnectionState,
      String hadoopVersionBuiltOn,
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

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterMetricsJSON(json);
  }

  @Test
  public void testClusterMetricsSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("metrics/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterMetricsJSON(json);
  }

  @Test
  public void testClusterMetricsDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("metrics").get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterMetricsJSON(json);
  }

  @Test
  public void testClusterMetricsXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("metrics").accept("application/xml").get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
          WebServicesTestUtils.getXmlInt(element, "pendingMB"),
          WebServicesTestUtils.getXmlInt(element, "reservedVirtualCores"),
          WebServicesTestUtils.getXmlInt(element, "availableVirtualCores"),
          WebServicesTestUtils.getXmlInt(element, "allocatedVirtualCores"),
          WebServicesTestUtils.getXmlInt(element, "pendingVirtualCores"),
          WebServicesTestUtils.getXmlInt(element, "totalVirtualCores"),
          WebServicesTestUtils.getXmlInt(element, "containersAllocated"),
          WebServicesTestUtils.getXmlInt(element, "totalMB"),
          WebServicesTestUtils.getXmlInt(element, "totalNodes"),
          WebServicesTestUtils.getXmlInt(element, "lostNodes"),
          WebServicesTestUtils.getXmlInt(element, "unhealthyNodes"),
          WebServicesTestUtils.getXmlInt(element, "decommissionedNodes"),
          WebServicesTestUtils.getXmlInt(element, "rebootedNodes"),
          WebServicesTestUtils.getXmlInt(element, "activeNodes"),
          WebServicesTestUtils.getXmlInt(element, "shutdownNodes"));
    }
  }

  public void verifyClusterMetricsJSON(JSONObject json) throws JSONException,
      Exception {
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject clusterinfo = json.getJSONObject("clusterMetrics");
    assertEquals("incorrect number of elements", 32, clusterinfo.length());
    verifyClusterMetrics(
        clusterinfo.getInt("appsSubmitted"), clusterinfo.getInt("appsCompleted"),
        clusterinfo.getInt("reservedMB"), clusterinfo.getInt("availableMB"),
        clusterinfo.getInt("allocatedMB"), clusterinfo.getInt("pendingMB"),
        clusterinfo.getInt("reservedVirtualCores"),
        clusterinfo.getInt("availableVirtualCores"),
        clusterinfo.getInt("allocatedVirtualCores"),
        clusterinfo.getInt("pendingVirtualCores"),
        clusterinfo.getInt("totalVirtualCores"),
        clusterinfo.getInt("containersAllocated"),
        clusterinfo.getInt("totalMB"), clusterinfo.getInt("totalNodes"),
        clusterinfo.getInt("lostNodes"), clusterinfo.getInt("unhealthyNodes"),
        clusterinfo.getInt("decommissionedNodes"),
        clusterinfo.getInt("rebootedNodes"),clusterinfo.getInt("activeNodes"),
        clusterinfo.getInt("shutdownNodes"));
  }

  public void verifyClusterMetrics(int submittedApps, int completedApps,
      int reservedMB, int availableMB, int allocMB, int pendingMB,
      int reservedVirtualCores, int availableVirtualCores,
      int allocVirtualCores, int pendingVirtualCores, int totalVirtualCores,
      int containersAlloc, int totalMB, int totalNodes, int lostNodes,
      int unhealthyNodes, int decommissionedNodes, int rebootedNodes,
      int activeNodes, int shutdownNodes) throws JSONException, Exception {

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
    assertEquals("pendingMB doesn't match",
            metrics.getPendingMB(), pendingMB);
    assertEquals("reservedVirtualCores doesn't match",
        metrics.getReservedVirtualCores(), reservedVirtualCores);
    assertEquals("availableVirtualCores doesn't match",
        metrics.getAvailableVirtualCores(), availableVirtualCores);
    assertEquals("pendingVirtualCores doesn't match",
        metrics.getPendingVirtualCores(), pendingVirtualCores);
    assertEquals("allocatedVirtualCores doesn't match",
        metrics.getAllocatedVirtualCores(), allocVirtualCores);
    assertEquals("totalVirtualCores doesn't match",
        totalVirtualCoresExpect, totalVirtualCores);

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
    assertEquals("shutdownNodes doesn't match",
        clusterMetrics.getNumShutdownNMs(), shutdownNodes);
  }

  @Test
  public void testClusterSchedulerFifo() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterSchedulerFifo(json);
  }

  @Test
  public void testClusterSchedulerFifoSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterSchedulerFifo(json);
  }

  @Test
  public void testClusterSchedulerFifoDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterSchedulerFifo(json);
  }

  @Test
  public void testClusterSchedulerFifoXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
    assertEquals("incorrect number of elements in: " + json, 1, json.length());
    JSONObject info = json.getJSONObject("scheduler");
    assertEquals("incorrect number of elements in: " + info, 1, info.length());
    info = info.getJSONObject("schedulerInfo");

    LOG.debug("schedulerInfo: {}", info);
    assertEquals("incorrect number of elements in: " + info, 11, info.length());

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

  // Test the scenario where the RM removes an app just as we try to
  // look at it in the apps list
  @Test
  public void testAppsRace() throws Exception {
    // mock up an RM that returns app reports for apps that don't exist
    // in the RMApps list
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationReport mockReport = mock(ApplicationReport.class);
    when(mockReport.getApplicationId()).thenReturn(appId);
    GetApplicationsResponse mockAppsResponse =
        mock(GetApplicationsResponse.class);
    when(mockAppsResponse.getApplicationList())
      .thenReturn(Arrays.asList(new ApplicationReport[] { mockReport }));
    ClientRMService mockClientSvc = mock(ClientRMService.class);
    when(mockClientSvc.getApplications(isA(GetApplicationsRequest.class)))
        .thenReturn(mockAppsResponse);
    ResourceManager mockRM = mock(ResourceManager.class);
    RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null,
        null, null, null, null, null);
    when(mockRM.getRMContext()).thenReturn(rmContext);
    when(mockRM.getClientRMService()).thenReturn(mockClientSvc);
    rmContext.setNodeLabelManager(mock(RMNodeLabelsManager.class));

    RMWebServices webSvc = new RMWebServices(mockRM, new Configuration(),
        mock(HttpServletResponse.class));

    final Set<String> emptySet =
        Collections.unmodifiableSet(Collections.<String>emptySet());

    // verify we don't get any apps when querying
    HttpServletRequest mockHsr = mock(HttpServletRequest.class);
    AppsInfo appsInfo = webSvc.getApps(mockHsr, null, emptySet, null,
        null, null, null, null, null, null, null, emptySet, emptySet, null,
        null);
    assertTrue(appsInfo.getApps().isEmpty());

    // verify we don't get an NPE when specifying a final status query
    appsInfo = webSvc.getApps(mockHsr, null, emptySet, "FAILED",
        null, null, null, null, null, null, null, emptySet, emptySet, null,
        null);
    assertTrue(appsInfo.getApps().isEmpty());
  }

  @Test
  public void testDumpingSchedulerLogs() throws Exception {

    ResourceManager mockRM = mock(ResourceManager.class);
    Configuration conf = new YarnConfiguration();
    HttpServletRequest mockHsr = mockHttpServletRequestByUserName("non-admin");
    ApplicationACLsManager aclsManager = new ApplicationACLsManager(conf);
    when(mockRM.getApplicationACLsManager()).thenReturn(aclsManager);
    RMWebServices webSvc =
        new RMWebServices(mockRM, conf, mock(HttpServletResponse.class));

    // nothing should happen
    webSvc.dumpSchedulerLogs("1", mockHsr);
    waitforLogDump(50);
    checkSchedulerLogFileAndCleanup();

    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.setStrings(YarnConfiguration.YARN_ADMIN_ACL, "admin");
    aclsManager = new ApplicationACLsManager(conf);
    when(mockRM.getApplicationACLsManager()).thenReturn(aclsManager);
    webSvc = new RMWebServices(mockRM, conf, mock(HttpServletResponse.class));
    boolean exceptionThrown = false;
    try {
      webSvc.dumpSchedulerLogs("1", mockHsr);
      fail("Dumping logs should fail");
    } catch (ForbiddenException ae) {
      exceptionThrown = true;
    }
    assertTrue("ForbiddenException expected", exceptionThrown);
    exceptionThrown = false;
    when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "testuser";
      }
    });
    try {
      webSvc.dumpSchedulerLogs("1", mockHsr);
      fail("Dumping logs should fail");
    } catch (ForbiddenException ae) {
      exceptionThrown = true;
    }
    assertTrue("ForbiddenException expected", exceptionThrown);

    when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "admin";
      }
    });
    webSvc.dumpSchedulerLogs("1", mockHsr);
    waitforLogDump(50);
    checkSchedulerLogFileAndCleanup();
  }

  private void checkSchedulerLogFileAndCleanup() {
    String targetFile;
    ResourceScheduler scheduler = rm.getResourceScheduler();
    if (scheduler instanceof FairScheduler) {
      targetFile = "yarn-fair-scheduler-debug.log";
    } else if (scheduler instanceof CapacityScheduler) {
      targetFile = "yarn-capacity-scheduler-debug.log";
    } else {
      targetFile = "yarn-scheduler-debug.log";
    }
    File logFile = new File(System.getProperty("yarn.log.dir"), targetFile);
    assertTrue("scheduler log file doesn't exist", logFile.exists());
    FileUtils.deleteQuietly(logFile);
  }

  private void waitforLogDump(int tickcount) throws InterruptedException {
    while (tickcount > 0) {
      Thread.sleep(100);
      if (!AdHocLogDumper.getState()) {
        return;
      }
      tickcount--;
    }
  }

  private HttpServletRequest mockHttpServletRequestByUserName(String username) {
    HttpServletRequest mockHsr = mock(HttpServletRequest.class);
    when(mockHsr.getRemoteUser()).thenReturn(username);
    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn(username);
    when(mockHsr.getUserPrincipal()).thenReturn(principal);
    return mockHsr;
  }

  @Test
  public void testCheckUserAccessToQueue() throws Exception {

    ResourceManager mockRM = mock(ResourceManager.class);
    Configuration conf = new YarnConfiguration();

    // Inject a mock scheduler implementation.
    // Only admin user has ADMINISTER_QUEUE access.
    // For SUBMIT_APPLICATION ACL, both of admin/yarn user have acess
    ResourceScheduler mockScheduler = new FifoScheduler() {
      @Override
      public synchronized boolean checkAccess(UserGroupInformation callerUGI,
          QueueACL acl, String queueName) {
        if (acl == QueueACL.ADMINISTER_QUEUE) {
          if (callerUGI.getUserName().equals("admin")) {
            return true;
          }
        } else {
          if (ImmutableSet.of("admin", "yarn").contains(callerUGI.getUserName())) {
            return true;
          }
        }
        return false;
      }
    };

    when(mockRM.getResourceScheduler()).thenReturn(mockScheduler);

    RMWebServices webSvc =
        new RMWebServices(mockRM, conf, mock(HttpServletResponse.class));

    boolean caughtException = false;

    // Case 1: Only queue admin user can access other user's information
    HttpServletRequest mockHsr = mockHttpServletRequestByUserName("non-admin");
    try {
      webSvc.checkUserAccessToQueue("queue", "jack",
          QueueACL.SUBMIT_APPLICATIONS.name(), mockHsr);
    } catch (ForbiddenException e) {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);

    // Case 2: request an unknown ACL causes BAD_REQUEST
    mockHsr = mockHttpServletRequestByUserName("admin");
    caughtException = false;
    try {
      webSvc.checkUserAccessToQueue("queue", "jack", "XYZ_ACL", mockHsr);
    } catch (BadRequestException e) {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);

    // Case 3: get FORBIDDEN for rejected ACL
    mockHsr = mockHttpServletRequestByUserName("admin");
    Assert.assertFalse(webSvc.checkUserAccessToQueue("queue", "jack",
        QueueACL.SUBMIT_APPLICATIONS.name(), mockHsr).isAllowed());
    Assert.assertFalse(webSvc.checkUserAccessToQueue("queue", "jack",
        QueueACL.ADMINISTER_QUEUE.name(), mockHsr).isAllowed());

    // Case 4: get OK for listed ACLs
    mockHsr = mockHttpServletRequestByUserName("admin");
    Assert.assertTrue(webSvc.checkUserAccessToQueue("queue", "admin",
        QueueACL.SUBMIT_APPLICATIONS.name(), mockHsr).isAllowed());
    Assert.assertTrue(webSvc.checkUserAccessToQueue("queue", "admin",
        QueueACL.ADMINISTER_QUEUE.name(), mockHsr).isAllowed());

    // Case 5: get OK only for SUBMIT_APP acl for "yarn" user
    mockHsr = mockHttpServletRequestByUserName("admin");
    Assert.assertTrue(webSvc.checkUserAccessToQueue("queue", "yarn",
        QueueACL.SUBMIT_APPLICATIONS.name(), mockHsr).isAllowed());
    Assert.assertFalse(webSvc.checkUserAccessToQueue("queue", "yarn",
        QueueACL.ADMINISTER_QUEUE.name(), mockHsr).isAllowed());
  }

  @Test
  public void testClusterUserInfo() throws JSONException, Exception {
    ResourceManager mockRM = mock(ResourceManager.class);
    Configuration conf = new YarnConfiguration();
    HttpServletRequest mockHsr = mockHttpServletRequestByUserName("admin");
    when(mockRM.getRMLoginUser()).thenReturn("yarn");
    RMWebServices webSvc =
            new RMWebServices(mockRM, conf, mock(HttpServletResponse.class));
    ClusterUserInfo userInfo = webSvc.getClusterUserInfo(mockHsr);
    verifyClusterUserInfo(userInfo, "yarn", "admin");
  }

  @Test
  public void testInvalidXMLChars() throws Exception {
    ResourceManager mockRM = mock(ResourceManager.class);

    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationReport appReport = ApplicationReport.newInstance(
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FAILED, "java.lang.Exception: \u0001", "url",
        0, 0, 0, FinalApplicationStatus.FAILED, null, "N/A", 0.53789f, "YARN",
        null, null, false, Priority.newInstance(0), "high-mem", "high-mem");
    List<ApplicationReport> appReports = new ArrayList<ApplicationReport>();
    appReports.add(appReport);

    GetApplicationsResponse response = mock(GetApplicationsResponse.class);
    when(response.getApplicationList()).thenReturn(appReports);
    ClientRMService clientRMService = mock(ClientRMService.class);
    when(clientRMService.getApplications(any(GetApplicationsRequest.class)))
        .thenReturn(response);
    when(mockRM.getClientRMService()).thenReturn(clientRMService);

    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getDispatcher()).thenReturn(mock(Dispatcher.class));

    ApplicationSubmissionContext applicationSubmissionContext = mock(
        ApplicationSubmissionContext.class);
    when(applicationSubmissionContext.getUnmanagedAM()).thenReturn(true);

    RMApp app = mock(RMApp.class);
    RMAppMetrics appMetrics = new RMAppMetrics(Resource.newInstance(0, 0),
        0, 0, new HashMap<>(), new HashMap<>(), 0);
    when(app.getDiagnostics()).thenReturn(
        new StringBuilder("java.lang.Exception: \u0001"));
    when(app.getApplicationId()).thenReturn(applicationId);
    when(app.getUser()).thenReturn("user");
    when(app.getName()).thenReturn("appname");
    when(app.getQueue()).thenReturn("queue");
    when(app.getRMAppMetrics()).thenReturn(appMetrics);
    when(app.getApplicationSubmissionContext()).thenReturn(
        applicationSubmissionContext);

    ConcurrentMap<ApplicationId, RMApp> applications =
        new ConcurrentHashMap<>();
    applications.put(applicationId, app);

    when(rmContext.getRMApps()).thenReturn(applications);
    when(mockRM.getRMContext()).thenReturn(rmContext);

    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.FILTER_INVALID_XML_CHARS, true);
    RMWebServices webSvc = new RMWebServices(mockRM, conf, mock(
        HttpServletResponse.class));

    HttpServletRequest mockHsr = mock(HttpServletRequest.class);
    when(mockHsr.getHeader(HttpHeaders.ACCEPT)).
         thenReturn(MediaType.APPLICATION_XML);
    Set<String> emptySet = Collections.unmodifiableSet(Collections.emptySet());

    AppsInfo appsInfo = webSvc.getApps(mockHsr, null, emptySet, null,
        null, null, null, null, null, null, null, emptySet, emptySet,
        null, null);

    assertEquals("Incorrect Number of Apps", 1, appsInfo.getApps().size());
    assertEquals("Invalid XML Characters Present",
        "java.lang.Exception: \uFFFD", appsInfo.getApps().get(0).getNote());
  }

  @Test
  public void testDisableRestAppSubmission() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.ENABLE_REST_APP_SUBMISSIONS, false);
    RMWebServices webSvc = new RMWebServices(mock(ResourceManager.class), conf,
        mock(HttpServletResponse.class));
    HttpServletRequest request = mock(HttpServletRequest.class);

    Response response = webSvc.createNewApplication(request);
    assertEquals(Status.FORBIDDEN.getStatusCode(), response.getStatus());
    assertEquals("App submission via REST is disabled.", response.getEntity());

    response = webSvc.submitApplication(
        mock(ApplicationSubmissionContextInfo.class), request);
    assertEquals(Status.FORBIDDEN.getStatusCode(), response.getStatus());
    assertEquals("App submission via REST is disabled.", response.getEntity());
  }

  public void verifyClusterUserInfo(ClusterUserInfo userInfo,
            String rmLoginUser, String requestedUser) {
    assertEquals("rmLoginUser doesn't match: ",
            rmLoginUser, userInfo.getRmLoginUser());
    assertEquals("requestedUser doesn't match: ",
            requestedUser, userInfo.getRequestedUser());
  }

  @Test
  public void testValidateAndGetSchedulerConfigurationInvalidScheduler()
          throws AuthorizationException {
    ResourceScheduler scheduler = new CapacityScheduler();
    RMWebServices webService = prepareWebServiceForValidation(scheduler);
    SchedConfUpdateInfo mutationInfo = new SchedConfUpdateInfo();
    HttpServletRequest mockHsr = prepareServletRequestForValidation();
    Response response = webService
            .validateAndGetSchedulerConfiguration(mutationInfo, mockHsr);
    Assert.assertEquals(Status.BAD_REQUEST
            .getStatusCode(), response.getStatus());
    Assert.assertTrue(response.getEntity().toString()
            .contains("Configuration change validation only supported by"
                    +" MutableConfScheduler."));
  }

  @Test
  public void testValidateAndGetSchedulerConfigurationInvalidConfig()
          throws IOException {
    Configuration config = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    ResourceScheduler scheduler = prepareCSForValidation(config);

    SchedConfUpdateInfo mutationInfo = new SchedConfUpdateInfo();
    ArrayList<String> queuesToRemove = new ArrayList();
    queuesToRemove.add("root.test1");
    mutationInfo.setRemoveQueueInfo(queuesToRemove);

    RMWebServices webService = prepareWebServiceForValidation(scheduler);
    HttpServletRequest mockHsr = prepareServletRequestForValidation();

    Response response = webService
            .validateAndGetSchedulerConfiguration(mutationInfo, mockHsr);
    Assert.assertEquals(Status.BAD_REQUEST
            .getStatusCode(), response.getStatus());
    Assert.assertTrue(response.getEntity().toString()
            .contains("Illegal capacity of 0.5 for children of queue"));
  }

  @Test
  public void testValidateAndGetSchedulerConfigurationValidScheduler()
          throws IOException {
    Configuration config = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    config.set("yarn.scheduler.capacity.root.test1.state", "STOPPED");
    config.set("yarn.scheduler.capacity.queue-mappings",
            "u:test2:test2");
    ResourceScheduler scheduler = prepareCSForValidation(config);

    SchedConfUpdateInfo mutationInfo = new SchedConfUpdateInfo();
    ArrayList<String> queuesToRemove = new ArrayList();
    queuesToRemove.add("root.test1");
    mutationInfo.setRemoveQueueInfo(queuesToRemove);
    ArrayList<QueueConfigInfo> updateQueueInfo = new ArrayList<>();
    String queueToUpdate = "root.test2";
    Map<String, String> propertiesToUpdate = new HashMap<>();
    propertiesToUpdate.put("capacity", "100");
    updateQueueInfo.add(new QueueConfigInfo(queueToUpdate, propertiesToUpdate));
    mutationInfo.setUpdateQueueInfo(updateQueueInfo);

    RMWebServices webService = prepareWebServiceForValidation(scheduler);
    HttpServletRequest mockHsr = prepareServletRequestForValidation();

    Response response = webService
            .validateAndGetSchedulerConfiguration(mutationInfo, mockHsr);
    Assert.assertEquals(Status.OK
            .getStatusCode(), response.getStatus());
  }

  private CapacityScheduler prepareCSForValidation(Configuration config)
          throws IOException {
    CapacityScheduler scheduler = mock(CapacityScheduler.class);
    when(scheduler.isConfigurationMutable())
            .thenReturn(true);
    MutableCSConfigurationProvider configurationProvider =
            mock(MutableCSConfigurationProvider.class);
    when(scheduler.getMutableConfProvider())
            .thenReturn(configurationProvider);

    when(configurationProvider.getConfiguration()).thenReturn(config);
    when(scheduler.getConf()).thenReturn(config);
    when(configurationProvider
            .applyChanges(any(), any())).thenCallRealMethod();
    return scheduler;
  }

  private HttpServletRequest prepareServletRequestForValidation() {
    HttpServletRequest mockHsr = mock(HttpServletRequest.class);
    when(mockHsr.getUserPrincipal()).thenReturn(() -> "yarn");
    return mockHsr;
  }

  private RMWebServices prepareWebServiceForValidation(
          ResourceScheduler scheduler) {
    ResourceManager mockRM = mock(ResourceManager.class);
    ApplicationACLsManager acLsManager = mock(ApplicationACLsManager.class);
    RMWebServices webService = new RMWebServices(mockRM, new Configuration(),
            mock(HttpServletResponse.class));
    when(mockRM.getResourceScheduler()).thenReturn(scheduler);
    when(acLsManager.areACLsEnabled()).thenReturn(false);
    when(mockRM.getApplicationACLsManager()).thenReturn(acLsManager);
    RMContext context = TestCapacitySchedulerConfigValidator.prepareRMContext();
    when(mockRM.getRMContext()).thenReturn(context);

    return  webService;
  }

}
