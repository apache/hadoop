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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
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
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.NotAcceptableException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.thirdparty.com.google.common.net.HttpHeaders;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.XMLUtils;
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
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.*;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfScheduler;
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
import org.apache.hadoop.yarn.util.AppsCacheKey;
import org.apache.hadoop.yarn.util.LRUCache;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;

public class TestRMWebServices extends JerseyTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRMWebServices.class);

  private static MockRM rm;

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

  private static class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      Configuration conf = new YarnConfiguration();
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);
      rm = new MockRM(conf);

      final HttpServletRequest request = mock(HttpServletRequest.class);
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
    }
  }

  public TestRMWebServices() {
  }

  @BeforeAll
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
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("info").request("application/xml").get(Response.class);
    assertEquals(MediaType.APPLICATION_XML + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    verifyClusterInfoXML(xml);
  }

  @Test
  public void testInvalidUri() throws JSONException, Exception {
    WebTarget r = target();
    String responseStr = "";
    try {
      Response response = r.path("ws").path("v1").path("cluster").path("bogus")
          .request(MediaType.APPLICATION_JSON).get();
      throw new NotFoundException(response);
    } catch (NotFoundException ue) {
      Response response = ue.getResponse();
      assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
      responseStr = response.readEntity(String.class);
      WebServicesTestUtils.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testInvalidUri2() throws JSONException, Exception {
    WebTarget r = target();
    String responseStr = "";
    try {
      Response response = r.request(MediaType.APPLICATION_JSON).get();
      throw new NotFoundException(response);
    } catch (NotFoundException ue) {
      Response response = ue.getResponse();
      responseStr = response.readEntity(String.class);
      assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testInvalidAccept() throws JSONException, Exception {
    WebTarget r = target();
    String responseStr = "";
    try {
      Response response = r.path("ws").path("v1").path("cluster")
          .request(MediaType.TEXT_PLAIN).get();
      throw new NotAcceptableException(response);
    } catch (NotAcceptableException ue) {
      Response response = ue.getResponse();
      responseStr = response.readEntity(String.class);
      assertResponseStatusCode(Response.Status.NOT_ACCEPTABLE,
          response.getStatusInfo());
      WebServicesTestUtils.checkStringContains(
          "error string exists and shouldn't", "NotAcceptableException", responseStr);
    }
  }

  @Test
  public void testCluster() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("cluster")
        .request(MediaType.APPLICATION_JSON).get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testClusterSlash() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    // test with trailing "/" to make sure acts same as without slash
    Response response = r.path("ws").path("v1").path("cluster/")
        .request(MediaType.APPLICATION_JSON).get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testClusterDefault() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    // test with trailing "/" to make sure acts same as without slash
    Response response = r.path("ws").path("v1").path("cluster").request()
        .get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testInfo() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("info").request(MediaType.APPLICATION_JSON)
        .get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testInfoSlash() throws JSONException, Exception {
    // test with trailing "/" to make sure acts same as without slash
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("info/").request(MediaType.APPLICATION_JSON)
        .get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  @Test
  public void testInfoDefault() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("info").request().get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyClusterInfo(json);
  }

  public void verifyClusterInfoXML(String xml) throws JSONException, Exception {
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("clusterInfo");
    assertEquals(1, nodes.getLength(), "incorrect number of elements");

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
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject info = json.getJSONObject("clusterInfo");
    assertEquals(12, info.length(), "incorrect number of elements");
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

    assertEquals(ResourceManager.getClusterTimeStamp(),
        clusterid, "clusterId doesn't match: ");
    assertEquals(ResourceManager.getClusterTimeStamp(),
        startedon, "startedOn doesn't match: ");
    assertTrue(state.matches(STATE.INITED.toString()),
        "stated doesn't match: " + state);
    assertTrue(haState.matches("INITIALIZING"), "HA state doesn't match: " + haState);

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
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("metrics").request(MediaType.APPLICATION_JSON)
        .get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyClusterMetricsJSON(json);
  }

  @Test
  public void testClusterMetricsSlash() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("metrics/").request(MediaType.APPLICATION_JSON)
        .get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyClusterMetricsJSON(json);
  }

  @Test
  public void testClusterMetricsDefault() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("metrics").request().get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json =response.readEntity(JSONObject.class);
    verifyClusterMetricsJSON(json);
  }

  @Test
  public void testClusterMetricsXML() throws JSONException, Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("metrics").request("application/xml").get(Response.class);
    assertEquals(MediaType.APPLICATION_XML + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    verifyClusterMetricsXML(xml);
  }

  public void verifyClusterMetricsXML(String xml) throws JSONException,
      Exception {
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("clusterMetrics");
    assertEquals(1, nodes.getLength(), "incorrect number of elements");

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
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject clusterinfo = json.getJSONObject("clusterMetrics");
    assertEquals(37, clusterinfo.length(), "incorrect number of elements");
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
    assertEquals(metrics.getAppsSubmitted(), submittedApps, "appsSubmitted doesn't match");
    assertEquals(metrics.getAppsCompleted(), completedApps, "appsCompleted doesn't match");
    assertEquals(metrics.getReservedMB(), reservedMB, "reservedMB doesn't match");
    assertEquals(metrics.getAvailableMB(), availableMB, "availableMB doesn't match");
    assertEquals(metrics.getAllocatedMB(), allocMB, "allocatedMB doesn't match");
    assertEquals(metrics.getPendingMB(), pendingMB, "pendingMB doesn't match");
    assertEquals(metrics.getReservedVirtualCores(), reservedVirtualCores,
        "reservedVirtualCores doesn't match");
    assertEquals(metrics.getAvailableVirtualCores(), availableVirtualCores,
        "availableVirtualCores doesn't match");
    assertEquals(metrics.getPendingVirtualCores(), pendingVirtualCores,
        "pendingVirtualCores doesn't match");
    assertEquals(metrics.getAllocatedVirtualCores(), allocVirtualCores,
        "allocatedVirtualCores doesn't match");
    assertEquals(totalVirtualCoresExpect, totalVirtualCores, "totalVirtualCores doesn't match");

    assertEquals(0, containersAlloc, "containersAllocated doesn't match");
    assertEquals(totalMBExpect, totalMB, "totalMB doesn't match");
    assertEquals(clusterMetrics.getNumActiveNMs() + clusterMetrics.getNumLostNMs()
        + clusterMetrics.getNumDecommisionedNMs()
        + clusterMetrics.getNumRebootedNMs()
        + clusterMetrics.getUnhealthyNMs(), totalNodes, "totalNodes doesn't match");
    assertEquals(clusterMetrics.getNumLostNMs(), lostNodes, "lostNodes doesn't match");
    assertEquals(clusterMetrics.getUnhealthyNMs(), unhealthyNodes,
        "unhealthyNodes doesn't match");
    assertEquals(clusterMetrics.getNumDecommisionedNMs(), decommissionedNodes,
        "decommissionedNodes doesn't match");
    assertEquals(clusterMetrics.getNumRebootedNMs(), rebootedNodes,
        "rebootedNodes doesn't match");
    assertEquals(clusterMetrics.getNumActiveNMs(), activeNodes, "activeNodes doesn't match");
    assertEquals(clusterMetrics.getNumShutdownNMs(), shutdownNodes, "shutdownNodes doesn't match");
  }

  @Test
  public void testClusterSchedulerFifo() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").request(MediaType.APPLICATION_JSON)
        .get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyClusterSchedulerFifo(json);
  }

  @Test
  public void testClusterSchedulerFifoSlash() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("scheduler/").request(MediaType.APPLICATION_JSON)
        .get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyClusterSchedulerFifo(json);
  }

  @Test
  public void testClusterSchedulerFifoDefault() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").request().get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyClusterSchedulerFifo(json);
  }

  @Test
  public void testClusterSchedulerFifoXML() throws JSONException, Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").request(MediaType.APPLICATION_XML)
        .get(Response.class);

    assertEquals(MediaType.APPLICATION_XML + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    verifySchedulerFifoXML(xml);
  }

  public void verifySchedulerFifoXML(String xml) throws JSONException,
      Exception {
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodesSched = dom.getElementsByTagName("scheduler");
    assertEquals(1, nodesSched.getLength(), "incorrect number of elements");
    NodeList nodes = dom.getElementsByTagName("schedulerInfo");
    assertEquals(1, nodes.getLength(), "incorrect number of elements");

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
    assertEquals(1, json.length(), "incorrect number of elements in: " + json);
    JSONObject info = json.getJSONObject("scheduler");
    assertEquals(1, info.length(), "incorrect number of elements in: " + info);
    info = info.getJSONObject("schedulerInfo");

    LOG.debug("schedulerInfo: {}", info);
    assertEquals(11, info.length(), "incorrect number of elements in: " + info);

    verifyClusterSchedulerFifoGeneric(info.getString("@xsi.type"),
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

    assertEquals("fifoScheduler", type, "type doesn't match");
    assertEquals(QueueState.RUNNING.toString(), state, "qstate doesn't match");
    assertEquals(1.0, capacity, 0.0, "capacity doesn't match");
    assertEquals(0.0, usedCapacity, 0.0, "usedCapacity doesn't match");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        minQueueCapacity, "minQueueMemoryCapacity doesn't match");
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        maxQueueCapacity, "maxQueueMemoryCapacity doesn't match");
    assertEquals(0, numNodes, "numNodes doesn't match");
    assertEquals(0, usedNodeCapacity, "usedNodeCapacity doesn't match");
    assertEquals(0, availNodeCapacity, "availNodeCapacity doesn't match");
    assertEquals(0, totalNodeCapacity, "totalNodeCapacity doesn't match");
    assertEquals(0, numContainers, "numContainers doesn't match");

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
    assertTrue(exceptionThrown, "ForbiddenException expected");
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
    assertTrue(exceptionThrown, "ForbiddenException expected");

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
    assertTrue(logFile.exists(), "scheduler log file doesn't exist");
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
    assertTrue(caughtException);

    // Case 2: request an unknown ACL causes BAD_REQUEST
    mockHsr = mockHttpServletRequestByUserName("admin");
    caughtException = false;
    try {
      webSvc.checkUserAccessToQueue("queue", "jack", "XYZ_ACL", mockHsr);
    } catch (BadRequestException e) {
      caughtException = true;
    }
    assertTrue(caughtException);

    // Case 3: get FORBIDDEN for rejected ACL
    mockHsr = mockHttpServletRequestByUserName("admin");
    assertFalse(webSvc.checkUserAccessToQueue("queue", "jack",
        QueueACL.SUBMIT_APPLICATIONS.name(), mockHsr).isAllowed());
    assertFalse(webSvc.checkUserAccessToQueue("queue", "jack",
        QueueACL.ADMINISTER_QUEUE.name(), mockHsr).isAllowed());

    // Case 4: get OK for listed ACLs
    mockHsr = mockHttpServletRequestByUserName("admin");
    assertTrue(webSvc.checkUserAccessToQueue("queue", "admin",
        QueueACL.SUBMIT_APPLICATIONS.name(), mockHsr).isAllowed());
    assertTrue(webSvc.checkUserAccessToQueue("queue", "admin",
        QueueACL.ADMINISTER_QUEUE.name(), mockHsr).isAllowed());

    // Case 5: get OK only for SUBMIT_APP acl for "yarn" user
    mockHsr = mockHttpServletRequestByUserName("admin");
    assertTrue(webSvc.checkUserAccessToQueue("queue", "yarn",
        QueueACL.SUBMIT_APPLICATIONS.name(), mockHsr).isAllowed());
    assertFalse(webSvc.checkUserAccessToQueue("queue", "yarn",
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

    assertEquals(1, appsInfo.getApps().size(), "Incorrect Number of Apps");
    assertEquals("java.lang.Exception: \uFFFD", appsInfo.getApps().get(0).getNote(),
        "Invalid XML Characters Present");
  }

  @Test
  public void testDisableRestAppSubmission() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.ENABLE_REST_APP_SUBMISSIONS, false);
    RMWebServices webSvc = new RMWebServices(mock(ResourceManager.class), conf,
        mock(HttpServletResponse.class));
    HttpServletRequest request = mock(HttpServletRequest.class);

    Response response = webSvc.createNewApplication(request);
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    assertEquals("App submission via REST is disabled.", response.getEntity());

    response = webSvc.submitApplication(
        mock(ApplicationSubmissionContextInfo.class), request);
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    assertEquals("App submission via REST is disabled.", response.getEntity());
  }

  public void verifyClusterUserInfo(ClusterUserInfo userInfo,
            String rmLoginUser, String requestedUser) {
    assertEquals(rmLoginUser, userInfo.getRmLoginUser(),
        "rmLoginUser doesn't match: ");
    assertEquals(requestedUser, userInfo.getRequestedUser(),
        "requestedUser doesn't match: ");
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
    assertEquals(Response.Status.BAD_REQUEST
        .getStatusCode(), response.getStatus());
    assertTrue(response.getEntity().toString()
        .contains(String.format("Configuration change validation only supported by %s.",
        MutableConfScheduler.class.getSimpleName())));
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

    Response response = webService.validateAndGetSchedulerConfiguration(mutationInfo, mockHsr);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertTrue(response.getEntity().toString().contains("IOException"));
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
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
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

  @Test
  public void testClusterSchedulerOverviewFifo() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-overview").request(MediaType.APPLICATION_JSON)
        .get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    JSONObject schedulerJson = json.getJSONObject("scheduler");
    verifyClusterSchedulerOverView(schedulerJson, "Fifo Scheduler");
  }

  public static void verifyClusterSchedulerOverView(
      JSONObject json, String expectedSchedulerType) throws Exception {

    // why json contains 8 elements because we defined 8 fields
    assertEquals(8, json.length(), "incorrect number of elements in: " + json);

    // 1.Verify that the schedulerType is as expected
    String schedulerType = json.getString("schedulerType");
    assertEquals(expectedSchedulerType, schedulerType);

    // 2.Verify that schedulingResourceType is as expected
    String schedulingResourceType = json.getString("schedulingResourceType");
    assertEquals("memory-mb (unit=Mi),vcores", schedulingResourceType);

    // 3.Verify that minimumAllocation is as expected
    JSONObject minimumAllocation = json.getJSONObject("minimumAllocation");
    String minMemory = minimumAllocation.getString("memory");
    String minVCores = minimumAllocation.getString("vCores");
    assertEquals("1024", minMemory);
    assertEquals("1", minVCores);

    // 4.Verify that maximumAllocation is as expected
    JSONObject maximumAllocation = json.getJSONObject("maximumAllocation");
    String maxMemory = maximumAllocation.getString("memory");
    String maxVCores = maximumAllocation.getString("vCores");
    assertEquals("8192", maxMemory);
    assertEquals("4", maxVCores);

    // 5.Verify that schedulerBusy is as expected
    int schedulerBusy = json.getInt("schedulerBusy");
    assertEquals(-1, schedulerBusy);

    // 6.Verify that rmDispatcherEventQueueSize is as expected
    int rmDispatcherEventQueueSize = json.getInt("rmDispatcherEventQueueSize");
    assertEquals(0, rmDispatcherEventQueueSize);

    // 7.Verify that schedulerDispatcherEventQueueSize is as expected
    int schedulerDispatcherEventQueueSize = json.getInt("schedulerDispatcherEventQueueSize");
    assertEquals(0, schedulerDispatcherEventQueueSize);

    // 8.Verify that applicationPriority is as expected
    int applicationPriority = json.getInt("applicationPriority");
    assertEquals(0, applicationPriority);
  }

  @Test
  public void testGetAppsCache() throws YarnException, InterruptedException, TimeoutException {
    // mock up an RM that returns app reports for apps that don't exist
    // in the RMApps list
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationReport mockReport = mock(ApplicationReport.class);
    when(mockReport.getApplicationId()).thenReturn(appId);
    GetApplicationsResponse mockAppsResponse =
        mock(GetApplicationsResponse.class);
    when(mockAppsResponse.getApplicationList())
        .thenReturn(Arrays.asList(new ApplicationReport[]{mockReport}));

    ClientRMService mockClientSvc = mock(ClientRMService.class);
    when(mockClientSvc.getApplications(isA(GetApplicationsRequest.class)))
        .thenReturn(mockAppsResponse);
    ResourceManager mockRM = mock(ResourceManager.class);
    RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, null, null,
        null, null);
    when(mockRM.getRMContext()).thenReturn(rmContext);
    when(mockRM.getClientRMService()).thenReturn(mockClientSvc);
    rmContext.setNodeLabelManager(mock(RMNodeLabelsManager.class));

    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.APPS_CACHE_ENABLE, true);
    conf.setInt(YarnConfiguration.APPS_CACHE_SIZE, 2);
    conf.setInt(YarnConfiguration.APPS_CACHE_EXPIRE, 100);
    RMWebServices webSvc = new RMWebServices(mockRM, conf,
        mock(HttpServletResponse.class));
    final Set<String> emptySet =
        Collections.unmodifiableSet(Collections.<String>emptySet());

    // verify we don't get any apps when querying
    HttpServletRequest mockHsr = mock(HttpServletRequest.class);
    AppsInfo appsInfo = webSvc.getApps(mockHsr, null, emptySet, null,
        "mock_user", "mock_queue", null, null, null, null, null, emptySet,
        emptySet, null, null);
    LRUCache<AppsCacheKey, AppsInfo> cache = webSvc.getAppsLRUCache();
    assertEquals(1, cache.size());
    AppsCacheKey appsCacheKey = AppsCacheKey.newInstance(null, emptySet,
        null, "mock_user", "mock_queue", null, null, null, null, null, emptySet,
        emptySet, null, null);
    assertEquals(appsInfo, cache.get(appsCacheKey));

    AppsInfo appsInfo1 = webSvc.getApps(mockHsr, null, emptySet, null,
        "mock_user1", "mock_queue", null, null, null, null, null, emptySet,
        emptySet, null, null);
    assertEquals(2, cache.size());
    AppsCacheKey appsCacheKey1 = AppsCacheKey.newInstance(null, emptySet,
        null, "mock_user1", "mock_queue", null, null, null, null, null, emptySet,
        emptySet, null, null);
    assertEquals(appsInfo1, cache.get(appsCacheKey1));

    AppsInfo appsInfo2 = webSvc.getApps(mockHsr, null, emptySet, null,
        "mock_user2", "mock_queue", null, null, null, null, null, emptySet,
        emptySet, null, null);
    assertEquals(2, cache.size());
    AppsCacheKey appsCacheKey2 = AppsCacheKey.newInstance(null, emptySet,
        null, "mock_user2", "mock_queue", null, null, null, null, null, emptySet,
        emptySet, null, null);
    assertEquals(appsInfo2, cache.get(appsCacheKey2));
    // appsCacheKey have removed
    assertNull(cache.get(appsCacheKey));

    GenericTestUtils.waitFor(() -> cache.get(appsCacheKey1) == null,
        300, 1000);
    GenericTestUtils.waitFor(() -> cache.get(appsCacheKey2) == null,
        300, 1000);
  }
}