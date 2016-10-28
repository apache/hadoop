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

import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestRMWebServicesForCSWithPartitions extends JerseyTestBase {
  private static final String DEFAULT_PARTITION = "";
  private static final String CAPACITIES = "capacities";
  private static final String RESOURCE_USAGES_BY_PARTITION =
      "resourceUsagesByPartition";
  private static final String QUEUE_CAPACITIES_BY_PARTITION =
      "queueCapacitiesByPartition";
  private static final String QUEUE_C = "Qc";
  private static final String LEAF_QUEUE_C1 = "Qc1";
  private static final String LEAF_QUEUE_C2 = "Qc2";
  private static final String QUEUE_B = "Qb";
  private static final String QUEUE_A = "Qa";
  private static final String LABEL_LY = "Ly";
  private static final String LABEL_LX = "Lx";
  private static final ImmutableSet<String> CLUSTER_LABELS =
      ImmutableSet.of(LABEL_LX, LABEL_LY, DEFAULT_PARTITION);
  private static MockRM rm;
  static private CapacitySchedulerConfiguration csConf;
  static private YarnConfiguration conf;

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      csConf = new CapacitySchedulerConfiguration();
      setupQueueConfiguration(csConf);
      conf = new YarnConfiguration(csConf);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      rm = new MockRM(conf);
      Set<NodeLabel> labels = new HashSet<NodeLabel>();
      labels.add(NodeLabel.newInstance(LABEL_LX));
      labels.add(NodeLabel.newInstance(LABEL_LY));
      try {
        RMNodeLabelsManager nodeLabelManager =
            rm.getRMContext().getNodeLabelManager();
        nodeLabelManager.addToCluserNodeLabels(labels);
      } catch (Exception e) {
        Assert.fail();
      }
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
    }
  };

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  private static void setupQueueConfiguration(
      CapacitySchedulerConfiguration config) {

    // Define top-level queues
    config.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { QUEUE_A, QUEUE_B, QUEUE_C });
    String interMediateQueueC =
        CapacitySchedulerConfiguration.ROOT + "." + QUEUE_C;
    config.setQueues(interMediateQueueC,
        new String[] { LEAF_QUEUE_C1, LEAF_QUEUE_C2 });
    config.setCapacityByLabel(
        CapacitySchedulerConfiguration.ROOT, LABEL_LX, 100);
    config.setCapacityByLabel(
        CapacitySchedulerConfiguration.ROOT, LABEL_LY, 100);

    String leafQueueA = CapacitySchedulerConfiguration.ROOT + "." + QUEUE_A;
    config.setCapacity(leafQueueA, 30);
    config.setMaximumCapacity(leafQueueA, 50);

    String leafQueueB = CapacitySchedulerConfiguration.ROOT + "." + QUEUE_B;
    config.setCapacity(leafQueueB, 30);
    config.setMaximumCapacity(leafQueueB, 50);

    config.setCapacity(interMediateQueueC, 40);
    config.setMaximumCapacity(interMediateQueueC, 50);

    String leafQueueC1 = interMediateQueueC + "." + LEAF_QUEUE_C1;
    config.setCapacity(leafQueueC1, 50);
    config.setMaximumCapacity(leafQueueC1, 60);

    String leafQueueC2 = interMediateQueueC + "." + LEAF_QUEUE_C2;
    config.setCapacity(leafQueueC2, 50);
    config.setMaximumCapacity(leafQueueC2, 70);

    // Define label specific configuration
    config.setAccessibleNodeLabels(
        leafQueueA, ImmutableSet.of(DEFAULT_PARTITION));
    config.setAccessibleNodeLabels(leafQueueB, ImmutableSet.of(LABEL_LX));
    config.setAccessibleNodeLabels(interMediateQueueC,
        ImmutableSet.of(LABEL_LX, LABEL_LY));
    config.setAccessibleNodeLabels(leafQueueC1,
        ImmutableSet.of(LABEL_LX, LABEL_LY));
    config.setAccessibleNodeLabels(leafQueueC2,
        ImmutableSet.of(LABEL_LX, LABEL_LY));
    config.setDefaultNodeLabelExpression(leafQueueB, LABEL_LX);
    config.setDefaultNodeLabelExpression(leafQueueC1, LABEL_LX);
    config.setDefaultNodeLabelExpression(leafQueueC2, LABEL_LY);

    config.setCapacityByLabel(leafQueueB, LABEL_LX, 30);
    config.setCapacityByLabel(interMediateQueueC, LABEL_LX, 70);
    config.setCapacityByLabel(leafQueueC1, LABEL_LX, 40);
    config.setCapacityByLabel(leafQueueC2, LABEL_LX, 60);

    config.setCapacityByLabel(interMediateQueueC, LABEL_LY, 100);
    config.setCapacityByLabel(leafQueueC1, LABEL_LY, 50);
    config.setCapacityByLabel(leafQueueC2, LABEL_LY, 50);
    config.setMaximumCapacityByLabel(leafQueueC1, LABEL_LY, 75);
    config.setMaximumCapacityByLabel(leafQueueC2, LABEL_LY, 75);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  public TestRMWebServicesForCSWithPartitions() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
            .contextListenerClass(GuiceServletConfig.class)
            .filterClass(com.google.inject.servlet.GuiceFilter.class)
            .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testSchedulerPartitions() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("scheduler")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifySchedulerInfoJson(json);
  }

  @Test
  public void testSchedulerPartitionsSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("scheduler/")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifySchedulerInfoJson(json);

  }

  @Test
  public void testSchedulerPartitionsDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifySchedulerInfoJson(json);
  }

  @Test
  public void testSchedulerPartitionsXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("scheduler")
            .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    verifySchedulerInfoXML(dom);
  }

  private void verifySchedulerInfoXML(Document dom) throws Exception {
    NodeList scheduler = dom.getElementsByTagName("scheduler");
    assertEquals("incorrect number of elements", 1, scheduler.getLength());
    NodeList schedulerInfo = dom.getElementsByTagName("schedulerInfo");
    assertEquals("incorrect number of elements", 1, schedulerInfo.getLength());
    for (int i = 0; i < schedulerInfo.getLength(); i++) {
      Element element = (Element) schedulerInfo.item(i);
      NodeList children = element.getChildNodes();
      for (int j = 0; j < children.getLength(); j++) {
        Element schedulerInfoElem = (Element) children.item(j);
        if (schedulerInfoElem.getTagName().equals("queues")) {
          NodeList qListInfos = schedulerInfoElem.getChildNodes();
          for (int k = 0; k < qListInfos.getLength(); k++) {
            Element qElem2 = (Element) qListInfos.item(k);
            String queue =
                WebServicesTestUtils.getXmlString(qElem2, "queueName");
            switch (queue) {
            case QUEUE_A:
              verifyQueueAInfoXML(qElem2);
              break;
            case QUEUE_B:
              verifyQueueBInfoXML(qElem2);
              break;
            case QUEUE_C:
              verifyQueueCInfoXML(qElem2);
              break;
            default:
              Assert.fail("Unexpected queue" + queue);
            }
          }
        } else if (schedulerInfoElem.getTagName().equals(CAPACITIES)) {
          NodeList capacitiesListInfos = schedulerInfoElem.getChildNodes();
          assertEquals("incorrect number of partitions", 3,
              capacitiesListInfos.getLength());
          for (int k = 0; k < capacitiesListInfos.getLength(); k++) {
            Element partitionCapacitiesInfo =
                (Element) capacitiesListInfos.item(k);
            String partitionName = WebServicesTestUtils
                .getXmlString(partitionCapacitiesInfo, "partitionName");
            assertTrue("invalid PartitionCapacityInfo",
                CLUSTER_LABELS.contains(partitionName));
            verifyPartitionCapacityInfoXML(partitionCapacitiesInfo, 100, 0, 100,
                100, 0, 100);
          }
        }
      }
    }
  }

  private void verifyQueueAInfoXML(Element queueElem) {
    NodeList children = queueElem.getChildNodes();
    for (int j = 0; j < children.getLength(); j++) {
      Element queueChildElem = (Element) children.item(j);
      if (queueChildElem.getTagName().equals(CAPACITIES)) {
        NodeList capacitiesListInfos = queueChildElem.getChildNodes();
        assertEquals("incorrect number of partitions", 1,
            capacitiesListInfos.getLength());
        Element partitionCapacitiesInfo = (Element) capacitiesListInfos.item(0);
        String partitionName = WebServicesTestUtils
            .getXmlString(partitionCapacitiesInfo, "partitionName");
        assertTrue("invalid PartitionCapacityInfo",
            partitionName.isEmpty());
        verifyPartitionCapacityInfoXML(partitionCapacitiesInfo, 30, 0, 50, 30,
            0, 50);
      } else if (queueChildElem.getTagName().equals("resources")) {
        verifyResourceUsageInfoXML(queueChildElem);
      }
    }
  }

  private void verifyQueueBInfoXML(Element queueElem) {
    assertEquals("Invalid default Label expression", LABEL_LX,
        WebServicesTestUtils.getXmlString(queueElem,
            "defaultNodeLabelExpression"));
    NodeList children = queueElem.getChildNodes();
    for (int j = 0; j < children.getLength(); j++) {
      Element queueChildElem = (Element) children.item(j);
      if (queueChildElem.getTagName().equals(CAPACITIES)) {
        NodeList capacitiesListInfos = queueChildElem.getChildNodes();
        assertEquals("incorrect number of partitions", 2,
            capacitiesListInfos.getLength());
        for (int k = 0; k < capacitiesListInfos.getLength(); k++) {
          Element partitionCapacitiesInfo =
              (Element) capacitiesListInfos.item(k);
          String partitionName = WebServicesTestUtils
              .getXmlString(partitionCapacitiesInfo, "partitionName");
          switch (partitionName) {
          case LABEL_LX:
            verifyPartitionCapacityInfoXML(partitionCapacitiesInfo, 30, 0, 100,
                30, 0, 100);
            break;
          case DEFAULT_PARTITION:
            verifyPartitionCapacityInfoXML(partitionCapacitiesInfo, 30, 0, 50,
                30, 0, 50);
            break;
          default:
            Assert.fail("Unexpected partition" + partitionName);
          }
        }
      } else if (queueChildElem.getTagName().equals("resources")) {
        verifyResourceUsageInfoXML(queueChildElem);
      }
    }
    assertEquals("Node Labels are not matching", LABEL_LX,
        WebServicesTestUtils.getXmlString(queueElem, "nodeLabels"));
  }

  private void verifyQueueCInfoXML(Element queueElem) {
    NodeList children = queueElem.getChildNodes();
    for (int j = 0; j < children.getLength(); j++) {
      Element queueChildElem = (Element) children.item(j);
      if (queueChildElem.getTagName().equals(CAPACITIES)) {
        verifyQcCapacitiesInfoXML(queueChildElem, 70, 100, 70, 100, 100, 100,
            100, 100, 40, 50, 40, 50);
      } else if (queueChildElem.getTagName().equals("resources")) {
        verifyResourceUsageInfoXML(queueChildElem);
      } else if (queueChildElem.getTagName().equals("queues")) {
        NodeList qListInfos = queueChildElem.getChildNodes();
        for (int k = 0; k < qListInfos.getLength(); k++) {
          Element qElem2 = (Element) qListInfos.item(k);
          String queue = WebServicesTestUtils.getXmlString(qElem2, "queueName");
          switch (queue) {
          case LEAF_QUEUE_C1:
            assertEquals("Invalid default Label expression", LABEL_LX,
                WebServicesTestUtils.getXmlString(qElem2,
                    "defaultNodeLabelExpression"));
            NodeList queuec1Children = qElem2.getChildNodes();
            for (int l = 0; l < queuec1Children.getLength(); l++) {
              Element queueC1ChildElem = (Element) queuec1Children.item(l);
              if (queueC1ChildElem.getTagName().equals(CAPACITIES)) {
                verifyQcCapacitiesInfoXML(queueC1ChildElem, 40, 100, 28, 100,
                    50, 75, 50, 75, 50, 60, 20, 30);
              }
            }
            break;
          case LEAF_QUEUE_C2:
            assertEquals("Invalid default Label expression", LABEL_LY,
                WebServicesTestUtils.getXmlString(qElem2,
                    "defaultNodeLabelExpression"));
            NodeList queuec2Children = qElem2.getChildNodes();
            for (int l = 0; l < queuec2Children.getLength(); l++) {
              Element queueC2ChildElem = (Element) queuec2Children.item(l);
              if (queueC2ChildElem.getTagName().equals(CAPACITIES)) {
                verifyQcCapacitiesInfoXML(queueC2ChildElem, 60, 100, 42, 100,
                    50, 75, 50, 75, 50, 70, 20, 35);
              }
            }
            break;
          default:
            Assert.fail("Unexpected queue" + queue);
          }
        }
      }
    }
  }

  private void verifyQcCapacitiesInfoXML(Element partitionCapacitiesElem,
      float lxCaps, float lxMaxCaps, float lxAbsCaps, float lxAbsMaxCaps,
      float lyCaps, float lyMaxCaps, float lyAbsCaps, float lyAbsMaxCaps,
      float defCaps, float defMaxCaps, float defAbsCaps, float defAbsMaxCaps) {
    NodeList capacitiesListInfos = partitionCapacitiesElem.getChildNodes();
    assertEquals("incorrect number of partitions", 3,
        capacitiesListInfos.getLength());
    for (int k = 0; k < capacitiesListInfos.getLength(); k++) {
      Element partitionCapacitiesInfo = (Element) capacitiesListInfos.item(k);
      String partitionName = WebServicesTestUtils
          .getXmlString(partitionCapacitiesInfo, "partitionName");
      switch (partitionName) {
      case LABEL_LX:
        verifyPartitionCapacityInfoXML(partitionCapacitiesInfo, lxCaps, 0,
            lxMaxCaps, lxAbsCaps, 0, lxAbsMaxCaps);
        break;
      case LABEL_LY:
        verifyPartitionCapacityInfoXML(partitionCapacitiesInfo, lyCaps, 0,
            lyMaxCaps, lyAbsCaps, 0, lyAbsMaxCaps);
        break;
      case DEFAULT_PARTITION:
        verifyPartitionCapacityInfoXML(partitionCapacitiesInfo, defCaps, 0,
            defMaxCaps, defAbsCaps, 0, defAbsMaxCaps);
        break;
      default:
        Assert.fail("Unexpected partition" + partitionName);
      }
    }
  }

  private void verifyResourceUsageInfoXML(Element queueChildElem) {
    NodeList resourceUsageInfo = queueChildElem.getChildNodes();
    assertEquals("incorrect number of partitions", 1,
        resourceUsageInfo.getLength());
    Element partitionResourceUsageInfo = (Element) resourceUsageInfo.item(0);
    String partitionName = WebServicesTestUtils
        .getXmlString(partitionResourceUsageInfo, "partitionName");
    assertTrue("invalid PartitionCapacityInfo",
        DEFAULT_PARTITION.equals(partitionName));
  }

  private void verifyPartitionCapacityInfoXML(Element partitionInfo,
      float capacity, float usedCapacity, float maxCapacity,
      float absoluteCapacity, float absoluteUsedCapacity,
      float absoluteMaxCapacity) {
    assertEquals("capacity doesn't match", capacity,
        WebServicesTestUtils.getXmlFloat(partitionInfo, "capacity"), 1e-3f);
    assertEquals("capacity doesn't match", usedCapacity,
        WebServicesTestUtils.getXmlFloat(partitionInfo, "usedCapacity"), 1e-3f);
    assertEquals("capacity doesn't match", maxCapacity,
        WebServicesTestUtils.getXmlFloat(partitionInfo, "maxCapacity"), 1e-3f);
    assertEquals("capacity doesn't match", absoluteCapacity,
        WebServicesTestUtils.getXmlFloat(partitionInfo, "absoluteCapacity"),
        1e-3f);
    assertEquals("capacity doesn't match", absoluteUsedCapacity,
        WebServicesTestUtils.getXmlFloat(partitionInfo, "absoluteUsedCapacity"),
        1e-3f);
    assertEquals("capacity doesn't match", absoluteMaxCapacity,
        WebServicesTestUtils.getXmlFloat(partitionInfo, "absoluteMaxCapacity"),
        1e-3f);
  }

  private void verifySchedulerInfoJson(JSONObject json)
      throws JSONException, Exception {
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject info = json.getJSONObject("scheduler");
    assertEquals("incorrect number of elements", 1, info.length());
    info = info.getJSONObject("schedulerInfo");
    assertEquals("incorrect number of elements", 8, info.length());
    JSONObject capacitiesJsonObject = info.getJSONObject(CAPACITIES);
    JSONArray partitionsCapsArray =
        capacitiesJsonObject.getJSONArray(QUEUE_CAPACITIES_BY_PARTITION);
    assertEquals("incorrect number of elements", CLUSTER_LABELS.size(),
        partitionsCapsArray.length());
    for (int i = 0; i < partitionsCapsArray.length(); i++) {
      JSONObject partitionInfo = partitionsCapsArray.getJSONObject(i);
      String partitionName = partitionInfo.getString("partitionName");
      assertTrue("Unknown partition received",
          CLUSTER_LABELS.contains(partitionName));
      verifyPartitionCapacityInfoJson(partitionInfo, 100, 0, 100, 100, 0, 100);
    }
    JSONObject jsonQueuesObject = info.getJSONObject("queues");
    JSONArray queuesArray = jsonQueuesObject.getJSONArray("queue");
    for (int i = 0; i < queuesArray.length(); i++) {
      JSONObject queueJson = queuesArray.getJSONObject(i);
      String queue = queueJson.getString("queueName");

      assertEquals("Partition resourceInfo is wrong", 1,
          queueJson.getJSONObject("resources")
              .getJSONArray(RESOURCE_USAGES_BY_PARTITION).length());

      JSONObject resourcesJsonObject = queueJson.getJSONObject("resources");
      JSONArray partitionsResourcesArray =
          resourcesJsonObject.getJSONArray("resourceUsagesByPartition");
      assertEquals("incorrect number of elements", 1,
          partitionsResourcesArray.length());

      capacitiesJsonObject = queueJson.getJSONObject(CAPACITIES);
      partitionsCapsArray =
          capacitiesJsonObject.getJSONArray(QUEUE_CAPACITIES_BY_PARTITION);

      JSONObject partitionInfo = null;
      String partitionName = null;
      switch (queue) {
      case QUEUE_A:
        assertEquals("incorrect number of partitions", 1,
            partitionsCapsArray.length());
        partitionInfo = partitionsCapsArray.getJSONObject(0);
        partitionName = partitionInfo.getString("partitionName");
        verifyPartitionCapacityInfoJson(partitionInfo, 30, 0, 50, 30, 0, 50);
        assertEquals("incorrect number of elements", 6,
            partitionsResourcesArray.getJSONObject(0).length());
        break;
      case QUEUE_B:
        assertEquals("Invalid default Label expression", LABEL_LX,
            queueJson.getString("defaultNodeLabelExpression"));
        assertEquals("incorrect number of elements", 6,
            partitionsResourcesArray.getJSONObject(0).length());
        verifyAccesibleNodeLabels(queueJson, ImmutableSet.of(LABEL_LX));
        assertEquals("incorrect number of partitions", 2,
            partitionsCapsArray.length());
        for (int j = 0; j < partitionsCapsArray.length(); j++) {
          partitionInfo = partitionsCapsArray.getJSONObject(j);
          partitionName = partitionInfo.getString("partitionName");
          switch (partitionName) {
          case LABEL_LX:
            verifyPartitionCapacityInfoJson(partitionInfo, 30, 0, 100, 30, 0,
                100);
            break;
          case DEFAULT_PARTITION:
            verifyPartitionCapacityInfoJson(partitionInfo, 30, 0, 50, 30, 0,
                50);
            break;
          default:
            Assert.fail("Unexpected partition" + partitionName);
          }
        }
        break;
      case QUEUE_C:
        verifyAccesibleNodeLabels(queueJson,
            ImmutableSet.of(LABEL_LX, LABEL_LY));
        assertEquals("incorrect number of elements", 4,
            partitionsResourcesArray.getJSONObject(0).length());
        verifyQcPartitionsCapacityInfoJson(partitionsCapsArray, 70, 100, 70,
            100, 100, 100, 100, 100, 40, 50, 40, 50);
        verifySubQueuesOfQc(queueJson);
        break;
      default:
        Assert.fail("Unexpected queue" + queue);
      }
    }
  }

  private void verifyAccesibleNodeLabels(JSONObject queueJson,
      Set<String> accesibleNodeLabels) throws JSONException {
    JSONArray nodeLabels = queueJson.getJSONArray("nodeLabels");
    assertEquals("number of accessible Node Labels not matching",
        accesibleNodeLabels.size(), nodeLabels.length());
    for (int i = 0; i < nodeLabels.length(); i++) {
      assertTrue("Invalid accessible node label : " + nodeLabels.getString(i),
          accesibleNodeLabels.contains(nodeLabels.getString(i)));
    }
  }

  private void verifySubQueuesOfQc(JSONObject queueCJson) throws JSONException {
    JSONObject jsonQueuesObject = queueCJson.getJSONObject("queues");
    JSONArray queuesArray = jsonQueuesObject.getJSONArray("queue");
    for (int i = 0; i < queuesArray.length(); i++) {
      JSONObject queueJson = queuesArray.getJSONObject(i);
      String queue = queueJson.getString("queueName");

      JSONObject capacitiesJsonObject = queueJson.getJSONObject(CAPACITIES);
      JSONArray partitionsCapsArray =
          capacitiesJsonObject.getJSONArray(QUEUE_CAPACITIES_BY_PARTITION);
      switch (queue) {
      case LEAF_QUEUE_C1:
        verifyAccesibleNodeLabels(queueJson,
            ImmutableSet.of(LABEL_LX, LABEL_LY));
        assertEquals("Invalid default Label expression", LABEL_LX,
            queueJson.getString("defaultNodeLabelExpression"));
        verifyQcPartitionsCapacityInfoJson(partitionsCapsArray, 40, 100, 28,
            100, 50, 75, 50, 75, 50, 60, 20, 30);
        break;
      case LEAF_QUEUE_C2:
        verifyAccesibleNodeLabels(queueJson,
            ImmutableSet.of(LABEL_LX, LABEL_LY));
        assertEquals("Invalid default Label expression", LABEL_LY,
            queueJson.getString("defaultNodeLabelExpression"));
        verifyQcPartitionsCapacityInfoJson(partitionsCapsArray, 60, 100, 42,
            100, 50, 75, 50, 75, 50, 70, 20, 35);
        break;
      default:
        Assert.fail("Unexpected queue" + queue);
      }
    }
  }

  private void verifyQcPartitionsCapacityInfoJson(JSONArray partitionsCapsArray,
      float lxCaps, float lxMaxCaps, float lxAbsCaps, float lxAbsMaxCaps,
      float lyCaps, float lyMaxCaps, float lyAbsCaps, float lyAbsMaxCaps,
      float defCaps, float defMaxCaps, float defAbsCaps, float defAbsMaxCaps)
          throws JSONException {
    assertEquals("incorrect number of partitions", CLUSTER_LABELS.size(),
        partitionsCapsArray.length());
    for (int j = 0; j < partitionsCapsArray.length(); j++) {
      JSONObject partitionInfo = partitionsCapsArray.getJSONObject(j);
      String partitionName = partitionInfo.getString("partitionName");
      switch (partitionName) {
      case LABEL_LX:
        verifyPartitionCapacityInfoJson(partitionInfo, lxCaps, 0, lxMaxCaps,
            lxAbsCaps, 0, lxAbsMaxCaps);
        break;
      case LABEL_LY:
        verifyPartitionCapacityInfoJson(partitionInfo, lyCaps, 0, lyMaxCaps,
            lyAbsCaps, 0, lyAbsMaxCaps);
        break;
      case DEFAULT_PARTITION:
        verifyPartitionCapacityInfoJson(partitionInfo, defCaps, 0, defMaxCaps,
            defAbsCaps, 0, defAbsMaxCaps);
        break;
      default:
        Assert.fail("Unexpected partition" + partitionName);
      }
    }
  }

  private void verifyPartitionCapacityInfoJson(
      JSONObject partitionCapacityInfoJson, float capacity, float usedCapacity,
      float maxCapacity, float absoluteCapacity, float absoluteUsedCapacity,
      float absoluteMaxCapacity) throws JSONException {
    assertEquals("capacity doesn't match", capacity,
        (float) partitionCapacityInfoJson.getDouble("capacity"), 1e-3f);
    assertEquals("capacity doesn't match", usedCapacity,
        (float) partitionCapacityInfoJson.getDouble("usedCapacity"), 1e-3f);
    assertEquals("capacity doesn't match", maxCapacity,
        (float) partitionCapacityInfoJson.getDouble("maxCapacity"), 1e-3f);
    assertEquals("capacity doesn't match", absoluteCapacity,
        (float) partitionCapacityInfoJson.getDouble("absoluteCapacity"), 1e-3f);
    assertEquals("capacity doesn't match", absoluteUsedCapacity,
        (float) partitionCapacityInfoJson.getDouble("absoluteUsedCapacity"),
        1e-3f);
    assertEquals("capacity doesn't match", absoluteMaxCapacity,
        (float) partitionCapacityInfoJson.getDouble("absoluteMaxCapacity"),
        1e-3f);
  }
}
