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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;
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
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestRMWebServicesCapacitySched extends JerseyTestBase {

  protected static MockRM rm;
  protected static CapacitySchedulerConfiguration csConf;
  protected static YarnConfiguration conf;

  private class QueueInfo {
    float capacity;
    float usedCapacity;
    float maxCapacity;
    float absoluteCapacity;
    float absoluteMaxCapacity;
    float absoluteUsedCapacity;
    int numApplications;
    String queueName;
    String state;
  }

  private class LeafQueueInfo extends QueueInfo {
    int numActiveApplications;
    int numPendingApplications;
    int numContainers;
    int maxApplications;
    int maxApplicationsPerUser;
    int userLimit;
    float userLimitFactor;
  }

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
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
    }
  }

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  private static void setupQueueConfiguration(
      CapacitySchedulerConfiguration config) {

    // Define top-level queues
    config.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {"a", "b"});

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    config.setCapacity(A, 10.5f);
    config.setMaximumCapacity(A, 50);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    config.setCapacity(B, 89.5f);

    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    config.setQueues(A, new String[] {"a1", "a2"});
    config.setCapacity(A1, 30);
    config.setMaximumCapacity(A1, 50);

    config.setUserLimitFactor(A1, 100.0f);
    config.setCapacity(A2, 70);
    config.setUserLimitFactor(A2, 100.0f);

    final String B1 = B + ".b1";
    final String B2 = B + ".b2";
    final String B3 = B + ".b3";
    config.setQueues(B, new String[] {"b1", "b2", "b3"});
    config.setCapacity(B1, 60);
    config.setUserLimitFactor(B1, 100.0f);
    config.setCapacity(B2, 39.5f);
    config.setUserLimitFactor(B2, 100.0f);
    config.setCapacity(B3, 0.5f);
    config.setUserLimitFactor(B3, 100.0f);
    
    config.setQueues(A1, new String[] {"a1a", "a1b"});
    final String A1A = A1 + ".a1a";
    config.setCapacity(A1A, 85);
    final String A1B = A1 + ".a1b";
    config.setCapacity(A1B, 15);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  public TestRMWebServicesCapacitySched() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testClusterScheduler() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterScheduler(json);
  }

  @Test
  public void testClusterSchedulerSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterScheduler(json);
  }

  @Test
  public void testClusterSchedulerDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterScheduler(json);
  }

  @Test
  public void testClusterSchedulerXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler/").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList scheduler = dom.getElementsByTagName("scheduler");
    assertEquals("incorrect number of elements", 1, scheduler.getLength());
    NodeList schedulerInfo = dom.getElementsByTagName("schedulerInfo");
    assertEquals("incorrect number of elements", 1, schedulerInfo.getLength());
    verifyClusterSchedulerXML(schedulerInfo);
  }

  public void verifyClusterSchedulerXML(NodeList nodes) throws Exception {

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyClusterSchedulerGeneric(
          WebServicesTestUtils.getXmlAttrString(element, "xsi:type"),
          WebServicesTestUtils.getXmlFloat(element, "usedCapacity"),
          WebServicesTestUtils.getXmlFloat(element, "capacity"),
          WebServicesTestUtils.getXmlFloat(element, "maxCapacity"),
          WebServicesTestUtils.getXmlString(element, "queueName"));

      NodeList children = element.getChildNodes();
      for (int j = 0; j < children.getLength(); j++) {
        Element qElem = (Element) children.item(j);
        if(qElem.getTagName().equals("queues")) {
          NodeList qListInfos = qElem.getChildNodes();
          for (int k = 0; k < qListInfos.getLength(); k++) {
            Element qElem2 = (Element) qListInfos.item(k);
            String qName2 = WebServicesTestUtils.getXmlString(qElem2, "queueName");
            String q2 = CapacitySchedulerConfiguration.ROOT + "." + qName2;
            verifySubQueueXML(qElem2, q2, 100, 100);
          }
        }
      }
    }
  }

  public void verifySubQueueXML(Element qElem, String q, 
      float parentAbsCapacity, float parentAbsMaxCapacity)
      throws Exception {
    NodeList children = qElem.getChildNodes();
    boolean hasSubQueues = false;
    for (int j = 0; j < children.getLength(); j++) {
      Element qElem2 = (Element) children.item(j);
      if(qElem2.getTagName().equals("queues")) {
        NodeList qListInfos = qElem2.getChildNodes();
        if (qListInfos.getLength() > 0) {
          hasSubQueues = true;
        }
      }
    }
    QueueInfo qi = (hasSubQueues) ? new QueueInfo() : new LeafQueueInfo();
    qi.capacity = WebServicesTestUtils.getXmlFloat(qElem, "capacity");
    qi.usedCapacity =
        WebServicesTestUtils.getXmlFloat(qElem, "usedCapacity");
    qi.maxCapacity = WebServicesTestUtils.getXmlFloat(qElem, "maxCapacity");
    qi.absoluteCapacity = WebServicesTestUtils.getXmlFloat(qElem, "absoluteCapacity");
    qi.absoluteMaxCapacity =
        WebServicesTestUtils.getXmlFloat(qElem, "absoluteMaxCapacity");
    qi.absoluteUsedCapacity =
      WebServicesTestUtils.getXmlFloat(qElem, "absoluteUsedCapacity");
    qi.numApplications =
        WebServicesTestUtils.getXmlInt(qElem, "numApplications");
    qi.queueName = WebServicesTestUtils.getXmlString(qElem, "queueName");
    qi.state = WebServicesTestUtils.getXmlString(qElem, "state");
    verifySubQueueGeneric(q, qi, parentAbsCapacity, parentAbsMaxCapacity);
    if (hasSubQueues) {
      for (int j = 0; j < children.getLength(); j++) {
        Element qElem2 = (Element) children.item(j);
        if(qElem2.getTagName().equals("queues")) {
          NodeList qListInfos = qElem2.getChildNodes();
          for (int k = 0; k < qListInfos.getLength(); k++) {
            Element qElem3 = (Element) qListInfos.item(k);
            String qName3 = WebServicesTestUtils.getXmlString(qElem3, "queueName");
            String q3 = q + "." + qName3;
            verifySubQueueXML(qElem3, q3, qi.absoluteCapacity, qi.absoluteMaxCapacity);
          }
        }
      }
    } else {
      LeafQueueInfo lqi = (LeafQueueInfo) qi;
      lqi.numActiveApplications =
          WebServicesTestUtils.getXmlInt(qElem, "numActiveApplications");
      lqi.numPendingApplications =
          WebServicesTestUtils.getXmlInt(qElem, "numPendingApplications");
      lqi.numContainers =
          WebServicesTestUtils.getXmlInt(qElem, "numContainers");
      lqi.maxApplications =
          WebServicesTestUtils.getXmlInt(qElem, "maxApplications");
      lqi.maxApplicationsPerUser =
          WebServicesTestUtils.getXmlInt(qElem, "maxApplicationsPerUser");
      lqi.userLimit = WebServicesTestUtils.getXmlInt(qElem, "userLimit");
      lqi.userLimitFactor =
          WebServicesTestUtils.getXmlFloat(qElem, "userLimitFactor");
      verifyLeafQueueGeneric(q, lqi);
    }
  }

  private void verifyClusterScheduler(JSONObject json) throws JSONException,
      Exception {
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject info = json.getJSONObject("scheduler");
    assertEquals("incorrect number of elements", 1, info.length());
    info = info.getJSONObject("schedulerInfo");
    assertEquals("incorrect number of elements", 8, info.length());
    verifyClusterSchedulerGeneric(info.getString("type"),
        (float) info.getDouble("usedCapacity"),
        (float) info.getDouble("capacity"),
        (float) info.getDouble("maxCapacity"), info.getString("queueName"));
    JSONObject health = info.getJSONObject("health");
    assertNotNull(health);
    assertEquals("incorrect number of elements", 3, health.length());

    JSONArray arr = info.getJSONObject("queues").getJSONArray("queue");
    assertEquals("incorrect number of elements", 2, arr.length());

    // test subqueues
    for (int i = 0; i < arr.length(); i++) {
      JSONObject obj = arr.getJSONObject(i);
      String q = CapacitySchedulerConfiguration.ROOT + "." + obj.getString("queueName");
      verifySubQueue(obj, q, 100, 100);
    }
  }

  private void verifyClusterSchedulerGeneric(String type, float usedCapacity,
      float capacity, float maxCapacity, String queueName) throws Exception {

    assertTrue("type doesn't match", "capacityScheduler".matches(type));
    assertEquals("usedCapacity doesn't match", 0, usedCapacity, 1e-3f);
    assertEquals("capacity doesn't match", 100, capacity, 1e-3f);
    assertEquals("maxCapacity doesn't match", 100, maxCapacity, 1e-3f);
    assertTrue("queueName doesn't match", "root".matches(queueName));
  }

  private void verifySubQueue(JSONObject info, String q, 
      float parentAbsCapacity, float parentAbsMaxCapacity)
      throws JSONException, Exception {
    int numExpectedElements = 18;
    boolean isParentQueue = true;
    if (!info.has("queues")) {
      numExpectedElements = 31;
      isParentQueue = false;
    }
    assertEquals("incorrect number of elements", numExpectedElements, info.length());

    QueueInfo qi = isParentQueue ? new QueueInfo() : new LeafQueueInfo();
    qi.capacity = (float) info.getDouble("capacity");
    qi.usedCapacity = (float) info.getDouble("usedCapacity");
    qi.maxCapacity = (float) info.getDouble("maxCapacity");
    qi.absoluteCapacity = (float) info.getDouble("absoluteCapacity");
    qi.absoluteMaxCapacity = (float) info.getDouble("absoluteMaxCapacity");
    qi.absoluteUsedCapacity = (float) info.getDouble("absoluteUsedCapacity");
    qi.numApplications = info.getInt("numApplications");
    qi.queueName = info.getString("queueName");
    qi.state = info.getString("state");

    verifySubQueueGeneric(q, qi, parentAbsCapacity, parentAbsMaxCapacity);

    if (isParentQueue) {
      JSONArray arr = info.getJSONObject("queues").getJSONArray("queue");
      // test subqueues
      for (int i = 0; i < arr.length(); i++) {
        JSONObject obj = arr.getJSONObject(i);
        String q2 = q + "." + obj.getString("queueName");
        verifySubQueue(obj, q2, qi.absoluteCapacity, qi.absoluteMaxCapacity);
      }
    } else {
      Assert.assertEquals("\"type\" field is incorrect",
          "capacitySchedulerLeafQueueInfo", info.getString("type"));
      LeafQueueInfo lqi = (LeafQueueInfo) qi;
      lqi.numActiveApplications = info.getInt("numActiveApplications");
      lqi.numPendingApplications = info.getInt("numPendingApplications");
      lqi.numContainers = info.getInt("numContainers");
      lqi.maxApplications = info.getInt("maxApplications");
      lqi.maxApplicationsPerUser = info.getInt("maxApplicationsPerUser");
      lqi.userLimit = info.getInt("userLimit");
      lqi.userLimitFactor = (float) info.getDouble("userLimitFactor");
      verifyLeafQueueGeneric(q, lqi);
      // resourcesUsed and users (per-user resources used) are checked in
      // testPerUserResource()
    }
  }

  private void verifySubQueueGeneric(String q, QueueInfo info,
      float parentAbsCapacity, float parentAbsMaxCapacity) throws Exception {
    String[] qArr = q.split("\\.");
    assertTrue("q name invalid: " + q, qArr.length > 1);
    String qshortName = qArr[qArr.length - 1];

    assertEquals("usedCapacity doesn't match", 0, info.usedCapacity, 1e-3f);
    assertEquals("capacity doesn't match", csConf.getNonLabeledQueueCapacity(q),
        info.capacity, 1e-3f);
    float expectCapacity = csConf.getNonLabeledQueueMaximumCapacity(q);
    float expectAbsMaxCapacity = parentAbsMaxCapacity * (info.maxCapacity/100);
    if (CapacitySchedulerConfiguration.UNDEFINED == expectCapacity) {
      expectCapacity = 100;
      expectAbsMaxCapacity = 100;
    }
    assertEquals("maxCapacity doesn't match", expectCapacity,
        info.maxCapacity, 1e-3f);
    assertEquals("absoluteCapacity doesn't match",
        parentAbsCapacity * (info.capacity/100), info.absoluteCapacity, 1e-3f);
    assertEquals("absoluteMaxCapacity doesn't match",
        expectAbsMaxCapacity, info.absoluteMaxCapacity, 1e-3f);
    assertEquals("absoluteUsedCapacity doesn't match",
        0, info.absoluteUsedCapacity, 1e-3f);
    assertEquals("numApplications doesn't match", 0, info.numApplications);
    assertTrue("queueName doesn't match, got: " + info.queueName
        + " expected: " + q, qshortName.matches(info.queueName));
    assertTrue("state doesn't match",
        (csConf.getState(q).toString()).matches(info.state));

  }

  private void verifyLeafQueueGeneric(String q, LeafQueueInfo info)
      throws Exception {
    assertEquals("numActiveApplications doesn't match",
        0, info.numActiveApplications);
    assertEquals("numPendingApplications doesn't match",
        0, info.numPendingApplications);
    assertEquals("numContainers doesn't match",
        0, info.numContainers);

    int maxSystemApps = csConf.getMaximumSystemApplications();
    int expectedMaxApps = (int)(maxSystemApps * (info.absoluteCapacity/100));
    int expectedMaxAppsPerUser = Math.min(expectedMaxApps,
        (int)(expectedMaxApps * (info.userLimit/100.0f) *
        info.userLimitFactor));

    // TODO: would like to use integer comparisons here but can't due to
    //       roundoff errors in absolute capacity calculations
    assertEquals("maxApplications doesn't match",
        (float)expectedMaxApps, (float)info.maxApplications, 1.0f);
    assertEquals("maxApplicationsPerUser doesn't match",
        (float)expectedMaxAppsPerUser,
        (float)info.maxApplicationsPerUser, info.userLimitFactor);

    assertEquals("userLimit doesn't match", csConf.getUserLimit(q),
        info.userLimit);
    assertEquals("userLimitFactor doesn't match",
        csConf.getUserLimitFactor(q), info.userLimitFactor, 1e-3f);
  }

  //Return a child Node of node with the tagname or null if none exists 
  private Node getChildNodeByName(Node node, String tagname) {
    NodeList nodeList = node.getChildNodes();
    for (int i=0; i < nodeList.getLength(); ++i) {
      if (nodeList.item(i).getNodeName().equals(tagname)) {
        return nodeList.item(i);
      }
    }
    return null;
  }

  /**
   * Test per user resources and resourcesUsed elements in the web services XML
   * @throws Exception
   */
  @Test
  public void testPerUserResourcesXML() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();
    try {
      rm.submitApp(10, "app1", "user1", null, "b1");
      rm.submitApp(20, "app2", "user2", null, "b1");

      //Get the XML from ws/v1/cluster/scheduler
      WebResource r = resource();
      ClientResponse response = r.path("ws/v1/cluster/scheduler")
        .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      String xml = response.getEntity(String.class);
      DocumentBuilder db = DocumentBuilderFactory.newInstance()
        .newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      //Parse the XML we got
      Document dom = db.parse(is);

      //Get all users elements (1 for each leaf queue)
      NodeList allUsers = dom.getElementsByTagName("users");
      for (int i=0; i<allUsers.getLength(); ++i) {
        Node perUserResources = allUsers.item(i);
        String queueName = getChildNodeByName(perUserResources
          .getParentNode(), "queueName").getTextContent();
        if (queueName.equals("b1")) {
          //b1 should have two users (user1 and user2) which submitted jobs
          assertEquals(2, perUserResources.getChildNodes().getLength());
          NodeList users = perUserResources.getChildNodes();
          for (int j=0; j<users.getLength(); ++j) {
            Node user = users.item(j);
            String username = getChildNodeByName(user, "username")
              .getTextContent(); 
            assertTrue(username.equals("user1") || username.equals("user2"));
            //Should be a parsable integer
            Integer.parseInt(getChildNodeByName(getChildNodeByName(user,
              "resourcesUsed"), "memory").getTextContent());
            Integer.parseInt(getChildNodeByName(user, "numActiveApplications")
              .getTextContent());
            Integer.parseInt(getChildNodeByName(user, "numPendingApplications")
                .getTextContent());
          }
        } else {
        //Queues other than b1 should have 0 users
          assertEquals(0, perUserResources.getChildNodes().getLength());
        }
      }
      NodeList allResourcesUsed = dom.getElementsByTagName("resourcesUsed");
      for (int i=0; i<allResourcesUsed.getLength(); ++i) {
        Node resourcesUsed = allResourcesUsed.item(i);
        Integer.parseInt(getChildNodeByName(resourcesUsed, "memory")
            .getTextContent());
        Integer.parseInt(getChildNodeByName(resourcesUsed, "vCores")
              .getTextContent());
      }
    } finally {
      rm.stop();
    }
  }

  private void checkResourcesUsed(JSONObject queue) throws JSONException {
    queue.getJSONObject("resourcesUsed").getInt("memory");
    queue.getJSONObject("resourcesUsed").getInt("vCores");
  }

  //Also checks resourcesUsed
  private JSONObject getSubQueue(JSONObject queue, String subQueue)
    throws JSONException {
    JSONArray queues = queue.getJSONObject("queues").getJSONArray("queue");
    for (int i=0; i<queues.length(); ++i) {
      checkResourcesUsed(queues.getJSONObject(i));
      if (queues.getJSONObject(i).getString("queueName").equals(subQueue) ) {
        return queues.getJSONObject(i);
      }
    }
    return null;
  }

  @Test
  public void testPerUserResourcesJSON() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();
    try {
      rm.submitApp(10, "app1", "user1", null, "b1");
      rm.submitApp(20, "app2", "user2", null, "b1");

      //Get JSON
      WebResource r = resource();
      ClientResponse response = r.path("ws").path("v1").path("cluster")
          .path("scheduler/").accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);

      JSONObject schedulerInfo = json.getJSONObject("scheduler").getJSONObject(
        "schedulerInfo");
      JSONObject b1 = getSubQueue(getSubQueue(schedulerInfo, "b"), "b1");
      //Check users user1 and user2 exist in b1
      JSONArray users = b1.getJSONObject("users").getJSONArray("user");
      for (int i=0; i<2; ++i) {
        JSONObject user = users.getJSONObject(i);
        assertTrue("User isn't user1 or user2",user.getString("username")
          .equals("user1") || user.getString("username").equals("user2"));
        user.getInt("numActiveApplications");
        user.getInt("numPendingApplications");
        checkResourcesUsed(user);
      }

      // Verify 'queues' field is omitted from CapacitySchedulerLeafQueueInfo.
      try {
        b1.getJSONObject("queues");
        fail("CapacitySchedulerQueueInfo should omit field 'queues'" +
             "if child queue is empty.");
      } catch (JSONException je) {
        assertEquals("JSONObject[\"queues\"] not found.", je.getMessage());
      }
    } finally {
      rm.stop();
    }
  }


  @Test
  public void testResourceInfo() {
    Resource res = Resources.createResource(10, 1);
    // If we add a new resource (e.g disks), then
    // CapacitySchedulerPage and these RM WebServices + docs need to be updated
    // eg. ResourceInfo
    assertEquals("<memory:10, vCores:1>", res.toString());
  }
}
