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

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
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
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestRMWebServicesCapacitySched extends JerseyTest {

  private static MockRM rm;
  private CapacitySchedulerConfiguration csConf;

  private Injector injector = Guice.createInjector(new ServletModule() {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      csConf = new CapacitySchedulerConfiguration();
      csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      setupQueueConfiguration(csConf);
      rm = new MockRM(csConf);
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

  private static void setupQueueConfiguration(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacityScheduler.ROOT, new String[] { "a", "b" });
    conf.setCapacity(CapacityScheduler.ROOT, 100);

    final String A = CapacityScheduler.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 50);

    final String B = CapacityScheduler.ROOT + ".b";
    conf.setCapacity(B, 90);

    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    conf.setQueues(A, new String[] { "a1", "a2" });
    conf.setCapacity(A1, 30);
    conf.setMaximumCapacity(A1, 50);

    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, 70);
    conf.setUserLimitFactor(A2, 100.0f);

    final String B1 = B + ".b1";
    final String B2 = B + ".b2";
    final String B3 = B + ".b3";
    conf.setQueues(B, new String[] { "b1", "b2", "b3" });
    conf.setCapacity(B1, 50);
    conf.setUserLimitFactor(B1, 100.0f);
    conf.setCapacity(B2, 30);
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, 20);
    conf.setUserLimitFactor(B3, 100.0f);

  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
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
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterScheduler(json);
  }

  @Test
  public void testClusterSchedulerSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterScheduler(json);
  }

  @Test
  public void testClusterSchedulerDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyClusterScheduler(json);
  }

  @Test
  public void testClusterSchedulerXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler/").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
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

      NodeList queues = element.getElementsByTagName("queues");
      for (int j = 0; j < queues.getLength(); j++) {
        Element qElem = (Element) queues.item(j);
        String qName = WebServicesTestUtils.getXmlString(qElem, "queueName");
        String q = CapacityScheduler.ROOT + "." + qName;
        verifySubQueueXML(qElem, q);
      }
    }
  }

  public void verifySubQueueXML(Element qElem, String q) throws Exception {

    verifySubQueueGeneric(q,
        WebServicesTestUtils.getXmlFloat(qElem, "usedCapacity"),
        WebServicesTestUtils.getXmlFloat(qElem, "capacity"),
        WebServicesTestUtils.getXmlFloat(qElem, "maxCapacity"),
        WebServicesTestUtils.getXmlString(qElem, "queueName"),
        WebServicesTestUtils.getXmlString(qElem, "state"));

    NodeList queues = qElem.getElementsByTagName("subQueues");
    if (queues != null) {
      for (int j = 0; j < queues.getLength(); j++) {
        Element subqElem = (Element) queues.item(j);
        String qName = WebServicesTestUtils.getXmlString(subqElem, "queueName");
        String q2 = q + "." + qName;
        verifySubQueueXML(subqElem, q2);
      }
    }
  }

  private void verifyClusterScheduler(JSONObject json) throws JSONException,
      Exception {
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject info = json.getJSONObject("scheduler");
    assertEquals("incorrect number of elements", 1, info.length());
    info = info.getJSONObject("schedulerInfo");
    assertEquals("incorrect number of elements", 6, info.length());
    verifyClusterSchedulerGeneric(info.getString("type"),
        (float) info.getDouble("usedCapacity"),
        (float) info.getDouble("capacity"),
        (float) info.getDouble("maxCapacity"), info.getString("queueName"));

    JSONArray arr = info.getJSONArray("queues");
    assertEquals("incorrect number of elements", 2, arr.length());

    // test subqueues
    for (int i = 0; i < arr.length(); i++) {
      JSONObject obj = arr.getJSONObject(i);
      String q = CapacityScheduler.ROOT + "." + obj.getString("queueName");
      verifySubQueue(obj, q);
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

  private void verifySubQueue(JSONObject info, String q) throws JSONException,
      Exception {
    if (info.has("subQueues")) {
      assertEquals("incorrect number of elements", 6, info.length());
    } else {
      assertEquals("incorrect number of elements", 5, info.length());
    }
    verifySubQueueGeneric(q, (float) info.getDouble("usedCapacity"),
        (float) info.getDouble("capacity"),
        (float) info.getDouble("maxCapacity"), info.getString("queueName"),
        info.getString("state"));

    if (info.has("subQueues")) {
      JSONArray arr = info.getJSONArray("subQueues");
      // test subqueues
      for (int i = 0; i < arr.length(); i++) {
        JSONObject obj = arr.getJSONObject(i);
        String q2 = q + "." + obj.getString("queueName");
        verifySubQueue(obj, q2);
      }
    }
  }

  private void verifySubQueueGeneric(String q, float usedCapacity,
      float capacity, float maxCapacity, String qname, String state)
      throws Exception {
    String[] qArr = q.split("\\.");
    assertTrue("q name invalid: " + q, qArr.length > 1);
    String qshortName = qArr[qArr.length - 1];

    assertEquals("usedCapacity doesn't match", 0, usedCapacity, 1e-3f);
    assertEquals("capacity doesn't match", csConf.getCapacity(q), capacity,
        1e-3f);
    float expectCapacity = csConf.getMaximumCapacity(q);
    if (CapacitySchedulerConfiguration.UNDEFINED == expectCapacity) {
      expectCapacity = 100;
    }
    assertEquals("maxCapacity doesn't match", expectCapacity, maxCapacity,
        1e-3f);
    assertTrue("queueName doesn't match, got: " + qname + " expected: " + q,
        qshortName.matches(qname));
    assertTrue("state doesn't match",
        (csConf.getState(q).toString()).matches(state));

  }
}
