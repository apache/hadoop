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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import java.io.IOException;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
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

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.*;

import static org.junit.Assert.*;

public class TestRMWebServicesAttemptContainers<cs> extends JerseyTest {

  private static MockRM rm;

  private Injector injector = Guice.createInjector(new ServletModule() {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      Configuration conf = new Configuration();
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);
      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
    }
  });

  protected CapacitySchedulerConfiguration setupSchedulerConfiguration() {
    Configuration schedConf = new Configuration();
    schedConf.setInt(YarnConfiguration.RESOURCE_TYPES
        + ".vcores.minimum-allocation", 1);
    schedConf.setInt(YarnConfiguration.RESOURCE_TYPES
        + ".vcores.maximum-allocation", 8);
    schedConf.setInt(YarnConfiguration.RESOURCE_TYPES
        + ".memory-mb.minimum-allocation", 1024);
    schedConf.setInt(YarnConfiguration.RESOURCE_TYPES
        + ".memory-mb.maximum-allocation", 16384);

    return new CapacitySchedulerConfiguration(schedConf);
  }

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

  public TestRMWebServicesAttemptContainers() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testContainersXML() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048 * 100);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("testwordcount")
            .withUser("user1")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    List<Container> containers = am1
        .allocate("127.0.0.1:1234", 1024, 5, new ArrayList<ContainerId>())
        .getAllocatedContainers();
    while (containers.size() != 5) {
      amNodeManager.nodeHeartbeat(true);
      containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(200);
    }

    //set containers state (remember AM container is id 1)
    amNodeManager.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    amNodeManager.nodeHeartbeat(am1.getApplicationAttemptId(), 3, ContainerState.RUNNING);
    amNodeManager.nodeHeartbeat(am1.getApplicationAttemptId(), 4, ContainerState.RUNNING);
    ContainerId containerId2 = ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    ContainerId containerId3 = ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
    ContainerId containerId4 = ContainerId.newContainerId(am1.getApplicationAttemptId(), 4);
    rm.waitForState(amNodeManager, containerId2, RMContainerState.RUNNING);
    rm.waitForState(amNodeManager, containerId3, RMContainerState.RUNNING);
    rm.waitForState(amNodeManager, containerId4, RMContainerState.RUNNING);

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(app1.getApplicationId().toString())
        .path("containers").path("running")
        .accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));

    Document dom = db.parse(is);
    NodeList nodesAttempt = dom.getElementsByTagName("attempt");
    assertEquals("incorrect number of elements", 1, nodesAttempt.getLength());

    String attemptId = WebServicesTestUtils.getXmlAttrString((Element) nodesAttempt.item(0), "id");
    assertEquals(app1.getCurrentAppAttempt().getAppAttemptId().toString(), attemptId);

    NodeList nodeAmList = dom.getElementsByTagName("am");
    assertEquals("incorrect number of elements", 1, nodeAmList.getLength());

    Element nodeAm = (Element) nodeAmList.item(0);

    Container am = app1.getCurrentAppAttempt().getMasterContainer();
    verifyContainerXml(am, nodeAm);

    NodeList nodeContainerList = dom.getElementsByTagName("container");
    //since only 3 containers are running, we expect only 3 results
    assertEquals("incorrect number of elements", 3, nodeContainerList.getLength());

    //transform nodelist to List and sort for easier testing
    List<Element> nodeContainerJavaList = transformNodeListToJavaList(nodeContainerList);
    Collections.sort(nodeContainerJavaList, new Comparator<Element>() {
      @Override
      public int compare(Element o1, Element o2) {
        return WebServicesTestUtils.getXmlString(o1, "id").compareTo(WebServicesTestUtils.getXmlString(o2, "id"));
      }
    });

    Element container2El = nodeContainerJavaList.get(0);
    verifyContainerXml(findContainer(containers, 2), container2El);

    Element container3El = nodeContainerJavaList.get(1);
    verifyContainerXml(findContainer(containers, 3), container3El);

    Element container4El = nodeContainerJavaList.get(2);
    verifyContainerXml(findContainer(containers, 4), container4El);

    rm.stop();
  }

  @Test
  public void testContainersJSON() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048 * 100);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("testwordcount")
            .withUser("user1")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    List<Container> containers = am1
        .allocate("127.0.0.1:1234", 1024, 5, new ArrayList<ContainerId>())
        .getAllocatedContainers();
    while (containers.size() != 5) {
      amNodeManager.nodeHeartbeat(true);
      containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(200);
    }

    //set containers state (remember AM container is id 1)
    amNodeManager.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    amNodeManager.nodeHeartbeat(am1.getApplicationAttemptId(), 3, ContainerState.RUNNING);
    amNodeManager.nodeHeartbeat(am1.getApplicationAttemptId(), 4, ContainerState.RUNNING);
    ContainerId containerId2 = ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    ContainerId containerId3 = ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
    ContainerId containerId4 = ContainerId.newContainerId(am1.getApplicationAttemptId(), 4);
    rm.waitForState(amNodeManager, containerId2, RMContainerState.RUNNING);
    rm.waitForState(amNodeManager, containerId3, RMContainerState.RUNNING);
    rm.waitForState(amNodeManager, containerId4, RMContainerState.RUNNING);

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").path(app1.getApplicationId().toString())
        .path("containers").path("running")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());

    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());

    JSONObject attemptJSON = json.getJSONObject("attempt");
    assertEquals("incorrect number of elements", 3, attemptJSON.length());

    String attemptId = attemptJSON.getString("id");
    assertEquals(app1.getCurrentAppAttempt().getAppAttemptId().toString(), attemptId);

    JSONObject amJSON = attemptJSON.getJSONObject("am");
    assertEquals("incorrect number of elements", 2, amJSON.length());

    Container am = app1.getCurrentAppAttempt().getMasterContainer();
    verifyContainerJSON(am, amJSON);

    JSONArray containersJSON = attemptJSON.getJSONArray("container");
    assertEquals("incorrect number of elements", 3, containersJSON.length());

    //transform containersJSON to List and sort for easier testing
    List<JSONObject> containersList = transformJSONArrayToJavaList(containersJSON);
    Collections.sort(containersList, new Comparator<JSONObject>() {
      @Override
      public int compare(JSONObject o1, JSONObject o2) {
        try {
          return o1.getString("id").compareTo(o2.getString("id"));
        } catch (JSONException e) {
          return 0;
        }
      }
    });

    JSONObject container2JSON = containersList.get(0);
    verifyContainerJSON(findContainer(containers, 2), container2JSON);

    JSONObject container3JSON = containersList.get(1);
    verifyContainerJSON(findContainer(containers, 3), container3JSON);

    JSONObject container4JSON = containersList.get(2);
    verifyContainerJSON(findContainer(containers, 4), container4JSON);

    rm.stop();
  }

  @Test
  public void testInvalidApp() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("apps").path("not_an_app_id")
        .path("containers").path("running")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils.checkStringMatch("exception message",
        "java.lang.Exception: not_an_app_id is not a valid appId. A valid appId looks like application_1539264296421_0013", message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);

  }

  private void verifyContainerXml(Container container, Element element) {
    String id = WebServicesTestUtils.getXmlString(element, "id");
    assertEquals(container.getId().toString(), id);
    String host = WebServicesTestUtils.getXmlString(element, "host");
    assertEquals(container.getNodeId().getHost(), host);
  }

  private void verifyContainerJSON(Container container, JSONObject object) throws JSONException {
    String id = object.getString("id");
    assertEquals(container.getId().toString(), id);
    String host = object.getString("host");
    assertEquals(container.getNodeId().getHost(), host);
  }

  private Container findContainer(List<Container> containers, long id){
    for(Container c: containers){
      if(c.getId().getContainerId() == id) {
        return c;
      }
    }
    throw new IllegalArgumentException("Could not find container " + id + " in provided list");
  }

  private List<Element> transformNodeListToJavaList(NodeList list){
    List<Element> nodes = new ArrayList<>();
    for(int i = 0; i < list.getLength(); i++){
      nodes.add((Element) list.item(i));
    }
    return nodes;
  }

  private List<JSONObject> transformJSONArrayToJavaList(JSONArray array) throws JSONException {
    List<JSONObject> objects = new ArrayList<>();
    for(int i = 0; i < array.length(); i++){
      objects.add((JSONObject) array.get(i));
    }
    return objects;
  }

}