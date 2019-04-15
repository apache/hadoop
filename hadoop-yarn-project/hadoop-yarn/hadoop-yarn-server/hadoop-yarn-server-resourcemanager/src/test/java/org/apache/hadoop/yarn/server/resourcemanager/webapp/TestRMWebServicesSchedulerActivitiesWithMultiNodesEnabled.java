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

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.ResourceUsageMultiNodeLookupPolicy;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Tests for scheduler/app activities when multi-nodes enabled.
 */
public class TestRMWebServicesSchedulerActivitiesWithMultiNodesEnabled
    extends JerseyTestBase {

  private static MockRM rm;
  private static CapacitySchedulerConfiguration csConf;
  private static YarnConfiguration conf;

  public TestRMWebServicesSchedulerActivitiesWithMultiNodesEnabled() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
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
      // enable multi-nodes placement
      conf.setBoolean(
          CapacitySchedulerConfiguration.MULTI_NODE_PLACEMENT_ENABLED, true);
      String policyName = "resource-based";
      conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES,
          policyName);
      conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME,
          policyName);
      String policyConfPrefix =
          CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME + "."
              + policyName;
      conf.set(policyConfPrefix + ".class",
          ResourceUsageMultiNodeLookupPolicy.class.getName());
      conf.set(policyConfPrefix + ".sorting-interval.ms", "0");
      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
    }
  }

  private static void setupQueueConfiguration(
      CapacitySchedulerConfiguration config) {
    // Define top-level queues
    config.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {"a", "b"});

    final String queueA = CapacitySchedulerConfiguration.ROOT + ".a";
    config.setCapacity(queueA, 10.5f);
    config.setMaximumCapacity(queueA, 50);

    final String queueB = CapacitySchedulerConfiguration.ROOT + ".b";
    config.setCapacity(queueB, 89.5f);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @Test (timeout=30000)
  public void testAssignContainer() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();

    MockNM nm = new MockNM("127.0.0.1:1234", 2 * 1024,
        rm.getResourceTrackerService());
    nm.registerNode();

    try {
      RMApp app1 = rm.submitApp(1024, "app1", "user1", null, "b");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm);
      am1.allocate(Arrays.asList(ResourceRequest
          .newInstance(Priority.UNDEFINED, "127.0.0.1",
              Resources.createResource(1024), 1), ResourceRequest
          .newInstance(Priority.UNDEFINED, "/default-rack",
              Resources.createResource(1024), 1), ResourceRequest
          .newInstance(Priority.UNDEFINED, "*", Resources.createResource(1024),
              1)), null);

      //Trigger recording for multi-nodes without params
      WebResource r = resource();
      ClientResponse response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      //Trigger scheduling for this app
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      RMNode rmNode = rm.getRMContext().getRMNodes().get(nm.getNodeId());
      cs.handle(new NodeUpdateSchedulerEvent(rmNode));

      //Check scheduler activities, it should contain one allocation and
      // final allocation state is ALLOCATED
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 1);

      JSONObject allocations = json.getJSONObject("allocations");
      verifyStateOfAllocations(allocations,
          "finalAllocationState", "ALLOCATED");
    } finally {
      rm.stop();
    }
  }

  @Test (timeout=30000)
  public void testSchedulingWithoutPendingRequests()
      throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();

    MockNM nm = new MockNM("127.0.0.1:1234", 8 * 1024,
        rm.getResourceTrackerService());
    nm.registerNode();

    try {
      //Trigger recording for multi-nodes without params
      WebResource r = resource();
      ClientResponse response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      //Trigger scheduling for this app
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      RMNode rmNode = rm.getRMContext().getRMNodes().get(nm.getNodeId());
      cs.handle(new NodeUpdateSchedulerEvent(rmNode));

      //Check scheduler activities, it should contain one allocation and
      // final allocation state is SKIPPED
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 1);
      JSONObject allocations = json.getJSONObject("allocations");
      verifyStateOfAllocations(allocations,
          "finalAllocationState", "SKIPPED");
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testAppAssignContainer() throws Exception {
    rm.start();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * 1024);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 2 * 1024);

    try {
      RMApp app1 = rm.submitApp(1024, "app1", "user1", null, "b");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      am1.allocate(Arrays.asList(ResourceRequest
          .newInstance(Priority.UNDEFINED, "*", Resources.createResource(3072),
              1)), null);

      //Trigger recording for this app
      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add(RMWSConsts.APP_ID, app1.getApplicationId().toString());
      ClientResponse response = r.path("ws").path("v1").path("cluster")
          .path("scheduler/app-activities").queryParams(params)
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("waiting for display", json.getString("diagnostic"));

      //Trigger scheduling for this app
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      RMNode rmNode = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
      cs.handle(new NodeUpdateSchedulerEvent(rmNode));

      //Check app activities, it should contain one allocation and
      // final allocation state is ALLOCATED
      response = r.path("ws").path("v1").path("cluster")
          .path("scheduler/app-activities").queryParams(params)
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 1);

      JSONObject allocations = json.getJSONObject("allocations");
      verifyStateOfAllocations(allocations, "allocationState", "ACCEPTED");
      JSONArray allocationAttempts =
          allocations.getJSONArray("allocationAttempt");
      assertEquals(2, allocationAttempts.length());
    } finally {
      rm.stop();
    }
  }

  private void verifyNumberOfAllocations(JSONObject json, int realValue)
      throws Exception {
    if (json.isNull("allocations")) {
      assertEquals("Number of allocations is wrong", 0, realValue);
    } else {
      Object object = json.get("allocations");
      if (object.getClass() == JSONObject.class) {
        assertEquals("Number of allocations is wrong", 1, realValue);
      } else if (object.getClass() == JSONArray.class) {
        assertEquals("Number of allocations is wrong in: " + object,
            ((JSONArray) object).length(), realValue);
      }
    }
  }

  private void verifyStateOfAllocations(JSONObject allocation,
      String nameToCheck, String realState) throws Exception {
    assertEquals("State of allocation is wrong", allocation.get(nameToCheck),
        realState);
  }
}
