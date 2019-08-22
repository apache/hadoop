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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.AllocationState;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.ActivitiesTestUtils.INSUFFICIENT_RESOURCE_DIAGNOSTIC_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.ActivitiesTestUtils.findInAllocations;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.ActivitiesTestUtils.verifyNumberOfAllocationAttempts;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.ActivitiesTestUtils.verifyNumberOfAllocations;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.ActivitiesTestUtils.verifyStateOfAllocations;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
      conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
          YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
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
    config.setMaximumApplicationMasterResourcePerQueuePercent(queueB, 100);
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

  @Test (timeout=30000)
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
      WebResource r = resource().path(RMWSConsts.RM_WEB_SERVICE_PATH)
          .path(ActivitiesTestUtils.format(RMWSConsts.SCHEDULER_APP_ACTIVITIES,
              app1.getApplicationId().toString()));
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      JSONObject json = ActivitiesTestUtils.requestWebResource(r, params);
      assertEquals("waiting for display", json.getString("diagnostic"));

      //Trigger scheduling for this app
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      RMNode rmNode = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
      cs.handle(new NodeUpdateSchedulerEvent(rmNode));

      //Check app activities, it should contain one allocation and
      // final allocation state is ALLOCATED
      json = ActivitiesTestUtils.requestWebResource(r, params);

      verifyNumberOfAllocations(json, 1);

      JSONObject allocationObj = json.getJSONObject("allocations");
      verifyStateOfAllocations(allocationObj, "allocationState", "ALLOCATED");
      JSONObject requestAllocationObj =
          allocationObj.getJSONObject("requestAllocation");
      verifyNumberOfAllocationAttempts(requestAllocationObj, 2);
      verifyStateOfAllocations(requestAllocationObj, "allocationState",
          "ALLOCATED");
      JSONArray allocationAttemptArray =
          requestAllocationObj.getJSONArray("allocationAttempt");
      JSONObject allocationAttempt1 = allocationAttemptArray.getJSONObject(0);
      verifyStateOfAllocations(allocationAttempt1, "allocationState",
          "SKIPPED");
      assertTrue(allocationAttempt1.optString("diagnostic")
          .contains(INSUFFICIENT_RESOURCE_DIAGNOSTIC_PREFIX));
      JSONObject allocationAttempt2 = allocationAttemptArray.getJSONObject(1);
      verifyStateOfAllocations(allocationAttempt2, "allocationState",
          "ALLOCATED");
    } finally {
      rm.stop();
    }
  }

  @Test (timeout=30000)
  public void testInsufficientResourceDiagnostic() throws Exception {
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * 1024);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 2 * 1024);
    MockNM nm3 = rm.registerNode("127.0.0.3:1234", 2 * 1024);
    MockNM nm4 = rm.registerNode("127.0.0.4:1234", 2 * 1024);

    try {
      RMApp app1 = rm.submitApp(3072, "app1", "user1", null, "b");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      RMApp app2 = rm.submitApp(1024, "app2", "user1", null, "b");
      MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm1);

      WebResource r = resource();
      ClientResponse response =
          r.path("ws").path("v1").path("cluster").path("scheduler/activities")
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("waiting for next allocation", json.getString("diagnostic"));

      //Request a container for am2, will reserve a container on nm1
      am2.allocate("*", 4096, 1, new ArrayList<>());
      cs.handle(new NodeUpdateSchedulerEvent(
          rm.getRMContext().getRMNodes().get(nm1.getNodeId())));

      response =
          r.path("ws").path("v1").path("cluster").path("scheduler/activities")
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      //Check app activities
      verifyNumberOfAllocations(json, 1);
      JSONObject allocationObj = json.getJSONObject("allocations");
      //Check diagnostic for request of app1
      Predicate<JSONObject> findApp1Pred = (obj) -> obj.optString("name")
          .equals(app1.getApplicationId().toString());
      JSONObject app1Obj =
          findInAllocations(allocationObj, findApp1Pred).get(0);
      assertEquals("SKIPPED", app1Obj.optString("allocationState"));
      assertEquals(ActivityDiagnosticConstant.APPLICATION_DO_NOT_NEED_RESOURCE,
          app1Obj.optString("diagnostic"));
      //Check diagnostic for request of app2
      Predicate<JSONObject> findApp2ReqPred =
          (obj) -> obj.optString("name").equals("request_1_-1");
      List<JSONObject> app2ReqObjs =
          findInAllocations(allocationObj, findApp2ReqPred);
      assertEquals(1, app2ReqObjs.size());
      JSONArray app2ReqChildren = app2ReqObjs.get(0).getJSONArray("children");
      assertEquals(4, app2ReqChildren.length());
      for (int i = 0; i < app2ReqChildren.length(); i++) {
        JSONObject reqChild = app2ReqChildren.getJSONObject(i);
        if (reqChild.getString("allocationState").equals("SKIPPED")) {
          String diagnostic = reqChild.getString("diagnostic");
          assertTrue(
              diagnostic.contains(INSUFFICIENT_RESOURCE_DIAGNOSTIC_PREFIX));
        }
      }
    } finally {
      rm.stop();
    }
  }

  @Test (timeout=30000)
  public void testAppInsufficientResourceDiagnostic() throws Exception {
    rm.start();
    CapacityScheduler cs = (CapacityScheduler)rm.getResourceScheduler();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * 1024);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 2 * 1024);
    MockNM nm3 = rm.registerNode("127.0.0.3:1234", 2 * 1024);
    MockNM nm4 = rm.registerNode("127.0.0.4:1234", 2 * 1024);

    try {
      RMApp app1 = rm.submitApp(3072, "app1", "user1", null, "b");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      WebResource r = resource().path(RMWSConsts.RM_WEB_SERVICE_PATH)
          .path(ActivitiesTestUtils.format(RMWSConsts.SCHEDULER_APP_ACTIVITIES,
              app1.getApplicationId().toString()));
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      JSONObject json = ActivitiesTestUtils.requestWebResource(r, params);
      assertEquals("waiting for display", json.getString("diagnostic"));

      //Request two containers with different priority for am1
      am1.allocate(Arrays.asList(ResourceRequest
          .newInstance(Priority.newInstance(0), "*",
              Resources.createResource(1024), 1), ResourceRequest
          .newInstance(Priority.newInstance(1), "*",
              Resources.createResource(4096), 1)), null);

      //Trigger scheduling, will allocate a container with priority 0
      cs.handle(new NodeUpdateSchedulerEvent(
          rm.getRMContext().getRMNodes().get(nm1.getNodeId())));

      //Trigger scheduling, will reserve a container with priority 1 on nm1
      cs.handle(new NodeUpdateSchedulerEvent(
          rm.getRMContext().getRMNodes().get(nm1.getNodeId())));

      //Check app activities
      json = ActivitiesTestUtils.requestWebResource(r, params);
      verifyNumberOfAllocations(json, 2);
      JSONArray allocationArray = json.getJSONArray("allocations");
      //Check first activity is for second allocation with RESERVED state
      JSONObject allocationObj = allocationArray.getJSONObject(0);
      verifyStateOfAllocations(allocationObj, "allocationState", "RESERVED");
      JSONObject requestAllocationObj =
          allocationObj.getJSONObject("requestAllocation");
      verifyNumberOfAllocationAttempts(requestAllocationObj, 4);
      JSONArray allocationAttemptArray =
          requestAllocationObj.getJSONArray("allocationAttempt");
      for (int i=0; i<allocationAttemptArray.length(); i++) {
        JSONObject allocationAttemptObj =
            allocationAttemptArray.getJSONObject(i);
        if (i != allocationAttemptArray.length()-1) {
          assertTrue(allocationAttemptObj.optString("diagnostic")
              .contains(INSUFFICIENT_RESOURCE_DIAGNOSTIC_PREFIX));
        }
      }
      // check second activity is for first allocation with ALLOCATED state
      allocationObj = allocationArray.getJSONObject(1);
      verifyStateOfAllocations(allocationObj, "allocationState", "ALLOCATED");
      requestAllocationObj = allocationObj.getJSONObject("requestAllocation");
      verifyNumberOfAllocationAttempts(requestAllocationObj, 1);
      verifyStateOfAllocations(requestAllocationObj, "allocationState",
          "ALLOCATED");
    }
    finally {
      rm.stop();
    }
  }

  @Test (timeout=30000)
  public void testGroupByDiagnostics() throws Exception {
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * 1024);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 2 * 1024);
    MockNM nm3 = rm.registerNode("127.0.0.3:1234", 2 * 1024);
    MockNM nm4 = rm.registerNode("127.0.0.4:1234", 2 * 1024);

    try {
      RMApp app1 = rm.submitApp(3072, "app1", "user1", null, "b");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      WebResource r = resource().path(RMWSConsts.RM_WEB_SERVICE_PATH)
          .path(RMWSConsts.SCHEDULER_ACTIVITIES);
      MultivaluedMapImpl params = new MultivaluedMapImpl();

      /*
       * test non-exist groupBy
       */
      params.add(RMWSConsts.GROUP_BY, "NON-EXIST-GROUP-BY");
      JSONObject json = ActivitiesTestUtils.requestWebResource(r, params);
      Assert.assertTrue(json.getString("diagnostic")
          .startsWith("Got invalid groupBy:"));
      params.remove(RMWSConsts.GROUP_BY);

      /*
       * test groupBy: DIAGNOSTIC
       */
      params.add(RMWSConsts.GROUP_BY, RMWSConsts.ActivitiesGroupBy.
          DIAGNOSTIC.name().toLowerCase());
      json = ActivitiesTestUtils.requestWebResource(r, params);
      assertEquals("waiting for next allocation", json.getString("diagnostic"));

      //Request a container for am2, will reserve a container on nm1
      am1.allocate("*", 4096, 1, new ArrayList<>());
      cs.handle(new NodeUpdateSchedulerEvent(
          rm.getRMContext().getRMNodes().get(nm1.getNodeId())));

      json = ActivitiesTestUtils.requestWebResource(r, params);

      //Check activities
      verifyNumberOfAllocations(json, 1);
      JSONObject allocationObj = json.getJSONObject("allocations");
      //Check diagnostic for request of app1
      Predicate<JSONObject> findReqPred =
          (obj) -> obj.optString("name").equals("request_1_-1");
      List<JSONObject> reqObjs =
          findInAllocations(allocationObj, findReqPred);
      assertEquals(1, reqObjs.size());
      JSONArray reqChildren = reqObjs.get(0).getJSONArray("children");
      assertEquals(2, reqChildren.length());
      for (int i = 0; i < reqChildren.length(); i++) {
        JSONObject reqChild = reqChildren.getJSONObject(i);
        if (reqChild.getString("allocationState")
            .equals(AllocationState.SKIPPED.name())) {
          assertEquals("3", reqChild.getString("count"));
          assertEquals(3, reqChild.getJSONArray("nodeIds").length());
          assertTrue(reqChild.optString("diagnostic")
              .contains(INSUFFICIENT_RESOURCE_DIAGNOSTIC_PREFIX));
        } else if (reqChild.getString("allocationState")
            .equals(AllocationState.RESERVED.name())) {
          assertEquals("1", reqChild.getString("count"));
          assertNotNull(reqChild.getString("nodeIds"));
        } else {
          Assert.fail("Allocation state should be "
              + AllocationState.SKIPPED.name() + " or "
              + AllocationState.RESERVED.name() + "!");
        }
      }
    } finally {
      rm.stop();
    }
  }

  @Test (timeout=30000)
  public void testAppGroupByDiagnostics() throws Exception {
    rm.start();
    CapacityScheduler cs = (CapacityScheduler)rm.getResourceScheduler();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * 1024);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 2 * 1024);
    MockNM nm3 = rm.registerNode("127.0.0.3:1234", 2 * 1024);
    MockNM nm4 = rm.registerNode("127.0.0.4:1234", 2 * 1024);

    try {
      RMApp app1 = rm.submitApp(3072, "app1", "user1", null, "b");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      WebResource r = resource().path(RMWSConsts.RM_WEB_SERVICE_PATH)
          .path(ActivitiesTestUtils.format(RMWSConsts.SCHEDULER_APP_ACTIVITIES,
              app1.getApplicationId().toString()));
      MultivaluedMapImpl params = new MultivaluedMapImpl();

      /*
       * test non-exist groupBy
       */
      params.add(RMWSConsts.GROUP_BY, "NON-EXIST-GROUP-BY");
      JSONObject json = ActivitiesTestUtils.requestWebResource(r, params);
      Assert.assertTrue(json.getString("diagnostic")
          .startsWith("Got invalid groupBy:"));
      params.remove(RMWSConsts.GROUP_BY);

      /*
       * test groupBy: DIAGNOSTIC
       */
      params.add(RMWSConsts.GROUP_BY, RMWSConsts.ActivitiesGroupBy.
          DIAGNOSTIC.name().toLowerCase());
      json = ActivitiesTestUtils.requestWebResource(r, params);
      assertEquals("waiting for display", json.getString("diagnostic"));

      //Request two containers with different priority for am1
      am1.allocate(Arrays.asList(ResourceRequest
          .newInstance(Priority.newInstance(0), "*",
              Resources.createResource(1024), 1), ResourceRequest
          .newInstance(Priority.newInstance(1), "*",
              Resources.createResource(4096), 1)), null);

      //Trigger scheduling, will allocate a container with priority 0
      cs.handle(new NodeUpdateSchedulerEvent(
          rm.getRMContext().getRMNodes().get(nm1.getNodeId())));

      //Trigger scheduling, will reserve a container with priority 1 on nm1
      cs.handle(new NodeUpdateSchedulerEvent(
          rm.getRMContext().getRMNodes().get(nm1.getNodeId())));

      json = ActivitiesTestUtils.requestWebResource(r, params);

      //Check app activities
      verifyNumberOfAllocations(json, 2);
      JSONArray allocationArray = json.getJSONArray("allocations");
      //Check first activity is for second allocation with RESERVED state
      JSONObject allocationObj = allocationArray.getJSONObject(0);
      verifyStateOfAllocations(allocationObj, "allocationState", "RESERVED");
      JSONObject requestAllocationObj =
          allocationObj.getJSONObject("requestAllocation");
      verifyNumberOfAllocationAttempts(requestAllocationObj, 2);
      JSONArray allocationAttemptArray =
          requestAllocationObj.getJSONArray("allocationAttempt");
      for (int i=0; i<allocationAttemptArray.length(); i++) {
        JSONObject allocationAttemptObj =
            allocationAttemptArray.getJSONObject(i);
        if (allocationAttemptObj.getString("allocationState")
            .equals(AllocationState.SKIPPED.name())) {
          assertEquals("3", allocationAttemptObj.getString("count"));
          assertEquals(3,
              allocationAttemptObj.getJSONArray("nodeIds").length());
          assertTrue(allocationAttemptObj.optString("diagnostic")
              .contains(INSUFFICIENT_RESOURCE_DIAGNOSTIC_PREFIX));
        } else if (allocationAttemptObj.getString("allocationState")
            .equals(AllocationState.RESERVED.name())) {
          assertEquals("1", allocationAttemptObj.getString("count"));
          assertNotNull(allocationAttemptObj.getString("nodeIds"));
        } else {
          Assert.fail("Allocation state should be "
              + AllocationState.SKIPPED.name() + " or "
              + AllocationState.RESERVED.name() + "!");
        }
      }
      // check second activity is for first allocation with ALLOCATED state
      allocationObj = allocationArray.getJSONObject(1);
      verifyStateOfAllocations(allocationObj, "allocationState", "ALLOCATED");
      requestAllocationObj = allocationObj.getJSONObject("requestAllocation");
      verifyNumberOfAllocationAttempts(requestAllocationObj, 1);
      verifyStateOfAllocations(requestAllocationObj, "allocationState",
          "ALLOCATED");
      JSONObject allocationAttemptObj =
          requestAllocationObj.getJSONObject("allocationAttempt");
      assertEquals("1", allocationAttemptObj.getString("count"));
      assertNotNull(allocationAttemptObj.getString("nodeIds"));
    } finally {
      rm.stop();
    }
  }
}
