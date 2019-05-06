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

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import javax.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.NODE;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTag;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.ActivitiesTestUtils.INSUFFICIENT_RESOURCE_DIAGNOSTIC_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.ActivitiesTestUtils.UNMATCHED_PARTITION_OR_PC_DIAGNOSTIC_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.ActivitiesTestUtils.verifyNumberOfAllocationAttempts;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.ActivitiesTestUtils.verifyNumberOfAllocations;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.ActivitiesTestUtils.verifyNumberOfNodes;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.ActivitiesTestUtils.verifyQueueOrder;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.ActivitiesTestUtils.verifyStateOfAllocations;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestRMWebServicesSchedulerActivities
    extends TestRMWebServicesCapacitySched {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestRMWebServicesSchedulerActivities.class);

  @Test
  public void testAssignMultipleContainersPerNodeHeartbeat()
      throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();

    MockNM nm = new MockNM("127.0.0.1:1234", 24 * 1024,
        rm.getResourceTrackerService());
    nm.registerNode();

    try {
      RMApp app1 = rm.submitApp(10, "app1", "user1", null, "b1");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm);
      am1.allocate(Arrays.asList(ResourceRequest
          .newInstance(Priority.UNDEFINED, "127.0.0.1",
              Resources.createResource(1024), 10), ResourceRequest
          .newInstance(Priority.UNDEFINED, "/default-rack",
              Resources.createResource(1024), 10), ResourceRequest
          .newInstance(Priority.UNDEFINED, "*", Resources.createResource(1024),
              10)), null);

      //Get JSON
      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add("nodeId", "127.0.0.1:1234");
      ClientResponse response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);

      nm.nodeHeartbeat(true);
      Thread.sleep(1000);

      //Get JSON
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      // Collection logic of scheduler activities changed after YARN-9313,
      // only one allocation should be recorded for all scenarios.
      verifyNumberOfAllocations(json, 1);
      verifyStateOfAllocations(json.getJSONObject("allocations"),
          "finalAllocationState", "ALLOCATED");
      verifyQueueOrder(json.getJSONObject("allocations"), "root-a-b-b2-b3-b1");
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testAssignWithoutAvailableResource() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();

    MockNM nm = new MockNM("127.0.0.1:1234", 1 * 1024,
        rm.getResourceTrackerService());
    nm.registerNode();

    try {
      RMApp app1 = rm.submitApp(1024, "app1", "user1", null, "b1");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm);
      am1.allocate(Arrays.asList(ResourceRequest
          .newInstance(Priority.UNDEFINED, "127.0.0.1",
              Resources.createResource(1024), 10), ResourceRequest
          .newInstance(Priority.UNDEFINED, "/default-rack",
              Resources.createResource(1024), 10), ResourceRequest
          .newInstance(Priority.UNDEFINED, "*", Resources.createResource(1024),
              10)), null);

      //Get JSON
      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add("nodeId", "127.0.0.1");
      ClientResponse response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);

      nm.nodeHeartbeat(true);
      Thread.sleep(1000);

      //Get JSON
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 0);
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testNoNM() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();

    try {
      //Get JSON
      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add("nodeId", "127.0.0.1:1234");
      ClientResponse response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);

      Thread.sleep(1000);

      //Get JSON
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 0);
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testWrongNodeId() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();

    MockNM nm = new MockNM("127.0.0.1:1234", 24 * 1024,
        rm.getResourceTrackerService());
    nm.registerNode();

    try {
      RMApp app1 = rm.submitApp(1024, "app1", "user1", null, "b1");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm);
      am1.allocate(Arrays.asList(ResourceRequest
          .newInstance(Priority.UNDEFINED, "127.0.0.1",
              Resources.createResource(1024), 10), ResourceRequest
          .newInstance(Priority.UNDEFINED, "/default-rack",
              Resources.createResource(1024), 10), ResourceRequest
          .newInstance(Priority.UNDEFINED, "*", Resources.createResource(1024),
              10)), null);

      //Get JSON
      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add("nodeId", "127.0.0.0");
      ClientResponse response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);

      nm.nodeHeartbeat(true);
      Thread.sleep(1000);

      //Get JSON
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 0);
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testReserveNewContainer() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();

    MockNM nm1 = new MockNM("127.0.0.1:1234", 4 * 1024,
        rm.getResourceTrackerService());
    MockNM nm2 = new MockNM("127.0.0.2:1234", 4 * 1024,
        rm.getResourceTrackerService());

    nm1.registerNode();
    nm2.registerNode();

    try {
      RMApp app1 = rm.submitApp(10, "app1", "user1", null, "b1");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      RMApp app2 = rm.submitApp(10, "app2", "user1", null, "b2");
      MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);

      am1.allocate(Arrays.asList(ResourceRequest
          .newInstance(Priority.UNDEFINED, "*", Resources.createResource(4096),
              10)), null);

      // Reserve new container
      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add("nodeId", "127.0.0.2");
      ClientResponse response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);

      nm2.nodeHeartbeat(true);
      Thread.sleep(1000);

      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 1);

      verifyQueueOrder(json.getJSONObject("allocations"), "root-a-b-b3-b1");

      JSONObject allocations = json.getJSONObject("allocations");
      verifyStateOfAllocations(allocations, "finalAllocationState", "RESERVED");

      // Do a node heartbeat again without releasing container from app2
      r = resource();
      params = new MultivaluedMapImpl();
      params.add("nodeId", "127.0.0.2");
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      nm2.nodeHeartbeat(true);
      Thread.sleep(1000);

      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 1);

      verifyQueueOrder(json.getJSONObject("allocations"), "b1");

      allocations = json.getJSONObject("allocations");
      verifyStateOfAllocations(allocations, "finalAllocationState", "SKIPPED");

      // Finish application 2
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      ContainerId containerId = ContainerId.newContainerId(
          am2.getApplicationAttemptId(), 1);
      cs.completedContainer(cs.getRMContainer(containerId), ContainerStatus
              .newInstance(containerId, ContainerState.COMPLETE, "", 0),
          RMContainerEventType.FINISHED);

      // Do a node heartbeat again
      r = resource();
      params = new MultivaluedMapImpl();
      params.add("nodeId", "127.0.0.2");
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      nm2.nodeHeartbeat(true);
      Thread.sleep(1000);

      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 1);

      verifyQueueOrder(json.getJSONObject("allocations"), "b1");

      allocations = json.getJSONObject("allocations");
      verifyStateOfAllocations(allocations, "finalAllocationState",
          "ALLOCATED_FROM_RESERVED");
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testActivityJSON() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();

    MockNM nm = new MockNM("127.0.0.1:1234", 24 * 1024,
        rm.getResourceTrackerService());
    nm.registerNode();

    try {
      RMApp app1 = rm.submitApp(10, "app1", "user1", null, "b1");

      //Get JSON
      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add("nodeId", "127.0.0.1");
      ClientResponse response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);

      nm.nodeHeartbeat(true);
      Thread.sleep(1000);

      //Get JSON
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 1);

      JSONObject allocations = json.getJSONObject("allocations");
      verifyStateOfAllocations(allocations, "finalAllocationState",
          "ALLOCATED");

      // Increase number of nodes to 6 since request node has been added
      verifyNumberOfNodes(allocations, 6);

      verifyQueueOrder(json.getJSONObject("allocations"), "root-b-b1");
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testAppActivityJSON() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();

    MockNM nm = new MockNM("127.0.0.1:1234", 24 * 1024,
        rm.getResourceTrackerService());
    nm.registerNode();

    try {
      RMApp app1 = rm.submitApp(10, "app1", "user1", null, "b1");

      //Get JSON
      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add("appId", app1.getApplicationId().toString());
      ClientResponse response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      nm.nodeHeartbeat(true);
      Thread.sleep(5000);

      //Get JSON
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      //Check app activities
      verifyNumberOfAllocations(json, 1);
      JSONObject allocations = json.getJSONObject("allocations");
      verifyStateOfAllocations(allocations, "allocationState", "ACCEPTED");
      //Check request allocation
      JSONObject requestAllocationObj =
          allocations.getJSONObject("requestAllocation");
      verifyStateOfAllocations(requestAllocationObj, "allocationState",
          "ALLOCATED");
      assertEquals("0", requestAllocationObj.optString("requestPriority"));
      assertEquals("-1", requestAllocationObj.optString("allocationRequestId"));
      //Check allocation attempts
      verifyNumberOfAllocationAttempts(requestAllocationObj, 1);
      JSONObject allocationAttemptObj =
          requestAllocationObj.getJSONObject("allocationAttempt");
      verifyStateOfAllocations(allocationAttemptObj, "allocationState",
          "ALLOCATED");
      assertNotNull(allocationAttemptObj.get("nodeId"));
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testAppAssignMultipleContainersPerNodeHeartbeat()
      throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();

    MockNM nm = new MockNM("127.0.0.1:1234", 24 * 1024,
        rm.getResourceTrackerService());
    nm.registerNode();

    try {
      RMApp app1 = rm.submitApp(1024, "app1", "user1", null, "b1");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm);
      am1.allocate(Arrays.asList(ResourceRequest
          .newInstance(Priority.UNDEFINED, "127.0.0.1",
              Resources.createResource(1024), 10), ResourceRequest
          .newInstance(Priority.UNDEFINED, "/default-rack",
              Resources.createResource(1024), 10), ResourceRequest
          .newInstance(Priority.UNDEFINED, "*", Resources.createResource(1024),
              10)), null);

      //Get JSON
      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add("appId", app1.getApplicationId().toString());
      ClientResponse response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      nm.nodeHeartbeat(true);
      Thread.sleep(5000);

      //Get JSON
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 10);

      JSONArray allocations = json.getJSONArray("allocations");
      for (int i = 0; i < allocations.length(); i++) {
        verifyStateOfAllocations(allocations.getJSONObject(i),
            "allocationState", "ACCEPTED");
      }
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testAppAssignWithoutAvailableResource() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();

    MockNM nm = new MockNM("127.0.0.1:1234", 1 * 1024,
        rm.getResourceTrackerService());
    nm.registerNode();

    try {
      RMApp app1 = rm.submitApp(1024, "app1", "user1", null, "b1");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm);
      am1.allocate(Arrays.asList(ResourceRequest
          .newInstance(Priority.UNDEFINED, "127.0.0.1",
              Resources.createResource(1024), 10), ResourceRequest
          .newInstance(Priority.UNDEFINED, "/default-rack",
              Resources.createResource(1024), 10), ResourceRequest
          .newInstance(Priority.UNDEFINED, "*", Resources.createResource(1024),
              10)), null);

      //Get JSON
      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add("appId", app1.getApplicationId().toString());
      ClientResponse response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      nm.nodeHeartbeat(true);
      Thread.sleep(5000);

      //Get JSON
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 0);
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testAppNoNM() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();

    try {
      RMApp app1 = rm.submitApp(1024, "app1", "user1", null, "b1");

      //Get JSON
      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add("appId", app1.getApplicationId().toString());
      ClientResponse response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);

      //Get JSON
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 0);
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testAppReserveNewContainer() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();

    MockNM nm1 = new MockNM("127.0.0.1:1234", 4 * 1024,
        rm.getResourceTrackerService());
    MockNM nm2 = new MockNM("127.0.0.2:1234", 4 * 1024,
        rm.getResourceTrackerService());

    nm1.registerNode();
    nm2.registerNode();

    try {
      RMApp app1 = rm.submitApp(10, "app1", "user1", null, "b1");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      RMApp app2 = rm.submitApp(10, "app2", "user1", null, "b2");
      MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);

      am1.allocate(Arrays.asList(ResourceRequest
          .newInstance(Priority.UNDEFINED, "*", Resources.createResource(4096),
              10)), null);

      // Reserve new container
      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add("appId", app1.getApplicationId().toString());
      ClientResponse response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);

      nm2.nodeHeartbeat(true);
      Thread.sleep(1000);

      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 1);

      // Do a node heartbeat again without releasing container from app2
      r = resource();
      params = new MultivaluedMapImpl();
      params.add("appId", app1.getApplicationId().toString());
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      nm2.nodeHeartbeat(true);
      Thread.sleep(1000);

      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 2);

      // Finish application 2
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      ContainerId containerId = ContainerId.newContainerId(
          am2.getApplicationAttemptId(), 1);
      cs.completedContainer(cs.getRMContainer(containerId), ContainerStatus
              .newInstance(containerId, ContainerState.COMPLETE, "", 0),
          RMContainerEventType.FINISHED);

      // Do a node heartbeat again
      r = resource();
      params = new MultivaluedMapImpl();
      params.add("appId", app1.getApplicationId().toString());
      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      nm2.nodeHeartbeat(true);
      Thread.sleep(1000);

      response = r.path("ws").path("v1").path("cluster").path(
          "scheduler/app-activities").queryParams(params).accept(
          MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 3);
    } finally {
      rm.stop();
    }
  }

  @Test (timeout=30000)
  public void testInsufficientResourceDiagnostic()
      throws Exception {
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * 1024);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 8 * 1024);

    try {
      RMApp app1 = rm.submitApp(512, "app1", "user1", null, "b1");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      WebResource r = resource();

      ClientResponse response =
          r.path("ws").path("v1").path("cluster").path("scheduler/activities")
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("waiting for next allocation",
          json.getString("diagnostic"));

      am1.allocate(Arrays.asList(ResourceRequest
          .newInstance(Priority.UNDEFINED, "*",
              Resources.createResource(5 * 1024), 1)), null);

      //will reserve a container on nm1
      cs.handle(new NodeUpdateSchedulerEvent(
          rm.getRMContext().getRMNodes().get(nm1.getNodeId())));

      response =
          r.path("ws").path("v1").path("cluster").path("scheduler/activities")
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 1);
      JSONObject allocationObj = json.getJSONObject("allocations");
      // check diagnostics
      Predicate<JSONObject> findReqPred =
          (obj) -> obj.optString("name").equals("request_-1_-1");
      List<JSONObject> app2ReqObjs =
          ActivitiesTestUtils.findInAllocations(allocationObj, findReqPred);
      assertEquals(1, app2ReqObjs.size());
      JSONObject reqChild = app2ReqObjs.get(0).getJSONObject("children");
      assertTrue(reqChild.getString("diagnostic")
          .contains(INSUFFICIENT_RESOURCE_DIAGNOSTIC_PREFIX));
    } finally {
      rm.stop();
    }
  }

  @Test (timeout=30000)
  public void testPlacementConstraintDiagnostic()
      throws Exception {
    rm.start();
    CapacityScheduler cs = (CapacityScheduler)rm.getResourceScheduler();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * 1024);

    try {
      RMApp app1 = rm.submitApp(512, "app1", "user1", null, "b1");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      // init scheduling request
      PlacementConstraint pcExpression = PlacementConstraints
          .build(PlacementConstraints.targetIn(NODE, allocationTag("foo")));
      List<SchedulingRequest> schedulingRequests = new ArrayList<>();
      schedulingRequests.add(ActivitiesTestUtils
          .schedulingRequest(5, 1, 1, 1, 512, pcExpression, "foo"));
      AllocateRequest allocateReq =
          AllocateRequest.newBuilder().schedulingRequests(schedulingRequests)
              .build();
      am1.allocate(allocateReq);

      WebResource r = resource();
      ClientResponse response =
          r.path("ws").path("v1").path("cluster").path("scheduler/activities")
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("waiting for next allocation",
          json.getString("diagnostic"));

      // trigger scheduling
      cs.handle(new NodeUpdateSchedulerEvent(
          rm.getRMContext().getRMNodes().get(nm1.getNodeId())));

      response =
          r.path("ws").path("v1").path("cluster").path("scheduler/activities")
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 1);
      JSONObject allocationObj = json.getJSONObject("allocations");
      // check diagnostics
      Predicate<JSONObject> findReqPred =
          (obj) -> obj.optString("name").equals("request_1_1");
      List<JSONObject> reqObjs =
          ActivitiesTestUtils.findInAllocations(allocationObj, findReqPred);
      assertEquals(1, reqObjs.size());
      JSONObject reqChild = reqObjs.get(0).getJSONObject("children");
      assertTrue(reqChild.getString("diagnostic")
          .contains(UNMATCHED_PARTITION_OR_PC_DIAGNOSTIC_PREFIX));
    } finally {
      rm.stop();
    }
  }

  @Test (timeout=30000)
  public void testAppInsufficientResourceDiagnostic()
      throws Exception {
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * 1024);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 8 * 1024);

    try {
      RMApp app1 = rm.submitApp(512, "app1", "user1", null, "b1");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add("appId", app1.getApplicationId().toString());
      ClientResponse response = r.path("ws").path("v1").path("cluster")
          .path("scheduler/app-activities").queryParams(params)
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("waiting for display",
          json.getString("diagnostic"));

      // am1 asks for 1 * 5GB container
      am1.allocate(Arrays.asList(ResourceRequest
          .newInstance(Priority.UNDEFINED, "*",
              Resources.createResource(5 * 1024), 1)), null);
      // trigger scheduling
      cs.handle(new NodeUpdateSchedulerEvent(
          rm.getRMContext().getRMNodes().get(nm1.getNodeId())));

      response =
          r.path("ws").path("v1").path("cluster")
              .path("scheduler/app-activities").queryParams(params)
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 1);
      JSONObject allocationObj = json.getJSONObject("allocations");
      JSONObject requestAllocationObj =
          allocationObj.getJSONObject("requestAllocation");
      verifyNumberOfAllocationAttempts(requestAllocationObj, 1);
      JSONObject allocationAttemptObj =
          requestAllocationObj.getJSONObject("allocationAttempt");
      verifyStateOfAllocations(allocationAttemptObj, "allocationState",
          "SKIPPED");
      assertTrue(allocationAttemptObj.optString("diagnostic")
          .contains(INSUFFICIENT_RESOURCE_DIAGNOSTIC_PREFIX));
    } finally {
      rm.stop();
    }
  }

  @Test (timeout=30000)
  public void testAppPlacementConstraintDiagnostic()
      throws Exception {
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * 1024);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 8 * 1024);

    try {
      RMApp app1 = rm.submitApp(512, "app1", "user1", null, "b1");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      WebResource r = resource();
      MultivaluedMapImpl params = new MultivaluedMapImpl();
      params.add("appId", app1.getApplicationId().toString());
      ClientResponse response = r.path("ws").path("v1").path("cluster")
          .path("scheduler/app-activities").queryParams(params)
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("waiting for display",
          json.getString("diagnostic"));

      // am1 asks for 1 * 5GB container with PC expression: in,node,foo
      PlacementConstraint pcExpression = PlacementConstraints
          .build(PlacementConstraints.targetIn(NODE, allocationTag("foo")));
      List<SchedulingRequest> schedulingRequests = new ArrayList<>();
      schedulingRequests.add(ActivitiesTestUtils
          .schedulingRequest(5, 1, 1, 1, 512, pcExpression, "foo"));
      AllocateRequest allocateReq =
          AllocateRequest.newBuilder().schedulingRequests(schedulingRequests)
              .build();
      am1.allocate(allocateReq);
      // trigger scheduling
      cs.handle(new NodeUpdateSchedulerEvent(
          rm.getRMContext().getRMNodes().get(nm1.getNodeId())));

      response =
          r.path("ws").path("v1").path("cluster")
              .path("scheduler/app-activities").queryParams(params)
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      json = response.getEntity(JSONObject.class);

      verifyNumberOfAllocations(json, 1);
      JSONObject allocationObj = json.getJSONObject("allocations");
      JSONObject requestAllocationObj =
          allocationObj.getJSONObject("requestAllocation");
      verifyNumberOfAllocationAttempts(requestAllocationObj, 1);
      JSONObject allocationAttemptObj =
          requestAllocationObj.getJSONObject("allocationAttempt");
      verifyStateOfAllocations(allocationAttemptObj, "allocationState",
          "SKIPPED");
      assertTrue(allocationAttemptObj.optString("diagnostic")
          .contains(UNMATCHED_PARTITION_OR_PC_DIAGNOSTIC_PREFIX));
    } finally {
      rm.stop();
    }
  }
}
