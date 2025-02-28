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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestRequestsHandler {

  private Configuration conf;
  private RMContext rmContext;
  private RequestsHandler requestsHandler;

  @Before
  public void setUp() {
    rmContext = mock(RMContext.class);
    CapacityScheduler scheduler = mock(CapacityScheduler.class);
    when(rmContext.getScheduler()).thenReturn(scheduler);
    ConcurrentHashMap<ApplicationId, RMApp> rmApps = new ConcurrentHashMap<>();
    when(rmContext.getRMApps()).thenReturn(rmApps);

    Function<ApplicationAttemptId, Pair<FiCaSchedulerApp, RMApp>> appProvider =
        appAttemptId -> {
          FiCaSchedulerApp app = scheduler.getApplicationAttempt(appAttemptId);
          RMApp rmApp =
              rmContext.getRMApps().get(appAttemptId.getApplicationId());
          if (app == null || rmApp == null) {
            return null;
          }
          return ImmutablePair.of(app, rmApp);
        };
    requestsHandler = new RequestsHandler(appProvider);
    conf = new Configuration();
    LogManager.getLogger(RequestsHandler.LOG.getName()).setLevel(Level.DEBUG);
  }

  @Test
  public void testInitialize() throws IOException, YarnException {
    // invalid conf
    conf.setBoolean(CapacitySchedulerConfiguration.REQUEST_HANDLER_ENABLED,
        true);
    conf.set(CapacitySchedulerConfiguration.REQUEST_HANDLER_UPDATES, "{");
    assertThrows(IOException.class, () -> requestsHandler.initialize(conf));

    // invalid allocation tags
    conf.setBoolean(CapacitySchedulerConfiguration.REQUEST_HANDLER_ENABLED,
        true);
    conf.set(CapacitySchedulerConfiguration.REQUEST_HANDLER_UPDATES,
        "{\"items\":[{\"allocationTags\":\"xxx\"}]}");
    assertThrows(IOException.class, () -> requestsHandler.initialize(conf));

    // invalid placement constraint
    conf.setBoolean(CapacitySchedulerConfiguration.REQUEST_HANDLER_ENABLED,
        true);
    conf.set(CapacitySchedulerConfiguration.REQUEST_HANDLER_UPDATES,
        "{\"items\":[{\"placementConstraint\":\"and\"}]}");
    assertThrows(YarnException.class, () -> requestsHandler.initialize(conf));
    try {
      requestsHandler.initialize(conf);
    } catch (YarnException e) {
      assertTrue(
          e.getMessage().contains("Failed to parse placement-constraint"));
    }

    // invalid placeholder for allocation tags
    conf.setBoolean(CapacitySchedulerConfiguration.REQUEST_HANDLER_ENABLED,
        true);
    conf.set(CapacitySchedulerConfiguration.REQUEST_HANDLER_UPDATES,
        "{\"items\":[{\"allocationTags\":[\"xxx\",\"${invalid}\"]}]}");
    assertThrows(YarnException.class, () -> requestsHandler.initialize(conf));
    try {
      requestsHandler.initialize(conf);
    } catch (YarnException e) {
      assertTrue(e.getMessage().contains("Invalid placeholder"));
    }

    // invalid placeholder for placement constraints
    conf.setBoolean(CapacitySchedulerConfiguration.REQUEST_HANDLER_ENABLED,
        true);
    conf.set(CapacitySchedulerConfiguration.REQUEST_HANDLER_UPDATES,
        "{\"items\":[{\"placementConstraint\":" +
            "\"and(in,rack,${name}:notin,node,${invalid})\"}]}");
    assertThrows(YarnException.class, () -> requestsHandler.initialize(conf));
    try {
      requestsHandler.initialize(conf);
    } catch (YarnException e) {
      assertTrue(e.getMessage().contains("Invalid placeholder"));
    }

    // disabled
    conf.setBoolean(CapacitySchedulerConfiguration.REQUEST_HANDLER_ENABLED,
        false);
    conf.set(CapacitySchedulerConfiguration.REQUEST_HANDLER_UPDATES,
        "{\"items\":[]}");
    requestsHandler.initialize(conf);
    assertFalse(requestsHandler.isEnabled());
    assertNull(requestsHandler.getUpdateItems());

    // enabled without items
    conf.setBoolean(CapacitySchedulerConfiguration.REQUEST_HANDLER_ENABLED,
        true);
    conf.unset(CapacitySchedulerConfiguration.REQUEST_HANDLER_UPDATES);
    requestsHandler.initialize(conf);
    assertTrue(requestsHandler.isEnabled());
    assertNull(requestsHandler.getUpdateItems());

    // enabled with 1 item
    conf.setBoolean(CapacitySchedulerConfiguration.REQUEST_HANDLER_ENABLED,
        true);
    conf.set(CapacitySchedulerConfiguration.REQUEST_HANDLER_UPDATES,
        "{\"items\":[{\"appMatchExpr\":\"queue=='test'\", " +
            "\"requestMatchExpr\":\"priority>10\", \"isRRToSR\":true," +
            " \"placementConstraint\":\"and(in,rack,tag_${id}:notin,node,zk)\"," +
            " \"executionType\":\"OPPORTUNISTIC\"," +
            " \"allocationTags\":[\"tag1\", \"tag_${id}\"]}]}");
    requestsHandler.initialize(conf);
    assertTrue(requestsHandler.isEnabled());
    assertNotNull(requestsHandler.getUpdateItems());
    List<RequestsHandler.UpdateItem> items =
        requestsHandler.getUpdateItems();
    assertEquals(1, items.size());

    RequestsHandler.UpdateItem item = items.get(0);
    assertEquals("and(in,rack,tag_${id}:notin,node,zk)",
        item.getPlacementConstraint().toString());
    assertTrue(item.hasPlaceholderForPC());
    assertTrue(item.hasPlaceholderForAllocTags());
    assertEquals(ExecutionType.OPPORTUNISTIC, item.getExecutionType());
    assertNotNull(item.getAppMatchScript());
    assertNotNull(item.getRequestMatchScript());

    RequestsHandler.UpdateItemConf itemConf = items.get(0).getUpdateItemConf();
    assertEquals("queue=='test'", itemConf.getAppMatchExpr());
    assertEquals("priority>10", itemConf.getRequestMatchExpr());
    assertEquals("and(in,rack,tag_${id}:notin,node,zk)",
        itemConf.getPlacementConstraint());
    assertEquals("OPPORTUNISTIC", itemConf.getExecutionType());
    assertEquals(2, itemConf.getAllocationTags().size());
    assertTrue(itemConf.getAllocationTags().contains("tag1"));
    assertTrue(itemConf.getAllocationTags().contains("tag_${id}"));

    // turned to disabled
    conf.setBoolean(CapacitySchedulerConfiguration.REQUEST_HANDLER_ENABLED,
        false);
    requestsHandler.initialize(conf);
    assertFalse(requestsHandler.isEnabled());
    assertNull(requestsHandler.getUpdateItems());

    // turned to enabled
    conf.setBoolean(CapacitySchedulerConfiguration.REQUEST_HANDLER_ENABLED,
        true);
    requestsHandler.initialize(conf);
    assertTrue(requestsHandler.isEnabled());
    assertNotNull(requestsHandler.getUpdateItems());
    assertEquals(1, requestsHandler.getUpdateItems().size());
  }

  private FiCaSchedulerApp mockApp(int id, int priority,
      String queueName, String user, String appName, String appType,
      List<String> tags) {
    ApplicationId appId = ApplicationId.newInstance(1, id);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 1);
    FiCaSchedulerApp app = mock(FiCaSchedulerApp.class);
    when(app.getApplicationAttemptId()).thenReturn(appAttemptId);
    when(app.getApplicationId()).thenReturn(appId);
    when(app.isWaitingForAMContainer()).thenReturn(true);
    when(app.getQueueName()).thenReturn(queueName);
    when(app.getUser()).thenReturn(user);
    when(app.getPriority()).thenReturn(Priority.newInstance(priority));
    CapacityScheduler scheduler = (CapacityScheduler) rmContext.getScheduler();
    when(scheduler.getApplicationAttempt(
        app.getApplicationAttemptId())).thenReturn(app);

    RMApp rmApp = mock(RMApp.class);
    when(rmApp.getApplicationId()).thenReturn(appId);
    when(rmApp.getName()).thenReturn(appName);
    when(rmApp.getApplicationType()).thenReturn(appType);
    when(rmApp.getApplicationTags())
        .thenReturn(ImmutableSet.copyOf(tags));

    ConcurrentMap<ApplicationId, RMApp> rmApps =
        rmContext.getRMApps();
    rmApps.put(appId, rmApp);
    return app;
  }

  @Test
  public void testHandleRequests() throws IOException, YarnException {
    // mock apps and request
    FiCaSchedulerApp app1 = mockApp(1, 0, "test1", "user1", "app1",
        "MapReduce", Lists.newArrayList("tag1", "tag2"));
    FiCaSchedulerApp app2 = mockApp(2, 1, "test", "user2", "app2",
        "MapReduce", Lists.newArrayList("tag1", "tag2"));

    ResourceRequest rr1 = ResourceRequest.newInstance(Priority.newInstance(3),
        "*", Resource.newInstance(4096, 2), 1, true);
    rr1.setExecutionTypeRequest(ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC));
    rr1.setNodeLabelExpression("x");

    /*
     * check choosing by app, converting RR to SR
     */
    conf.setBoolean(CapacitySchedulerConfiguration.REQUEST_HANDLER_ENABLED,
        true);
    conf.set(CapacitySchedulerConfiguration.REQUEST_HANDLER_UPDATES,
        "{\"items\":[{\"appMatchExpr\":\"queue=='test'\", \"isRRToSR\":true}]}");
    requestsHandler.initialize(conf);

    // app1 not-matched, won't convert to scheduling request
    RequestsHandleResponse response =
        requestsHandler.handle(app1.getApplicationAttemptId(),
            Lists.newArrayList(rr1), null);
    assertFalse(response.isUpdated());
    assertEquals(1, response.getResourceRequests().size());
    assertNull(response.getSchedulingRequests());

    // app2 matched, will be converted to scheduling request
    response = requestsHandler.handle(app2.getApplicationAttemptId(),
        Lists.newArrayList(rr1), null);
    assertTrue(response.isUpdated());
    assertEquals(1, response.getSchedulingRequests().size());
    assertEquals(0, response.getResourceRequests().size());
    SchedulingRequest gotSR1 = response.getSchedulingRequests().get(0);
    assertEquals(rr1.getPriority(), gotSR1.getPriority());
    assertEquals(rr1.getCapability(), gotSR1.getResourceSizing().getResources());
    assertEquals(rr1.getNumContainers(), gotSR1.getResourceSizing().getNumAllocations());
    assertEquals(rr1.getExecutionTypeRequest(), gotSR1.getExecutionType());
    assertEquals("node,EQ,yarn_node_partition/=[x]", gotSR1.getPlacementConstraint().toString());

    /*
     * check choosing by app and request, converting RR to SR,
     * then updating priority, execution-type, and allocation-tags
     */
    conf.set(CapacitySchedulerConfiguration.REQUEST_HANDLER_UPDATES,
        "{\"items\":[{\"appMatchExpr\":\"queue=='test'\", " +
            "\"requestMatchExpr\":\"priority>10\", \"isRRToSR\":true," +
            " \"placementConstraint\":\"and(in,rack,${id}:notin,node,zk)\"," +
            " \"executionType\":\"OPPORTUNISTIC\"," +
            " \"allocationTags\":[\"tag1\", \"${id}\"]}]}");
    requestsHandler.initialize(conf);

    ResourceRequest rr2 =
        ResourceRequest.newInstance(Priority.newInstance(20), "*",
            Resource.newInstance(1024, 1), 5, true);
    response = requestsHandler.handle(app2.getApplicationAttemptId(),
        Lists.newArrayList(rr1, rr2), null);
    assertTrue(response.isUpdated());
    assertEquals(2, response.getSchedulingRequests().size());

    // both rr1 and rr2 should be converted to scheduling requests
    // rr1 not matched
    gotSR1 = response.getSchedulingRequests().get(0);
    assertEquals(rr1.getPriority(), gotSR1.getPriority());
    assertEquals(rr1.getCapability(),
        gotSR1.getResourceSizing().getResources());
    assertEquals(rr1.getNumContainers(),
        gotSR1.getResourceSizing().getNumAllocations());
    assertEquals(rr1.getExecutionTypeRequest(), gotSR1.getExecutionType());
    assertEquals("node,EQ,yarn_node_partition/=[x]",
        gotSR1.getPlacementConstraint().toString());

    // rr2 matched, should be updated
    SchedulingRequest gotSR2 = response.getSchedulingRequests().get(1);
    assertEquals(rr2.getCapability(),
        gotSR2.getResourceSizing().getResources());
    assertEquals(rr2.getNumContainers(),
        gotSR2.getResourceSizing().getNumAllocations());
    assertEquals(ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC),
        gotSR2.getExecutionType());
    assertEquals(Sets.newHashSet("tag1", app2.getApplicationId().toString()),
        gotSR2.getAllocationTags());
    assertEquals(String.format("and(in,rack,%s:notin,node,zk)",
            app2.getApplicationId().toString()),
        gotSR2.getPlacementConstraint().toString());
  }
}
