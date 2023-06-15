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

import javax.ws.rs.core.MediaType;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.*;
import static org.junit.Assert.assertEquals;

public class TestRMWebServicesCapacitySched extends JerseyTestBase {

  private MockRM rm;

  public static class WebServletModule extends ServletModule {
    private final MockRM rm;

    WebServletModule(MockRM rm) {
      this.rm = rm;
    }

    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
    }
  }

  public TestRMWebServicesCapacitySched() {
    super(createWebAppDescriptor());
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    rm = createMockRM(setupQueueConfiguration(new CapacitySchedulerConfiguration(
        new Configuration(false))));
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule(rm)));
  }

  public static CapacitySchedulerConfiguration setupQueueConfiguration(
      CapacitySchedulerConfiguration config) {
    config.set("yarn.scheduler.capacity.root.queues", "a, b, c");
    config.set("yarn.scheduler.capacity.root.a.queues", "a1, a2");
    config.set("yarn.scheduler.capacity.root.b.queues", "b1, b2, b3");
    config.set("yarn.scheduler.capacity.root.a.a1.queues", "a1a, a1b, a1c");
    config.set("yarn.scheduler.capacity.root.a.capacity", "10.5");
    config.set("yarn.scheduler.capacity.root.a.maximum-capacity", "50");
    config.set("yarn.scheduler.capacity.root.a.max-parallel-app", "42");
    config.set("yarn.scheduler.capacity.root.b.capacity", "79.5");
    config.set("yarn.scheduler.capacity.root.c.capacity", "10");
    config.set("yarn.scheduler.capacity.root.a.a1.capacity", "30");
    config.set("yarn.scheduler.capacity.root.a.a1.maximum-capacity", "50");
    config.set("yarn.scheduler.capacity.root.a.a1.user-limit-factor", "100");
    config.set("yarn.scheduler.capacity.root.a.a2.capacity", "70");
    config.set("yarn.scheduler.capacity.root.a.a2.maximum-application-lifetime", "100");
    config.set("yarn.scheduler.capacity.root.a.a2.default-application-lifetime", "50");
    config.set("yarn.scheduler.capacity.root.a.a2.user-limit-factor", "100");
    config.set("yarn.scheduler.capacity.root.b.b1.capacity", "60");
    config.set("yarn.scheduler.capacity.root.b.b2.capacity", "39.5");
    config.set("yarn.scheduler.capacity.root.b.b3.capacity", "0.5");
    config.set("yarn.scheduler.capacity.root.b.b1.user-limit-factor", "100");
    config.set("yarn.scheduler.capacity.root.b.b2.user-limit-factor", "100");
    config.set("yarn.scheduler.capacity.root.b.b3.user-limit-factor", "100");
    config.set("yarn.scheduler.capacity.root.a.a1.a1a.capacity", "65");
    config.set("yarn.scheduler.capacity.root.a.a1.a1b.capacity", "15");
    config.set("yarn.scheduler.capacity.root.a.a1.a1c.capacity", "20");
    config.set("yarn.scheduler.capacity.root.a.a1.a1c.auto-create-child-queue.enabled", "true");
    config.set("yarn.scheduler.capacity.root.a.a1.a1c.leaf-queue-template.capacity", "50");
    return config;
  }

  @Test
  public void testClusterScheduler() throws Exception {
    ClientResponse response = resource().path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertJsonResponse(response, "webapp/scheduler-response.json");
  }

  @Test
  public void testClusterSchedulerSlash() throws Exception {
    ClientResponse response = resource().path("ws").path("v1").path("cluster")
        .path("scheduler/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertJsonResponse(response, "webapp/scheduler-response.json");
  }

  @Test
  public void testClusterSchedulerDefault() throws Exception {
    ClientResponse response = resource().path("ws").path("v1").path("cluster")
        .path("scheduler").get(ClientResponse.class);
    assertJsonResponse(response, "webapp/scheduler-response.json");
  }

  @Test
  public void testClusterSchedulerXML() throws Exception {
    ClientResponse response = resource().path("ws").path("v1").path("cluster")
        .path("scheduler/").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertXmlResponse(response, "webapp/scheduler-response.xml");
  }

  @Test
  public void testPerUserResourcesXML() throws Exception {
    // Start RM so that it accepts app submissions
    rm.start();
    try {
      MockRMAppSubmissionData data1 =
          MockRMAppSubmissionData.Builder.createWithMemory(10, rm)
              .withAppName("app1")
              .withUser("user1")
              .withAcls(null)
              .withQueue("b1")
              .withUnmanagedAM(false)
              .build();
      MockRMAppSubmitter.submit(rm, data1);
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(20, rm)
              .withAppName("app2")
              .withUser("user2")
              .withAcls(null)
              .withQueue("b1")
              .withUnmanagedAM(false)
              .build();
      MockRMAppSubmitter.submit(rm, data);

      //Get the XML from ws/v1/cluster/scheduler
      ClientResponse response = resource().path("ws/v1/cluster/scheduler")
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertXmlResponse(response, "webapp/scheduler-response-PerUserResources.xml");
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testNodeLabelDefaultAPI() throws Exception {
    CapacitySchedulerConfiguration config =
        ((CapacityScheduler)rm.getResourceScheduler()).getConfiguration();

    config.setDefaultNodeLabelExpression("root", "ROOT-INHERITED");
    config.setDefaultNodeLabelExpression("root.a", "root-a-default-label");
    rm.getResourceScheduler().reinitialize(config, rm.getRMContext());

    //Start RM so that it accepts app submissions
    rm.start();
    try {
      //Get the XML from ws/v1/cluster/scheduler
      ClientResponse response = resource().path("ws/v1/cluster/scheduler")
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertXmlResponse(response, "webapp/scheduler-response-NodeLabelDefaultAPI.xml");
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testPerUserResourcesJSON() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();
    try {
      MockRMAppSubmissionData data1 =
          MockRMAppSubmissionData.Builder.createWithMemory(10, rm)
              .withAppName("app1")
              .withUser("user1")
              .withAcls(null)
              .withQueue("b1")
              .withUnmanagedAM(false)
              .build();
      MockRMAppSubmitter.submit(rm, data1);
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(20, rm)
              .withAppName("app2")
              .withUser("user2")
              .withAcls(null)
              .withQueue("b1")
              .withUnmanagedAM(false)
              .build();
      MockRMAppSubmitter.submit(rm, data);

      //Get JSON
      ClientResponse response = resource().path("ws").path("v1").path("cluster")
          .path("scheduler/").accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertJsonResponse(response, "webapp/scheduler-response-PerUserResources.json");
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testResourceInfo() {
    Resource res = Resources.createResource(10, 1);
    // If we add a new resource (e.g. disks), then
    // CapacitySchedulerPage and these RM WebServices + docs need to be updated
    // e.g. ResourceInfo
    assertEquals("<memory:10, vCores:1>", res.toString());
  }


  public static MockRM createMockRM(CapacitySchedulerConfiguration csConf) {
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    return new MockRM(conf);
  }

  @Test
  public void testClusterSchedulerOverviewCapacity() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-overview").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    TestRMWebServices.verifyClusterSchedulerOverView(json, "Capacity Scheduler");
  }
}
