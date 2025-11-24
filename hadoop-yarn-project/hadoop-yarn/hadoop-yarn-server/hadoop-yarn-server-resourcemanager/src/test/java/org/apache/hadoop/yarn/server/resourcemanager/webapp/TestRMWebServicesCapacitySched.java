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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.reader.ApplicationSubmissionContextInfoReader;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.writer.ApplicationSubmissionContextInfoWriter;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertJsonResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertJsonType;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertXmlResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.backupSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createRM;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.restoreSchedulerConfigFileInTarget;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRMWebServicesCapacitySched extends JerseyTestBase {

  private boolean legacyQueueMode;
  private MockRM rm;
  private ResourceConfig config;

  public static Collection<Boolean> getParameters() {
    return Arrays.asList(true, false);
  }

  @Override
  protected Application configure() {
    config = new ResourceConfig();
    config.register(RMWebServices.class);
    config.register(new JerseyBinder());
    config.register(GenericExceptionHandler.class);
    config.register(ApplicationSubmissionContextInfoWriter.class);
    config.register(ApplicationSubmissionContextInfoReader.class);
    config.register(TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      Configuration conf = createConfig();
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);
      conf.set(YarnConfiguration.RM_CLUSTER_ID, "subCluster1");

      rm = createRM(conf);
      final HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getScheme()).thenReturn("http");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
    }
  }

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public void initTestRMWebServicesCapacitySched(boolean pLegacyQueueMode) {
    this.legacyQueueMode = pLegacyQueueMode;
    backupSchedulerConfigFileInTarget();
  }

  @AfterAll
  public static void afterClass() {
    restoreSchedulerConfigFileInTarget();
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{index}: legacy-queue-mode={0}")
  public void testClusterScheduler(boolean pLegacyQueueMode) throws Exception {
    initTestRMWebServicesCapacitySched(pLegacyQueueMode);
    rm.registerNode("h1:1234", 32 * GB, 32);
    assertJsonResponse(target().path("ws/v1/cluster/scheduler")
        .request(MediaType.APPLICATION_JSON).get(Response.class),
        "webapp/scheduler-response.json");
    assertJsonResponse(target().path("ws/v1/cluster/scheduler/")
        .request(MediaType.APPLICATION_JSON).get(Response.class),
        "webapp/scheduler-response.json");
    assertJsonResponse(target().path("ws/v1/cluster/scheduler")
        .request().get(Response.class),
        "webapp/scheduler-response.json");
    assertXmlResponse(target().path("ws/v1/cluster/scheduler/")
        .request(MediaType.APPLICATION_XML).get(Response.class),
        "webapp/scheduler-response.xml");
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{index}: legacy-queue-mode={0}")
  public void testPerUserResources(boolean pLegacyQueueMode) throws Exception {
    initTestRMWebServicesCapacitySched(pLegacyQueueMode);
    rm.registerNode("h1:1234", 32 * GB, 32);

    MockRMAppSubmitter.submit(rm, MockRMAppSubmissionData.Builder
        .createWithMemory(32, rm)
        .withAppName("app1")
        .withUser("user1")
        .withAcls(null)
        .withQueue("a")
        .withUnmanagedAM(false)
        .build()
    );

    MockRMAppSubmitter.submit(rm, MockRMAppSubmissionData.Builder
        .createWithMemory(64, rm)
        .withAppName("app2")
        .withUser("user2")
        .withAcls(null)
        .withQueue("b")
        .withUnmanagedAM(false)
        .build()
    );

    assertXmlResponse(target().path("ws/v1/cluster/scheduler")
        .request(MediaType.APPLICATION_XML).get(Response.class),
        "webapp/scheduler-response-PerUserResources.xml");
    assertJsonResponse(target().path("ws/v1/cluster/scheduler")
        .request(MediaType.APPLICATION_JSON).get(Response.class),
        "webapp/scheduler-response-PerUserResources.json");
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{index}: legacy-queue-mode={0}")
  public void testClusterSchedulerOverviewCapacity(boolean pLegacyQueueMode) throws Exception {
    initTestRMWebServicesCapacitySched(pLegacyQueueMode);
    rm.registerNode("h1:1234", 32 * GB, 32);
    Response response = targetWithJsonObject().path("ws/v1/cluster/scheduler-overview")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertJsonType(response);
    JSONObject json = response.readEntity(JSONObject.class);
    JSONObject scheduler = json.getJSONObject("scheduler");
    TestRMWebServices.verifyClusterSchedulerOverView(scheduler, "Capacity Scheduler");
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{index}: legacy-queue-mode={0}")
  public void testResourceInfo(boolean pLegacyQueueMode) {
    initTestRMWebServicesCapacitySched(pLegacyQueueMode);
    Resource res = Resources.createResource(10, 1);
    // If we add a new resource (e.g. disks), then
    // CapacitySchedulerPage and these RM WebServices + docs need to be updated
    // e.g. ResourceInfo
    assertEquals("<memory:10, vCores:1>", res.toString());
  }

  private Configuration createConfig() {
    Configuration conf = new Configuration();
    conf.set("yarn.scheduler.capacity.legacy-queue-mode.enabled", String.valueOf(legacyQueueMode));
    conf.set("yarn.scheduler.capacity.root.queues", "a, b, c");
    conf.set("yarn.scheduler.capacity.root.a.capacity", "12.5");
    conf.set("yarn.scheduler.capacity.root.a.accessible-node-labels", "root-a-default-label");
    conf.set("yarn.scheduler.capacity.root.a.maximum-capacity", "50");
    conf.set("yarn.scheduler.capacity.root.a.max-parallel-app", "42");
    conf.set("yarn.scheduler.capacity.root.b.capacity", "50");
    conf.set("yarn.scheduler.capacity.root.c.capacity", "37.5");
    conf.set("yarn.scheduler.capacity.schedule-asynchronously.enable", "false");
    return conf;
  }
}
