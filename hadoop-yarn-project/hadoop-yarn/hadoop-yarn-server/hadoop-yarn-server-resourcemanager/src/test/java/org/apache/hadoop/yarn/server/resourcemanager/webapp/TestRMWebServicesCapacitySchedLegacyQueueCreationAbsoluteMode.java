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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.createConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertJsonResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.backupSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createMutableRM;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.restoreSchedulerConfigFileInTarget;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRMWebServicesCapacitySchedLegacyQueueCreationAbsoluteMode extends
    JerseyTestBase {

  private boolean legacyQueueMode;

  private MockRM rm;

  public static Collection<Boolean> getParameters() {
    return Arrays.asList(true, false);
  }

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(RMWebServices.class);
    config.register(new JerseyBinder());
    config.register(GenericExceptionHandler.class);
    config.register(TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    forceSet(TestProperties.CONTAINER_PORT, JERSEY_RANDOM_PORT);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      Map<String, String> config = new HashMap<>();
      config.put("yarn.scheduler.capacity.legacy-queue-mode.enabled",
          String.valueOf(legacyQueueMode));
      config.put("yarn.scheduler.capacity.root.queues", "default, managed");
      config.put("yarn.scheduler.capacity.root.default.capacity", "[memory=28672,vcores=28]");
      config.put("yarn.scheduler.capacity.root.managed.capacity", "[memory=4096,vcores=4]");
      config.put("yarn.scheduler.capacity.root.managed.leaf-queue-template.capacity",
          "[memory=2048,vcores=2]");
      config.put("yarn.scheduler.capacity.root.managed." +
          "auto-create-child-queue.enabled", "true");
      config.put("yarn.scheduler.capacity.root.managed.leaf-queue-template.acl_submit_applications",
          "user");
      config.put("yarn.scheduler.capacity.root.managed.leaf-queue-template.acl_administer_queue",
          "admin");

      Configuration conf = createConfiguration(config);
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);

      rm = createMutableRM(conf, false);
      final HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getScheme()).thenReturn("http");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public void initTestRMWebServicesCapacitySchedLegacyQueueCreationAbsoluteMode(
      boolean pLegacyQueueMode) throws Exception {
    this.legacyQueueMode = pLegacyQueueMode;
    backupSchedulerConfigFileInTarget();
    setUp();
  }

  @AfterAll
  public static void afterClass() {
    restoreSchedulerConfigFileInTarget();
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{index}: legacy-queue-mode={0}")
  public void testSchedulerResponseAbsoluteModeLegacyAutoCreation(boolean pLegacyQueueMode)
      throws Exception {
    initTestRMWebServicesCapacitySchedLegacyQueueCreationAbsoluteMode(pLegacyQueueMode);
    rm.registerNode("h1:1234", 32 * GB, 32);
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CapacitySchedulerQueueManager autoQueueHandler = cs.getCapacitySchedulerQueueManager();
    autoQueueHandler.createQueue(new QueuePath("root.managed.queue1"));
    assertJsonResponse(sendRequest(),
        "webapp/scheduler-response-AbsoluteModeLegacyAutoCreation.json");
  }

  private Response sendRequest() {
    return target().path("ws").path("v1").path("cluster")
        .path("scheduler").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
  }
}