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
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.FileOutputStream;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.reader.ApplicationSubmissionContextInfoReader;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.writer.ApplicationSubmissionContextInfoWriter;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.writer.SchedConfUpdateInfoWriter;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertJsonResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createMutableRM;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.backupSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.getCapacitySchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.getExpectedResourceFile;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.restoreSchedulerConfigFileInTarget;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRMWebServicesCapacitySchedulerConfigMutation extends JerseyTestBase {
  private static final String EXPECTED_FILE_TMPL = "webapp/configmutation-%s-%s.json";
  private boolean legacyQueueMode;
  private String userName;
  private Configuration absoluteConfig;
  private MockRM rm;
  private HttpServletRequest request;

  public static Collection<Boolean> getParameters() {
    return Arrays.asList(true, false);
  }

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(RMWebServices.class);
    config.register(new JerseyBinder());
    config.register(GenericExceptionHandler.class);
    config.register(ApplicationSubmissionContextInfoWriter.class);
    config.register(SchedConfUpdateInfoWriter.class);
    config.register(ApplicationSubmissionContextInfoReader.class);
    config.register(TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      try {
        absoluteConfig = createAbsoluteConfig();
        FileOutputStream out = new FileOutputStream(getCapacitySchedulerConfigFileInTarget());
        absoluteConfig.writeXml(out);
        out.close();
      } catch (Exception e) {
        System.out.println(e);
      }

      rm = createMutableRM(absoluteConfig, true);
      request = mock(HttpServletRequest.class);
      when(request.getScheme()).thenReturn("http");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(absoluteConfig).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
    }
  }

  @BeforeAll
  public static void beforeClass() {
    backupSchedulerConfigFileInTarget();
  }

  @AfterAll
  public static void afterClass() {
    restoreSchedulerConfigFileInTarget();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public void initTestRMWebServicesCapacitySchedulerConfigMutation(boolean pLegacyQueueMode)
      throws Exception {
    this.legacyQueueMode = pLegacyQueueMode;
    this.userName = UserGroupInformation.getCurrentUser().getShortUserName();
    setUp();
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{index}: legacy-queue-mode={0}")
  public void testUpdateAbsoluteHierarchyWithZeroCapacities(boolean pLegacyQueueMode)
      throws Exception {
    initTestRMWebServicesCapacitySchedulerConfigMutation(pLegacyQueueMode);
    Principal principal = () -> userName;
    when(request.getUserPrincipal()).thenReturn(principal);

    rm.registerNode("h1:1234", 32 * GB, 32);

    assertJsonResponse(target().path("ws/v1/cluster/scheduler")
        .queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON).get(Response.class),
        getExpectedResourceFile(EXPECTED_FILE_TMPL, "absolute-hierarchy", "before-update",
        legacyQueueMode));

    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> capacityChange = new HashMap<>();
    capacityChange.put(CapacitySchedulerConfiguration.CAPACITY,
        "[memory=4096, vcores=4]");
    capacityChange.put(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY,
        "[memory=32768, vcores=32]");
    QueueConfigInfo b = new QueueConfigInfo("root.a", capacityChange);
    updateInfo.getUpdateQueueInfo().add(b);

    Response response = target().register(SchedConfUpdateInfoWriter.class)
        .path("ws/v1/cluster/scheduler-conf")
        .queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.entity(updateInfo,
        MediaType.APPLICATION_JSON), Response.class);

     // HTTP 400 - Bad Request is encountered, check the logs for the failure
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    assertJsonResponse(target().path("ws/v1/cluster/scheduler")
        .queryParam("user.name", userName)
        .request(MediaType.APPLICATION_JSON).get(Response.class),
        getExpectedResourceFile(EXPECTED_FILE_TMPL, "absolute-hierarchy", "after-update",
        legacyQueueMode));
  }

  private Configuration createAbsoluteConfig() {
    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, userName);
    conf.set("yarn.scheduler.capacity.legacy-queue-mode.enabled", String.valueOf(legacyQueueMode));
    conf.set("yarn.scheduler.capacity.root.capacity", "[memory=32768, vcores=32]");
    conf.set("yarn.scheduler.capacity.root.queues", "default, a");
    conf.set("yarn.scheduler.capacity.root.default.capacity", "[memory=1024, vcores=1]");
    conf.set("yarn.scheduler.capacity.root.a.capacity", "[memory=0, vcores=0]");
    conf.set("yarn.scheduler.capacity.root.a.max-capacity", "[memory=32768, vcores=32]");
    conf.set("yarn.scheduler.capacity.root.a.queues", "b, c");
    conf.set("yarn.scheduler.capacity.root.a.b.capacity", "[memory=0, vcores=0]");
    conf.set("yarn.scheduler.capacity.root.a.b.max-capacity", "[memory=32768, vcores=32]");
    conf.set("yarn.scheduler.capacity.root.a.c.capacity", "[memory=0, vcores=0]");
    conf.set("yarn.scheduler.capacity.root.a.c.max-capacity", "[memory=32768, vcores=32]");
    return conf;
  }
}
