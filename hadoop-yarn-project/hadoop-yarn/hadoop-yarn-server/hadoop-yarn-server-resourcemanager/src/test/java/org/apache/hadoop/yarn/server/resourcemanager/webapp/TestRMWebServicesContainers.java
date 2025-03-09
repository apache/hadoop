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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Principal;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.reader.ApplicationSubmissionContextInfoReader;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.writer.ApplicationSubmissionContextInfoWriter;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;

/**
 * Testing containers REST API.
 */
public class TestRMWebServicesContainers extends JerseyTestBase {

  private static MockRM rm;
  private static String userName;

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(RMWebServices.class);
    config.register(new JerseyBinder());
    config.register(GenericExceptionHandler.class);
    config.register(ApplicationSubmissionContextInfoWriter.class);
    config.register(ApplicationSubmissionContextInfoReader.class);
    config.register(TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    forceSet(TestProperties.CONTAINER_PORT, JERSEY_RANDOM_PORT);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {

    @Override
    protected void configure() {
      try {
        userName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException ioe) {
        throw new RuntimeException("Unable to get current user name "
            + ioe.getMessage(), ioe);
      }
      Configuration conf = new Configuration();
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);
      conf.set(YarnConfiguration.YARN_ADMIN_ACL, "admin");
      rm = new MockRM(conf);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      HttpServletRequest request = mock(HttpServletRequest.class);
      Principal principal = () -> userName;
      when(request.getUserPrincipal()).thenReturn(principal);
      HttpServletResponse response = mock(HttpServletResponse.class);
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
    }
  }

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestRMWebServicesContainers() {
  }

  @Test
  public void testSignalContainer() throws Exception {
    rm.start();
    MockNM nm = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm).build());
    nm.nodeHeartbeat(true);
    MockRM
        .waitForState(app.getCurrentAppAttempt(), RMAppAttemptState.ALLOCATED);
    rm.sendAMLaunched(app.getCurrentAppAttempt().getAppAttemptId());
    WebTarget r = target();

    // test error command
    Response response =
        r.path("ws").path("v1").path("cluster").path("containers").path(
            app.getCurrentAppAttempt().getMasterContainer().getId().toString())
            .path("signal")
            .path("not-exist-signal")
            .queryParam("user.name", userName)
            .request(MediaType.APPLICATION_JSON).post(null, Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertTrue(response.readEntity(String.class).contains("Invalid command: NOT-EXIST-SIGNAL"));

    // test error containerId
    response =
        r.path("ws").path("v1").path("cluster").path("containers").path("XXX")
            .path("signal")
            .path(SignalContainerCommand.OUTPUT_THREAD_DUMP.name())
            .queryParam("user.name", userName)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(null, Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    assertTrue(
        response.readEntity(String.class).contains("Invalid ContainerId"));

    // test correct signal by owner
    response =
        r.path("ws").path("v1").path("cluster").path("containers").path(
            app.getCurrentAppAttempt().getMasterContainer().getId().toString())
            .path("signal")
            .path(SignalContainerCommand.OUTPUT_THREAD_DUMP.name())
            .queryParam("user.name", userName)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(null, Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    // test correct signal by admin
    response =
        r.path("ws").path("v1").path("cluster").path("containers").path(
            app.getCurrentAppAttempt().getMasterContainer().getId().toString())
            .path("signal")
            .path(SignalContainerCommand.OUTPUT_THREAD_DUMP.name())
            .queryParam("user.name", "admin")
            .request(MediaType.APPLICATION_JSON).post(null, Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    rm.stop();
  }
}
