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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import javax.ws.rs.core.MediaType;

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
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.eclipse.jetty.server.Response;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

/**
 * Testing containers REST API.
 */
public class TestRMWebServicesContainers extends JerseyTestBase {

  private static MockRM rm;
  private static String userName;

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
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
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
      filter("/*").through(TestRMWebServicesAppsModification
          .TestRMCustomAuthFilter.class);
    }
  }

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  public TestRMWebServicesContainers() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
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
    WebResource r = resource();

    // test error command
    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("containers").path(
            app.getCurrentAppAttempt().getMasterContainer().getId().toString())
            .path("signal")
            .path("not-exist-signal")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).post(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    assertEquals(Response.SC_BAD_REQUEST, response.getStatus());
    assertTrue(response.getEntity(String.class)
        .contains("Invalid command: NOT-EXIST-SIGNAL"));

    // test error containerId
    response =
        r.path("ws").path("v1").path("cluster").path("containers").path("XXX")
            .path("signal")
            .path(SignalContainerCommand.OUTPUT_THREAD_DUMP.name())
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).post(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    assertEquals(Response.SC_INTERNAL_SERVER_ERROR, response.getStatus());
    assertTrue(
        response.getEntity(String.class).contains("Invalid ContainerId"));

    // test correct signal by owner
    response =
        r.path("ws").path("v1").path("cluster").path("containers").path(
            app.getCurrentAppAttempt().getMasterContainer().getId().toString())
            .path("signal")
            .path(SignalContainerCommand.OUTPUT_THREAD_DUMP.name())
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).post(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    assertEquals(Response.SC_OK, response.getStatus());

    // test correct signal by admin
    response =
        r.path("ws").path("v1").path("cluster").path("containers").path(
            app.getCurrentAppAttempt().getMasterContainer().getId().toString())
            .path("signal")
            .path(SignalContainerCommand.OUTPUT_THREAD_DUMP.name())
            .queryParam("user.name", "admin")
            .accept(MediaType.APPLICATION_JSON).post(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    assertEquals(Response.SC_OK, response.getStatus());

    rm.stop();
  }
}
