/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.util.resource.CustomResourceTypesConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.helper.BufferedClientResponse;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.helper.JsonCustomResourceTypeTestcase;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.helper.XmlCustomResourceTypeTestCase;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Test;
import org.w3c.dom.NodeList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCustomResourceTypesCommons.verifyAppInfoJson;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCustomResourceTypesCommons.verifyAppsXML;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test verifies that custom resource types are correctly serialized to XML
 * and JSON when HTTP GET request is sent to the resource: ws/v1/cluster/apps.
 */
public class TestRMWebServicesAppCustomResourceTypes extends JerseyTestBase {

  private static MockRM rm;
  private static final int CONTAINER_MB = 1024;

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(RMWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    forceSet(TestProperties.CONTAINER_PORT, JERSEY_RANDOM_PORT);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      Configuration conf = new YarnConfiguration();
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);
      initResourceTypes(conf);
      rm = new MockRM(conf);

      final HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getScheme()).thenReturn("http");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
    }

    private void initResourceTypes(Configuration conf) {
      conf.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
          CustomResourceTypesConfigurationProvider.class.getName());
      ResourceUtils.resetResourceTypes(conf);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestRMWebServicesAppCustomResourceTypes() {
  }

  private WebTarget getWebResourcePathForApp(RMApp app1, WebTarget r) {
    return r.path("ws").path("v1").path("cluster").path("apps")
        .path(String.valueOf(app1.getApplicationId().toString()));
  }

  @Test
  public void testRunningAppXml() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("testwordcount")
            .withUser("user1")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    am1.allocate("*", 2048, 1, new ArrayList<>());
    amNodeManager.nodeHeartbeat(true);

    WebTarget r = target();
    WebTarget path = getWebResourcePathForApp(app1, r);
    Response response =
        path.request(MediaType.APPLICATION_XML).get(Response.class);

    XmlCustomResourceTypeTestCase testCase =
            new XmlCustomResourceTypeTestCase(path,
                    new BufferedClientResponse(response));
    testCase.verify(document -> {
      NodeList appArray = document
              .getElementsByTagName("app");
      assertEquals("incorrect number of app elements", 1, appArray.getLength());

      verifyAppsXML(appArray, app1, rm);
    });

    rm.stop();
  }

  @Test
  public void testRunningAppJson() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
            .withAppName("testwordcount")
            .withUser("user1")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    am1.allocate("*", 2048, 1, new ArrayList<>());
    amNodeManager.nodeHeartbeat(true);

    WebTarget r = target();
    WebTarget path = getWebResourcePathForApp(app1, r);
    Response response = path.request(MediaType.APPLICATION_JSON).get(Response.class);

    JsonCustomResourceTypeTestcase testCase =
        new JsonCustomResourceTypeTestcase(path,
            new BufferedClientResponse(response));
    testCase.verify(json -> {
      try {
        assertEquals("incorrect number of app elements", 1, json.length());
        JSONObject app = json.getJSONObject("app");
        verifyAppInfoJson(app, app1, rm);
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
    });

    rm.stop();
  }
}
