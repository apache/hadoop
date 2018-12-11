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
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
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
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp
        .TestRMWebServicesCustomResourceTypesCommons.verifyAppInfoJson;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp
        .TestRMWebServicesCustomResourceTypesCommons.verifyAppsXML;
import static org.junit.Assert.assertEquals;

/**
 * This test verifies that custom resource types are correctly serialized to XML
 * and JSON when HTTP GET request is sent to the resource: ws/v1/cluster/apps.
 */
public class TestRMWebServicesAppsCustomResourceTypes extends JerseyTestBase {

  private static MockRM rm;
  private static final int CONTAINER_MB = 1024;

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      Configuration conf = new Configuration();
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);
      initResourceTypes(conf);
      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
    }

    private void initResourceTypes(Configuration conf) {
      conf.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
          CustomResourceTypesConfigurationProvider.class.getName());
      ResourceUtils.resetResourceTypes(conf);
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    createInjectorForWebServletModule();
  }

  private void createInjectorForWebServletModule() {
    GuiceServletConfig
        .setInjector(Guice.createInjector(new WebServletModule()));
  }

  public TestRMWebServicesAppsCustomResourceTypes() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
            .contextListenerClass(GuiceServletConfig.class)
            .filterClass(com.google.inject.servlet.GuiceFilter.class)
            .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testRunningAppsXml() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    am1.allocate("*", 2048, 1, new ArrayList<>());
    amNodeManager.nodeHeartbeat(true);

    WebResource r = resource();
    WebResource path = r.path("ws").path("v1").path("cluster").path("apps");
    ClientResponse response =
        path.accept(MediaType.APPLICATION_XML).get(ClientResponse.class);

    XmlCustomResourceTypeTestCase testCase =
            new XmlCustomResourceTypeTestCase(path,
                    new BufferedClientResponse(response));
    testCase.verify(document -> {
      NodeList apps = document.getElementsByTagName("apps");
      assertEquals("incorrect number of apps elements", 1, apps.getLength());

      NodeList appArray = ((Element)(apps.item(0)))
              .getElementsByTagName("app");
      assertEquals("incorrect number of app elements", 1, appArray.getLength());

      verifyAppsXML(appArray, app1, rm);
    });

    rm.stop();
  }

  @Test
  public void testRunningAppsJson() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    RMApp app1 = rm.submitApp(CONTAINER_MB, "testwordcount", "user1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, amNodeManager);
    am1.allocate("*", 2048, 1, new ArrayList<>());
    amNodeManager.nodeHeartbeat(true);

    WebResource r = resource();
    WebResource path = r.path("ws").path("v1").path("cluster").path("apps");
    ClientResponse response =
        path.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    JsonCustomResourceTypeTestcase testCase =
        new JsonCustomResourceTypeTestcase(path,
            new BufferedClientResponse(response));
    testCase.verify(json -> {
      try {
        assertEquals("incorrect number of apps elements", 1, json.length());
        JSONObject apps = json.getJSONObject("apps");
        assertEquals("incorrect number of app elements", 1, apps.length());
        JSONArray array = apps.getJSONArray("app");
        assertEquals("incorrect count of app", 1, array.length());

        verifyAppInfoJson(array.getJSONObject(0), app1, rm);
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
    });

    rm.stop();
  }
}
