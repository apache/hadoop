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

package org.apache.hadoop.yarn.webapp;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.http.XFrameOptionsFilter;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServices;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Used TestRMWebServices as an example of web invocations of RM and added
 * test for XFS Filter.
 */
public class TestRMWithXFSFilter extends JerseyTestBase {

  private static MockRM rm;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestRMWithXFSFilter() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testDefaultBehavior() throws Exception {
    createInjector();

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("info").accept("application/xml")
        .get(ClientResponse.class);
    assertEquals("Should have received DENY x-frame options header",
        "DENY",
        response.getHeaders().get(XFrameOptionsFilter.X_FRAME_OPTIONS).get(0));
  }

  protected void createInjector(String headerValue) {
    createInjector(headerValue, false);
  }


  protected void createInjector() {
    createInjector(null, false);
  }

  protected void createInjector(final String headerValue,
      final boolean explicitlyDisabled) {
    GuiceServletConfig.setInjector(Guice.createInjector(new ServletModule() {
      @Override
      protected void configureServlets() {
        bind(JAXBContextResolver.class);
        bind(RMWebServices.class);
        bind(GenericExceptionHandler.class);
        Configuration conf = new Configuration();
        conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
            ResourceScheduler.class);
        rm = new MockRM(conf);
        bind(ResourceManager.class).toInstance(rm);
        serve("/*").with(GuiceContainer.class);
        XFrameOptionsFilter xfsFilter = new XFrameOptionsFilter();
        Map<String, String> initParams = new HashMap<>();
        if (headerValue != null) {
          initParams.put(XFrameOptionsFilter.CUSTOM_HEADER_PARAM, headerValue);
        }
        if (explicitlyDisabled) {
          initParams.put(
              "xframe-options-enabled", "false");
        }

        filter("/*").through(xfsFilter, initParams);
      }
    }));
  }

  @Test
  public void testSameOrigin() throws Exception {
    createInjector("SAMEORIGIN");

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("info").accept("application/xml")
        .get(ClientResponse.class);
    assertEquals("Should have received SAMEORIGIN x-frame options header",
        "SAMEORIGIN",
        response.getHeaders().get(XFrameOptionsFilter.X_FRAME_OPTIONS).get(0));
  }

  @Test
  public void testExplicitlyDisabled() throws Exception {
    createInjector(null, true);

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("info").accept("application/xml")
        .get(ClientResponse.class);
    assertFalse("Should have not received x-frame options header",
        response.getHeaders().get(XFrameOptionsFilter.X_FRAME_OPTIONS) == null);
  }

}
