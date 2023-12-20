/** * Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

import com.google.inject.Guice;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContext;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;

import static org.junit.Assert.assertNotNull;

public class TestGPGWebServices extends JerseyTestBase {
  private static GlobalPolicyGenerator gpg;
  private static GPGWebApp webApp;

  private static class XWebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(GPGWebServices.class);
      bind(GenericExceptionHandler.class);
      gpg = new GlobalPolicyGenerator();
      webApp = new GPGWebApp(gpg);
      bind(WebApp.class).toInstance(webApp);
      bind(GlobalPolicyGenerator.class).toInstance(gpg);
      bind(GPGContext.class).toInstance(gpg.getGPGContext());
      serve("/*").with(GuiceContainer.class);
    }
  }

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new XWebServletModule()));
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new XWebServletModule()));
  }

  public TestGPGWebServices() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.globalpolicygenerator.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testGetGPG() throws JSONException, Exception {
    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("gpg")
        .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
    assertNotNull(json);
  }

  @Test
  public void testGetGPGInfo() throws JSONException, Exception {
    WebResource r = resource();
    JSONObject json = r.path("ws").path("v1").path("gpg").path("info")
        .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
    assertNotNull(json);
  }
}
