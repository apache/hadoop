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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.jettison.internal.entity.JettisonObjectProvider.App;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class TestGPGWebServices extends JerseyTestBase {
  private static GlobalPolicyGenerator gpg;
  private static GPGWebApp webApp;

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(GPGWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  private static class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      gpg = new GlobalPolicyGenerator();
      webApp = new GPGWebApp(gpg);
      Configuration conf = new Configuration();

      bind(gpg).to(GlobalPolicyGenerator.class).named("gpg");
      bind(webApp).to(WebApp.class).named("webapp");
      bind(conf).to(Configuration.class).named("conf");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      final HttpServletRequest request = mock(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
      bind(request).to(HttpServletRequest.class);
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestGPGWebServices() {
  }

  @Test
  public void testGetGPG() throws JSONException, Exception {
    WebTarget r = target().register(App.class);
    JSONObject json = r.path("ws").path("v1").path("gpg")
        .request(MediaType.APPLICATION_JSON).get(JSONObject.class);
    assertNotNull(json);
  }

  @Test
  public void testGetGPGInfo() throws JSONException, Exception {
    WebTarget r = target().register(App.class);
    JSONObject json = r.path("ws").path("v1").path("gpg").path("info")
        .request(MediaType.APPLICATION_JSON).get(JSONObject.class);
    assertNotNull(json);
  }
}
