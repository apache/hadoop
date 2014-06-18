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
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Properties;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;

@RunWith(Parameterized.class)
public class TestRMWebServicesAppsModification extends JerseyTest {
  private static MockRM rm;

  private static final int CONTAINER_MB = 1024;

  private Injector injector;
  private String webserviceUserName = "testuser";

  public class GuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  /*
   * Helper class to allow testing of RM web services which require
   * authorization Add this class as a filter in the Guice injector for the
   * MockRM
   */

  @Singleton
  public static class TestRMCustomAuthFilter extends AuthenticationFilter {

    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) throws ServletException {
      Properties props = new Properties();
      Enumeration<?> names = filterConfig.getInitParameterNames();
      while (names.hasMoreElements()) {
        String name = (String) names.nextElement();
        if (name.startsWith(configPrefix)) {
          String value = filterConfig.getInitParameter(name);
          props.put(name.substring(configPrefix.length()), value);
        }
      }
      props.put(AuthenticationFilter.AUTH_TYPE, "simple");
      props.put(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
      return props;
    }

  }

  private class TestServletModule extends ServletModule {
    public Configuration conf = new Configuration();
    boolean setAuthFilter = false;

    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      bind(RMContext.class).toInstance(rm.getRMContext());
      bind(ApplicationACLsManager.class).toInstance(
        rm.getApplicationACLsManager());
      bind(QueueACLsManager.class).toInstance(rm.getQueueACLsManager());
      if (setAuthFilter) {
        filter("/*").through(TestRMCustomAuthFilter.class);
      }
      serve("/*").with(GuiceContainer.class);
    }
  }

  private Injector getNoAuthInjector() {
    return Guice.createInjector(new TestServletModule() {
      @Override
      protected void configureServlets() {
        super.configureServlets();
      }
    });
  }

  private Injector getSimpleAuthInjector() {
    return Guice.createInjector(new TestServletModule() {
      @Override
      protected void configureServlets() {
        setAuthFilter = true;
        conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
        // set the admin acls otherwise all users are considered admins
        // and we can't test authorization
        conf.setStrings(YarnConfiguration.YARN_ADMIN_ACL, "testuser1");
        super.configureServlets();
      }
    });
  }

  @Parameters
  public static Collection<Object[]> guiceConfigs() {
    return Arrays.asList(new Object[][] { { 0 }, { 1 } });
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestRMWebServicesAppsModification(int run) {
    super(new WebAppDescriptor.Builder(
      "org.apache.hadoop.yarn.server.resourcemanager.webapp")
      .contextListenerClass(GuiceServletConfig.class)
      .filterClass(com.google.inject.servlet.GuiceFilter.class)
      .contextPath("jersey-guice-filter").servletPath("/").build());
    switch (run) {
    case 0:
    default:
      injector = getNoAuthInjector();
      break;
    case 1:
      injector = getSimpleAuthInjector();
      break;
    }
  }

  private boolean isAuthorizationEnabled() {
    return rm.getConfig().getBoolean(YarnConfiguration.YARN_ACL_ENABLE, false);
  }

  private WebResource constructWebResource(WebResource r, String... paths) {
    WebResource rt = r;
    for (String path : paths) {
      rt = rt.path(path);
    }
    if (isAuthorizationEnabled()) {
      rt = rt.queryParam("user.name", webserviceUserName);
    }
    return rt;
  }

  private WebResource constructWebResource(String... paths) {
    WebResource r = resource();
    WebResource ws = r.path("ws").path("v1").path("cluster");
    return this.constructWebResource(ws, paths);
  }

  @Test
  public void testSingleAppState() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    for (String mediaType : mediaTypes) {
      RMApp app = rm.submitApp(CONTAINER_MB, "", webserviceUserName);
      amNodeManager.nodeHeartbeat(true);
      ClientResponse response =
          this
            .constructWebResource("apps", app.getApplicationId().toString(),
              "state").accept(mediaType).get(ClientResponse.class);
      assertEquals(Status.OK, response.getClientResponseStatus());
      if (mediaType == MediaType.APPLICATION_JSON) {
        verifyAppStateJson(response, RMAppState.ACCEPTED);
      } else if (mediaType == MediaType.APPLICATION_XML) {
        verifyAppStateXML(response, RMAppState.ACCEPTED);
      }
    }
    rm.stop();
  }

  @Test(timeout = 90000)
  public void testSingleAppKill() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    MediaType[] contentTypes =
        { MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_XML_TYPE };
    for (String mediaType : mediaTypes) {
      for (MediaType contentType : contentTypes) {
        RMApp app = rm.submitApp(CONTAINER_MB, "", webserviceUserName);
        amNodeManager.nodeHeartbeat(true);

        ClientResponse response =
            this
              .constructWebResource("apps", app.getApplicationId().toString(),
                "state").accept(mediaType).get(ClientResponse.class);
        AppState targetState =
            new AppState(YarnApplicationState.KILLED.toString());

        Object entity;
        if (contentType == MediaType.APPLICATION_JSON_TYPE) {
          entity = appStateToJSON(targetState);
        } else {
          entity = targetState;
        }
        response =
            this
              .constructWebResource("apps", app.getApplicationId().toString(),
                "state").entity(entity, contentType).accept(mediaType)
              .put(ClientResponse.class);

        if (!isAuthorizationEnabled()) {
          assertEquals(Status.UNAUTHORIZED, response.getClientResponseStatus());
          continue;
        }
        assertEquals(Status.ACCEPTED, response.getClientResponseStatus());
        if (mediaType == MediaType.APPLICATION_JSON) {
          verifyAppStateJson(response, RMAppState.KILLING, RMAppState.ACCEPTED);
        } else {
          verifyAppStateXML(response, RMAppState.KILLING, RMAppState.ACCEPTED);
        }

        String locationHeaderValue =
            response.getHeaders().getFirst(HttpHeaders.LOCATION);
        Client c = Client.create();
        WebResource tmp = c.resource(locationHeaderValue);
        if (isAuthorizationEnabled()) {
          tmp = tmp.queryParam("user.name", webserviceUserName);
        }
        response = tmp.get(ClientResponse.class);
        assertEquals(Status.OK, response.getClientResponseStatus());
        assertTrue(locationHeaderValue.endsWith("/ws/v1/cluster/apps/"
            + app.getApplicationId().toString() + "/state"));

        while (true) {
          Thread.sleep(100);
          response =
              this
                .constructWebResource("apps",
                  app.getApplicationId().toString(), "state").accept(mediaType)
                .entity(entity, contentType).put(ClientResponse.class);
          assertTrue((response.getClientResponseStatus() == Status.ACCEPTED)
              || (response.getClientResponseStatus() == Status.OK));
          if (response.getClientResponseStatus() == Status.OK) {
            assertEquals(RMAppState.KILLED, app.getState());
            if (mediaType == MediaType.APPLICATION_JSON) {
              verifyAppStateJson(response, RMAppState.KILLED);
            } else {
              verifyAppStateXML(response, RMAppState.KILLED);
            }
            break;
          }
        }
      }
    }

    rm.stop();
    return;
  }

  @Test
  public void testSingleAppKillInvalidState() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);

    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    MediaType[] contentTypes =
        { MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_XML_TYPE };
    String[] targetStates =
        { YarnApplicationState.FINISHED.toString(), "blah" };

    for (String mediaType : mediaTypes) {
      for (MediaType contentType : contentTypes) {
        for (String targetStateString : targetStates) {
          RMApp app = rm.submitApp(CONTAINER_MB, "", webserviceUserName);
          amNodeManager.nodeHeartbeat(true);
          ClientResponse response;
          AppState targetState = new AppState(targetStateString);
          Object entity;
          if (contentType == MediaType.APPLICATION_JSON_TYPE) {
            entity = appStateToJSON(targetState);
          } else {
            entity = targetState;
          }
          response =
              this
                .constructWebResource("apps",
                  app.getApplicationId().toString(), "state")
                .entity(entity, contentType).accept(mediaType)
                .put(ClientResponse.class);

          if (!isAuthorizationEnabled()) {
            assertEquals(Status.UNAUTHORIZED,
              response.getClientResponseStatus());
            continue;
          }
          assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
        }
      }
    }

    rm.stop();
    return;
  }

  private static String appStateToJSON(AppState state) throws Exception {
    StringWriter sw = new StringWriter();
    JSONJAXBContext ctx = new JSONJAXBContext(AppState.class);
    JSONMarshaller jm = ctx.createJSONMarshaller();
    jm.marshallToJSON(state, sw);
    return sw.toString();
  }

  protected static void verifyAppStateJson(ClientResponse response,
      RMAppState... states) throws JSONException {

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    boolean valid = false;
    for (RMAppState state : states) {
      if (state.toString().equals(json.getString("state"))) {
        valid = true;
      }
    }
    assertTrue("app state incorrect", valid);
    return;
  }

  protected static void verifyAppStateXML(ClientResponse response,
      RMAppState... appStates) throws ParserConfigurationException,
      IOException, SAXException {
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("appstate");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    Element element = (Element) nodes.item(0);
    String state = WebServicesTestUtils.getXmlString(element, "state");
    boolean valid = false;
    for (RMAppState appState : appStates) {
      if (appState.toString().equals(state)) {
        valid = true;
      }
    }
    assertTrue("app state incorrect", valid);
    return;
  }

  @Test(timeout = 30000)
  public void testSingleAppKillUnauthorized() throws Exception {

    // default root queue allows anyone to have admin acl
    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    csconf.setAcl("root", QueueACL.ADMINISTER_QUEUE, "someuser");
    csconf.setAcl("root.default", QueueACL.ADMINISTER_QUEUE, "someuser");
    rm.getResourceScheduler().reinitialize(csconf, rm.getRMContext());
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);

    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    for (String mediaType : mediaTypes) {
      RMApp app = rm.submitApp(CONTAINER_MB, "test", "someuser");
      amNodeManager.nodeHeartbeat(true);
      ClientResponse response =
          this
            .constructWebResource("apps", app.getApplicationId().toString(),
              "state").accept(mediaType).get(ClientResponse.class);
      AppState info = response.getEntity(AppState.class);
      info.setState(YarnApplicationState.KILLED.toString());

      response =
          this
            .constructWebResource("apps", app.getApplicationId().toString(),
              "state").accept(mediaType)
            .entity(info, MediaType.APPLICATION_XML).put(ClientResponse.class);
      if (!isAuthorizationEnabled()) {
        assertEquals(Status.UNAUTHORIZED, response.getClientResponseStatus());
      } else {
        assertEquals(Status.FORBIDDEN, response.getClientResponseStatus());
      }
    }
    rm.stop();
    return;

  }

  @Test
  public void testSingleAppKillInvalidId() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    String[] testAppIds = { "application_1391705042196_0001", "random_string" };
    for (String testAppId : testAppIds) {
      AppState info = new AppState("KILLED");
      ClientResponse response =
          this.constructWebResource("apps", testAppId, "state")
            .accept(MediaType.APPLICATION_XML)
            .entity(info, MediaType.APPLICATION_XML).put(ClientResponse.class);
      if (!isAuthorizationEnabled()) {
        assertEquals(Status.UNAUTHORIZED, response.getClientResponseStatus());
        continue;
      }
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
    }
    rm.stop();
    return;
  }

  @After
  @Override
  public void tearDown() throws Exception {
    if (rm != null) {
      rm.stop();
    }
    super.tearDown();
  }
}
