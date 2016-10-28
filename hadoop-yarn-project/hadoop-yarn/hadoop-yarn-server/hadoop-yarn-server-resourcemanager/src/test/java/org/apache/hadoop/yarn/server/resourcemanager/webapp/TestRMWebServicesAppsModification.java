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

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CredentialsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LocalResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LogAggregationContextInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
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
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.LoggingFilter;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

@RunWith(Parameterized.class)
public class TestRMWebServicesAppsModification extends JerseyTestBase {
  private static MockRM rm;

  private static final int CONTAINER_MB = 1024;

  private String webserviceUserName = "testuser";

  private boolean setAuthFilter = false;

  private static final String TEST_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).getAbsolutePath();
  private static final String FS_ALLOC_FILE = new File(TEST_DIR,
      "test-fs-queues.xml").getAbsolutePath();

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

  private abstract class TestServletModule extends ServletModule {
    public Configuration conf = new Configuration();

    public abstract void configureScheduler();

    @Override
    protected void configureServlets() {
      configureScheduler();
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      if (setAuthFilter) {
        filter("/*").through(TestRMCustomAuthFilter.class);
      }
      serve("/*").with(GuiceContainer.class);
    }
  }

  private class CapTestServletModule extends TestServletModule {
    @Override
    public void configureScheduler() {
      conf.set(YarnConfiguration.RM_SCHEDULER,
          CapacityScheduler.class.getName());
    }
  }

  private class FairTestServletModule extends TestServletModule {
    @Override
    public void configureScheduler() {
      try {
        PrintWriter out = new PrintWriter(new FileWriter(FS_ALLOC_FILE));
        out.println("<?xml version=\"1.0\"?>");
        out.println("<allocations>");
        out.println("<queue name=\"root\">");
        out.println("  <aclAdministerApps>someuser </aclAdministerApps>");
        out.println("  <queue name=\"default\">");
        out.println("    <aclAdministerApps>someuser </aclAdministerApps>");
        out.println("  </queue>");
        out.println("  <queue name=\"test\">");
        out.println("    <aclAdministerApps>someuser </aclAdministerApps>");
        out.println("  </queue>");
        out.println("</queue>");
        out.println("</allocations>");
        out.close();
      } catch(IOException e) {
      }
      conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, FS_ALLOC_FILE);
      conf.set(YarnConfiguration.RM_SCHEDULER, FairScheduler.class.getName());
    }
  }

  private Injector getNoAuthInjectorCap() {
    return Guice.createInjector(new CapTestServletModule() {
      @Override
      protected void configureServlets() {
        setAuthFilter = false;
        super.configureServlets();
      }
    });
  }

  private Injector getSimpleAuthInjectorCap() {
    return Guice.createInjector(new CapTestServletModule() {
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

  private Injector getNoAuthInjectorFair() {
    return Guice.createInjector(new FairTestServletModule() {
      @Override
      protected void configureServlets() {
        setAuthFilter = false;
        super.configureServlets();
      }
    });
  }

  private Injector getSimpleAuthInjectorFair() {
    return Guice.createInjector(new FairTestServletModule() {
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
    return Arrays.asList(new Object[][] { { 0 }, { 1 }, { 2 }, { 3 } });
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
      .clientConfig(new DefaultClientConfig(JAXBContextResolver.class))
      .contextPath("jersey-guice-filter").servletPath("/").build());
    switch (run) {
    case 0:
    default:
      // No Auth Capacity Scheduler
      GuiceServletConfig.setInjector(getNoAuthInjectorCap());
      break;
    case 1:
      // Simple Auth Capacity Scheduler
      GuiceServletConfig.setInjector(getSimpleAuthInjectorCap());
      break;
    case 2:
      // No Auth Fair Scheduler
      GuiceServletConfig.setInjector(getNoAuthInjectorFair());
      break;
    case 3:
      // Simple Auth Fair Scheduler
      GuiceServletConfig.setInjector(getSimpleAuthInjectorFair());
      break;
    }
  }

  private boolean isAuthenticationEnabled() {
    return setAuthFilter;
  }

  private WebResource constructWebResource(WebResource r, String... paths) {
    WebResource rt = r;
    for (String path : paths) {
      rt = rt.path(path);
    }
    if (isAuthenticationEnabled()) {
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
      assertResponseStatusCode(Status.OK, response.getStatusInfo());
      if (mediaType.contains(MediaType.APPLICATION_JSON)) {
        verifyAppStateJson(response, RMAppState.ACCEPTED);
      } else if (mediaType.contains(MediaType.APPLICATION_XML)) {
        verifyAppStateXML(response, RMAppState.ACCEPTED);
      }
    }
    rm.stop();
  }

  @Test(timeout = 120000)
  public void testSingleAppKill() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    MediaType[] contentTypes =
        { MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_XML_TYPE };
    String diagnostic = "message1";
    for (String mediaType : mediaTypes) {
      for (MediaType contentType : contentTypes) {
        RMApp app = rm.submitApp(CONTAINER_MB, "", webserviceUserName);
        amNodeManager.nodeHeartbeat(true);

        AppState targetState =
            new AppState(YarnApplicationState.KILLED.toString());
        targetState.setDiagnostics(diagnostic);

        Object entity;
        if (contentType.equals(MediaType.APPLICATION_JSON_TYPE)) {
          entity = appStateToJSON(targetState);
        } else {
          entity = targetState;
        }
        ClientResponse response =
            this
              .constructWebResource("apps", app.getApplicationId().toString(),
                "state").entity(entity, contentType).accept(mediaType)
              .put(ClientResponse.class);

        if (!isAuthenticationEnabled()) {
          assertResponseStatusCode(Status.UNAUTHORIZED,
              response.getStatusInfo());
          continue;
        }
        assertResponseStatusCode(Status.ACCEPTED, response.getStatusInfo());
        if (mediaType.contains(MediaType.APPLICATION_JSON)) {
          verifyAppStateJson(response, RMAppState.FINAL_SAVING,
            RMAppState.KILLED, RMAppState.KILLING, RMAppState.ACCEPTED);
        } else {
          verifyAppStateXML(response, RMAppState.FINAL_SAVING,
            RMAppState.KILLED, RMAppState.KILLING, RMAppState.ACCEPTED);
        }

        String locationHeaderValue =
            response.getHeaders().getFirst(HttpHeaders.LOCATION);
        Client c = Client.create();
        WebResource tmp = c.resource(locationHeaderValue);
        if (isAuthenticationEnabled()) {
          tmp = tmp.queryParam("user.name", webserviceUserName);
        }
        response = tmp.get(ClientResponse.class);
        assertResponseStatusCode(Status.OK, response.getStatusInfo());
        assertTrue(locationHeaderValue.endsWith("/ws/v1/cluster/apps/"
            + app.getApplicationId().toString() + "/state"));

        while (true) {
          Thread.sleep(100);
          response =
              this
                .constructWebResource("apps",
                  app.getApplicationId().toString(), "state").accept(mediaType)
                .entity(entity, contentType).put(ClientResponse.class);
          assertTrue(
              (response.getStatusInfo().getStatusCode()
                  == Status.ACCEPTED.getStatusCode())
              || (response.getStatusInfo().getStatusCode()
                  == Status.OK.getStatusCode()));
          if (response.getStatusInfo().getStatusCode()
              == Status.OK.getStatusCode()) {
            assertEquals(RMAppState.KILLED, app.getState());
            if (mediaType.equals(MediaType.APPLICATION_JSON)) {
              verifyAppStateJson(response, RMAppState.KILLED);
            } else {
              verifyAppStateXML(response, RMAppState.KILLED);
            }
            assertTrue("Diagnostic message is incorrect",
                app.getDiagnostics().toString().contains(diagnostic));
            break;
          }
        }
      }
    }

    rm.stop();
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
          if (contentType.equals(MediaType.APPLICATION_JSON_TYPE)) {
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

          if (!isAuthenticationEnabled()) {
            assertResponseStatusCode(Status.UNAUTHORIZED,
                response.getStatusInfo());
            continue;
          }
          assertResponseStatusCode(Status.BAD_REQUEST,
              response.getStatusInfo());
        }
      }
    }

    rm.stop();
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

    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    String responseState = json.getString("state");
    boolean valid = false;
    for (RMAppState state : states) {
      if (state.toString().equals(responseState)) {
        valid = true;
      }
    }
    String msg = "app state incorrect, got " + responseState;
    assertTrue(msg, valid);
  }

  protected static void verifyAppStateXML(ClientResponse response,
      RMAppState... appStates) throws ParserConfigurationException,
      IOException, SAXException {
    assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
    String msg = "app state incorrect, got " + state;
    assertTrue(msg, valid);
  }

  @Test(timeout = 60000)
  public void testSingleAppKillUnauthorized() throws Exception {

    boolean isCapacityScheduler =
        rm.getResourceScheduler() instanceof CapacityScheduler;
    boolean isFairScheduler =
        rm.getResourceScheduler() instanceof FairScheduler;
    assumeTrue("This test is only supported on Capacity and Fair Scheduler",
        isCapacityScheduler || isFairScheduler);
    // FairScheduler use ALLOCATION_FILE to configure ACL
    if (isCapacityScheduler) {
      // default root queue allows anyone to have admin acl
      CapacitySchedulerConfiguration csconf =
          new CapacitySchedulerConfiguration();
      csconf.setAcl("root", QueueACL.ADMINISTER_QUEUE, "someuser");
      csconf.setAcl("root.default", QueueACL.ADMINISTER_QUEUE, "someuser");
      rm.getResourceScheduler().reinitialize(csconf, rm.getRMContext());
    }

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
      validateResponseStatus(response, Status.FORBIDDEN);
    }
    rm.stop();
  }

  @Test
  public void testSingleAppKillInvalidId() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    String[] testAppIds = { "application_1391705042196_0001", "random_string" };
    for (int i = 0; i < testAppIds.length; i++) {
      AppState info = new AppState("KILLED");
      ClientResponse response =
          this.constructWebResource("apps", testAppIds[i], "state")
            .accept(MediaType.APPLICATION_XML)
            .entity(info, MediaType.APPLICATION_XML).put(ClientResponse.class);
      if (!isAuthenticationEnabled()) {
        assertResponseStatusCode(Status.UNAUTHORIZED,
            response.getStatusInfo());
        continue;
      }
      if (i == 0) {
        assertResponseStatusCode(Status.NOT_FOUND,
            response.getStatusInfo());
      } else {
        assertResponseStatusCode(Status.BAD_REQUEST,
            response.getStatusInfo());
      }
    }
    rm.stop();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    if (rm != null) {
      rm.stop();
    }
    super.tearDown();
  }

  /**
   * Helper function to wrap frequently used code. It checks the response status
   * and checks if it UNAUTHORIZED if we are running with authorization turned
   * off or the param passed if we are running with authorization turned on.
   * 
   * @param response
   *          the ClientResponse object to be checked
   * @param expectedAuthorizedMode
   *          the expected Status in authorized mode.
   */
  public void validateResponseStatus(ClientResponse response,
      Status expectedAuthorizedMode) {
    validateResponseStatus(response, Status.UNAUTHORIZED,
      expectedAuthorizedMode);
  }

  /**
   * Helper function to wrap frequently used code. It checks the response status
   * and checks if it is the param expectedUnauthorizedMode if we are running
   * with authorization turned off or the param expectedAuthorizedMode passed if
   * we are running with authorization turned on.
   * 
   * @param response
   *          the ClientResponse object to be checked
   * @param expectedUnauthorizedMode
   *          the expected Status in unauthorized mode.
   * @param expectedAuthorizedMode
   *          the expected Status in authorized mode.
   */
  public void validateResponseStatus(ClientResponse response,
      Status expectedUnauthorizedMode, Status expectedAuthorizedMode) {
    if (!isAuthenticationEnabled()) {
      assertResponseStatusCode(expectedUnauthorizedMode,
          response.getStatusInfo());
    } else {
      assertResponseStatusCode(expectedAuthorizedMode,
          response.getStatusInfo());
    }
  }

  // Simple test - just post to /apps/new-application and validate the response
  @Test
  public void testGetNewApplication() throws Exception {
    client().addFilter(new LoggingFilter(System.out));
    rm.start();
    String mediaTypes[] =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    for (String acceptMedia : mediaTypes) {
      testGetNewApplication(acceptMedia);
    }
    rm.stop();
  }

  protected String testGetNewApplication(String mediaType) throws JSONException,
      ParserConfigurationException, IOException, SAXException {
    ClientResponse response =
        this.constructWebResource("apps", "new-application").accept(mediaType)
          .post(ClientResponse.class);
    validateResponseStatus(response, Status.OK);
    if (!isAuthenticationEnabled()) {
      return "";
    }
    return validateGetNewApplicationResponse(response);
  }

  protected String validateGetNewApplicationResponse(ClientResponse resp)
      throws JSONException, ParserConfigurationException, IOException,
      SAXException {
    String ret = "";
    if (resp.getType().toString().contains(MediaType.APPLICATION_JSON)) {
      JSONObject json = resp.getEntity(JSONObject.class);
      ret = validateGetNewApplicationJsonResponse(json);
    } else if (resp.getType().toString().contains(MediaType.APPLICATION_XML)) {
      String xml = resp.getEntity(String.class);
      ret = validateGetNewApplicationXMLResponse(xml);
    } else {
      // we should not be here
      assertTrue(false);
    }
    return ret;
  }

  protected String validateGetNewApplicationJsonResponse(JSONObject json)
      throws JSONException {
    String appId = json.getString("application-id");
    assertTrue(!appId.isEmpty());
    JSONObject maxResources = json.getJSONObject("maximum-resource-capability");
    long memory = maxResources.getLong("memory");
    long vCores = maxResources.getLong("vCores");
    assertTrue(memory != 0);
    assertTrue(vCores != 0);
    return appId;
  }

  protected String validateGetNewApplicationXMLResponse(String response)
      throws ParserConfigurationException, IOException, SAXException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(response));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("NewApplication");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    Element element = (Element) nodes.item(0);
    String appId = WebServicesTestUtils.getXmlString(element, "application-id");
    assertTrue(!appId.isEmpty());
    NodeList maxResourceNodes =
        element.getElementsByTagName("maximum-resource-capability");
    assertEquals(1, maxResourceNodes.getLength());
    Element maxResourceCapability = (Element) maxResourceNodes.item(0);
    long memory =
        WebServicesTestUtils.getXmlLong(maxResourceCapability, "memory");
    long vCores =
        WebServicesTestUtils.getXmlLong(maxResourceCapability, "vCores");
    assertTrue(memory != 0);
    assertTrue(vCores != 0);
    return appId;
  }

  // Test to validate the process of submitting apps - test for appropriate
  // errors as well
  @Test
  public void testGetNewApplicationAndSubmit() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    String mediaTypes[] =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    for (String acceptMedia : mediaTypes) {
      for (String contentMedia : mediaTypes) {
        testAppSubmit(acceptMedia, contentMedia);
        testAppSubmitErrors(acceptMedia, contentMedia);
      }
    }
    rm.stop();
  }

  public void testAppSubmit(String acceptMedia, String contentMedia)
      throws Exception {

    // create a test app and submit it via rest(after getting an app-id) then
    // get the app details from the rmcontext and check that everything matches

    client().addFilter(new LoggingFilter(System.out));
    String lrKey = "example";
    String queueName = "testqueue";

    // create the queue
    String[] queues = { "default", "testqueue" };
    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    csconf.setQueues("root", queues);
    csconf.setCapacity("root.default", 50.0f);
    csconf.setCapacity("root.testqueue", 50.0f);
    rm.getResourceScheduler().reinitialize(csconf, rm.getRMContext());

    String appName = "test";
    String appType = "test-type";
    String urlPath = "apps";
    String appId = testGetNewApplication(acceptMedia);
    List<String> commands = new ArrayList<>();
    commands.add("/bin/sleep 5");
    HashMap<String, String> environment = new HashMap<>();
    environment.put("APP_VAR", "ENV_SETTING");
    HashMap<ApplicationAccessType, String> acls = new HashMap<>();
    acls.put(ApplicationAccessType.MODIFY_APP, "testuser1, testuser2");
    acls.put(ApplicationAccessType.VIEW_APP, "testuser3, testuser4");
    Set<String> tags = new HashSet<>();
    tags.add("tag1");
    tags.add("tag 2");
    CredentialsInfo credentials = new CredentialsInfo();
    HashMap<String, String> tokens = new HashMap<>();
    HashMap<String, String> secrets = new HashMap<>();
    secrets.put("secret1", Base64.encodeBase64String(
        "mysecret".getBytes("UTF8")));
    credentials.setSecrets(secrets);
    credentials.setTokens(tokens);
    ApplicationSubmissionContextInfo appInfo = new ApplicationSubmissionContextInfo();
    appInfo.setApplicationId(appId);
    appInfo.setApplicationName(appName);
    appInfo.setMaxAppAttempts(2);
    appInfo.setQueue(queueName);
    appInfo.setApplicationType(appType);
    appInfo.setPriority(0);
    HashMap<String, LocalResourceInfo> lr =  new HashMap<>();
    LocalResourceInfo y = new LocalResourceInfo();
    y.setUrl(new URI("http://www.test.com/file.txt"));
    y.setSize(100);
    y.setTimestamp(System.currentTimeMillis());
    y.setType(LocalResourceType.FILE);
    y.setVisibility(LocalResourceVisibility.APPLICATION);
    lr.put(lrKey, y);
    appInfo.getContainerLaunchContextInfo().setResources(lr);
    appInfo.getContainerLaunchContextInfo().setCommands(commands);
    appInfo.getContainerLaunchContextInfo().setEnvironment(environment);
    appInfo.getContainerLaunchContextInfo().setAcls(acls);
    appInfo.getContainerLaunchContextInfo().getAuxillaryServiceData()
      .put("test", Base64.encodeBase64URLSafeString("value12".getBytes("UTF8")));
    appInfo.getContainerLaunchContextInfo().setCredentials(credentials);
    appInfo.getResource().setMemory(1024);
    appInfo.getResource().setvCores(1);
    appInfo.setApplicationTags(tags);

    // Set LogAggregationContextInfo
    String includePattern = "file1";
    String excludePattern = "file2";
    String rolledLogsIncludePattern = "file3";
    String rolledLogsExcludePattern = "file4";
    String className = "policy_class";
    String parameters = "policy_parameter";

    LogAggregationContextInfo logAggregationContextInfo
        = new LogAggregationContextInfo();
    logAggregationContextInfo.setIncludePattern(includePattern);
    logAggregationContextInfo.setExcludePattern(excludePattern);
    logAggregationContextInfo.setRolledLogsIncludePattern(
        rolledLogsIncludePattern);
    logAggregationContextInfo.setRolledLogsExcludePattern(
        rolledLogsExcludePattern);
    logAggregationContextInfo.setLogAggregationPolicyClassName(className);
    logAggregationContextInfo.setLogAggregationPolicyParameters(parameters);
    appInfo.setLogAggregationContextInfo(logAggregationContextInfo);

    // Set attemptFailuresValidityInterval
    long attemptFailuresValidityInterval = 5000;
    appInfo.setAttemptFailuresValidityInterval(
        attemptFailuresValidityInterval);

    // Set ReservationId
    String reservationId = ReservationId.newInstance(
        System.currentTimeMillis(), 1).toString();
    appInfo.setReservationId(reservationId);

    ClientResponse response =
        this.constructWebResource(urlPath).accept(acceptMedia)
          .entity(appInfo, contentMedia).post(ClientResponse.class);

    if (!this.isAuthenticationEnabled()) {
      assertResponseStatusCode(Status.UNAUTHORIZED, response.getStatusInfo());
      return;
    }
    assertResponseStatusCode(Status.ACCEPTED, response.getStatusInfo());
    assertTrue(!response.getHeaders().getFirst(HttpHeaders.LOCATION).isEmpty());
    String locURL = response.getHeaders().getFirst(HttpHeaders.LOCATION);
    assertTrue(locURL.contains("/apps/application"));
    appId = locURL.substring(locURL.indexOf("/apps/") + "/apps/".length());

    WebResource res = resource().uri(new URI(locURL));
    res = res.queryParam("user.name", webserviceUserName);
    response = res.get(ClientResponse.class);
    assertResponseStatusCode(Status.OK, response.getStatusInfo());

    RMApp app =
        rm.getRMContext().getRMApps()
          .get(ApplicationId.fromString(appId));
    assertEquals(appName, app.getName());
    assertEquals(webserviceUserName, app.getUser());
    assertEquals(2, app.getMaxAppAttempts());
    if (app.getQueue().contains("root.")) {
      queueName = "root." + queueName;
    }
    assertEquals(queueName, app.getQueue());
    assertEquals(appType, app.getApplicationType());
    assertEquals(tags, app.getApplicationTags());
    ContainerLaunchContext ctx =
        app.getApplicationSubmissionContext().getAMContainerSpec();
    assertEquals(commands, ctx.getCommands());
    assertEquals(environment, ctx.getEnvironment());
    assertEquals(acls, ctx.getApplicationACLs());
    Map<String, LocalResource> appLRs = ctx.getLocalResources();
    assertTrue(appLRs.containsKey(lrKey));
    LocalResource exampleLR = appLRs.get(lrKey);
    assertEquals(URL.fromURI(y.getUrl()), exampleLR.getResource());
    assertEquals(y.getSize(), exampleLR.getSize());
    assertEquals(y.getTimestamp(), exampleLR.getTimestamp());
    assertEquals(y.getType(), exampleLR.getType());
    assertEquals(y.getPattern(), exampleLR.getPattern());
    assertEquals(y.getVisibility(), exampleLR.getVisibility());
    Credentials cs = new Credentials();
    ByteArrayInputStream str =
        new ByteArrayInputStream(app.getApplicationSubmissionContext()
          .getAMContainerSpec().getTokens().array());
    DataInputStream di = new DataInputStream(str);
    cs.readTokenStorageStream(di);
    Text key = new Text("secret1");
    assertTrue("Secrets missing from credentials object", cs
        .getAllSecretKeys().contains(key));
    assertEquals("mysecret", new String(cs.getSecretKey(key), "UTF-8"));

    // Check LogAggregationContext
    ApplicationSubmissionContext asc = app.getApplicationSubmissionContext();
    LogAggregationContext lac = asc.getLogAggregationContext();
    assertEquals(includePattern, lac.getIncludePattern());
    assertEquals(excludePattern, lac.getExcludePattern());
    assertEquals(rolledLogsIncludePattern, lac.getRolledLogsIncludePattern());
    assertEquals(rolledLogsExcludePattern, lac.getRolledLogsExcludePattern());
    assertEquals(className, lac.getLogAggregationPolicyClassName());
    assertEquals(parameters, lac.getLogAggregationPolicyParameters());

    // Check attemptFailuresValidityInterval
    assertEquals(attemptFailuresValidityInterval,
        asc.getAttemptFailuresValidityInterval());

    // Check ReservationId
    assertEquals(reservationId, app.getReservationId().toString());

    response =
        this.constructWebResource("apps", appId).accept(acceptMedia)
          .get(ClientResponse.class);
    assertResponseStatusCode(Status.OK, response.getStatusInfo());
  }

  public void testAppSubmitErrors(String acceptMedia, String contentMedia)
      throws Exception {

    // submit a bunch of bad requests(correct format but bad values) via the
    // REST API and make sure we get the right error response codes

    String urlPath = "apps";
    ApplicationSubmissionContextInfo appInfo = new ApplicationSubmissionContextInfo();
    ClientResponse response =
        this.constructWebResource(urlPath).accept(acceptMedia)
          .entity(appInfo, contentMedia).post(ClientResponse.class);
    validateResponseStatus(response, Status.BAD_REQUEST);

    String appId = "random";
    appInfo.setApplicationId(appId);
    response =
        this.constructWebResource(urlPath).accept(acceptMedia)
          .entity(appInfo, contentMedia).post(ClientResponse.class);
    validateResponseStatus(response, Status.BAD_REQUEST);

    appId = "random_junk";
    appInfo.setApplicationId(appId);
    response =
        this.constructWebResource(urlPath).accept(acceptMedia)
          .entity(appInfo, contentMedia).post(ClientResponse.class);
    validateResponseStatus(response, Status.BAD_REQUEST);

    // bad resource info
    appInfo.getResource().setMemory(
      rm.getConfig().getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB) + 1);
    appInfo.getResource().setvCores(1);
    response =
        this.constructWebResource(urlPath).accept(acceptMedia)
          .entity(appInfo, contentMedia).post(ClientResponse.class);

    validateResponseStatus(response, Status.BAD_REQUEST);

    appInfo.getResource().setvCores(
      rm.getConfig().getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES) + 1);
    appInfo.getResource().setMemory(CONTAINER_MB);
    response =
        this.constructWebResource(urlPath).accept(acceptMedia)
          .entity(appInfo, contentMedia).post(ClientResponse.class);
    validateResponseStatus(response, Status.BAD_REQUEST);
  }

  @Test
  public void testAppSubmitBadJsonAndXML() throws Exception {

    // submit a bunch of bad XML and JSON via the
    // REST API and make sure we get error response codes

    String urlPath = "apps";
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    amNodeManager.nodeHeartbeat(true);

    ApplicationSubmissionContextInfo appInfo = new ApplicationSubmissionContextInfo();
    appInfo.setApplicationName("test");
    appInfo.setPriority(3);
    appInfo.setMaxAppAttempts(2);
    appInfo.setQueue("testqueue");
    appInfo.setApplicationType("test-type");
    HashMap<String, LocalResourceInfo> lr = new HashMap<>();
    LocalResourceInfo y = new LocalResourceInfo();
    y.setUrl(new URI("http://www.test.com/file.txt"));
    y.setSize(100);
    y.setTimestamp(System.currentTimeMillis());
    y.setType(LocalResourceType.FILE);
    y.setVisibility(LocalResourceVisibility.APPLICATION);
    lr.put("example", y);
    appInfo.getContainerLaunchContextInfo().setResources(lr);
    appInfo.getResource().setMemory(1024);
    appInfo.getResource().setvCores(1);

    String body =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" "
            + "standalone=\"yes\"?><blah/>";
    ClientResponse response =
        this.constructWebResource(urlPath).accept(MediaType.APPLICATION_XML)
          .entity(body, MediaType.APPLICATION_XML).post(ClientResponse.class);
    assertResponseStatusCode(Status.BAD_REQUEST, response.getStatusInfo());
    body = "{\"a\" : \"b\"}";
    response =
        this.constructWebResource(urlPath).accept(MediaType.APPLICATION_XML)
          .entity(body, MediaType.APPLICATION_JSON).post(ClientResponse.class);
    validateResponseStatus(response, Status.BAD_REQUEST);
    rm.stop();
  }

  @Test
  public void testGetAppQueue() throws Exception {
    client().addFilter(new LoggingFilter(System.out));
    boolean isCapacityScheduler =
        rm.getResourceScheduler() instanceof CapacityScheduler;
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    String[] contentTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    for (String contentType : contentTypes) {
      RMApp app = rm.submitApp(CONTAINER_MB, "", webserviceUserName);
      amNodeManager.nodeHeartbeat(true);
      ClientResponse response =
          this
            .constructWebResource("apps", app.getApplicationId().toString(),
              "queue").accept(contentType).get(ClientResponse.class);
      assertResponseStatusCode(Status.OK, response.getStatusInfo());
      String expectedQueue = "default";
      if(!isCapacityScheduler) {
        expectedQueue = "root." + webserviceUserName;
      }
      if (contentType.contains(MediaType.APPLICATION_JSON)) {
        verifyAppQueueJson(response, expectedQueue);
      } else {
        verifyAppQueueXML(response, expectedQueue);
      }
    }
    rm.stop();
  }

  @Test(timeout = 90000)
  public void testUpdateAppPriority() throws Exception {
    client().addFilter(new LoggingFilter(System.out));

    if (!(rm.getResourceScheduler() instanceof CapacityScheduler)) {
      // till the fair scheduler modifications for priority is completed
      return;
    }

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);
    cs.setClusterMaxPriority(conf);

    // default root queue allows anyone to have admin acl
    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    String[] queues = { "default", "test" };
    csconf.setQueues("root", queues);
    csconf.setCapacity("root.default", 50.0f);
    csconf.setCapacity("root.test", 50.0f);
    csconf.setAcl("root", QueueACL.ADMINISTER_QUEUE, "someuser");
    csconf.setAcl("root.default", QueueACL.ADMINISTER_QUEUE, "someuser");
    csconf.setAcl("root.test", QueueACL.ADMINISTER_QUEUE, "someuser");
    rm.getResourceScheduler().reinitialize(csconf, rm.getRMContext());

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
        int modifiedPriority = 8;
        AppPriority priority = new AppPriority(modifiedPriority);
        Object entity;
        if (contentType.equals(MediaType.APPLICATION_JSON_TYPE)) {
          entity = appPriorityToJSON(priority);
        } else {
          entity = priority;
        }
        ClientResponse response = this
            .constructWebResource("apps", app.getApplicationId().toString(),
                "priority")
            .entity(entity, contentType).accept(mediaType)
            .put(ClientResponse.class);

        if (!isAuthenticationEnabled()) {
          assertResponseStatusCode(Status.UNAUTHORIZED,
              response.getStatusInfo());
          continue;
        }
        assertResponseStatusCode(Status.OK, response.getStatusInfo());
        if (mediaType.contains(MediaType.APPLICATION_JSON)) {
          verifyAppPriorityJson(response, modifiedPriority);
        } else {
          verifyAppPriorityXML(response, modifiedPriority);
        }

        response = this
            .constructWebResource("apps", app.getApplicationId().toString(),
                "priority")
            .accept(mediaType).get(ClientResponse.class);
        assertResponseStatusCode(Status.OK, response.getStatusInfo());
        if (mediaType.contains(MediaType.APPLICATION_JSON)) {
          verifyAppPriorityJson(response, modifiedPriority);
        } else {
          verifyAppPriorityXML(response, modifiedPriority);
        }

        // check unauthorized
        app = rm.submitApp(CONTAINER_MB, "", "someuser");
        amNodeManager.nodeHeartbeat(true);
        response = this
            .constructWebResource("apps", app.getApplicationId().toString(),
                "priority")
            .entity(entity, contentType).accept(mediaType)
            .put(ClientResponse.class);
        assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());
      }
    }
    rm.stop();
  }

  @Test(timeout = 90000)
  public void testAppMove() throws Exception {

    client().addFilter(new LoggingFilter(System.out));

    boolean isCapacityScheduler =
        rm.getResourceScheduler() instanceof CapacityScheduler;

    // default root queue allows anyone to have admin acl
    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    String[] queues = { "default", "test" };
    csconf.setQueues("root", queues);
    csconf.setCapacity("root.default", 50.0f);
    csconf.setCapacity("root.test", 50.0f);
    csconf.setAcl("root", QueueACL.ADMINISTER_QUEUE, "someuser");
    csconf.setAcl("root.default", QueueACL.ADMINISTER_QUEUE, "someuser");
    csconf.setAcl("root.test", QueueACL.ADMINISTER_QUEUE, "someuser");
    rm.getResourceScheduler().reinitialize(csconf, rm.getRMContext());

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
        AppQueue targetQueue = new AppQueue("test");
        Object entity;
        if (contentType.equals(MediaType.APPLICATION_JSON_TYPE)) {
          entity = appQueueToJSON(targetQueue);
        } else {
          entity = targetQueue;
        }
        ClientResponse response =
            this
              .constructWebResource("apps", app.getApplicationId().toString(),
                "queue").entity(entity, contentType).accept(mediaType)
              .put(ClientResponse.class);

        if (!isAuthenticationEnabled()) {
          assertResponseStatusCode(Status.UNAUTHORIZED,
              response.getStatusInfo());
          continue;
        }
        assertResponseStatusCode(Status.OK, response.getStatusInfo());
        String expectedQueue = "test";
        if(!isCapacityScheduler) {
          expectedQueue = "root.test";
        }
        if (mediaType.contains(MediaType.APPLICATION_JSON)) {
          verifyAppQueueJson(response, expectedQueue);
        } else {
          verifyAppQueueXML(response, expectedQueue);
        }
        Assert.assertEquals(expectedQueue, app.getQueue());

        // check unauthorized
        app = rm.submitApp(CONTAINER_MB, "", "someuser");
        amNodeManager.nodeHeartbeat(true);
        response =
            this
              .constructWebResource("apps", app.getApplicationId().toString(),
                "queue").entity(entity, contentType).accept(mediaType)
              .put(ClientResponse.class);
        assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());
        if(isCapacityScheduler) {
          Assert.assertEquals("default", app.getQueue());
        }
        else {
          Assert.assertEquals("root.someuser", app.getQueue());
        }

      }
    }
    rm.stop();
  }

  protected static String appPriorityToJSON(AppPriority targetPriority)
      throws Exception {
    StringWriter sw = new StringWriter();
    JSONJAXBContext ctx = new JSONJAXBContext(AppPriority.class);
    JSONMarshaller jm = ctx.createJSONMarshaller();
    jm.marshallToJSON(targetPriority, sw);
    return sw.toString();
  }

  protected static String appQueueToJSON(AppQueue targetQueue) throws Exception {
    StringWriter sw = new StringWriter();
    JSONJAXBContext ctx = new JSONJAXBContext(AppQueue.class);
    JSONMarshaller jm = ctx.createJSONMarshaller();
    jm.marshallToJSON(targetQueue, sw);
    return sw.toString();
  }

  protected static void verifyAppPriorityJson(ClientResponse response,
      int expectedPriority) throws JSONException {
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    int responsePriority = json.getInt("priority");
    assertEquals(expectedPriority, responsePriority);
  }

  protected static void verifyAppPriorityXML(ClientResponse response,
      int expectedPriority)
          throws ParserConfigurationException, IOException, SAXException {
    assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("applicationpriority");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    Element element = (Element) nodes.item(0);
    int responsePriority = WebServicesTestUtils.getXmlInt(element, "priority");
    assertEquals(expectedPriority, responsePriority);
  }

  protected static void
      verifyAppQueueJson(ClientResponse response, String queue)
          throws JSONException {

    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    String responseQueue = json.getString("queue");
    assertEquals(queue, responseQueue);
  }

  protected static void
      verifyAppQueueXML(ClientResponse response, String queue)
          throws ParserConfigurationException, IOException, SAXException {
    assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("appqueue");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    Element element = (Element) nodes.item(0);
    String responseQueue = WebServicesTestUtils.getXmlString(element, "queue");
    assertEquals(queue, responseQueue);
  }

}
