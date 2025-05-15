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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;
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
import org.apache.hadoop.thirdparty.com.google.common.net.HttpHeaders;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
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
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocationfile.AllocationFileQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocationfile.AllocationFileWriter;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CredentialsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LocalResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LogAggregationContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.reader.AppStateReader;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.reader.ApplicationSubmissionContextInfoReader;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.writer.ApplicationSubmissionContextInfoWriter;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.inject.Singleton;
import org.glassfish.jersey.jettison.JettisonJaxbContext;
import org.glassfish.jersey.jettison.JettisonMarshaller;
import org.glassfish.jersey.jettison.internal.entity.JettisonObjectProvider.App;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;
import static org.mockito.Mockito.when;

public class TestRMWebServicesAppsModification extends JerseyTestBase {
  private static MockRM rm;

  private static final int CONTAINER_MB = 1024;

  private String webserviceUserName = "testuser";

  private boolean setAuthFilter = false;

  private static final String TEST_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).getAbsolutePath();
  private static final String FS_ALLOC_FILE = new File(TEST_DIR,
      "test-fs-queues.xml").getAbsolutePath();
  private static final QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  private static final QueuePath DEFAULT = ROOT.createNewLeaf("default");
  private static final QueuePath TEST = ROOT.createNewLeaf("test");
  private static final QueuePath TEST_QUEUE = ROOT.createNewLeaf("testqueue");
  private ResourceConfig config;
  private HttpServletRequest hsRequest = mock(HttpServletRequest.class);
  private HttpServletResponse hsResponse = mock(HttpServletResponse.class);

  private static final JettisonMarshaller APP_STATE_WRITER;
  static {
    try {
      JettisonJaxbContext jettisonJaxbContext = new JettisonJaxbContext(AppState.class);
      APP_STATE_WRITER = jettisonJaxbContext.createJsonMarshaller();
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
  }

  private static final JettisonMarshaller APP_PRIORITY_WRITER;
  static {
    try {
      JettisonJaxbContext jettisonJaxbContext = new JettisonJaxbContext(AppPriority.class);
      APP_PRIORITY_WRITER = jettisonJaxbContext.createJsonMarshaller();
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
  }

  private static final JettisonMarshaller APP_QUEUE_WRITER;
  static {
    try {
      JettisonJaxbContext jettisonJaxbContext = new JettisonJaxbContext(AppQueue.class);
      APP_QUEUE_WRITER = jettisonJaxbContext.createJsonMarshaller();
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
  }

  private static final JettisonMarshaller APP_TIMEOUT_WRITER;
  static {
    try {
      JettisonJaxbContext jettisonJaxbContext = new JettisonJaxbContext(AppTimeoutInfo.class);
      APP_TIMEOUT_WRITER = jettisonJaxbContext.createJsonMarshaller();
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Application configure() {
    config = new ResourceConfig();
    config.register(RMWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(ApplicationSubmissionContextInfoWriter.class);
    config.register(ApplicationSubmissionContextInfoReader.class);
    config.register(TestRMCustomAuthFilter.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    forceSet(TestProperties.CONTAINER_PORT, JERSEY_RANDOM_PORT);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {
    private Configuration conf = new YarnConfiguration();

    @Override
    protected void configure() {
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      configureScheduler();
      rm = new MockRM(conf);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(hsRequest).to(HttpServletRequest.class);
      bind(hsResponse).to(HttpServletResponse.class);
    }

    public void configureScheduler() {
    }

    public Configuration getConf() {
      return conf;
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

  private class CapTestServletModule extends JerseyBinder {
    CapTestServletModule(boolean flag) {
      if(flag) {
        getConf().setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
        getConf().setStrings(YarnConfiguration.YARN_ADMIN_ACL, "testuser1");
      }
    }

    @Override
    public void configureScheduler() {
      getConf().set(YarnConfiguration.RM_SCHEDULER,
          CapacityScheduler.class.getName());
    }
  }

  private class FairTestServletModule extends JerseyBinder {

    FairTestServletModule(boolean flag) {
      if(flag) {
        getConf().setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
        // set the admin acls otherwise all users are considered admins
        // and we can't test authorization
        getConf().setStrings(YarnConfiguration.YARN_ADMIN_ACL, "testuser1");
      }
    }

    @Override
    public void configureScheduler() {
      AllocationFileWriter.create()
          .addQueue(new AllocationFileQueue.Builder("root")
          .aclAdministerApps("someuser ")
          .subQueue(new AllocationFileQueue.Builder("default")
          .aclAdministerApps("someuser ").build())
          .subQueue(new AllocationFileQueue.Builder("test")
          .aclAdministerApps("someuser ").build())
          .build())
          .writeToFile(FS_ALLOC_FILE);
      getConf().set(FairSchedulerConfiguration.ALLOCATION_FILE, FS_ALLOC_FILE);
      getConf().set(YarnConfiguration.RM_SCHEDULER, FairScheduler.class.getName());
    }
  }

  private CapTestServletModule getNoAuthInjectorCap() {
    setAuthFilter = false;
    return new CapTestServletModule(false);
  }

  private CapTestServletModule getSimpleAuthInjectorCap() {
    setAuthFilter = true;
    Principal principal = () -> "testuser";
    when(hsRequest.getUserPrincipal()).thenReturn(principal);
    return new CapTestServletModule(true);
  }

  private FairTestServletModule getNoAuthInjectorFair() {
    setAuthFilter = false;
    return new FairTestServletModule(false);
  }

  private FairTestServletModule getSimpleAuthInjectorFair() {
    setAuthFilter = true;
    Principal principal = () -> "testuser";
    when(hsRequest.getUserPrincipal()).thenReturn(principal);
    return new FairTestServletModule(true);
  }

  public static Collection<Object[]> guiceConfigs() {
    return Arrays.asList(new Object[][]{{0}, {1}, {2}, {3}});
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public void initTestRMWebServicesAppsModification(int run) throws Exception {
    switch (run) {
    case 0:
    default:
      // No Auth Capacity Scheduler
      config.register(getNoAuthInjectorCap());
      break;
    case 1:
      // Simple Auth Capacity Scheduler
      config.register(getSimpleAuthInjectorCap());
      break;
    case 2:
      // No Auth Fair Scheduler
      config.register(getNoAuthInjectorFair());
      break;
    case 3:
      // Simple Auth Fair Scheduler
      config.register(getSimpleAuthInjectorFair());
      break;
    }
    setUp();
  }

  private boolean isAuthenticationEnabled() {
    return setAuthFilter;
  }

  private WebTarget constructWebResource(WebTarget r, String... paths) {
    WebTarget rt = r;
    for (String path : paths) {
      rt = rt.path(path);
    }
    if (isAuthenticationEnabled()) {
      rt = rt.queryParam("user.name", webserviceUserName);
    }
    return rt;
  }

  private WebTarget constructWebResource(String... paths) {
    WebTarget r = target()
        .register(App.class)
        .register(AppStateReader.class)
        .register(ApplicationSubmissionContextInfoReader.class)
        .register(ApplicationSubmissionContextInfoWriter.class);
    WebTarget ws = r.path("ws").path("v1").path("cluster");
    return this.constructWebResource(ws, paths);
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testSingleAppState(int run) throws Exception {
    initTestRMWebServicesAppsModification(run);
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    for (String mediaType : mediaTypes) {
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
              .withAppName("")
              .withUser(webserviceUserName)
              .build();
      RMApp app = MockRMAppSubmitter.submit(rm, data);
      amNodeManager.nodeHeartbeat(true);
      Response response =
          this.constructWebResource("apps", app.getApplicationId().toString(),
          "state").request(mediaType).get(Response.class);
      assertResponseStatusCode(OK, response.getStatusInfo());
      if (mediaType.contains(MediaType.APPLICATION_JSON)) {
        verifyAppStateJson(response, RMAppState.ACCEPTED);
      } else if (mediaType.contains(MediaType.APPLICATION_XML)) {
        verifyAppStateXML(response, RMAppState.ACCEPTED);
      }
    }
    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  @Timeout(value = 120)
  public void testSingleAppKill(int run) throws Exception {
    initTestRMWebServicesAppsModification(run);
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    MediaType[] contentTypes =
        { MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_XML_TYPE };
    String diagnostic = "message1";
    for (String mediaType : mediaTypes) {
      for (MediaType contentType : contentTypes) {
        MockRMAppSubmissionData data =
            MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
                .withAppName("")
                .withUser(webserviceUserName)
                .build();
        RMApp app = MockRMAppSubmitter.submit(rm, data);
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
        Response response =
            this.constructWebResource("apps", app.getApplicationId().toString(),
            "state").request(mediaType)
            .put(Entity.entity(entity, contentType), Response.class);

        if (!isAuthenticationEnabled()) {
          assertResponseStatusCode(UNAUTHORIZED,
              response.getStatusInfo());
          continue;
        }
        assertResponseStatusCode(ACCEPTED, response.getStatusInfo());
        if (mediaType.contains(MediaType.APPLICATION_JSON)) {
          verifyAppStateJson(response, RMAppState.FINAL_SAVING,
            RMAppState.KILLED, RMAppState.KILLING, RMAppState.ACCEPTED);
        } else {
          verifyAppStateXML(response, RMAppState.FINAL_SAVING,
            RMAppState.KILLED, RMAppState.KILLING, RMAppState.ACCEPTED);
        }

        String locationHeaderValue = this.constructWebResource(
            "apps", app.getApplicationId().toString(), "state")
            .getUri().toString().replace("?user.name=testuser", "");

        Client c = ClientBuilder.newClient();
        WebTarget tmp = c.target(locationHeaderValue);
        if (isAuthenticationEnabled()) {
          tmp = tmp.queryParam("user.name", webserviceUserName);
        }
        response = tmp.request().get(Response.class);
        assertResponseStatusCode(OK, response.getStatusInfo());
        assertTrue(locationHeaderValue.endsWith("/ws/v1/cluster/apps/"
            + app.getApplicationId().toString() + "/state"));

        while (true) {
          Thread.sleep(100);
          response =
              this
                .constructWebResource("apps",
                app.getApplicationId().toString(), "state").request(mediaType)
                .put(Entity.entity(entity, contentType), Response.class);
          assertTrue(
              (response.getStatusInfo().getStatusCode()
                  == ACCEPTED.getStatusCode())
              || (response.getStatusInfo().getStatusCode()
                  == OK.getStatusCode()));
          if (response.getStatusInfo().getStatusCode()
              == OK.getStatusCode()) {
            assertEquals(RMAppState.KILLED, app.getState());
            if (mediaType.equals(MediaType.APPLICATION_JSON)) {
              verifyAppStateJson(response, RMAppState.KILLED);
            } else {
              verifyAppStateXML(response, RMAppState.KILLED);
            }
            assertTrue(app.getDiagnostics().toString().contains(diagnostic),
                "Diagnostic message is incorrect");
            break;
          }
        }
      }
    }

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testSingleAppKillInvalidState(int run) throws Exception {
    initTestRMWebServicesAppsModification(run);
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
          MockRMAppSubmissionData data =
              MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
                  .withAppName("")
                  .withUser(webserviceUserName)
                  .build();
          RMApp app = MockRMAppSubmitter.submit(rm, data);
          amNodeManager.nodeHeartbeat(true);
          Response response;
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
                .request(mediaType)
                .put(Entity.entity(entity, contentType), Response.class);

          if (!isAuthenticationEnabled()) {
            assertResponseStatusCode(Response.Status.UNAUTHORIZED,
                response.getStatusInfo());
            continue;
          }
          assertResponseStatusCode(BAD_REQUEST, response.getStatusInfo());
        }
      }
    }

    rm.stop();
  }

  private static String appStateToJSON(AppState state) throws Exception {
    StringWriter stringWriter = new StringWriter();
    APP_STATE_WRITER.marshallToJSON(state, stringWriter);
    return stringWriter.toString();
  }

  protected static void verifyAppStateJson(Response response,
      RMAppState... states) throws JSONException {

    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    String responseState = json.getJSONObject("appstate").getString("state");
    boolean valid = false;
    for (RMAppState state : states) {
      if (state.toString().equals(responseState)) {
        valid = true;
      }
    }
    String msg = "app state incorrect, got " + responseState;
    assertTrue(valid, msg);
  }

  protected static void verifyAppStateXML(Response response,
      RMAppState... appStates) throws ParserConfigurationException,
      IOException, SAXException {
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("appstate");
    assertEquals(1, nodes.getLength(), "incorrect number of elements");
    Element element = (Element) nodes.item(0);
    String state = WebServicesTestUtils.getXmlString(element, "state");
    boolean valid = false;
    for (RMAppState appState : appStates) {
      if (appState.toString().equals(state)) {
        valid = true;
      }
    }
    String msg = "app state incorrect, got " + state;
    assertTrue(valid, msg);
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  @Timeout(value = 60)
  public void testSingleAppKillUnauthorized(int run) throws Exception {
    initTestRMWebServicesAppsModification(run);
    boolean isCapacityScheduler =
        rm.getResourceScheduler() instanceof CapacityScheduler;
    boolean isFairScheduler =
        rm.getResourceScheduler() instanceof FairScheduler;
    assumeTrue(isCapacityScheduler || isFairScheduler,
        "This test is only supported on Capacity and Fair Scheduler");
    // FairScheduler use ALLOCATION_FILE to configure ACL
    if (isCapacityScheduler) {
      // default root queue allows anyone to have admin acl
      CapacitySchedulerConfiguration csconf =
          new CapacitySchedulerConfiguration();
      csconf.setAcl(ROOT, QueueACL.ADMINISTER_QUEUE, "someuser");
      csconf.setAcl(DEFAULT, QueueACL.ADMINISTER_QUEUE, "someuser");
      rm.getResourceScheduler().reinitialize(csconf, rm.getRMContext());
    }

    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);

    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    for (String mediaType : mediaTypes) {
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
              .withAppName("test")
              .withUser("someuser")
              .build();
      RMApp app = MockRMAppSubmitter.submit(rm, data);
      amNodeManager.nodeHeartbeat(true);
      Response response =
          this.constructWebResource("apps", app.getApplicationId().toString(),
          "state").request(mediaType).get(Response.class);
      AppState info = response.readEntity(AppState.class);
      info.setState(YarnApplicationState.KILLED.toString());

      response =
          this
            .constructWebResource("apps", app.getApplicationId().toString(),
              "state").request(mediaType)
            .put(Entity.entity(info, MediaType.APPLICATION_XML), Response.class);
      validateResponseStatus(response, Response.Status.FORBIDDEN);
    }
    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testSingleAppKillInvalidId(int run) throws Exception {
    initTestRMWebServicesAppsModification(run);
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    String[] testAppIds = { "application_1391705042196_0001", "random_string" };
    for (int i = 0; i < testAppIds.length; i++) {
      AppState info = new AppState("KILLED");
      Response response =
          this.constructWebResource("apps", testAppIds[i], "state")
          .request(MediaType.APPLICATION_XML)
          .put(Entity.xml(info), Response.class);
      if (!isAuthenticationEnabled()) {
        assertResponseStatusCode(Response.Status.UNAUTHORIZED,
            response.getStatusInfo());
        continue;
      }
      if (i == 0) {
        assertResponseStatusCode(Response.Status.NOT_FOUND,
            response.getStatusInfo());
      } else {
        assertResponseStatusCode(BAD_REQUEST,
            response.getStatusInfo());
      }
    }
    rm.stop();
  }

  @AfterEach
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
  public void validateResponseStatus(Response response,
      Response.Status expectedAuthorizedMode) {
    validateResponseStatus(response, Response.Status.UNAUTHORIZED,
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
  public void validateResponseStatus(Response response,
      Response.Status expectedUnauthorizedMode, Response.Status expectedAuthorizedMode) {
    if (!isAuthenticationEnabled()) {
      assertResponseStatusCode(expectedUnauthorizedMode,
          response.getStatusInfo());
    } else {
      assertResponseStatusCode(expectedAuthorizedMode,
          response.getStatusInfo());
    }
  }

  // Simple test - just post to /apps/new-application and validate the response
  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testGetNewApplication(int run) throws Exception {
    initTestRMWebServicesAppsModification(run);
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
    Response response =
        this.constructWebResource("apps", "new-application").request(mediaType)
          .post(null, Response.class);
    validateResponseStatus(response, OK);
    if (!isAuthenticationEnabled()) {
      return "";
    }
    return validateGetNewApplicationResponse(response);
  }

  protected String validateGetNewApplicationResponse(Response resp)
      throws JSONException, ParserConfigurationException, IOException,
      SAXException {
    String ret = "";
    if (resp.getMediaType().toString().contains(MediaType.APPLICATION_JSON)) {
      JSONObject json = resp.readEntity(JSONObject.class);
      JSONObject newApplication = json.getJSONObject("NewApplication");
      ret = validateGetNewApplicationJsonResponse(newApplication);
    } else if (resp.getMediaType().toString().contains(MediaType.APPLICATION_XML)) {
      String xml = resp.readEntity(String.class);
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
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(response));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("NewApplication");
    assertEquals(1, nodes.getLength(), "incorrect number of elements");
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
  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testGetNewApplicationAndSubmit(int run) throws Exception {
    initTestRMWebServicesAppsModification(run);
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

    String lrKey = "example";
    String queueName = "testqueue";

    // create the queue
    String[] queues = { "default", "testqueue" };
    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    csconf.setQueues(ROOT, queues);
    csconf.setCapacity(DEFAULT, 50.0f);
    csconf.setCapacity(TEST_QUEUE, 50.0f);
    when(hsRequest.getRequestURL()).thenReturn(new StringBuffer("/apps"));
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
        "mysecret".getBytes(StandardCharsets.UTF_8)));
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
      .put("test", Base64.encodeBase64URLSafeString("value12".getBytes(StandardCharsets.UTF_8)));
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

    Response response =
        this.constructWebResource(urlPath).request(acceptMedia)
        .post(Entity.entity(appInfo, contentMedia), Response.class);

    if (!this.isAuthenticationEnabled()) {
      assertResponseStatusCode(Response.Status.UNAUTHORIZED, response.getStatusInfo());
      return;
    }
    assertResponseStatusCode(ACCEPTED, response.getStatusInfo());
    assertTrue(response.getHeaders().getFirst(HttpHeaders.LOCATION) != null);
    String locURL = (String) response.getHeaders().getFirst(HttpHeaders.LOCATION);
    assertTrue(locURL.contains("/apps/application"));
    appId = locURL.substring(locURL.indexOf("/apps/") + "/apps/".length());
    locURL = "/ws/v1/cluster" + locURL.substring(locURL.indexOf("/apps/"));

    WebTarget res = target(locURL);
    res = res.queryParam("user.name", webserviceUserName);
    response = res.request().get();
    assertResponseStatusCode(OK, response.getStatusInfo());

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
    assertTrue(cs
        .getAllSecretKeys().contains(key), "Secrets missing from credentials object");
    assertEquals("mysecret", new String(cs.getSecretKey(key), StandardCharsets.UTF_8));

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
        this.constructWebResource("apps", appId).request(acceptMedia)
        .get(Response.class);
    assertResponseStatusCode(OK, response.getStatusInfo());
  }

  public void testAppSubmitErrors(String acceptMedia, String contentMedia)
      throws Exception {

    // submit a bunch of bad requests(correct format but bad values) via the
    // REST API and make sure we get the right error response codes

    String urlPath = "apps";
    ApplicationSubmissionContextInfo appInfo = new ApplicationSubmissionContextInfo();
    Response response =
        this.constructWebResource(urlPath).request(acceptMedia)
          .post(Entity.entity(appInfo, contentMedia), Response.class);
    validateResponseStatus(response, BAD_REQUEST);

    String appId = "random";
    appInfo.setApplicationId(appId);
    response =
        this.constructWebResource(urlPath).request(acceptMedia)
          .post(Entity.entity(appInfo, contentMedia), Response.class);
    validateResponseStatus(response, BAD_REQUEST);

    appId = "random_junk";
    appInfo.setApplicationId(appId);
    response =
        this.constructWebResource(urlPath).request(acceptMedia)
        .post(Entity.entity(appInfo, contentMedia), Response.class);
    validateResponseStatus(response, BAD_REQUEST);

    // bad resource info
    appInfo.getResource().setMemory(
      rm.getConfig().getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB) + 1);
    appInfo.getResource().setvCores(1);
    response =
        this.constructWebResource(urlPath).request(acceptMedia)
        .post(Entity.entity(appInfo, contentMedia), Response.class);

    validateResponseStatus(response, BAD_REQUEST);

    appInfo.getResource().setvCores(
      rm.getConfig().getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES) + 1);
    appInfo.getResource().setMemory(CONTAINER_MB);
    response =
        this.constructWebResource(urlPath).request(acceptMedia)
        .post(Entity.entity(appInfo, contentMedia), Response.class);
    validateResponseStatus(response, BAD_REQUEST);
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testAppSubmitBadJsonAndXML(int run) throws Exception {
    initTestRMWebServicesAppsModification(run);
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
    Response response =
        this.constructWebResource(urlPath).request(MediaType.APPLICATION_XML)
        .post(Entity.entity(body, MediaType.APPLICATION_XML), Response.class);
    assertResponseStatusCode(BAD_REQUEST, response.getStatusInfo());
    ApplicationSubmissionContextInfo aa = new ApplicationSubmissionContextInfo();
    aa.setApplicationId("a");
    aa.setApplicationName("b");
    body = "application-submission-context:\"{\"a\" : \"b\"}\"";
    response =
        this.constructWebResource(urlPath).request(MediaType.APPLICATION_XML)
        .post(Entity.entity(aa, MediaType.APPLICATION_JSON), Response.class);
    validateResponseStatus(response, BAD_REQUEST);
    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testGetAppQueue(int run) throws Exception {
    initTestRMWebServicesAppsModification(run);
    boolean isCapacityScheduler =
        rm.getResourceScheduler() instanceof CapacityScheduler;
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    String[] contentTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    for (String contentType : contentTypes) {
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
              .withAppName("")
              .withUser(webserviceUserName)
              .build();
      RMApp app = MockRMAppSubmitter.submit(rm, data);
      amNodeManager.nodeHeartbeat(true);
      Response response = this.constructWebResource("apps", app.getApplicationId().toString(),
          "queue").request(contentType).get(Response.class);
      assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
      String expectedQueue = "root.default";
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

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  @Timeout(value = 90)
  public void testUpdateAppPriority(int run) throws Exception {
    initTestRMWebServicesAppsModification(run);

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
    csconf.setQueues(ROOT, queues);
    csconf.setCapacity(DEFAULT, 50.0f);
    csconf.setCapacity(TEST, 50.0f);
    csconf.setAcl(ROOT, QueueACL.ADMINISTER_QUEUE, "someuser");
    csconf.setAcl(DEFAULT, QueueACL.ADMINISTER_QUEUE, "someuser");
    csconf.setAcl(TEST, QueueACL.ADMINISTER_QUEUE, "someuser");
    rm.getResourceScheduler().reinitialize(csconf, rm.getRMContext());

    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    MediaType[] contentTypes =
        { MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_XML_TYPE };
    for (String mediaType : mediaTypes) {
      for (MediaType contentType : contentTypes) {
        MockRMAppSubmissionData data1 =
            MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
                .withAppName("")
                .withUser(webserviceUserName)
                .build();
        RMApp app = MockRMAppSubmitter.submit(rm, data1);
        amNodeManager.nodeHeartbeat(true);
        int modifiedPriority = 8;
        AppPriority priority = new AppPriority(modifiedPriority);
        Object entity;
        if (contentType.equals(MediaType.APPLICATION_JSON_TYPE)) {
          entity = appPriorityToJSON(priority);
        } else {
          entity = priority;
        }
        Response response = this
            .constructWebResource("apps", app.getApplicationId().toString(),
                "priority")
            .request(mediaType)
            .put(Entity.entity(entity, contentType), Response.class);

        if (!isAuthenticationEnabled()) {
          assertResponseStatusCode(Response.Status.UNAUTHORIZED,
              response.getStatusInfo());
          continue;
        }
        assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
        if (mediaType.contains(MediaType.APPLICATION_JSON)) {
          verifyAppPriorityJson(response, modifiedPriority);
        } else {
          verifyAppPriorityXML(response, modifiedPriority);
        }

        response = this
            .constructWebResource("apps", app.getApplicationId().toString(), "priority")
            .request(mediaType).get(Response.class);
        assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
        if (mediaType.contains(MediaType.APPLICATION_JSON)) {
          verifyAppPriorityJson(response, modifiedPriority);
        } else {
          verifyAppPriorityXML(response, modifiedPriority);
        }

        // check unauthorized
        MockRMAppSubmissionData data =
            MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
                .withAppName("")
                .withUser("someuser")
                .build();
        app = MockRMAppSubmitter.submit(rm, data);
        amNodeManager.nodeHeartbeat(true);
        response = this
            .constructWebResource("apps", app.getApplicationId().toString(),
                "priority")
            .request(mediaType)
            .put(Entity.entity(entity, contentType), Response.class);
        assertResponseStatusCode(Response.Status.FORBIDDEN, response.getStatusInfo());
      }
    }
    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  @Timeout(value = 90)
  public void testAppMove(int run) throws Exception {
    initTestRMWebServicesAppsModification(run);
    boolean isCapacityScheduler =
        rm.getResourceScheduler() instanceof CapacityScheduler;

    // default root queue allows anyone to have admin acl
    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    String[] queues = { "default", "test" };
    csconf.setQueues(ROOT, queues);
    csconf.setCapacity(DEFAULT, 50.0f);
    csconf.setCapacity(TEST, 50.0f);
    csconf.setAcl(ROOT, QueueACL.ADMINISTER_QUEUE, "someuser");
    csconf.setAcl(DEFAULT, QueueACL.ADMINISTER_QUEUE, "someuser");
    csconf.setAcl(TEST, QueueACL.ADMINISTER_QUEUE, "someuser");
    rm.getResourceScheduler().reinitialize(csconf, rm.getRMContext());

    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    MediaType[] contentTypes =
        { MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_XML_TYPE };
    for (String mediaType : mediaTypes) {
      for (MediaType contentType : contentTypes) {
        MockRMAppSubmissionData data1 =
            MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
                .withAppName("")
                .withUser(webserviceUserName)
                .build();
        RMApp app = MockRMAppSubmitter.submit(rm, data1);
        amNodeManager.nodeHeartbeat(true);
        AppQueue targetQueue = new AppQueue("test");
        Object entity;
        if (contentType.equals(MediaType.APPLICATION_JSON_TYPE)) {
          entity = appQueueToJSON(targetQueue);
        } else {
          entity = targetQueue;
        }

        Response response =
            this.constructWebResource("apps", app.getApplicationId().toString(),
            "queue").request(mediaType).
            put(Entity.entity(entity, contentType), Response.class);

        if (!isAuthenticationEnabled()) {
          assertResponseStatusCode(Response.Status.UNAUTHORIZED,
              response.getStatusInfo());
          continue;
        }
        assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
        String expectedQueue = "root.test";
        if (mediaType.contains(MediaType.APPLICATION_JSON)) {
          verifyAppQueueJson(response, expectedQueue);
        } else {
          verifyAppQueueXML(response, expectedQueue);
        }
        assertEquals(expectedQueue, app.getQueue());

        // check unauthorized
        MockRMAppSubmissionData data =
            MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
                .withAppName("")
                .withUser("someuser")
                .build();
        app = MockRMAppSubmitter.submit(rm, data);
        amNodeManager.nodeHeartbeat(true);
        response = this.constructWebResource("apps", app.getApplicationId().toString(),
            "queue").request().put(Entity.entity(entity, contentType), Response.class);
        assertResponseStatusCode(Response.Status.FORBIDDEN, response.getStatusInfo());
        if(isCapacityScheduler) {
          assertEquals("root.default", app.getQueue());
        }
        else {
          assertEquals("root.someuser", app.getQueue());
        }

      }
    }
    rm.stop();
  }

  protected static String appPriorityToJSON(AppPriority targetPriority)
      throws Exception {
    StringWriter stringWriter = new StringWriter();
    APP_PRIORITY_WRITER.marshallToJSON(targetPriority, stringWriter);
    return stringWriter.toString();
  }

  protected static String appQueueToJSON(AppQueue targetQueue) throws Exception {
    StringWriter stringWriter = new StringWriter();
    APP_QUEUE_WRITER.marshallToJSON(targetQueue, stringWriter);
    return stringWriter.toString();
  }

  protected static void verifyAppPriorityJson(Response response,
      int expectedPriority) throws JSONException {
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject applicationpriority = json.getJSONObject("applicationpriority");
    int responsePriority = applicationpriority.getInt("priority");
    assertEquals(expectedPriority, responsePriority);
  }

  protected static void verifyAppPriorityXML(Response response,
      int expectedPriority)
          throws ParserConfigurationException, IOException, SAXException {
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("applicationpriority");
    assertEquals(1, nodes.getLength(), "incorrect number of elements");
    Element element = (Element) nodes.item(0);
    int responsePriority = WebServicesTestUtils.getXmlInt(element, "priority");
    assertEquals(expectedPriority, responsePriority);
  }

  protected static void verifyAppQueueJson(Response response, String queue)
      throws JSONException {
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    String responseQueue = json.getJSONObject("appqueue").getString("queue");
    assertEquals(queue, responseQueue);
  }

  protected static void verifyAppQueueXML(Response response, String queue)
      throws ParserConfigurationException, IOException, SAXException {
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("appqueue");
    assertEquals(1, nodes.getLength(), "incorrect number of elements");
    Element element = (Element) nodes.item(0);
    String responseQueue = WebServicesTestUtils.getXmlString(element, "queue");
    assertEquals(queue, responseQueue);
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  @Timeout(value = 90)
  public void testUpdateAppTimeout(int run) throws Exception {
    initTestRMWebServicesAppsModification(run);
    rm.start();
    rm.registerNode("127.0.0.1:1234", 2048);
    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    MediaType[] contentTypes =
        { MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_XML_TYPE };
    for (String mediaType : mediaTypes) {
      for (MediaType contentType : contentTypes) {
        // application submitted without timeout
        MockRMAppSubmissionData data =
            MockRMAppSubmissionData.Builder.createWithMemory(CONTAINER_MB, rm)
                .withAppName("")
                .withUser(webserviceUserName)
                .build();
        RMApp app = MockRMAppSubmitter.submit(rm, data);

        Response response =
            this.constructWebResource("apps", app.getApplicationId().toString(),
                "timeouts").request(mediaType).get(Response.class);
        if (mediaType.contains(MediaType.APPLICATION_JSON)) {
          assertEquals(
              MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
              response.getMediaType().toString());
          JSONObject js =
              response.readEntity(JSONObject.class).getJSONObject("timeouts");
          JSONObject entity = js.getJSONObject("timeout");
          verifyAppTimeoutJson(entity,
              ApplicationTimeoutType.LIFETIME, "UNLIMITED", -1);
        }

        long timeOutFromNow = 60;
        String expireTime = Times
            .formatISO8601(System.currentTimeMillis() + timeOutFromNow * 1000);
        Object entity = getAppTimeoutInfoEntity(ApplicationTimeoutType.LIFETIME,
            contentType, expireTime);
        response = this
            .constructWebResource("apps", app.getApplicationId().toString(),
                "timeout")
            .request(mediaType)
            .put(Entity.entity(entity, contentType), Response.class);

        if (!isAuthenticationEnabled()) {
          assertResponseStatusCode(Response.Status.UNAUTHORIZED,
              response.getStatusInfo());
          continue;
        }
        assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
        if (mediaType.contains(MediaType.APPLICATION_JSON)) {
          verifyAppTimeoutJson(response, ApplicationTimeoutType.LIFETIME,
              expireTime, timeOutFromNow);
        } else {
          verifyAppTimeoutXML(response, ApplicationTimeoutType.LIFETIME,
              expireTime, timeOutFromNow);
        }

        // verify for negative cases
        entity = getAppTimeoutInfoEntity(null,
            contentType, null);
        response = this
            .constructWebResource("apps", app.getApplicationId().toString(),
                "timeout")
            .request(mediaType)
            .put(Entity.entity(entity, contentType), Response.class);
        assertResponseStatusCode(BAD_REQUEST, response.getStatusInfo());

        // invoke get
        response =
            this.constructWebResource("apps", app.getApplicationId().toString(),
                "timeouts", ApplicationTimeoutType.LIFETIME.toString())
                .request(mediaType).get(Response.class);
        assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
        if (mediaType.contains(MediaType.APPLICATION_JSON)) {
          verifyAppTimeoutJson(response, ApplicationTimeoutType.LIFETIME,
              expireTime, timeOutFromNow);
        }
      }
    }
    rm.stop();
  }

  private Object getAppTimeoutInfoEntity(ApplicationTimeoutType type,
      MediaType contentType, String expireTime) throws Exception {
    AppTimeoutInfo timeoutUpdate = new AppTimeoutInfo();
    timeoutUpdate.setTimeoutType(type);
    timeoutUpdate.setExpiryTime(expireTime);

    Object entity;
    if (contentType.equals(MediaType.APPLICATION_JSON_TYPE)) {
      entity = appTimeoutToJSON(timeoutUpdate);
    } else {
      entity = timeoutUpdate;
    }
    return entity;
  }

  protected static void verifyAppTimeoutJson(Response response,
      ApplicationTimeoutType type, String expireTime, long timeOutFromNow)
      throws JSONException {
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject jsonTimeout = response.readEntity(JSONObject.class);
    assertEquals(1, jsonTimeout.length(), "incorrect number of elements");
    JSONObject json = jsonTimeout.getJSONObject("timeout");
    verifyAppTimeoutJson(json, type, expireTime, timeOutFromNow);
  }

  protected static void verifyAppTimeoutJson(JSONObject json,
      ApplicationTimeoutType type, String expireTime, long timeOutFromNow)
      throws JSONException {
    assertEquals(3, json.length(), "incorrect number of elements");
    assertEquals(type.toString(), json.getString("type"));
    assertEquals(expireTime, json.getString("expiryTime"));
    assertTrue(json.getLong("remainingTimeInSeconds") <= timeOutFromNow);
  }

  protected static void verifyAppTimeoutXML(Response response,
      ApplicationTimeoutType type, String expireTime, long timeOutFromNow)
      throws ParserConfigurationException, IOException, SAXException {
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("timeout");
    assertEquals(1, nodes.getLength(), "incorrect number of elements");
    Element element = (Element) nodes.item(0);
    assertEquals(type.toString(),
        WebServicesTestUtils.getXmlString(element, "type"));
    assertEquals(expireTime,
        WebServicesTestUtils.getXmlString(element, "expiryTime"));
    assertTrue(WebServicesTestUtils.getXmlLong(element,
        "remainingTimeInSeconds") < timeOutFromNow);
  }

  protected static String appTimeoutToJSON(AppTimeoutInfo timeout)
      throws Exception {
    StringWriter stringWriter = new StringWriter();
    APP_TIMEOUT_WRITER.marshallToJSON(timeout, stringWriter);
    return stringWriter.toString();
  }
}
