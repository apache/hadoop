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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.security.Principal;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer.NMWebApp;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.inject.Guice;
import com.google.inject.Scopes;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.ServletModule;

public class TestNMWebServicesApps extends JerseyTest {

  private static Context nmContext;
  private static ResourceView resourceView;
  private static ApplicationACLsManager aclsManager;
  private static LocalDirsHandlerService dirsHandler;
  private static WebApp nmWebApp;
  private static Configuration conf = new Configuration();

  private static final File testRootDir = new File("target",
      TestNMWebServicesApps.class.getSimpleName());
  private static File testLogDir = new File("target",
      TestNMWebServicesApps.class.getSimpleName() + "LogDir");

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      conf.set(YarnConfiguration.NM_LOCAL_DIRS, testRootDir.getAbsolutePath());
      conf.set(YarnConfiguration.NM_LOG_DIRS, testLogDir.getAbsolutePath());
      LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
      NodeHealthCheckerService healthChecker =
          new NodeHealthCheckerService(dirsHandler);
      healthChecker.init(conf);
      dirsHandler = healthChecker.getDiskHandler();
      aclsManager = new ApplicationACLsManager(conf);
      nmContext = new NodeManager.NMContext(null, null, dirsHandler,
          aclsManager, null, false, conf);
      NodeId nodeId = NodeId.newInstance("testhost.foo.com", 9999);
      ((NodeManager.NMContext)nmContext).setNodeId(nodeId);
      resourceView = new ResourceView() {
        @Override
        public long getVmemAllocatedForContainers() {
          // 15.5G in bytes
          return new Long("16642998272");
        }

        @Override
        public long getPmemAllocatedForContainers() {
          // 16G in bytes
          return new Long("17179869184");
        }

        @Override
        public long getVCoresAllocatedForContainers() {
          return new Long("4000");
        }


        @Override
        public boolean isVmemCheckEnabled() {
          return true;
        }

        @Override
        public boolean isPmemCheckEnabled() {
          return true;
        }
      };
      nmWebApp = new NMWebApp(resourceView, aclsManager, dirsHandler);
      bind(JAXBContextResolver.class);
      bind(NMWebServices.class);
      bind(GenericExceptionHandler.class);
      bind(Context.class).toInstance(nmContext);
      bind(WebApp.class).toInstance(nmWebApp);
      bind(ResourceView.class).toInstance(resourceView);
      bind(ApplicationACLsManager.class).toInstance(aclsManager);
      bind(LocalDirsHandlerService.class).toInstance(dirsHandler);
      bind(GuiceFilter.class).in(Scopes.SINGLETON);
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
    testRootDir.mkdirs();
    testLogDir.mkdir();
  }

  @AfterClass
  static public void cleanup() {
    FileUtil.fullyDelete(testRootDir);
    FileUtil.fullyDelete(testLogDir);
  }

  public TestNMWebServicesApps() {
    /* TODO: Remove or fix this
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.nodemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
     */
  }

  @Test
  public void testNodeAppsNone() throws Exception {
    WebTarget t = target();
    Response response = t.path("ws").path("v1").path("node").path("apps")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("apps isn't empty",
        new JSONObject().toString(), json.get("apps").toString());
  }

  private HashMap<String, String> addAppContainers(Application app) 
      throws IOException {
    return addAppContainers(app, nmContext);
  }

  private HashMap<String, String> addAppContainers(Application app,
      Context context) throws IOException {
    Dispatcher dispatcher = new AsyncDispatcher();
    ApplicationAttemptId appAttemptId = BuilderUtils
        .newApplicationAttemptId(app.getAppId(), 1);
    Container container1 = new MockContainer(appAttemptId, dispatcher, conf,
        app.getUser(), app.getAppId(), 1);
    Container container2 = new MockContainer(appAttemptId, dispatcher, conf,
        app.getUser(), app.getAppId(), 2);
    context.getContainers().put(container1.getContainerId(), container1);
    context.getContainers().put(container2.getContainerId(), container2);

    app.getContainers().put(container1.getContainerId(), container1);
    app.getContainers().put(container2.getContainerId(), container2);
    HashMap<String, String> hash = new HashMap<String, String>();
    hash.put(container1.getContainerId().toString(),
        container1.getContainerId().toString());
    hash.put(container2.getContainerId().toString(),
        container2.getContainerId().toString());
    return hash;
  }

  @Test
  public void testNodeApps() throws Exception {
    testNodeHelper("apps", MediaType.APPLICATION_JSON);
  }

  @Test
  public void testNodeAppsSlash() throws Exception {
    testNodeHelper("apps/", MediaType.APPLICATION_JSON);
  }

  // make sure default is json output
  @Test
  public void testNodeAppsDefault() throws Exception {
    testNodeHelper("apps/", "");

  }

  public void testNodeHelper(String path, String media) throws JSONException,
      Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    HashMap<String, String> hash = addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    HashMap<String, String> hash2 = addAppContainers(app2);

    Response response = t.path("ws").path("v1").path("node").path(path)
        .request(media).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    JSONObject info = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, info.length());
    JSONArray appInfo = info.getJSONArray("app");
    assertEquals("incorrect number of elements", 2, appInfo.length());
    String id = appInfo.getJSONObject(0).getString("id");
    if (id.matches(app.getAppId().toString())) {
      verifyNodeAppInfo(appInfo.getJSONObject(0), app, hash);
      verifyNodeAppInfo(appInfo.getJSONObject(1), app2, hash2);
    } else {
      verifyNodeAppInfo(appInfo.getJSONObject(0), app2, hash2);
      verifyNodeAppInfo(appInfo.getJSONObject(1), app, hash);
    }
  }

  @Test
  public void testNodeAppsUser() throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    HashMap<String, String> hash = addAppContainers(app);
    Application app2 = new MockApp("foo", 1234, 2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    Response response = t.path("ws").path("v1").path("node").path("apps")
        .queryParam("user", "mockUser").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);

    JSONObject info = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, info.length());
    JSONArray appInfo = info.getJSONArray("app");
    assertEquals("incorrect number of elements", 1, appInfo.length());
    verifyNodeAppInfo(appInfo.getJSONObject(0), app, hash);
  }

  @Test
  public void testNodeAppsUserNone() throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp("foo", 1234, 2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    Response response = t.path("ws").path("v1").path("node").path("apps")
        .queryParam("user", "george").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("apps is not empty",
        new JSONObject().toString(), json.get("apps").toString());
  }

  @Test
  public void testNodeAppsUserEmpty() throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp("foo", 1234, 2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    try {
      t.path("ws").path("v1").path("node").path("apps").queryParam("user", "")
          .request(MediaType.APPLICATION_JSON).get(JSONObject.class);
      fail("should have thrown exception on invalid user query");
    } catch (WebApplicationException we) {
      Response response = we.getResponse();

      assertResponseStatusCode(Response.Status.BAD_REQUEST,
          response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject msg = response.readEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils
          .checkStringMatch(
              "exception message",
              "java.lang.Exception: Error: You must specify a non-empty string for the user",
              message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
    }
  }

  @Test
  public void testNodeAppsState() throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    MockApp app2 = new MockApp("foo", 1234, 2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    HashMap<String, String> hash2 = addAppContainers(app2);
    app2.setState(ApplicationState.RUNNING);

    Response response = t.path("ws").path("v1").path("node").path("apps")
        .queryParam("state", ApplicationState.RUNNING.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);

    JSONObject info = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, info.length());
    JSONArray appInfo = info.getJSONArray("app");
    assertEquals("incorrect number of elements", 1, appInfo.length());
    verifyNodeAppInfo(appInfo.getJSONObject(0), app2, hash2);

  }

  @Test
  public void testNodeAppsStateNone() throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp("foo", 1234, 2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    Response response = t.path("ws").path("v1").path("node").path("apps")
        .queryParam("state", ApplicationState.INITING.toString())
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);

    assertEquals("apps is not empty",
        new JSONObject().toString(), json.get("apps").toString());
  }

  @Test
  public void testNodeAppsStateInvalid() throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp("foo", 1234, 2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    try {
      t.path("ws").path("v1").path("node").path("apps")
          .queryParam("state", "FOO_STATE").request(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid user query");
    } catch (WebApplicationException we) {
      Response response = we.getResponse();

      assertResponseStatusCode(Response.Status.BAD_REQUEST,
          response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject msg = response.readEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      verifyStateInvalidException(message, type, classname);
    }
  }

  // verify the exception object default format is JSON
  @Test
  public void testNodeAppsStateInvalidDefault() throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp("foo", 1234, 2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    try {
      t.path("ws").path("v1").path("node").path("apps")
          .queryParam("state", "FOO_STATE")
          .request().get(JSONObject.class);
      fail("should have thrown exception on invalid user query");
    } catch (WebApplicationException we) {
      Response response = we.getResponse();

      assertResponseStatusCode(Response.Status.BAD_REQUEST,
          response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject msg = response.readEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      verifyStateInvalidException(message, type, classname);
    }
  }

  // test that the exception output also returns XML
  @Test
  public void testNodeAppsStateInvalidXML() throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp("foo", 1234, 2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    try {
      t.path("ws").path("v1").path("node").path("apps")
          .queryParam("state", "FOO_STATE").request(MediaType.APPLICATION_XML)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid user query");
    } catch (WebApplicationException we) {
      Response response = we.getResponse();

      assertResponseStatusCode(Response.Status.BAD_REQUEST,
          response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String msg = response.readEntity(String.class);

      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(msg));
      Document dom = db.parse(is);
      NodeList nodes = dom.getElementsByTagName("RemoteException");
      Element element = (Element) nodes.item(0);
      String message = WebServicesTestUtils.getXmlString(element, "message");
      String type = WebServicesTestUtils.getXmlString(element, "exception");
      String classname = WebServicesTestUtils.getXmlString(element,
          "javaClassName");
      verifyStateInvalidException(message, type, classname);
    }
  }

  private void verifyStateInvalidException(String message, String type,
      String classname) {
    WebServicesTestUtils
        .checkStringContains(
            "exception message",
            "org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState.FOO_STATE",
            message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "IllegalArgumentException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "java.lang.IllegalArgumentException", classname);
  }

  @Test
  public void testNodeSingleApps() throws Exception {
    testNodeSingleAppHelper(MediaType.APPLICATION_JSON);
  }

  // make sure default is json output
  @Test
  public void testNodeSingleAppsDefault() throws Exception {
    testNodeSingleAppHelper("");
  }

  public void testNodeSingleAppHelper(String media) throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    HashMap<String, String> hash = addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    Response response = t.path("ws").path("v1").path("node").path("apps")
        .path(app.getAppId().toString()).request(media)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyNodeAppInfo(json.getJSONObject("app"), app, hash);
  }

  @Test
  public void testNodeSingleAppsSlash() throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    HashMap<String, String> hash = addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);
    Response response = t.path("ws").path("v1").path("node").path("apps")
        .path(app.getAppId().toString() + "/")
        .request(MediaType.APPLICATION_JSON).get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyNodeAppInfo(json.getJSONObject("app"), app, hash);
  }

  @Test
  public void testNodeSingleAppsInvalid() throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    try {
      t.path("ws").path("v1").path("node").path("apps").path("app_foo_0000")
          .request(MediaType.APPLICATION_JSON).get(JSONObject.class);
      fail("should have thrown exception on invalid user query");
    } catch (WebApplicationException we) {
      Response response = we.getResponse();
      assertResponseStatusCode(Response.Status.BAD_REQUEST,
          response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject msg = response.readEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.IllegalArgumentException: Invalid ApplicationId prefix: "
              + "app_foo_0000. The valid ApplicationId should start with prefix"
              + " application",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
    }
  }

  @Test
  public void testNodeSingleAppsMissing() throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    try {
      t.path("ws").path("v1").path("node").path("apps")
          .path("application_1234_0009").request(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid user query");
    } catch (WebApplicationException we) {
      Response response = we.getResponse();
      assertResponseStatusCode(Response.Status.NOT_FOUND,
          response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject msg = response.readEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.Exception: app with id application_1234_0009 not found",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "NotFoundException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    }
  }

  @Test
  public void testNodeAppsXML() throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    Response response = t.path("ws").path("v1").path("node").path("apps")
        .request(MediaType.APPLICATION_XML).get(Response.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("app");
    assertEquals("incorrect number of elements", 2, nodes.getLength());
  }

  @Test
  public void testNodeSingleAppsXML() throws Exception {
    WebTarget t = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    HashMap<String, String> hash = addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    Response response = t.path("ws").path("v1").path("node").path("apps")
        .path(app.getAppId().toString() + "/")
        .request(MediaType.APPLICATION_XML).get(Response.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("app");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    verifyNodeAppInfoXML(nodes, app, hash);
  }

  public void verifyNodeAppInfoXML(NodeList nodes, Application app,
      HashMap<String, String> hash) throws JSONException, Exception {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyNodeAppInfoGeneric(app,
          WebServicesTestUtils.getXmlString(element, "id"),
          WebServicesTestUtils.getXmlString(element, "state"),
          WebServicesTestUtils.getXmlString(element, "user"));

      NodeList ids = element.getElementsByTagName("containerids");
      for (int j = 0; j < ids.getLength(); j++) {
        Element line = (Element) ids.item(j);
        Node first = line.getFirstChild();
        String val = first.getNodeValue();
        assertEquals("extra containerid: " + val, val, hash.remove(val));
      }
      assertTrue("missing containerids", hash.isEmpty());
    }
  }

  public void verifyNodeAppInfo(JSONObject info, Application app,
      HashMap<String, String> hash) throws JSONException, Exception {
    assertEquals("incorrect number of elements", 4, info.length());

    verifyNodeAppInfoGeneric(app, info.getString("id"),
        info.getString("state"), info.getString("user"));

    JSONArray containerids = info.getJSONArray("containerids");
    for (int i = 0; i < containerids.length(); i++) {
      String id = containerids.getString(i);
      assertEquals("extra containerid: " + id, id, hash.remove(id));
    }
    assertTrue("missing containerids", hash.isEmpty());
  }

  public void verifyNodeAppInfoGeneric(Application app, String id,
      String state, String user) throws JSONException, Exception {
    WebServicesTestUtils.checkStringMatch("id", app.getAppId().toString(), id);
    WebServicesTestUtils.checkStringMatch("state", app.getApplicationState()
        .toString(), state);
    WebServicesTestUtils.checkStringMatch("user", app.getUser().toString(),
        user);
  }

  @Test
  public void testNodeAppsUserFiltering() throws Exception {
    Configuration yarnConf = new Configuration();
    yarnConf.setBoolean(YarnConfiguration.FILTER_ENTITY_LIST_BY_USER, true);
    yarnConf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    yarnConf.setStrings(YarnConfiguration.YARN_ADMIN_ACL, "admin");
    ApplicationACLsManager aclManager = new ApplicationACLsManager(yarnConf);

    NMContext context = new NodeManager.NMContext(null, null, dirsHandler,
        aclManager, null, false, yarnConf);
    Application app = new MockApp(1);
    context.getApplications().put(app.getAppId(), app);
    addAppContainers(app, context);
    Application app2 = new MockApp("foo", 1234, 2);
    context.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2, context);

    // User "foo" could only see its own apps/containers.
    NMWebServices webSvc = new NMWebServices(context, null, nmWebApp,
        mock(HttpServletResponse.class));
    HttpServletRequest mockHsr = mockHttpServletRequestByUserName("foo");
    AppsInfo appsInfo = webSvc.getNodeApps(mockHsr, null, null);
    assertEquals(1, appsInfo.getApps().size());

    // Admin could see all apps and containers.
    HttpServletRequest mockHsrAdmin = mockHttpServletRequestByUserName("admin");
    AppsInfo appsInfo2 = webSvc.getNodeApps(mockHsrAdmin, null, null);
    assertEquals(2, appsInfo2.getApps().size());
  }

  private HttpServletRequest mockHttpServletRequestByUserName(String username) {
    HttpServletRequest mockHsr = mock(HttpServletRequest.class);
    when(mockHsr.getRemoteUser()).thenReturn(username);
    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn(username);
    when(mockHsr.getUserPrincipal()).thenReturn(principal);
    return mockHsr;
  }
}
