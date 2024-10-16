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
import static org.apache.hadoop.yarn.util.StringHelper.ujoin;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.http.JettyUtils;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer.NMWebApp;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class TestNMWebServicesContainers extends JerseyTest {

  private static Context nmContext;
  private static ResourceView resourceView;
  private static ApplicationACLsManager aclsManager;
  private static LocalDirsHandlerService dirsHandler;
  private static WebApp nmWebApp;
  private static Configuration conf = new Configuration();

  private static final File testRootDir = new File("target",
      TestNMWebServicesContainers.class.getSimpleName());
  private static File testLogDir = new File("target",
      TestNMWebServicesContainers.class.getSimpleName() + "LogDir");

  @Override
  protected javax.ws.rs.core.Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(NMWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    forceSet(TestProperties.CONTAINER_PORT, "9999");
    return config;
  }

  private static class JerseyBinder extends AbstractBinder {

    @Override
    protected void configure() {
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
      conf.set(YarnConfiguration.NM_LOCAL_DIRS, testRootDir.getAbsolutePath());
      conf.set(YarnConfiguration.NM_LOG_DIRS, testLogDir.getAbsolutePath());
      LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
      NodeHealthCheckerService healthChecker =
              new NodeHealthCheckerService(dirsHandler);
      healthChecker.init(conf);
      dirsHandler = healthChecker.getDiskHandler();
      aclsManager = new ApplicationACLsManager(conf);
      nmContext = new NodeManager.NMContext(null, null, dirsHandler,
              aclsManager, null, false, conf) {
        public NodeId getNodeId() {
          return NodeId.newInstance("testhost.foo.com", 8042);
        };

        public int getHttpPort() {
          return 1234;
        };
      };

      nmWebApp = new NMWebApp(resourceView, aclsManager, dirsHandler);
      final HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getQueryString()).thenReturn("?user.name=user&nm.id=localhost:1111");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(nmContext).to(Context.class).named("nm");
      bind(nmWebApp).to(WebApp.class).named("webapp");
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
      bind(aclsManager).to(ApplicationACLsManager.class);
      bind(dirsHandler).to(LocalDirsHandlerService.class);
      bind(resourceView).to(ResourceView.class).named("view");
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Before
  public void before() throws Exception {
    testRootDir.mkdirs();
    testLogDir.mkdir();
  }


  @AfterClass
  static public void cleanup() {
    FileUtil.fullyDelete(testRootDir);
    FileUtil.fullyDelete(testLogDir);
  }

  public TestNMWebServicesContainers() {
  }

  @Test
  public void testNodeContainersNone() throws JSONException, Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("node")
        .path("containers").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("apps isn't empty", "{\"containers\":\"\"}", json.toString());
  }

  private HashMap<String, String> addAppContainers(Application app) 
      throws IOException {
    Dispatcher dispatcher = new AsyncDispatcher();
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        app.getAppId(), 1);
    Container container1 = new MockContainer(appAttemptId, dispatcher, conf,
        app.getUser(), app.getAppId(), 1);
    ((MockContainer)container1).setState(ContainerState.RUNNING);
    Container container2 = new MockContainer(appAttemptId, dispatcher, conf,
        app.getUser(), app.getAppId(), 2);
    ((MockContainer)container2).setState(ContainerState.RUNNING);
    nmContext.getContainers()
        .put(container1.getContainerId(), container1);
    nmContext.getContainers()
        .put(container2.getContainerId(), container2);
    File appDir = new File(testLogDir + "/" + app.getAppId().toString());
    appDir.mkdir();
    File container1Dir =
        new File(appDir + "/" + container1.getContainerId().toString());
    container1Dir.mkdir();
    // Create log files for containers.
    new File(container1Dir + "/" + "syslog").createNewFile();
    new File(container1Dir + "/" + "stdout").createNewFile();
    File container2Dir =
        new File(appDir + "/" + container2.getContainerId().toString());
    container2Dir.mkdir();
    new File(container2Dir + "/" + "syslog").createNewFile();
    new File(container2Dir + "/" + "stdout").createNewFile();

    app.getContainers().put(container1.getContainerId(), container1);
    app.getContainers().put(container2.getContainerId(), container2);
    HashMap<String, String> hash = new HashMap<>();
    hash.put(container1.getContainerId().toString(), container1
        .getContainerId().toString());
    hash.put(container2.getContainerId().toString(), container2
        .getContainerId().toString());
    return hash;
  }

  @Test
  public void testNodeContainers() throws JSONException, Exception {
    testNodeHelper("containers", MediaType.APPLICATION_JSON);
  }

  @Test
  public void testNodeContainersSlash() throws JSONException, Exception {
    testNodeHelper("containers/", MediaType.APPLICATION_JSON);
  }

  // make sure default is json output
  @Test
  public void testNodeContainersDefault() throws JSONException, Exception {
    testNodeHelper("containers/", "");

  }

  public void testNodeHelper(String path, String media) throws JSONException,
      Exception {
    WebTarget r = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    Response response = r.path("ws").path("v1").path("node").path(path)
        .request(media).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    JSONObject info = json.getJSONObject("containers");
    assertEquals("incorrect number of elements", 1, info.length());
    JSONArray conInfo = info.getJSONArray("container");
    assertEquals("incorrect number of elements", 4, conInfo.length());

    for (int i = 0; i < conInfo.length(); i++) {
      verifyNodeContainerInfo(conInfo.getJSONObject(i),
          nmContext.getContainers().get(
          ContainerId.fromString(conInfo.getJSONObject(i).getString("id"))));
    }
  }

  @Test
  public void testNodeSingleContainers() throws JSONException, Exception {
    testNodeSingleContainersHelper(MediaType.APPLICATION_JSON);
  }

  @Test
  public void testNodeSingleContainersSlash() throws Exception {
    testNodeSingleContainersHelper(MediaType.APPLICATION_JSON);
  }

  @Test
  public void testNodeSingleContainersDefault() throws Exception {
    testNodeSingleContainersHelper("");
  }

  public void testNodeSingleContainersHelper(String media) throws Exception {
    WebTarget target = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    HashMap<String, String> hash = addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    for (String id : hash.keySet()) {
      Response response = target.path("ws").path("v1").path("node")
          .path("containers").path(id).request(media).get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String entity = response.readEntity(String.class);
      JSONObject json = new JSONObject(entity);
      verifyNodeContainerInfo(json.getJSONObject("container"), nmContext
          .getContainers().get(ContainerId.fromString(id)));
    }
  }

  @Test
  public void testSingleContainerInvalid() throws Exception {
    WebTarget target = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    Response response = target.path("ws").path("v1").path("node").path("containers")
        .path("container_foo_1234").request(MediaType.APPLICATION_JSON).get();
    assertResponseStatusCode(Response.Status.BAD_REQUEST, response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject msg = new JSONObject(entity);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 4, exception.length());
    String message = exception.getString("message");
    String cause = exception.getString("cause");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils.checkStringMatch("exception message", "HTTP 400 Bad Request", message);
    WebServicesTestUtils.checkStringMatch("exception cause",
        "invalid container id, container_foo_1234", cause);
    WebServicesTestUtils.checkStringMatch("exception type", "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
  }

  @Test
  public void testSingleContainerInvalid2() throws JSONException, Exception {
    WebTarget r = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    Response response = r.path("ws").path("v1").path("node").path("containers")
        .path("container_1234_0001").request(MediaType.APPLICATION_JSON).get();
    assertResponseStatusCode(Response.Status.BAD_REQUEST, response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject msg = new JSONObject(entity);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 4, exception.length());
    String message = exception.getString("message");
    String cause = exception.getString("cause");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils.checkStringMatch("exception message", "HTTP 400 Bad Request", message);
    WebServicesTestUtils.checkStringMatch("exception cause",
        "invalid container id, container_1234_0001", cause);
    WebServicesTestUtils.checkStringMatch("exception type", "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
  }

  @Test
  public void testSingleContainerWrong() throws Exception {
    WebTarget target = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    Response response = target.path("ws").path("v1").path("node").path("containers")
        .path("container_1234_0001_01_000005")
        .request(MediaType.APPLICATION_JSON).get();
    assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject msg = new JSONObject(entity);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 4, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils.checkStringMatch("exception message",
        "java.lang.Exception: container with id, container_1234_0001_01_000005, not found",
        message);
    WebServicesTestUtils.checkStringMatch("exception type", "NotFoundException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
  }

  @Test
  public void testNodeSingleContainerXML() throws JSONException, Exception {
    WebTarget r = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    HashMap<String, String> hash = addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    for (String id : hash.keySet()) {
      Response response = r.path("ws").path("v1").path("node")
          .path("containers").path(id).request(MediaType.APPLICATION_XML)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String xml = response.readEntity(String.class);
      DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      Document dom = db.parse(is);
      NodeList nodes = dom.getElementsByTagName("container");
      assertEquals("incorrect number of elements", 1, nodes.getLength());
      verifyContainersInfoXML(nodes,
          nmContext.getContainers().get(ContainerId.fromString(id)));
    }
  }

  @Test
  public void testNodeContainerXML() throws JSONException, Exception {
    WebTarget r = target();
    Application app = new MockApp(1);
    nmContext.getApplications().put(app.getAppId(), app);
    addAppContainers(app);
    Application app2 = new MockApp(2);
    nmContext.getApplications().put(app2.getAppId(), app2);
    addAppContainers(app2);

    Response response = r.path("ws").path("v1").path("node")
        .path("containers").request(MediaType.APPLICATION_XML)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("container");
    assertEquals("incorrect number of elements", 4, nodes.getLength());
  }

  public void verifyContainersInfoXML(NodeList nodes, Container cont)
      throws JSONException, Exception {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyNodeContainerInfoGeneric(cont,
          WebServicesTestUtils.getXmlString(element, "id"),
          WebServicesTestUtils.getXmlString(element, "state"),
          WebServicesTestUtils.getXmlString(element, "user"),
          WebServicesTestUtils.getXmlInt(element, "exitCode"),
          WebServicesTestUtils.getXmlString(element, "diagnostics"),
          WebServicesTestUtils.getXmlString(element, "nodeId"),
          WebServicesTestUtils.getXmlInt(element, "totalMemoryNeededMB"),
          WebServicesTestUtils.getXmlInt(element, "totalVCoresNeeded"),
          WebServicesTestUtils.getXmlString(element, "containerLogsLink"));
      // verify that the container log files element exists
      List<String> containerLogFiles =
          WebServicesTestUtils.getXmlStrings(element, "containerLogFiles");
      assertFalse("containerLogFiles missing",containerLogFiles.isEmpty());
      assertEquals(2, containerLogFiles.size());
      assertTrue("syslog and stdout expected",
          containerLogFiles.contains("syslog") &&
          containerLogFiles.contains("stdout"));
    }
  }

  public void verifyNodeContainerInfo(JSONObject info, Container cont)
      throws JSONException, Exception {
    assertEquals("incorrect number of elements", 11, info.length());

    verifyNodeContainerInfoGeneric(cont, info.getString("id"),
        info.getString("state"), info.getString("user"),
        info.getInt("exitCode"), info.getString("diagnostics"),
        info.getString("nodeId"), info.getInt("totalMemoryNeededMB"),
        info.getInt("totalVCoresNeeded"),
        info.getString("containerLogsLink"));
    // verify that the container log files element exists
    JSONArray containerLogFilesArr = info.getJSONArray("containerLogFiles");
    assertTrue("containerLogFiles missing", containerLogFilesArr != null);
    assertEquals(2, containerLogFilesArr.length());
    for (int i = 0; i < 2; i++) {
      assertTrue("syslog and stdout expected",
          containerLogFilesArr.get(i).equals("syslog") ||
          containerLogFilesArr.get(i).equals("stdout"));
    }
  }

  public void verifyNodeContainerInfoGeneric(Container cont, String id,
      String state, String user, int exitCode, String diagnostics,
      String nodeId, int totalMemoryNeededMB, int totalVCoresNeeded,
      String logsLink)
      throws JSONException, Exception {
    WebServicesTestUtils.checkStringMatch("id", cont.getContainerId()
        .toString(), id);
    WebServicesTestUtils.checkStringMatch("state", cont.getContainerState()
        .toString(), state);
    WebServicesTestUtils.checkStringMatch("user", cont.getUser().toString(),
        user);
    assertEquals("exitCode wrong", 0, exitCode);
    WebServicesTestUtils
        .checkStringMatch("diagnostics", "testing", diagnostics);

    WebServicesTestUtils.checkStringMatch("nodeId", nmContext.getNodeId()
        .toString(), nodeId);
    assertEquals("totalMemoryNeededMB wrong",
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      totalMemoryNeededMB);
    assertEquals("totalVCoresNeeded wrong",
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
      totalVCoresNeeded);
    String shortLink =
        ujoin("containerlogs", cont.getContainerId().toString(),
            cont.getUser());
    assertTrue("containerLogsLink wrong", logsLink.contains(shortLink));
  }
}
