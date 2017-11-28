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

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.ContainerLogFileInfo;
import org.apache.hadoop.yarn.logaggregation.TestContainerLogsUtils;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.AssignedGpuDevice;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDevice;
import org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer.NMWebApp;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.GpuDeviceInformation;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.NMGpuResourceInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.PerGpuDeviceInformation;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.webapp.YarnWebServiceParams;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfo;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test the nodemanager node info web services api's
 */
public class TestNMWebServices extends JerseyTestBase {

  private static NodeManager.NMContext nmContext;
  private static ResourceView resourceView;
  private static ApplicationACLsManager aclsManager;
  private static LocalDirsHandlerService dirsHandler;
  private static WebApp nmWebApp;
  private static final String LOGSERVICEWSADDR = "test:1234";

  private static final File testRootDir = new File("target",
      TestNMWebServices.class.getSimpleName());
  private static File testLogDir = new File("target",
      TestNMWebServices.class.getSimpleName() + "LogDir");
  private static File testRemoteLogDir = new File("target",
      TestNMWebServices.class.getSimpleName() + "remote-log-dir");

  private static class WebServletModule extends ServletModule {

    @Override
    protected void configureServlets() {
      Configuration conf = new Configuration();
      conf.set(YarnConfiguration.NM_LOCAL_DIRS, testRootDir.getAbsolutePath());
      conf.set(YarnConfiguration.NM_LOG_DIRS, testLogDir.getAbsolutePath());
      conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
      conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
          testRemoteLogDir.getAbsolutePath());
      conf.set(YarnConfiguration.YARN_LOG_SERVER_WEBSERVICE_URL,
          LOGSERVICEWSADDR);
      dirsHandler = new LocalDirsHandlerService();
      NodeHealthCheckerService healthChecker = new NodeHealthCheckerService(
          NodeManager.getNodeHealthScriptRunner(conf), dirsHandler);
      healthChecker.init(conf);
      aclsManager = new ApplicationACLsManager(conf);
      nmContext = new NodeManager.NMContext(null, null, dirsHandler,
          aclsManager, null, false, conf);
      NodeId nodeId = NodeId.newInstance("testhost.foo.com", 8042);
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

      serve("/*").with(GuiceContainer.class);
    }
  };

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    testRemoteLogDir.mkdir();
    testRootDir.mkdirs();
    testLogDir.mkdir();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @AfterClass
  static public void stop() {
    FileUtil.fullyDelete(testRootDir);
    FileUtil.fullyDelete(testLogDir);
    FileUtil.fullyDelete(testRemoteLogDir);
  }

  public TestNMWebServices() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.nodemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testInvalidUri() throws JSONException, Exception {
    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr = r.path("ws").path("v1").path("node").path("bogus")
          .accept(MediaType.APPLICATION_JSON).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testInvalidAccept() throws JSONException, Exception {
    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr = r.path("ws").path("v1").path("node")
          .accept(MediaType.TEXT_PLAIN).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.INTERNAL_SERVER_ERROR,
          response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testInvalidUri2() throws JSONException, Exception {
    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr = r.accept(MediaType.APPLICATION_JSON).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testNode() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyNodeInfo(json);
  }

  @Test
  public void testNodeSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node/")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyNodeInfo(json);
  }

  // make sure default is json output
  @Test
  public void testNodeDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node")
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyNodeInfo(json);
  }

  @Test
  public void testNodeInfo() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node").path("info")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyNodeInfo(json);
  }

  @Test
  public void testNodeInfoSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node")
        .path("info/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyNodeInfo(json);
  }

  // make sure default is json output
  @Test
  public void testNodeInfoDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node").path("info")
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyNodeInfo(json);
  }

  @Test
  public void testSingleNodesXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node")
        .path("info/").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML+ "; " + JettyUtils.UTF_8,
        response.getType().toString());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("nodeInfo");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    verifyNodesXML(nodes);
  }

  @Test (timeout = 5000)
  public void testContainerLogsWithNewAPI() throws IOException, JSONException{
    final ContainerId containerId = BuilderUtils.newContainerId(0, 0, 0, 0);
    WebResource r = resource();
    r = r.path("ws").path("v1").path("node").path("containers")
        .path(containerId.toString()).path("logs");
    testContainerLogs(r, containerId);
  }

  @Test (timeout = 5000)
  public void testContainerLogsWithOldAPI() throws IOException, JSONException{
    final ContainerId containerId = BuilderUtils.newContainerId(1, 1, 0, 1);
    WebResource r = resource();
    r = r.path("ws").path("v1").path("node").path("containerlogs")
        .path(containerId.toString());
    testContainerLogs(r, containerId);
  }

  @Test (timeout = 10000)
  public void testNMRedirect() {
    ApplicationId noExistAppId = ApplicationId.newInstance(
        System.currentTimeMillis(), 2000);
    ApplicationAttemptId noExistAttemptId = ApplicationAttemptId.newInstance(
        noExistAppId, 150);
    ContainerId noExistContainerId = ContainerId.newContainerId(
        noExistAttemptId, 250);
    String fileName = "syslog";
    WebResource r = resource();

    // check the old api
    URI requestURI = r.path("ws").path("v1").path("node")
        .path("containerlogs").path(noExistContainerId.toString())
        .path(fileName).queryParam("user.name", "user")
        .queryParam(YarnWebServiceParams.NM_ID, "localhost:1111")
        .getURI();
    String redirectURL = getRedirectURL(requestURI.toString());
    assertTrue(redirectURL != null);
    assertTrue(redirectURL.contains(LOGSERVICEWSADDR));
    assertTrue(redirectURL.contains(noExistContainerId.toString()));
    assertTrue(redirectURL.contains("/logs/" + fileName));
    assertTrue(redirectURL.contains("user.name=" + "user"));
    assertTrue(redirectURL.contains(
        YarnWebServiceParams.REDIRECTED_FROM_NODE + "=true"));
    assertFalse(redirectURL.contains(YarnWebServiceParams.NM_ID));

    // check the new api
    requestURI = r.path("ws").path("v1").path("node")
        .path("containers").path(noExistContainerId.toString())
        .path("logs").path(fileName).queryParam("user.name", "user")
        .queryParam(YarnWebServiceParams.NM_ID, "localhost:1111")
        .getURI();
    redirectURL = getRedirectURL(requestURI.toString());
    assertTrue(redirectURL != null);
    assertTrue(redirectURL.contains(LOGSERVICEWSADDR));
    assertTrue(redirectURL.contains(noExistContainerId.toString()));
    assertTrue(redirectURL.contains("/logs/" + fileName));
    assertTrue(redirectURL.contains("user.name=" + "user"));
    assertTrue(redirectURL.contains(
        YarnWebServiceParams.REDIRECTED_FROM_NODE + "=true"));
    assertFalse(redirectURL.contains(YarnWebServiceParams.NM_ID));

    requestURI = r.path("ws").path("v1").path("node")
        .path("containers").path(noExistContainerId.toString())
        .path("logs").queryParam("user.name", "user")
        .queryParam(YarnWebServiceParams.NM_ID, "localhost:1111")
        .getURI();
    redirectURL = getRedirectURL(requestURI.toString());
    assertTrue(redirectURL != null);
    assertTrue(redirectURL.contains(LOGSERVICEWSADDR));
    assertTrue(redirectURL.contains(noExistContainerId.toString()));
    assertTrue(redirectURL.contains("user.name=" + "user"));
    assertTrue(redirectURL.contains(
        YarnWebServiceParams.REDIRECTED_FROM_NODE + "=true"));
    assertFalse(redirectURL.contains(YarnWebServiceParams.NM_ID));
  }

  @Test
  public void testGetNMResourceInfo()
      throws YarnException, InterruptedException, JSONException {
    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    Map<String, ResourcePlugin> namesToPlugins = new HashMap<>();
    ResourcePlugin mockPlugin1 = mock(ResourcePlugin.class);
    NMResourceInfo nmResourceInfo1 = new NMResourceInfo() {
      public long a = 1000L;
    };
    when(mockPlugin1.getNMResourceInfo()).thenReturn(nmResourceInfo1);
    namesToPlugins.put("resource-1", mockPlugin1);
    namesToPlugins.put("yarn.io/resource-1", mockPlugin1);
    ResourcePlugin mockPlugin2 = mock(ResourcePlugin.class);
    namesToPlugins.put("resource-2", mockPlugin2);
    when(rpm.getNameToPlugins()).thenReturn(namesToPlugins);

    nmContext.setResourcePluginManager(rpm);

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node").path(
        "resources").path("resource-2").accept(MediaType.APPLICATION_JSON).get(
        ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());

    // Access resource-2 should fail (null NMResourceInfo returned).
    JSONObject json = response.getEntity(JSONObject.class);
    assertIncludesException(json);

    // Access resource-3 should fail (unkown plugin)
    response = r.path("ws").path("v1").path("node").path(
        "resources").path("resource-3").accept(MediaType.APPLICATION_JSON).get(
        ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    json = response.getEntity(JSONObject.class);
    assertIncludesException(json);

    // Access resource-1 should success
    response = r.path("ws").path("v1").path("node").path(
        "resources").path("resource-1").accept(MediaType.APPLICATION_JSON).get(
        ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    json = response.getEntity(JSONObject.class);
    Assert.assertEquals(1000, json.get("a"));

    // Access resource-1 should success (encoded yarn.io/Fresource-1).
    response = r.path("ws").path("v1").path("node").path("resources").path(
        "yarn.io%2Fresource-1").accept(MediaType.APPLICATION_JSON).get(
        ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    json = response.getEntity(JSONObject.class);
    Assert.assertEquals(1000, json.get("a"));
  }

  private ContainerId createContainerId(int id) {
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, id);
    return containerId;
  }

  @Test
  public void testGetYarnGpuResourceInfo()
      throws YarnException, InterruptedException, JSONException {
    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    Map<String, ResourcePlugin> namesToPlugins = new HashMap<>();
    ResourcePlugin mockPlugin1 = mock(ResourcePlugin.class);
    GpuDeviceInformation gpuDeviceInformation = new GpuDeviceInformation();
    gpuDeviceInformation.setDriverVersion("1.2.3");
    gpuDeviceInformation.setGpus(Arrays.asList(new PerGpuDeviceInformation()));
    NMResourceInfo nmResourceInfo1 = new NMGpuResourceInfo(gpuDeviceInformation,
        Arrays.asList(new GpuDevice(1, 1), new GpuDevice(2, 2),
            new GpuDevice(3, 3)), Arrays
        .asList(new AssignedGpuDevice(2, 2, createContainerId(1)),
            new AssignedGpuDevice(3, 3, createContainerId(2))));
    when(mockPlugin1.getNMResourceInfo()).thenReturn(nmResourceInfo1);
    namesToPlugins.put("resource-1", mockPlugin1);
    namesToPlugins.put("yarn.io/resource-1", mockPlugin1);
    ResourcePlugin mockPlugin2 = mock(ResourcePlugin.class);
    namesToPlugins.put("resource-2", mockPlugin2);
    when(rpm.getNameToPlugins()).thenReturn(namesToPlugins);

    nmContext.setResourcePluginManager(rpm);

    WebResource r = resource();
    ClientResponse response;
    JSONObject json;

    // Access resource-1 should success
    response = r.path("ws").path("v1").path("node").path(
        "resources").path("resource-1").accept(MediaType.APPLICATION_JSON).get(
        ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    json = response.getEntity(JSONObject.class);
    Assert.assertEquals("1.2.3",
        json.getJSONObject("gpuDeviceInformation").get("driverVersion"));
    Assert.assertEquals(3, json.getJSONArray("totalGpuDevices").length());
    Assert.assertEquals(2, json.getJSONArray("assignedGpuDevices").length());
    Assert.assertEquals(2, json.getJSONArray("assignedGpuDevices").length());
  }

  private void assertIncludesException(JSONObject json) {
    Assert.assertTrue(json.has("RemoteException"));
  }

  private void testContainerLogs(WebResource r, ContainerId containerId)
      throws IOException {
    final String containerIdStr = containerId.toString();
    final ApplicationAttemptId appAttemptId = containerId
        .getApplicationAttemptId();
    final ApplicationId appId = appAttemptId.getApplicationId();
    final String appIdStr = appId.toString();
    final String filename = "logfile1";
    final String logMessage = "log message\n";
    nmContext.getApplications().put(appId, new ApplicationImpl(null, "user",
        appId, null, nmContext));
    
    MockContainer container = new MockContainer(appAttemptId,
        new AsyncDispatcher(), new Configuration(), "user", appId, 1);
    container.setState(ContainerState.RUNNING);
    nmContext.getContainers().put(containerId, container);
    
    // write out log file
    Path path = dirsHandler.getLogPathForWrite(
        ContainerLaunch.getRelativeContainerLogDir(
            appIdStr, containerIdStr) + "/" + filename, false);
    
    File logFile = new File(path.toUri().getPath());
    logFile.deleteOnExit();
    assertTrue("Failed to create log dir", logFile.getParentFile().mkdirs());
    PrintWriter pw = new PrintWriter(logFile);
    pw.print(logMessage);
    pw.close();

    // ask for it
    ClientResponse response = r.path(filename)
        .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
    String responseText = response.getEntity(String.class);
    String responseLogMessage = getLogContext(responseText);
    assertEquals(logMessage, responseLogMessage);
    int fullTextSize = responseLogMessage.getBytes().length;

    // specify how many bytes we should get from logs
    // specify a position number, it would get the first n bytes from
    // container log
    response = r.path(filename)
        .queryParam("size", "5")
        .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    responseLogMessage = getLogContext(responseText);
    assertEquals(5, responseLogMessage.getBytes().length);
    assertEquals(new String(logMessage.getBytes(), 0, 5), responseLogMessage);
    assertTrue(fullTextSize >= responseLogMessage.getBytes().length);

    // specify the bytes which is larger than the actual file size,
    // we would get the full logs
    response = r.path(filename)
        .queryParam("size", "10000")
        .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    responseLogMessage = getLogContext(responseText);
    assertEquals(fullTextSize, responseLogMessage.getBytes().length);
    assertEquals(logMessage, responseLogMessage);

    // specify a negative number, it would get the last n bytes from
    // container log
    response = r.path(filename)
        .queryParam("size", "-5")
        .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    responseLogMessage = getLogContext(responseText);
    assertEquals(5, responseLogMessage.getBytes().length);
    assertEquals(new String(logMessage.getBytes(),
        logMessage.getBytes().length - 5, 5), responseLogMessage);
    assertTrue(fullTextSize >= responseLogMessage.getBytes().length);

    response = r.path(filename)
        .queryParam("size", "-10000")
        .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    responseLogMessage = getLogContext(responseText);
    assertEquals("text/plain; charset=utf-8", response.getType().toString());
    assertEquals(fullTextSize, responseLogMessage.getBytes().length);
    assertEquals(logMessage, responseLogMessage);

    // ask and download it
    response = r.path(filename)
        .queryParam("format", "octet-stream")
        .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    responseLogMessage = getLogContext(responseText);
    assertEquals(logMessage, responseLogMessage);
    assertEquals(200, response.getStatus());
    assertEquals("application/octet-stream; charset=utf-8",
        response.getType().toString());

    // specify a invalid format value
    response = r.path(filename)
        .queryParam("format", "123")
        .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    assertEquals("The valid values for the parameter : format are "
        + WebAppUtils.listSupportedLogContentType(), responseText);
    assertEquals(400, response.getStatus());

    // ask for file that doesn't exist and it will re-direct to
    // the log server
    URI requestURI = r.path("uhhh").getURI();
    String redirectURL = getRedirectURL(requestURI.toString());
    assertTrue(redirectURL != null);
    assertTrue(redirectURL.contains(LOGSERVICEWSADDR));

    // Get container log files' name
    WebResource r1 = resource();
    response = r1.path("ws").path("v1").path("node")
        .path("containers").path(containerIdStr)
        .path("logs").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(200, response.getStatus());
    List<ContainerLogsInfo> responseList = response.getEntity(new GenericType<
        List<ContainerLogsInfo>>(){});
    assertTrue(responseList.size() == 1);
    assertEquals(responseList.get(0).getLogType(),
        ContainerLogAggregationType.LOCAL.toString());
    List<ContainerLogFileInfo> logMeta = responseList.get(0)
        .getContainerLogsInfo();
    assertTrue(logMeta.size() == 1);
    assertEquals(logMeta.get(0).getFileName(), filename);

    // now create an aggregated log in Remote File system
    File tempLogDir = new File("target",
        TestNMWebServices.class.getSimpleName() + "temp-log-dir");
    try {
      String aggregatedLogFile = filename + "-aggregated";
      String aggregatedLogMessage = "This is aggregated ;og.";
      TestContainerLogsUtils.createContainerLogFileInRemoteFS(
          nmContext.getConf(), FileSystem.get(nmContext.getConf()),
          tempLogDir.getAbsolutePath(), containerId, nmContext.getNodeId(),
          aggregatedLogFile, "user", aggregatedLogMessage, true);
      r1 = resource();
      response = r1.path("ws").path("v1").path("node")
          .path("containers").path(containerIdStr)
          .path("logs").accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(200, response.getStatus());
      responseList = response.getEntity(new GenericType<
          List<ContainerLogsInfo>>(){});
      assertEquals(responseList.size(), 2);
      for (ContainerLogsInfo logInfo : responseList) {
        if(logInfo.getLogType().equals(
            ContainerLogAggregationType.AGGREGATED.toString())) {
          List<ContainerLogFileInfo> meta = logInfo.getContainerLogsInfo();
          assertTrue(meta.size() == 1);
          assertEquals(meta.get(0).getFileName(), aggregatedLogFile);
        } else {
          assertEquals(logInfo.getLogType(),
              ContainerLogAggregationType.LOCAL.toString());
          List<ContainerLogFileInfo> meta = logInfo.getContainerLogsInfo();
          assertTrue(meta.size() == 1);
          assertEquals(meta.get(0).getFileName(), filename);
        }
      }

      // Test whether we could get aggregated log as well
      TestContainerLogsUtils.createContainerLogFileInRemoteFS(
          nmContext.getConf(), FileSystem.get(nmContext.getConf()),
          tempLogDir.getAbsolutePath(), containerId, nmContext.getNodeId(),
          filename, "user", aggregatedLogMessage, true);
      response = r.path(filename)
          .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
      responseText = response.getEntity(String.class);
      assertTrue(responseText.contains("LogAggregationType: "
          + ContainerLogAggregationType.AGGREGATED));
      assertTrue(responseText.contains(aggregatedLogMessage));
      assertTrue(responseText.contains("LogAggregationType: "
              + ContainerLogAggregationType.LOCAL));
      assertTrue(responseText.contains(logMessage));
    } finally {
      FileUtil.fullyDelete(tempLogDir);
    }
    // After container is completed, it is removed from nmContext
    nmContext.getContainers().remove(containerId);
    Assert.assertNull(nmContext.getContainers().get(containerId));
    response =
        r.path(filename).accept(MediaType.TEXT_PLAIN)
            .get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    assertTrue(responseText.contains(logMessage));
  }

  public void verifyNodesXML(NodeList nodes) throws JSONException, Exception {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyNodeInfoGeneric(WebServicesTestUtils.getXmlString(element, "id"),
          WebServicesTestUtils.getXmlString(element, "healthReport"),
          WebServicesTestUtils.getXmlLong(element,
              "totalVmemAllocatedContainersMB"),
          WebServicesTestUtils.getXmlLong(element,
              "totalPmemAllocatedContainersMB"),
          WebServicesTestUtils.getXmlLong(element,
              "totalVCoresAllocatedContainers"),
          WebServicesTestUtils.getXmlBoolean(element, "vmemCheckEnabled"),
          WebServicesTestUtils.getXmlBoolean(element, "pmemCheckEnabled"),
          WebServicesTestUtils.getXmlLong(element, "lastNodeUpdateTime"),
          WebServicesTestUtils.getXmlBoolean(element, "nodeHealthy"),
          WebServicesTestUtils.getXmlString(element, "nodeHostName"),
          WebServicesTestUtils.getXmlString(element, "hadoopVersionBuiltOn"),
          WebServicesTestUtils.getXmlString(element, "hadoopBuildVersion"),
          WebServicesTestUtils.getXmlString(element, "hadoopVersion"),
          WebServicesTestUtils.getXmlString(element,
              "nodeManagerVersionBuiltOn"), WebServicesTestUtils.getXmlString(
              element, "nodeManagerBuildVersion"),
          WebServicesTestUtils.getXmlString(element, "nodeManagerVersion"));
    }
  }

  public void verifyNodeInfo(JSONObject json) throws JSONException, Exception {
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject info = json.getJSONObject("nodeInfo");
    assertEquals("incorrect number of elements", 17, info.length());
    verifyNodeInfoGeneric(info.getString("id"), info.getString("healthReport"),
        info.getLong("totalVmemAllocatedContainersMB"),
        info.getLong("totalPmemAllocatedContainersMB"),
        info.getLong("totalVCoresAllocatedContainers"),
        info.getBoolean("vmemCheckEnabled"),
        info.getBoolean("pmemCheckEnabled"),
        info.getLong("lastNodeUpdateTime"), info.getBoolean("nodeHealthy"),
        info.getString("nodeHostName"), info.getString("hadoopVersionBuiltOn"),
        info.getString("hadoopBuildVersion"), info.getString("hadoopVersion"),
        info.getString("nodeManagerVersionBuiltOn"),
        info.getString("nodeManagerBuildVersion"),
        info.getString("nodeManagerVersion"));

  }

  public void verifyNodeInfoGeneric(String id, String healthReport,
      long totalVmemAllocatedContainersMB, long totalPmemAllocatedContainersMB,
      long totalVCoresAllocatedContainers,
      boolean vmemCheckEnabled, boolean pmemCheckEnabled,
      long lastNodeUpdateTime, Boolean nodeHealthy, String nodeHostName,
      String hadoopVersionBuiltOn, String hadoopBuildVersion,
      String hadoopVersion, String resourceManagerVersionBuiltOn,
      String resourceManagerBuildVersion, String resourceManagerVersion) {

    WebServicesTestUtils.checkStringMatch("id", "testhost.foo.com:8042", id);
    WebServicesTestUtils.checkStringMatch("healthReport", "Healthy",
        healthReport);
    assertEquals("totalVmemAllocatedContainersMB incorrect", 15872,
        totalVmemAllocatedContainersMB);
    assertEquals("totalPmemAllocatedContainersMB incorrect", 16384,
        totalPmemAllocatedContainersMB);
    assertEquals("totalVCoresAllocatedContainers incorrect", 4000,
        totalVCoresAllocatedContainers);
    assertEquals("vmemCheckEnabled incorrect",  true, vmemCheckEnabled);
    assertEquals("pmemCheckEnabled incorrect",  true, pmemCheckEnabled);
    assertTrue("lastNodeUpdateTime incorrect", lastNodeUpdateTime == nmContext
        .getNodeHealthStatus().getLastHealthReportTime());
    assertTrue("nodeHealthy isn't true", nodeHealthy);
    WebServicesTestUtils.checkStringMatch("nodeHostName", "testhost.foo.com",
        nodeHostName);

    WebServicesTestUtils.checkStringMatch("hadoopVersionBuiltOn",
        VersionInfo.getDate(), hadoopVersionBuiltOn);
    WebServicesTestUtils.checkStringEqual("hadoopBuildVersion",
        VersionInfo.getBuildVersion(), hadoopBuildVersion);
    WebServicesTestUtils.checkStringMatch("hadoopVersion",
        VersionInfo.getVersion(), hadoopVersion);

    WebServicesTestUtils.checkStringMatch("resourceManagerVersionBuiltOn",
        YarnVersionInfo.getDate(), resourceManagerVersionBuiltOn);
    WebServicesTestUtils.checkStringEqual("resourceManagerBuildVersion",
        YarnVersionInfo.getBuildVersion(), resourceManagerBuildVersion);
    WebServicesTestUtils.checkStringMatch("resourceManagerVersion",
        YarnVersionInfo.getVersion(), resourceManagerVersion);
  }

  private String getLogContext(String fullMessage) {
    String prefix = "LogContents:\n";
    String postfix = "End of LogType:";
    int prefixIndex = fullMessage.indexOf(prefix) + prefix.length();
    int postfixIndex = fullMessage.indexOf(postfix);
    return fullMessage.substring(prefixIndex, postfixIndex);
  }

  private static String getRedirectURL(String url) {
    String redirectUrl = null;
    try {
      HttpURLConnection conn = (HttpURLConnection) new URL(url)
          .openConnection();
      // do not automatically follow the redirection
      // otherwise we get too many redirections exception
      conn.setInstanceFollowRedirects(false);
      if(conn.getResponseCode() == HttpServletResponse.SC_TEMPORARY_REDIRECT) {
        redirectUrl = conn.getHeaderField("Location");
      }
    } catch (Exception e) {
      // throw new RuntimeException(e);
    }
    return redirectUrl;
  }
}