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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer.NMWebApp;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;

/**
 * Test the nodemanager node info web services api's
 */
public class TestNMWebServices extends JerseyTest {

  private static Context nmContext;
  private static ResourceView resourceView;
  private static ApplicationACLsManager aclsManager;
  private static LocalDirsHandlerService dirsHandler;
  private static WebApp nmWebApp;

  private static final File testRootDir = new File("target",
      TestNMWebServices.class.getSimpleName());
  private static File testLogDir = new File("target",
      TestNMWebServices.class.getSimpleName() + "LogDir");

  private Injector injector = Guice.createInjector(new ServletModule() {
    @Override
    protected void configureServlets() {
      Configuration conf = new Configuration();
      conf.set(YarnConfiguration.NM_LOCAL_DIRS, testRootDir.getAbsolutePath());
      conf.set(YarnConfiguration.NM_LOG_DIRS, testLogDir.getAbsolutePath());
      NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
      healthChecker.init(conf);
      dirsHandler = healthChecker.getDiskHandler();
      aclsManager = new ApplicationACLsManager(conf);
      nmContext = new NodeManager.NMContext(null, null, dirsHandler,
          aclsManager, null);
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
  });

  public class GuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    testRootDir.mkdirs();
    testLogDir.mkdir();
  }

  @AfterClass
  static public void stop() {
    FileUtil.fullyDelete(testRootDir);
    FileUtil.fullyDelete(testLogDir);
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
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
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
      assertEquals(Status.INTERNAL_SERVER_ERROR,
          response.getClientResponseStatus());
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
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      WebServicesTestUtils.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testNode() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyNodeInfo(json);
  }

  @Test
  public void testNodeSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node/")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyNodeInfo(json);
  }

  // make sure default is json output
  @Test
  public void testNodeDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node")
        .get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyNodeInfo(json);
  }

  @Test
  public void testNodeInfo() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node").path("info")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyNodeInfo(json);
  }

  @Test
  public void testNodeInfoSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node")
        .path("info/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyNodeInfo(json);
  }

  // make sure default is json output
  @Test
  public void testNodeInfoDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node").path("info")
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    verifyNodeInfo(json);
  }

  @Test
  public void testSingleNodesXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node")
        .path("info/").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
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
  
  @Test
  public void testContainerLogs() throws IOException {
    WebResource r = resource();
    final ContainerId containerId = BuilderUtils.newContainerId(0, 0, 0, 0);
    final String containerIdStr = BuilderUtils.newContainerId(0, 0, 0, 0)
        .toString();
    final ApplicationAttemptId appAttemptId = containerId.getApplicationAttemptId();
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
    ClientResponse response = r.path("ws").path("v1").path("node")
        .path("containerlogs").path(containerIdStr).path(filename)
        .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
    String responseText = response.getEntity(String.class);
    assertEquals(logMessage, responseText);
    
    // ask for file that doesn't exist
    response = r.path("ws").path("v1").path("node")
        .path("containerlogs").path(containerIdStr).path("uhhh")
        .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
    responseText = response.getEntity(String.class);
    assertTrue(responseText.contains("Cannot find this log on the local disk."));
    
    // After container is completed, it is removed from nmContext
    nmContext.getContainers().remove(containerId);
    Assert.assertNull(nmContext.getContainers().get(containerId));
    response =
        r.path("ws").path("v1").path("node").path("containerlogs")
            .path(containerIdStr).path(filename).accept(MediaType.TEXT_PLAIN)
            .get(ClientResponse.class);
    responseText = response.getEntity(String.class);
    assertEquals(logMessage, responseText);
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
    assertEquals("incorrect number of elements", 16, info.length());
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

}
