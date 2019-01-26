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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.filter.LoggingFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServices;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceRecord;
import org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer.NMWebApp;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONArray;
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
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

/**
 * Basic sanity Tests for AuxServices.
 *
 */
public class TestNMWebServicesAuxServices extends JerseyTestBase {
  private static final String AUX_SERVICES_PATH = "auxiliaryservices";
  private static Context nmContext;
  private static Configuration conf = new Configuration();
  private DateFormat dateFormat =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static final File testRootDir = new File("target",
      TestNMWebServicesContainers.class.getSimpleName());
  private static final File testLogDir = new File("target",
      TestNMWebServicesContainers.class.getSimpleName() + "LogDir");

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      ResourceView resourceView = new ResourceView() {
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
      NodeHealthCheckerService healthChecker = new NodeHealthCheckerService(
          NodeManager.getNodeHealthScriptRunner(conf), dirsHandler);
      healthChecker.init(conf);
      dirsHandler = healthChecker.getDiskHandler();
      ApplicationACLsManager aclsManager = new ApplicationACLsManager(conf);
      nmContext = new NodeManager.NMContext(null, null, dirsHandler,
          aclsManager, null, false, conf) {
        public NodeId getNodeId() {
          return NodeId.newInstance("testhost.foo.com", 8042);
        };

        public int getHttpPort() {
          return 1234;
        };
      };
      WebApp nmWebApp = new NMWebApp(resourceView, aclsManager, dirsHandler);
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

  public TestNMWebServicesAuxServices() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.nodemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testNodeAuxServicesNone() throws Exception {
    addAuxServices();
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("node")
        .path(AUX_SERVICES_PATH).accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("aux services isn't empty",
        new JSONObject().toString(), json.get("services").toString());
  }

  private void addAuxServices(AuxServiceRecord... records) {
    AuxServices auxServices = mock(AuxServices.class);
    when(auxServices.getServiceRecords()).thenReturn(Arrays.asList(records));
    when(auxServices.isManifestEnabled()).thenReturn(true);
    nmContext.setAuxServices(auxServices);
  }

  @Test
  public void testNodeAuxServices() throws Exception {
    testNodeHelper(AUX_SERVICES_PATH, MediaType.APPLICATION_JSON);
  }

  @Test
  public void testNodeAuxServicesSlash() throws Exception {
    testNodeHelper(AUX_SERVICES_PATH + "/", MediaType.APPLICATION_JSON);
  }

  // make sure default is json output
  @Test
  public void testNodeAuxServicesDefault() throws Exception {
    testNodeHelper(AUX_SERVICES_PATH + "/", "");
  }

  public void testNodeHelper(String path, String media) throws Exception {
    AuxServiceRecord r1 = new AuxServiceRecord().name("name1").launchTime(new
        Date(123L)).version("1");
    AuxServiceRecord r2 = new AuxServiceRecord().name("name2").launchTime(new
        Date(456L));
    addAuxServices(r1, r2);
    WebResource r = resource();
    client().addFilter(new LoggingFilter());

    ClientResponse response = r.path("ws").path("v1").path("node").path(path)
        .accept(media).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    JSONObject info = json.getJSONObject("services");
    assertEquals("incorrect number of elements", 1, info.length());
    JSONArray auxInfo = info.getJSONArray("service");
    assertEquals("incorrect number of elements", 2, auxInfo.length());

    verifyNodeAuxServiceInfo(auxInfo.getJSONObject(0), r1);
    verifyNodeAuxServiceInfo(auxInfo.getJSONObject(1), r2);
  }

  @Test
  public void testNodeAuxServicesXML() throws Exception {
    AuxServiceRecord r1 = new AuxServiceRecord().name("name1").launchTime(new
        Date(123L)).version("1");
    AuxServiceRecord r2 = new AuxServiceRecord().name("name2").launchTime(new
        Date(456L));
    addAuxServices(r1, r2);
    WebResource r = resource();

    ClientResponse response = r.path("ws").path("v1").path("node")
        .path(AUX_SERVICES_PATH).accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("service");
    assertEquals("incorrect number of elements", 2, nodes.getLength());
    verifyAuxServicesInfoXML(nodes, r1, r2);
  }

  @Test
  public void testAuxServicesDisabled() throws JSONException, Exception {
    AuxServices auxServices = mock(AuxServices.class);
    when(auxServices.isManifestEnabled()).thenReturn(false);
    nmContext.setAuxServices(auxServices);
    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("node").path(AUX_SERVICES_PATH)
          .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
      fail("should have thrown exception on invalid user query");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(ClientResponse.Status.BAD_REQUEST,
          response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch(
          "exception message",
          "java.lang.Exception: Auxiliary services manifest is not enabled",
          message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "BadRequestException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
    }
  }

  public void verifyAuxServicesInfoXML(NodeList nodes, AuxServiceRecord...
      records) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyNodeAuxServiceInfoGeneric(records[i],
          WebServicesTestUtils.getXmlString(element, "name"),
          WebServicesTestUtils.getXmlString(element, "version"),
          WebServicesTestUtils.getXmlString(element, "startTime"));
    }
  }

  public void verifyNodeAuxServiceInfo(JSONObject info, AuxServiceRecord r)
      throws Exception {
    String version = null;
    if (info.has("version")) {
      version = info.getString("version");
    }
    assertEquals("incorrect number of elements",
        version == null ? 2 : 3, info.length());
    verifyNodeAuxServiceInfoGeneric(r, info.getString("name"),
        version, info.getString("startTime"));
  }

  public void verifyNodeAuxServiceInfoGeneric(AuxServiceRecord r, String name,
      String version, String startTime) {
    assertEquals(r.getName(), name);
    assertEquals(r.getVersion(), version);
    assertEquals("startTime", dateFormat.format(r.getLaunchTime()),
        startTime);
  }
}
