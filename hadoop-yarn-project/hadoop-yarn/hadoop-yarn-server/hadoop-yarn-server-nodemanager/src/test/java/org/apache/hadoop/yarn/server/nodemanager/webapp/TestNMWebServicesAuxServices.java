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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServices;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceRecord;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer.NMWebApp;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;

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

  @Override
  protected Application configure() {
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
      NodeHealthCheckerService healthChecker =
          new NodeHealthCheckerService(dirsHandler);
      healthChecker.init(conf);
      dirsHandler = healthChecker.getDiskHandler();
      ApplicationACLsManager aclsManager = new ApplicationACLsManager(conf);
      nmContext = new NodeManager.NMContext(null, null, dirsHandler,
          aclsManager, null, false, conf) {
        public NodeId getNodeId() {
          return NodeId.newInstance("testhost.foo.com", 8042);
        }

        public int getHttpPort() {
          return 1234;
        }
      };
      WebApp nmWebApp = new NMWebApp(resourceView, aclsManager, dirsHandler);
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

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @BeforeEach
  public void before() throws Exception {
    testRootDir.mkdirs();
    testLogDir.mkdir();
  }

  @AfterAll
  static public void cleanup() {
    FileUtil.fullyDelete(testRootDir);
    FileUtil.fullyDelete(testLogDir);
  }

  public TestNMWebServicesAuxServices() {
  }

  @Test
  public void testNodeAuxServicesNone() throws Exception {
    addAuxServices();
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("node")
        .path(AUX_SERVICES_PATH).request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("{\"services\":\"\"}", json.toString(), "aux services isn't empty");
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
    WebTarget r = targetWithJsonObject();

    Response response = r.path("ws").path("v1").path("node").path(path)
        .request(media).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    JSONObject info = json.getJSONObject("services");
    assertEquals(1, info.length(), "incorrect number of elements");
    JSONArray auxInfo = info.getJSONArray("service");
    assertEquals(2, auxInfo.length(), "incorrect number of elements");

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
    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("node")
        .path(AUX_SERVICES_PATH).request(MediaType.APPLICATION_XML)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("service");
    assertEquals(2, nodes.getLength(), "incorrect number of elements");
    verifyAuxServicesInfoXML(nodes, r1, r2);
  }

  @Test
  public void testAuxServicesDisabled() throws JSONException, Exception {
    AuxServices auxServices = mock(AuxServices.class);
    when(auxServices.isManifestEnabled()).thenReturn(false);
    nmContext.setAuxServices(auxServices);
    WebTarget r = targetWithJsonObject();
    try {
      Response response = r.path("ws").path("v1").path("node").path(AUX_SERVICES_PATH)
          .request(MediaType.APPLICATION_JSON).get();
      throw new BadRequestException(response);
    } catch (BadRequestException ue) {
      Response response = ue.getResponse();
      assertResponseStatusCode(Response.Status.BAD_REQUEST, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject msg = response.readEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals(3, exception.length(), "incorrect number of elements");
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "Auxiliary services manifest is not enabled", message);
      WebServicesTestUtils.checkStringMatch("exception type", "BadRequestException", type);
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
    assertEquals(version == null ? 2 : 3, info.length(),
        "incorrect number of elements");
    verifyNodeAuxServiceInfoGeneric(r, info.getString("name"),
        version, info.getString("startTime"));
  }

  public void verifyNodeAuxServiceInfoGeneric(AuxServiceRecord r, String name,
      String version, String startTime) {
    assertEquals(r.getName(), name);
    assertEquals(r.getVersion(), version);
    assertEquals(dateFormat.format(r.getLaunchTime()),
        startTime, "startTime");
  }
}
