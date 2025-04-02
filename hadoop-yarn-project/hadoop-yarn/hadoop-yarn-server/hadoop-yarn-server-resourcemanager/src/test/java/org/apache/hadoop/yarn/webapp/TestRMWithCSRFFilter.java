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

package org.apache.hadoop.yarn.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServices;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Used TestRMWebServices as an example of web invocations of RM and added
 * test for CSRF Filter.
 */
public class TestRMWithCSRFFilter extends JerseyTestBase {

  private static MockRM rm;

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(RMWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    forceSet(TestProperties.CONTAINER_PORT, JERSEY_RANDOM_PORT);
    return config;
  }

  private static class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      Configuration conf = new Configuration();
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);

      rm = new MockRM(conf);
      rm.getRMContext().getContainerTokenSecretManager().rollMasterKey();
      rm.getRMContext().getNMTokenSecretManager().rollMasterKey();
      rm.disableDrainEventsImplicitly();

      RestCsrfPreventionFilter csrfFilter = new RestCsrfPreventionFilter();
      Map<String,String> initParams = new HashMap<>();
      // adding GET as protected method to make things a little easier...
      initParams.put(RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM,
          "OPTIONS,HEAD,TRACE");

      bind(csrfFilter).to(RestCsrfPreventionFilter.class);
      final HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getScheme()).thenReturn("http");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
    }
  }

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestRMWithCSRFFilter() {
  }

  @Test
  public void testNoCustomHeaderFromBrowser() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("info").request("application/xml")
        .header(RestCsrfPreventionFilter.HEADER_USER_AGENT,"Mozilla/5.0")
        .get(Response.class);
    // TODO: Custom filters are needed to implement related functions
    // assertTrue("Should have been rejected", response.getStatus() ==
    // Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  public void testIncludeCustomHeaderFromBrowser() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("info").request("application/xml")
        .header(RestCsrfPreventionFilter.HEADER_USER_AGENT,"Mozilla/5.0")
        .header("X-XSRF-HEADER", "")
        .get(Response.class);
    assertTrue(response.getStatus() ==
        Response.Status.OK.getStatusCode(), "Should have been accepted");
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    verifyClusterInfoXML(xml);
  }

  @Test
  public void testAllowedMethod() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("info").request("application/xml")
        .header(RestCsrfPreventionFilter.HEADER_USER_AGENT,"Mozilla/5.0")
        .head();
    assertTrue(response.getStatus() ==
        Response.Status.OK.getStatusCode(), "Should have been allowed");
  }

  @Test
  public void testAllowNonBrowserInteractionWithoutHeader() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("info").request("application/xml")
        .get(Response.class);
    assertTrue(response.getStatus() ==
        Response.Status.OK.getStatusCode(), "Should have been accepted");
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    verifyClusterInfoXML(xml);
  }

  public void verifyClusterInfoXML(String xml) throws Exception {
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("clusterInfo");
    assertEquals(1, nodes.getLength(), "incorrect number of elements");

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      verifyClusterGeneric(WebServicesTestUtils.getXmlLong(element, "id"),
         WebServicesTestUtils.getXmlLong(element, "startedOn"),
         WebServicesTestUtils.getXmlString(element, "state"),
         WebServicesTestUtils.getXmlString(element, "haState"),
         WebServicesTestUtils.getXmlString(
             element, "haZooKeeperConnectionState"),
         WebServicesTestUtils.getXmlString(element, "hadoopVersionBuiltOn"),
         WebServicesTestUtils.getXmlString(element, "hadoopBuildVersion"),
         WebServicesTestUtils.getXmlString(element, "hadoopVersion"),
         WebServicesTestUtils.getXmlString(element,
                                           "resourceManagerVersionBuiltOn"),
         WebServicesTestUtils.getXmlString(element,
                                           "resourceManagerBuildVersion"),
         WebServicesTestUtils.getXmlString(element, "resourceManagerVersion"));
    }
  }

  public void verifyClusterGeneric(long clusterid, long startedon,
                                   String state, String haState,
                                   String haZooKeeperConnectionState,
                                   String hadoopVersionBuiltOn,
                                   String hadoopBuildVersion,
                                   String hadoopVersion,
                                   String resourceManagerVersionBuiltOn,
                                   String resourceManagerBuildVersion,
                                   String resourceManagerVersion) {

    assertEquals(ResourceManager.getClusterTimeStamp(), clusterid,
        "clusterId doesn't match: ");
    assertEquals(ResourceManager.getClusterTimeStamp(), startedon,
        "startedOn doesn't match: ");
    assertTrue(state.matches(STATE.INITED.toString()),
        "stated doesn't match: " + state);
    assertTrue(haState.matches("INITIALIZING"),
        "HA state doesn't match: " + haState);

    WebServicesTestUtils.checkStringMatch("hadoopVersionBuiltOn",
                                          VersionInfo.getDate(), hadoopVersionBuiltOn);
    WebServicesTestUtils.checkStringEqual("hadoopBuildVersion",
                                          VersionInfo.getBuildVersion(), hadoopBuildVersion);
    WebServicesTestUtils.checkStringMatch("hadoopVersion",
                                          VersionInfo.getVersion(), hadoopVersion);

    WebServicesTestUtils.checkStringMatch("resourceManagerVersionBuiltOn",
                                          YarnVersionInfo.getDate(),
                                          resourceManagerVersionBuiltOn);
    WebServicesTestUtils.checkStringEqual("resourceManagerBuildVersion",
                                          YarnVersionInfo.getBuildVersion(), resourceManagerBuildVersion);
    WebServicesTestUtils.checkStringMatch("resourceManagerVersion",
                                          YarnVersionInfo.getVersion(),
                                          resourceManagerVersion);
  }

}
