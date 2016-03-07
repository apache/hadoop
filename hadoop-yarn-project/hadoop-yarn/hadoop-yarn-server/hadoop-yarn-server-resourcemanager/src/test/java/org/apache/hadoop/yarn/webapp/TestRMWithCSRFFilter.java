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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServices;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Used TestRMWebServices as an example of web invocations of RM and added
 * test for CSRF Filter.
 */
public class TestRMWithCSRFFilter extends JerseyTestBase {

  private static MockRM rm;

  private Injector injector = Guice.createInjector(new ServletModule() {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      Configuration conf = new Configuration();
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);
      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
      RestCsrfPreventionFilter csrfFilter = new RestCsrfPreventionFilter();
      Map<String,String> initParams = new HashMap<>();
      // adding GET as protected method to make things a little easier...
      initParams.put(RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM,
                     "OPTIONS,HEAD,TRACE");
      filter("/*").through(csrfFilter, initParams);
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
  }

  public TestRMWithCSRFFilter() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
              .contextListenerClass(GuiceServletConfig.class)
              .filterClass(com.google.inject.servlet.GuiceFilter.class)
              .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testNoCustomHeaderFromBrowser() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("info").accept("application/xml")
        .header(RestCsrfPreventionFilter.HEADER_USER_AGENT,"Mozilla/5.0")
        .get(ClientResponse.class);
    assertTrue("Should have been rejected", response.getStatus() ==
                                            Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  public void testIncludeCustomHeaderFromBrowser() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("info").accept("application/xml")
        .header(RestCsrfPreventionFilter.HEADER_USER_AGENT,"Mozilla/5.0")
        .header("X-XSRF-HEADER", "")
        .get(ClientResponse.class);
    assertTrue("Should have been accepted", response.getStatus() ==
                                            Status.OK.getStatusCode());
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    verifyClusterInfoXML(xml);
  }

  @Test
  public void testAllowedMethod() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("info").accept("application/xml")
        .header(RestCsrfPreventionFilter.HEADER_USER_AGENT,"Mozilla/5.0")
        .head();
    assertTrue("Should have been allowed", response.getStatus() ==
                                           Status.OK.getStatusCode());
  }

  @Test
  public void testAllowNonBrowserInteractionWithoutHeader() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("info").accept("application/xml")
        .get(ClientResponse.class);
    assertTrue("Should have been accepted", response.getStatus() ==
                                            Status.OK.getStatusCode());
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    verifyClusterInfoXML(xml);
  }

  public void verifyClusterInfoXML(String xml) throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("clusterInfo");
    assertEquals("incorrect number of elements", 1, nodes.getLength());

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

    assertEquals("clusterId doesn't match: ",
                 ResourceManager.getClusterTimeStamp(), clusterid);
    assertEquals("startedOn doesn't match: ",
                 ResourceManager.getClusterTimeStamp(), startedon);
    assertTrue("stated doesn't match: " + state,
               state.matches(STATE.INITED.toString()));
    assertTrue("HA state doesn't match: " + haState,
               haState.matches("INITIALIZING"));

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
