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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAX_PARALLEL_APPLICATIONS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.junit.Assert.assertEquals;

public class TestRMWebServicesCapacitySched extends JerseyTestBase {

  private MockRM rm;

  public static class WebServletModule extends ServletModule {
    private final MockRM rm;

    WebServletModule(MockRM rm) {
      this.rm = rm;
    }

    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
    }
  }

  public TestRMWebServicesCapacitySched() {
    super(createWebAppDescriptor());
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    rm = createMockRM(new CapacitySchedulerConfiguration(
        new Configuration(false)));
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule(rm)));
  }

  public static void setupQueueConfiguration(
      CapacitySchedulerConfiguration config) {

    // Define top-level queues
    config.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {"a", "b", "c"});

    final String a = CapacitySchedulerConfiguration.ROOT + ".a";
    config.setCapacity(a, 10.5f);
    config.setMaximumCapacity(a, 50);
    config.setInt(CapacitySchedulerConfiguration.getQueuePrefix(a) + MAX_PARALLEL_APPLICATIONS, 42);

    final String b = CapacitySchedulerConfiguration.ROOT + ".b";
    config.setCapacity(b, 89.5f);

    final String c = CapacitySchedulerConfiguration.ROOT + ".c";
    config.setCapacity(c, "[memory=1024]");

    // Define 2nd-level queues
    final String a1 = a + ".a1";
    final String a2 = a + ".a2";
    config.setQueues(a, new String[] {"a1", "a2"});
    config.setCapacity(a1, 30);
    config.setMaximumCapacity(a1, 50);
    config.setMaximumLifetimePerQueue(a2, 100);
    config.setDefaultLifetimePerQueue(a2, 50);

    config.setUserLimitFactor(a1, 100.0f);
    config.setCapacity(a2, 70);
    config.setUserLimitFactor(a2, 100.0f);

    final String b1 = b + ".b1";
    final String b2 = b + ".b2";
    final String b3 = b + ".b3";
    config.setQueues(b, new String[] {"b1", "b2", "b3"});
    config.setCapacity(b1, 60);
    config.setUserLimitFactor(b1, 100.0f);
    config.setCapacity(b2, 39.5f);
    config.setUserLimitFactor(b2, 100.0f);
    config.setCapacity(b3, 0.5f);
    config.setUserLimitFactor(b3, 100.0f);

    config.setQueues(a1, new String[] {"a1a", "a1b", "a1c"});
    final String a1A = a1 + ".a1a";
    config.setCapacity(a1A, 65);
    final String a1B = a1 + ".a1b";
    config.setCapacity(a1B, 15);
    final String a1C = a1 + ".a1c";
    config.setCapacity(a1C, 20);

    config.setAutoCreateChildQueueEnabled(a1C, true);
    config.setInt(PREFIX + a1C + DOT + AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX
        + DOT + CAPACITY, 50);
  }

  @Test
  public void testClusterScheduler() throws Exception {
    ClientResponse response = resource().path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertJsonResponse(response, "webapp/scheduler-response.json");
  }

  @Test
  public void testClusterSchedulerSlash() throws Exception {
    ClientResponse response = resource().path("ws").path("v1").path("cluster")
        .path("scheduler/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertJsonResponse(response, "webapp/scheduler-response.json");
  }

  @Test
  public void testClusterSchedulerDefault() throws Exception {
    ClientResponse response = resource().path("ws").path("v1").path("cluster")
        .path("scheduler").get(ClientResponse.class);
    assertJsonResponse(response, "webapp/scheduler-response.json");
  }

  @Test
  public void testClusterSchedulerXML() throws Exception {
    ClientResponse response = resource().path("ws").path("v1").path("cluster")
        .path("scheduler/").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertXmlResponse(response, "webapp/scheduler-response.xml");
  }

  @Test
  public void testPerUserResourcesXML() throws Exception {
    // Start RM so that it accepts app submissions
    rm.start();
    try {
      MockRMAppSubmissionData data1 =
          MockRMAppSubmissionData.Builder.createWithMemory(10, rm)
              .withAppName("app1")
              .withUser("user1")
              .withAcls(null)
              .withQueue("b1")
              .withUnmanagedAM(false)
              .build();
      MockRMAppSubmitter.submit(rm, data1);
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(20, rm)
              .withAppName("app2")
              .withUser("user2")
              .withAcls(null)
              .withQueue("b1")
              .withUnmanagedAM(false)
              .build();
      MockRMAppSubmitter.submit(rm, data);

      //Get the XML from ws/v1/cluster/scheduler
      ClientResponse response = resource().path("ws/v1/cluster/scheduler")
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertXmlResponse(response, "webapp/scheduler-response-PerUserResources.xml");
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testNodeLabelDefaultAPI() throws Exception {
    CapacitySchedulerConfiguration config =
        ((CapacityScheduler)rm.getResourceScheduler()).getConfiguration();

    config.setDefaultNodeLabelExpression("root", "ROOT-INHERITED");
    config.setDefaultNodeLabelExpression("root.a", "root-a-default-label");
    rm.getResourceScheduler().reinitialize(config, rm.getRMContext());

    //Start RM so that it accepts app submissions
    rm.start();
    try {
      //Get the XML from ws/v1/cluster/scheduler
      ClientResponse response = resource().path("ws/v1/cluster/scheduler")
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertXmlResponse(response, "webapp/scheduler-response-NodeLabelDefaultAPI.xml");
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testPerUserResourcesJSON() throws Exception {
    //Start RM so that it accepts app submissions
    rm.start();
    try {
      MockRMAppSubmissionData data1 =
          MockRMAppSubmissionData.Builder.createWithMemory(10, rm)
              .withAppName("app1")
              .withUser("user1")
              .withAcls(null)
              .withQueue("b1")
              .withUnmanagedAM(false)
              .build();
      MockRMAppSubmitter.submit(rm, data1);
      MockRMAppSubmissionData data =
          MockRMAppSubmissionData.Builder.createWithMemory(20, rm)
              .withAppName("app2")
              .withUser("user2")
              .withAcls(null)
              .withQueue("b1")
              .withUnmanagedAM(false)
              .build();
      MockRMAppSubmitter.submit(rm, data);

      //Get JSON
      ClientResponse response = resource().path("ws").path("v1").path("cluster")
          .path("scheduler/").accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertJsonResponse(response, "webapp/scheduler-response-PerUserResources.json");
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testResourceInfo() {
    Resource res = Resources.createResource(10, 1);
    // If we add a new resource (e.g. disks), then
    // CapacitySchedulerPage and these RM WebServices + docs need to be updated
    // e.g. ResourceInfo
    assertEquals("<memory:10, vCores:1>", res.toString());
  }

  public static void assertXmlType(ClientResponse response) {
    assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
  }

  public static void assertXmlResponse(ClientResponse response,
                                       String expectedResourceFilename) throws
      Exception {
    assertXmlType(response);
    Document document = loadDocument(response.getEntity(String.class));
    String actual = serializeDocument(document).trim();
    updateTestDataAutomatically(expectedResourceFilename, actual);
    assertEquals(getResourceAsString(expectedResourceFilename), actual);
  }

  public static String serializeDocument(Document document) throws TransformerException {
    DOMSource domSource = new DOMSource(document);
    StringWriter writer = new StringWriter();
    StreamResult result = new StreamResult(writer);
    TransformerFactory tf = TransformerFactory.newInstance();
    Transformer transformer = tf.newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
    transformer.transform(domSource, result);
    return writer.toString();
  }

  public static Document loadDocument(String xml) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    InputSource is = new InputSource(new StringReader(xml));
    return builder.parse(is);
  }

  public static void assertJsonResponse(ClientResponse response,
                                        String expectedResourceFilename) throws
      JSONException, IOException {
    assertJsonType(response);
    JSONObject json = response.getEntity(JSONObject.class);
    String actual = json.toString(2);
    updateTestDataAutomatically(expectedResourceFilename, actual);
    assertEquals(
        prettyPrintJson(getResourceAsString(expectedResourceFilename)),
        prettyPrintJson(actual));
  }

  private static String prettyPrintJson(String in) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(objectMapper.readTree(in));
  }

  public static void assertJsonType(ClientResponse response) {
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
  }

  public static InputStream getResourceAsStream(String configFilename) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return classLoader.getResourceAsStream(configFilename);
  }

  public static String getResourceAsString(String configFilename) throws IOException {
    try (InputStream is = getResourceAsStream(configFilename)) {
      if (is == null) {
        return null;
      }
      try (InputStreamReader isr = new InputStreamReader(is);
           BufferedReader reader = new BufferedReader(isr)) {
        return reader.lines().collect(Collectors.joining(System.lineSeparator()));
      }
    }
  }

  public static void updateTestDataAutomatically(String configFilename, String actualContent) {
    /*
     Set UPDATE_TESTDATA=1 environment variable for auto update the expected data
     or uncomment this return statement.

     It's safe in a way that, this updates the source directory so the test will still fail,
     because the target directory is untouched.
     */
    if (System.getenv("UPDATE_TESTDATA") == null) {
      return;
    }

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      String resource = Objects.requireNonNull(
          Objects.requireNonNull(classLoader.getResource(configFilename)).toURI().getPath())
          .replaceAll("/target/test-classes/", "/src/test/resources/");
      try (FileWriter writer = new FileWriter(resource, false)) {
        writer.write(actualContent);
      }
    } catch (URISyntaxException | IOException e) {
      e.printStackTrace();
      Assert.fail("overwrite should not fail " + e.getMessage());
    }
  }

  public static WebAppDescriptor createWebAppDescriptor() {
    return new WebAppDescriptor.Builder(
        TestRMWebServicesCapacitySched.class.getPackage().getName())
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build();
  }

  public static MockRM createMockRM(CapacitySchedulerConfiguration csConf) {
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    return new MockRM(conf);
  }
}
