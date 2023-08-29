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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.MEMORY_CONFIGURATION_STORE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.junit.Assert.assertEquals;

public final class TestWebServiceUtil {
  private TestWebServiceUtil(){
  }

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

  public static void runTest(String template, String name,
      MockRM rm,
      WebResource resource) throws Exception {
    final boolean reinitAfterNodeChane = isMutableConfig(rm.getConfig());
    try {
      boolean legacyQueueMode = ((CapacityScheduler) rm.getResourceScheduler())
          .getConfiguration().isLegacyQueueMode();

      // capacity is not set when there are no cluster resources available in non-legacy queue mode
      assertJsonResponse(sendRequest(resource),
          getExpectedResourceFile(template, name, "0", legacyQueueMode));

      MockNM nm1 = rm.registerNode("h1:1234", 8 * GB, 8);
      rm.registerNode("h2:1234", 8 * GB, 8);
      if (reinitAfterNodeChane) {
        reinitialize(rm, rm.getConfig());
      }
      assertJsonResponse(sendRequest(resource),
          getExpectedResourceFile(template, name, "16", legacyQueueMode));
      rm.registerNode("h3:1234", 8 * GB, 8);
      MockNM nm4 = rm.registerNode("h4:1234", 8 * GB, 8);
      if (reinitAfterNodeChane) {
        reinitialize(rm, rm.getConfig());
      }

      assertJsonResponse(sendRequest(resource),
          getExpectedResourceFile(template, name, "32", legacyQueueMode));
      rm.unRegisterNode(nm1);
      rm.unRegisterNode(nm4);
      assertJsonResponse(sendRequest(resource),
          getExpectedResourceFile(template, name, "16", legacyQueueMode));
    } finally {
      rm.close();
    }
  }

  /**
   * There are some differences between legacy and non-legacy queue mode.
   *   - capacity/maxCapacity shows effective values instead of configured on non-legacy mode
   *   - no cluster resource -> no capacity in non-legacy mode
   *   - no cluster resource -> maxApplications is set to the configured value in non-legacy mode
   *   - normalizedWeight is not set in non-legacy queue mode
   *  To address this tests may add separate test files for legacy queue mode.
   *
   * @param template The file template to use
   * @param name The base test name (-legacy suffix will be searched if legacy-queue-mode)
   * @param suffix The test suffix
   * @param legacyQueueMode Is legacy-queue-mode enabled
   * @return The expected test file name. In legacy-queue mode returns the basename-legacy
   * filepath if exists.
   *
   * @throws IOException when the resource file cannot be opened for some reason.
   */
  public static String getExpectedResourceFile(String template, String name, String suffix,
                                               boolean legacyQueueMode) throws IOException {
    String legacyResource = String.format(template, legacySuffix(legacyQueueMode, name), suffix);
    try (InputStream stream = getResourceAsStream(legacyResource)) {
      if (stream != null) {
        return legacyResource;
      }
    }

    return String.format(template, name, suffix);
  }

  public static String legacySuffix(boolean legacyQueueMode, String text) {
    if (legacyQueueMode) {
      return text + "-legacy";
    }
    return text;
  }

  public static boolean isMutableConfig(Configuration config) {
    return Objects.equals(config.get(SCHEDULER_CONFIGURATION_STORE_CLASS),
        MEMORY_CONFIGURATION_STORE);
  }

  public static ClientResponse sendRequest(WebResource resource) {
    return resource.path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
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
    TransformerFactory tf = XMLUtils.newSecureTransformerFactory();
    Transformer transformer = tf.newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
    transformer.transform(domSource, result);
    return writer.toString();
  }

  public static Document loadDocument(String xml) throws Exception {
    DocumentBuilderFactory factory = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder builder = factory.newDocumentBuilder();
    InputSource is = new InputSource(new StringReader(xml));
    return builder.parse(is);
  }

  public static void assertJsonResponse(ClientResponse response,
      String expectedResourceFilename) throws JSONException,
      IOException {
    assertJsonType(response);
    JSONObject json = response.getEntity(JSONObject.class);
    String actual = prettyPrintJson(json.toString(2));
    updateTestDataAutomatically(expectedResourceFilename, actual);
    assertEquals(
        prettyPrintJson(getResourceAsString(expectedResourceFilename)),
        actual);
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

  public static MockRM createRM(Configuration config) {
    config.setClass(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class, ResourceScheduler.class);
    config.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    MockRM rm = new MockRM(config);
    GuiceServletConfig.setInjector(Guice.createInjector(new WebServletModule(rm)));
    rm.start();
    return rm;
  }

  public static MockRM createMutableRM(Configuration conf) throws IOException {
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    MockRM rm = createRM(new CapacitySchedulerConfiguration(conf));
    reinitialize(rm, conf);
    return rm;
  }

  public static void reinitialize(MockRM rm, Configuration conf) throws IOException {
    // Need to call reinitialize as
    // MutableCSConfigurationProvider with InMemoryConfigurationStore
    // somehow does not load the queues properly and falls back to default config.
    // Therefore CS will think there's only the default queue there.
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    cs.reinitialize(conf, rm.getRMContext(), true);
  }
}
