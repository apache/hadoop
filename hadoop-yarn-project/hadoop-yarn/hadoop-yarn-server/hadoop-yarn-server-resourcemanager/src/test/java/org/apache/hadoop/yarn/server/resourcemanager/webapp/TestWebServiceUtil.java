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
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;

import static org.junit.Assert.assertEquals;

public class TestWebServiceUtil {
  private TestWebServiceUtil(){

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
}
