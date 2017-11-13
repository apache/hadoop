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
package org.apache.hadoop.conf;

import java.io.StringWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.ws.rs.core.HttpHeaders;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.eclipse.jetty.util.ajax.JSON;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.common.base.Strings;

import org.apache.hadoop.http.HttpServer2;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.*;

/**
 * Basic test case that the ConfServlet can write configuration
 * to its output in XML and JSON format.
 */
public class TestConfServlet {
  private static final String TEST_KEY = "testconfservlet.key";
  private static final String TEST_VAL = "testval";
  private static final Map<String, String> TEST_PROPERTIES =
      new HashMap<String, String>();
  private static final Map<String, String> TEST_FORMATS =
      new HashMap<String, String>();

  @BeforeClass
  public static void initTestProperties() {
    TEST_PROPERTIES.put("test.key1", "value1");
    TEST_PROPERTIES.put("test.key2", "value2");
    TEST_PROPERTIES.put("test.key3", "value3");
    TEST_FORMATS.put(ConfServlet.FORMAT_XML, "application/xml");
    TEST_FORMATS.put(ConfServlet.FORMAT_JSON, "application/json");
  }

  private Configuration getTestConf() {
    Configuration testConf = new Configuration();
    testConf.set(TEST_KEY, TEST_VAL);
    return testConf;
  }

  private Configuration getMultiPropertiesConf() {
    Configuration testConf = new Configuration(false);
    for(String key : TEST_PROPERTIES.keySet()) {
      testConf.set(key, TEST_PROPERTIES.get(key));
    }
    return testConf;
  }

  @Test
  public void testParseHeaders() throws Exception {
    HashMap<String, String> verifyMap = new HashMap<String, String>();
    verifyMap.put("text/plain", ConfServlet.FORMAT_XML);
    verifyMap.put(null, ConfServlet.FORMAT_XML);
    verifyMap.put("text/xml", ConfServlet.FORMAT_XML);
    verifyMap.put("application/xml", ConfServlet.FORMAT_XML);
    verifyMap.put("application/json", ConfServlet.FORMAT_JSON);

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    for(String contentTypeExpected : verifyMap.keySet()) {
      String contenTypeActual = verifyMap.get(contentTypeExpected);
      Mockito.when(request.getHeader(HttpHeaders.ACCEPT))
          .thenReturn(contentTypeExpected);
      assertEquals(contenTypeActual,
          ConfServlet.parseAcceptHeader(request));
    }
  }

  private void verifyGetProperty(Configuration conf, String format,
      String propertyName) throws Exception {
    StringWriter sw = null;
    PrintWriter pw = null;
    ConfServlet service = null;
    try {
      service = new ConfServlet();
      ServletConfig servletConf = mock(ServletConfig.class);
      ServletContext context = mock(ServletContext.class);
      service.init(servletConf);
      when(context.getAttribute(HttpServer2.CONF_CONTEXT_ATTRIBUTE))
        .thenReturn(conf);
      when(service.getServletContext())
        .thenReturn(context);

      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getHeader(HttpHeaders.ACCEPT))
        .thenReturn(TEST_FORMATS.get(format));
      when(request.getParameter("name"))
        .thenReturn(propertyName);

      HttpServletResponse response = mock(HttpServletResponse.class);
      sw = new StringWriter();
      pw = new PrintWriter(sw);
      when(response.getWriter()).thenReturn(pw);

      // response request
      service.doGet(request, response);
      String result = sw.toString().trim();

      // if property name is null or empty, expect all properties
      // in the response
      if (Strings.isNullOrEmpty(propertyName)) {
        for(String key : TEST_PROPERTIES.keySet()) {
          assertTrue(result.contains(key) &&
              result.contains(TEST_PROPERTIES.get(key)));
        }
      } else {
        if(conf.get(propertyName) != null) {
          // if property name is not empty and property is found
          assertTrue(result.contains(propertyName));
          for(String key : TEST_PROPERTIES.keySet()) {
            if(!key.equals(propertyName)) {
              assertFalse(result.contains(key));
            }
          }
        } else {
          // if property name is not empty, and it's not in configuration
          // expect proper error code and error message is set to the response
          Mockito.verify(response).sendError(
              Mockito.eq(HttpServletResponse.SC_NOT_FOUND),
              Mockito.eq("Property " + propertyName + " not found"));
        }
      }
    } finally {
      if (sw != null) {
        sw.close();
      }
      if (pw != null) {
        pw.close();
      }
      if (service != null) {
        service.destroy();
      }
    }
  }

  @Test
  public void testGetProperty() throws Exception {
    Configuration configurations = getMultiPropertiesConf();
    // list various of property names
    String[] testKeys = new String[] {
        "test.key1",
        "test.unknown.key",
        "",
        "test.key2",
        null
    };

    for(String format : TEST_FORMATS.keySet()) {
      for(String key : testKeys) {
        verifyGetProperty(configurations, format, key);
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteJson() throws Exception {
    StringWriter sw = new StringWriter();
    ConfServlet.writeResponse(getTestConf(), sw, "json");
    String json = sw.toString();
    boolean foundSetting = false;
    Object parsed = JSON.parse(json);
    Object[] properties = ((Map<String, Object[]>)parsed).get("properties");
    for (Object o : properties) {
      Map<String, Object> propertyInfo = (Map<String, Object>)o;
      String key = (String)propertyInfo.get("key");
      String val = (String)propertyInfo.get("value");
      String resource = (String)propertyInfo.get("resource");
      System.err.println("k: " + key + " v: " + val + " r: " + resource);
      if (TEST_KEY.equals(key) && TEST_VAL.equals(val)
          && "programmatically".equals(resource)) {
        foundSetting = true;
      }
    }
    assertTrue(foundSetting);
  }

  @Test
  public void testWriteXml() throws Exception {
    StringWriter sw = new StringWriter();
    ConfServlet.writeResponse(getTestConf(), sw, "xml");
    String xml = sw.toString();

    DocumentBuilderFactory docBuilderFactory 
      = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(new InputSource(new StringReader(xml)));
    NodeList nameNodes = doc.getElementsByTagName("name");
    boolean foundSetting = false;
    for (int i = 0; i < nameNodes.getLength(); i++) {
      Node nameNode = nameNodes.item(i);
      String key = nameNode.getTextContent();
      if (TEST_KEY.equals(key)) {
        foundSetting = true;
        Element propertyElem = (Element)nameNode.getParentNode();
        String val = propertyElem.getElementsByTagName("value").item(0).getTextContent();
        assertEquals(TEST_VAL, val);
      }
    }
    assertTrue(foundSetting);
  }

  @Test
  public void testBadFormat() throws Exception {
    StringWriter sw = new StringWriter();
    try {
      ConfServlet.writeResponse(getTestConf(), sw, "not a format");
      fail("writeResponse with bad format didn't throw!");
    } catch (ConfServlet.BadFormatException bfe) {
      // expected
    }
    assertEquals("", sw.toString());
  }
}