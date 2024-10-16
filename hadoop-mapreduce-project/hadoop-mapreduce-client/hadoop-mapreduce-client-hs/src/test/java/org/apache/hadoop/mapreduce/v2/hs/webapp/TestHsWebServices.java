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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.StringReader;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.JobHistory;
import org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer;
import org.apache.hadoop.mapreduce.v2.hs.MockHistoryContext;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * Test the History Server info web services api's. Also test non-existent urls.
 *
 *  /ws/v1/history
 *  /ws/v1/history/info
 */
public class TestHsWebServices extends JerseyTestBase {

  private static Configuration conf = new Configuration();
  private static ApplicationClientProtocol acp = mock(ApplicationClientProtocol.class);

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(HsWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  private static class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      HistoryContext appContext = new MockHistoryContext(0, 1, 1, 1);
      HistoryContext historyContext = new JobHistory();
      HsWebApp webApp = new HsWebApp(historyContext);

      bind(webApp).to(WebApp.class).named("hsWebApp");
      bind(appContext).to(AppContext.class);
      bind(appContext).to(HistoryContext.class).named("ctx");
      bind(conf).to(Configuration.class).named("conf");
      bind(acp).to(ApplicationClientProtocol.class).named("appClient");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(response).to(HttpServletResponse.class);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testHS() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyHSInfo(json.getJSONObject("historyInfo"));
  }

  @Test
  public void testHSSlash() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history/")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyHSInfo(json.getJSONObject("historyInfo"));
  }

  @Test
  public void testHSDefault() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history/")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyHSInfo(json.getJSONObject("historyInfo"));
  }

  @Test
  public void testHSXML() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history")
        .request(MediaType.APPLICATION_XML).get(Response.class);
    assertEquals(MediaType.APPLICATION_XML + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    verifyHSInfoXML(xml);
  }

  @Test
  public void testInfo() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history")
        .path("info").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyHSInfo(json.getJSONObject("historyInfo"));
  }

  @Test
  public void testInfoSlash() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history")
        .path("info/").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyHSInfo(json.getJSONObject("historyInfo"));
  }

  @Test
  public void testInfoDefault() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history")
        .path("info/").request().get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyHSInfo(json.getJSONObject("historyInfo"));
  }

  @Test
  public void testInfoXML() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history")
        .path("info/").request(MediaType.APPLICATION_XML)
        .get();
    assertEquals(MediaType.APPLICATION_XML + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    verifyHSInfoXML(xml);
  }

  @Test
  public void testInvalidUri() {
    WebTarget r = target();
    String responseStr = "";
    Response response = r.path("ws").path("v1").path("history").path("bogus")
        .request(MediaType.APPLICATION_JSON).get();
    assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
    WebServicesTestUtils.checkStringMatch(
        "error string exists and shouldn't", "", responseStr);
  }

  @Test
  public void testInvalidUri2() {
    WebTarget r = target();
    String responseStr = "";
    Response response = r.path("ws").path("v1").path("invalid")
        .request(MediaType.APPLICATION_JSON).get();
    assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
    WebServicesTestUtils.checkStringMatch(
        "error string exists and shouldn't", "", responseStr);
  }

  @Test
  public void testInvalidAccept() {
    WebTarget r = target();
    String responseStr = "";
    Response response =
        r.path("ws").path("v1").path("history").request(MediaType.TEXT_PLAIN).get();
    assertResponseStatusCode(Response.Status.SERVICE_UNAVAILABLE,
        response.getStatusInfo());
    WebServicesTestUtils.checkStringMatch(
        "error string exists and shouldn't", "", responseStr);
  }

  public void verifyHsInfoGeneric(String hadoopVersionBuiltOn,
      String hadoopBuildVersion, String hadoopVersion, long startedOn) {
    WebServicesTestUtils.checkStringMatch("hadoopVersionBuiltOn",
        VersionInfo.getDate(), hadoopVersionBuiltOn);
    WebServicesTestUtils.checkStringEqual("hadoopBuildVersion",
        VersionInfo.getBuildVersion(), hadoopBuildVersion);
    WebServicesTestUtils.checkStringMatch("hadoopVersion",
        VersionInfo.getVersion(), hadoopVersion);
    assertEquals("startedOn doesn't match: ",
        JobHistoryServer.historyServerTimeStamp, startedOn);
  }

  public void verifyHSInfo(JSONObject info)
      throws JSONException {
    assertEquals("incorrect number of elements", 4, info.length());

    verifyHsInfoGeneric(info.getString("hadoopVersionBuiltOn"),
        info.getString("hadoopBuildVersion"), info.getString("hadoopVersion"),
        info.getLong("startedOn"));
  }

  public void verifyHSInfoXML(String xml) throws Exception {
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("historyInfo");
    assertEquals("incorrect number of elements", 1, nodes.getLength());

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);
      verifyHsInfoGeneric(
          WebServicesTestUtils.getXmlString(element, "hadoopVersionBuiltOn"),
          WebServicesTestUtils.getXmlString(element, "hadoopBuildVersion"),
          WebServicesTestUtils.getXmlString(element, "hadoopVersion"),
          WebServicesTestUtils.getXmlLong(element, "startedOn"));
    }
  }
}
