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
import static org.junit.Assert.fail;

import java.io.StringReader;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import com.google.inject.util.Providers;
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
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
/**
 * Test the History Server info web services api's. Also test non-existent urls.
 *
 *  /ws/v1/history
 *  /ws/v1/history/info
 */
public class TestHsWebServices {

  private static Configuration conf = new Configuration();
  private static HistoryContext appContext;
  private static HsWebApp webApp;

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      appContext = new MockHistoryContext(0, 1, 1, 1);
      JobHistory jobHistoryService = new JobHistory();
      HistoryContext historyContext = (HistoryContext) jobHistoryService;
      webApp = new HsWebApp(historyContext);

      bind(JAXBContextResolver.class);
      bind(HsWebServices.class);
      bind(GenericExceptionHandler.class);
      bind(WebApp.class).toInstance(webApp);
      bind(AppContext.class).toInstance(appContext);
      bind(HistoryContext.class).toInstance(appContext);
      bind(Configuration.class).toInstance(conf);
      bind(ApplicationClientProtocol.class).toProvider(Providers.of(null));

      // serve("/*").with(GuiceContainer.class);
    }
  }

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @Before
  public void setUp() throws Exception {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  public TestHsWebServices() {
  }

  @Test
  public void testHS() throws JSONException, Exception {
    WebTarget r = null;
    Response response = r.path("ws").path("v1").path("history")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyHSInfo(json.getJSONObject("historyInfo"), appContext);
  }

  @Test
  public void testHSSlash() throws JSONException, Exception {
    WebTarget r = null;
    Response response = r.path("ws").path("v1").path("history/")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyHSInfo(json.getJSONObject("historyInfo"), appContext);
  }

  @Test
  public void testHSDefault() throws JSONException, Exception {
    WebTarget r = null;
    Response response = r.path("ws").path("v1").path("history/")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyHSInfo(json.getJSONObject("historyInfo"), appContext);
  }

  @Test
  public void testHSXML() throws JSONException, Exception {
    WebTarget r = null;
    Response response = r.path("ws").path("v1").path("history")
        .request(MediaType.APPLICATION_XML).get(Response.class);
    assertEquals(MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    verifyHSInfoXML(xml, appContext);
  }

  @Test
  public void testInfo() throws JSONException, Exception {
    WebTarget r = null;
    Response response = r.path("ws").path("v1").path("history")
        .path("info").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyHSInfo(json.getJSONObject("historyInfo"), appContext);
  }

  @Test
  public void testInfoSlash() throws JSONException, Exception {
    WebTarget r = null;
    Response response = r.path("ws").path("v1").path("history")
        .path("info/").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyHSInfo(json.getJSONObject("historyInfo"), appContext);
  }

  @Test
  public void testInfoDefault() throws JSONException, Exception {
    WebTarget r = null;
    Response response = r.path("ws").path("v1").path("history")
        .path("info/").request().get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyHSInfo(json.getJSONObject("historyInfo"), appContext);
  }

  @Test
  public void testInfoXML() throws JSONException, Exception {
    WebTarget r = null;
    Response response = r.path("ws").path("v1").path("history")
        .path("info/").request(MediaType.APPLICATION_XML)
        .get();
    assertEquals(MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    verifyHSInfoXML(xml, appContext);
  }

  @Test
  public void testInvalidUri() throws JSONException, Exception {
    WebTarget r = null;
    String responseStr = "";
    Response response = r.path("ws").path("v1").path("history").path("bogus")
        .request(MediaType.APPLICATION_JSON).get();
    assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
    WebServicesTestUtils.checkStringMatch(
        "error string exists and shouldn't", "", responseStr);
  }

  @Test
  public void testInvalidUri2() throws JSONException, Exception {
    WebTarget r = null;
    String responseStr = "";
    Response response = r.path("ws").path("v1").path("invalid")
        .request(MediaType.APPLICATION_JSON).get();
    assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
    WebServicesTestUtils.checkStringMatch(
        "error string exists and shouldn't", "", responseStr);
  }

  @Test
  public void testInvalidAccept() throws JSONException, Exception {
    WebTarget r = null;
    String responseStr = "";
    Response response =
        r.path("ws").path("v1").path("history").request(MediaType.TEXT_PLAIN).get();
    assertResponseStatusCode(Response.Status.INTERNAL_SERVER_ERROR,
        response.getStatusInfo());
    WebServicesTestUtils.checkStringMatch(
        "error string exists and shouldn't", "", responseStr);
  }

  public void verifyHsInfoGeneric(String hadoopVersionBuiltOn,
      String hadoopBuildVersion, String hadoopVersion, long startedon) {
    WebServicesTestUtils.checkStringMatch("hadoopVersionBuiltOn",
        VersionInfo.getDate(), hadoopVersionBuiltOn);
    WebServicesTestUtils.checkStringEqual("hadoopBuildVersion",
        VersionInfo.getBuildVersion(), hadoopBuildVersion);
    WebServicesTestUtils.checkStringMatch("hadoopVersion",
        VersionInfo.getVersion(), hadoopVersion);
    assertEquals("startedOn doesn't match: ",
        JobHistoryServer.historyServerTimeStamp, startedon);
  }

  public void verifyHSInfo(JSONObject info, AppContext ctx)
      throws JSONException {
    assertEquals("incorrect number of elements", 4, info.length());

    verifyHsInfoGeneric(info.getString("hadoopVersionBuiltOn"),
        info.getString("hadoopBuildVersion"), info.getString("hadoopVersion"),
        info.getLong("startedOn"));
  }

  public void verifyHSInfoXML(String xml, AppContext ctx)
      throws JSONException, Exception {
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
