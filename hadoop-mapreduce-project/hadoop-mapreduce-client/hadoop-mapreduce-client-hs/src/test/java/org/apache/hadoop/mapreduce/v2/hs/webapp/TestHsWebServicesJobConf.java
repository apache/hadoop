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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import com.google.inject.util.Providers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.MockHistoryContext;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
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

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

/**
 * Test the history server Rest API for getting the job conf. This
 * requires created a temporary configuration file.
 *
 *   /ws/v1/history/mapreduce/jobs/{jobid}/conf
 */
public class TestHsWebServicesJobConf extends JerseyTestBase {

  private static Configuration conf = new Configuration();
  private static HistoryContext appContext;
  private static HsWebApp webApp;

  private static File testConfDir = new File("target",
      TestHsWebServicesJobConf.class.getSimpleName() + "confDir");

  private static class WebServletModule extends ServletModule {

    @Override
    protected void configureServlets() {

      Path confPath = new Path(testConfDir.toString(),
          MRJobConfig.JOB_CONF_FILE);
      Configuration config = new Configuration();

      FileSystem localFs;
      try {
        localFs = FileSystem.getLocal(config);
        confPath = localFs.makeQualified(confPath);

        OutputStream out = localFs.create(confPath);
        try {
          conf.writeXml(out);
        } finally {
          out.close();
        }
        if (!localFs.exists(confPath)) {
          fail("error creating config file: " + confPath);
        }

      } catch (IOException e) {
        fail("error creating config file: " + e.getMessage());
      }

      appContext = new MockHistoryContext(0, 2, 1, confPath);

      webApp = mock(HsWebApp.class);
      when(webApp.name()).thenReturn("hsmockwebapp");

      bind(JAXBContextResolver.class);
      bind(HsWebServices.class);
      bind(GenericExceptionHandler.class);
      bind(WebApp.class).toInstance(webApp);
      bind(AppContext.class).toInstance(appContext);
      bind(HistoryContext.class).toInstance(appContext);
      bind(Configuration.class).toInstance(conf);
      bind(ApplicationClientProtocol.class).toProvider(Providers.of(null));

      serve("/*").with(GuiceContainer.class);
    }
  };

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    testConfDir.mkdir();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @AfterAll
  static public void stop() {
    FileUtil.fullyDelete(testConfDir);
  }

  public TestHsWebServicesJobConf() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.mapreduce.v2.hs.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  void testJobConf() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce")
          .path("jobs").path(jobId).path("conf")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("conf");
      verifyHsJobConf(info, jobsMap.get(id));
    }
  }

  @Test
  void testJobConfSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history").path("mapreduce")
          .path("jobs").path(jobId).path("conf/")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("conf");
      verifyHsJobConf(info, jobsMap.get(id));
    }
  }

  @Test
  void testJobConfDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history").path("mapreduce")
          .path("jobs").path(jobId).path("conf").get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("conf");
      verifyHsJobConf(info, jobsMap.get(id));
    }
  }

  @Test
  void testJobConfXML() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history").path("mapreduce")
          .path("jobs").path(jobId).path("conf")
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      String xml = response.getEntity(String.class);
      DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      Document dom = db.parse(is);
      NodeList info = dom.getElementsByTagName("conf");
      verifyHsJobConfXML(info, jobsMap.get(id));
    }
  }

  public void verifyHsJobConf(JSONObject info, Job job) throws JSONException {

    assertEquals(2, info.length(), "incorrect number of elements");

    WebServicesTestUtils.checkStringMatch("path", job.getConfFile().toString(),
        info.getString("path"));
    // just do simple verification of fields - not data is correct
    // in the fields
    JSONArray properties = info.getJSONArray("property");
    for (int i = 0; i < properties.length(); i++) {
      JSONObject prop = properties.getJSONObject(i);
      String name = prop.getString("name");
      String value = prop.getString("value");
      assertTrue((name != null && !name.isEmpty()), "name not set");
      assertTrue((value != null && !value.isEmpty()), "value not set");
    }
  }

  public void verifyHsJobConfXML(NodeList nodes, Job job) {

    assertEquals(1, nodes.getLength(), "incorrect number of elements");

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);
      WebServicesTestUtils.checkStringMatch("path", job.getConfFile()
          .toString(), WebServicesTestUtils.getXmlString(element, "path"));

      // just do simple verification of fields - not data is correct
      // in the fields
      NodeList properties = element.getElementsByTagName("property");

      for (int j = 0; j < properties.getLength(); j++) {
        Element property = (Element) properties.item(j);
        assertNotNull(property, "should have counters in the web service info");
        String name = WebServicesTestUtils.getXmlString(property, "name");
        String value = WebServicesTestUtils.getXmlString(property, "value");
        assertTrue((name != null && !name.isEmpty()), "name not set");
        assertTrue((value != null && !value.isEmpty()), "name not set");
      }
    }
  }

}
