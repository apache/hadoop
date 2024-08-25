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

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.util.StringHelper.ujoin;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import com.google.inject.util.Providers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

/**
 * Test the history server Rest API for getting jobs, a specific job, job
 * counters, and job attempts.
 *
 * /ws/v1/history/mapreduce/jobs /ws/v1/history/mapreduce/jobs/{jobid}
 * /ws/v1/history/mapreduce/jobs/{jobid}/counters
 * /ws/v1/history/mapreduce/jobs/{jobid}/jobattempts
 */
public class TestHsWebServicesJobs extends JerseyTestBase {

  private static Configuration conf = new Configuration();
  private static MockHistoryContext appContext;
  private static HsWebApp webApp;

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      appContext = new MockHistoryContext(0, 1, 2, 1, false);
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
  }

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  public TestHsWebServicesJobs() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.mapreduce.v2.hs.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  void testJobs() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    assertEquals(1, arr.length(), "incorrect number of elements");
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getPartialJob(MRApps.toJobID(info.getString("id")));
    VerifyJobsUtils.verifyHsJobPartial(info, job);

  }

  @Test
  void testJobsSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    assertEquals(1, arr.length(), "incorrect number of elements");
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getPartialJob(MRApps.toJobID(info.getString("id")));
    VerifyJobsUtils.verifyHsJobPartial(info, job);

  }

  @Test
  void testJobsDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    assertEquals(1, arr.length(), "incorrect number of elements");
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getPartialJob(MRApps.toJobID(info.getString("id")));
    VerifyJobsUtils.verifyHsJobPartial(info, job);

  }

  @Test
  void testJobsXML() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList jobs = dom.getElementsByTagName("jobs");
    assertEquals(1, jobs.getLength(), "incorrect number of elements");
    NodeList job = dom.getElementsByTagName("job");
    assertEquals(1, job.getLength(), "incorrect number of elements");
    verifyHsJobPartialXML(job, appContext);
  }

  public void verifyHsJobPartialXML(NodeList nodes, MockHistoryContext appContext) {

    assertEquals(1, nodes.getLength(), "incorrect number of elements");

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      Job job = appContext.getPartialJob(MRApps.toJobID(WebServicesTestUtils
          .getXmlString(element, "id")));
      assertNotNull(job, "Job not found - output incorrect");

      VerifyJobsUtils.verifyHsJobGeneric(job,
          WebServicesTestUtils.getXmlString(element, "id"),
          WebServicesTestUtils.getXmlString(element, "user"),
          WebServicesTestUtils.getXmlString(element, "name"),
          WebServicesTestUtils.getXmlString(element, "state"),
          WebServicesTestUtils.getXmlString(element, "queue"),
          WebServicesTestUtils.getXmlLong(element, "startTime"),
          WebServicesTestUtils.getXmlLong(element, "finishTime"),
          WebServicesTestUtils.getXmlInt(element, "mapsTotal"),
          WebServicesTestUtils.getXmlInt(element, "mapsCompleted"),
          WebServicesTestUtils.getXmlInt(element, "reducesTotal"),
          WebServicesTestUtils.getXmlInt(element, "reducesCompleted"));
    }
  }
  
  public void verifyHsJobXML(NodeList nodes, AppContext appContext) {

    assertEquals(1, nodes.getLength(), "incorrect number of elements");

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      Job job = appContext.getJob(MRApps.toJobID(WebServicesTestUtils
          .getXmlString(element, "id")));
      assertNotNull(job, "Job not found - output incorrect");

      VerifyJobsUtils.verifyHsJobGeneric(job,
          WebServicesTestUtils.getXmlString(element, "id"),
          WebServicesTestUtils.getXmlString(element, "user"),
          WebServicesTestUtils.getXmlString(element, "name"),
          WebServicesTestUtils.getXmlString(element, "state"),
          WebServicesTestUtils.getXmlString(element, "queue"),
          WebServicesTestUtils.getXmlLong(element, "startTime"),
          WebServicesTestUtils.getXmlLong(element, "finishTime"),
          WebServicesTestUtils.getXmlInt(element, "mapsTotal"),
          WebServicesTestUtils.getXmlInt(element, "mapsCompleted"),
          WebServicesTestUtils.getXmlInt(element, "reducesTotal"),
          WebServicesTestUtils.getXmlInt(element, "reducesCompleted"));

      // restricted access fields - if security and acls set
      VerifyJobsUtils.verifyHsJobGenericSecure(job,
          WebServicesTestUtils.getXmlBoolean(element, "uberized"),
          WebServicesTestUtils.getXmlString(element, "diagnostics"),
          WebServicesTestUtils.getXmlLong(element, "avgMapTime"),
          WebServicesTestUtils.getXmlLong(element, "avgReduceTime"),
          WebServicesTestUtils.getXmlLong(element, "avgShuffleTime"),
          WebServicesTestUtils.getXmlLong(element, "avgMergeTime"),
          WebServicesTestUtils.getXmlInt(element, "failedReduceAttempts"),
          WebServicesTestUtils.getXmlInt(element, "killedReduceAttempts"),
          WebServicesTestUtils.getXmlInt(element, "successfulReduceAttempts"),
          WebServicesTestUtils.getXmlInt(element, "failedMapAttempts"),
          WebServicesTestUtils.getXmlInt(element, "killedMapAttempts"),
          WebServicesTestUtils.getXmlInt(element, "successfulMapAttempts"));
    }
  }

  @Test
  void testJobId() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId)
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("job");
      VerifyJobsUtils.verifyHsJob(info, appContext.getJob(id));
    }

  }

  @Test
  void testJobIdSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId + "/")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("job");

      VerifyJobsUtils.verifyHsJob(info, appContext.getJob(id));
    }
  }

  @Test
  void testJobIdDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("job");
      VerifyJobsUtils.verifyHsJob(info, appContext.getJob(id));
    }

  }

  @Test
  void testJobIdNonExist() throws JSONException, Exception {
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
          .path("job_0_1234").get(JSONObject.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals(3, exception.length(), "incorrect number of elements");
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.Exception: job, job_0_1234, is not found", message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "NotFoundException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    }
  }

  @Test
  void testJobIdInvalid() throws JSONException, Exception {
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
          .path("job_foo").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals(3, exception.length(), "incorrect number of elements");
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      verifyJobIdInvalid(message, type, classname);

    }
  }

  // verify the exception output default is JSON
  @Test
  void testJobIdInvalidDefault() throws JSONException, Exception {
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
          .path("job_foo").get(JSONObject.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals(3, exception.length(), "incorrect number of elements");
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      verifyJobIdInvalid(message, type, classname);
    }
  }

  // test that the exception output works in XML
  @Test
  void testJobIdInvalidXML() throws JSONException, Exception {
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
          .path("job_foo").accept(MediaType.APPLICATION_XML)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      String msg = response.getEntity(String.class);
      System.out.println(msg);
      DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(msg));
      Document dom = db.parse(is);
      NodeList nodes = dom.getElementsByTagName("RemoteException");
      Element element = (Element) nodes.item(0);
      String message = WebServicesTestUtils.getXmlString(element, "message");
      String type = WebServicesTestUtils.getXmlString(element, "exception");
      String classname = WebServicesTestUtils.getXmlString(element,
          "javaClassName");
      verifyJobIdInvalid(message, type, classname);
    }
  }

  private void verifyJobIdInvalid(String message, String type, String classname) {
    WebServicesTestUtils.checkStringMatch("exception message",
        "java.lang.Exception: JobId string : job_foo is not properly formed",
        message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "NotFoundException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
  }

  @Test
  void testJobIdInvalidBogus() throws JSONException, Exception {
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
          .path("bogusfoo").get(JSONObject.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals(3, exception.length(), "incorrect number of elements");
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "java.lang.Exception: JobId string : "
              + "bogusfoo is not properly formed", message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "NotFoundException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    }
  }

  @Test
  void testJobIdXML() throws Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId)
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      String xml = response.getEntity(String.class);
      DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      Document dom = db.parse(is);
      NodeList job = dom.getElementsByTagName("job");
      verifyHsJobXML(job, appContext);
    }

  }

  @Test
  void testJobCounters() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("counters")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("jobCounters");
      verifyHsJobCounters(info, appContext.getJob(id));
    }
  }

  @Test
  void testJobCountersSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("counters/")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("jobCounters");
      verifyHsJobCounters(info, appContext.getJob(id));
    }
  }

  @Test
  void testJobCountersForKilledJob() throws Exception {
    WebResource r = resource();
    appContext = new MockHistoryContext(0, 1, 1, 1, true);
    GuiceServletConfig.setInjector(Guice.createInjector(new ServletModule() {
      @Override
      protected void configureServlets() {

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
    }));

    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("counters/")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("jobCounters");
      WebServicesTestUtils.checkStringMatch("id", MRApps.toString(id),
          info.getString("id"));
      assertTrue(info.length() == 1, "Job shouldn't contain any counters");
    }
  }

  @Test
  void testJobCountersDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("counters/")
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("jobCounters");
      verifyHsJobCounters(info, appContext.getJob(id));
    }
  }

  @Test
  void testJobCountersXML() throws Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("counters")
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      String xml = response.getEntity(String.class);
      DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      Document dom = db.parse(is);
      NodeList info = dom.getElementsByTagName("jobCounters");
      verifyHsJobCountersXML(info, appContext.getJob(id));
    }
  }

  public void verifyHsJobCounters(JSONObject info, Job job)
      throws JSONException {

    assertEquals(2, info.length(), "incorrect number of elements");

    WebServicesTestUtils.checkStringMatch("id", MRApps.toString(job.getID()),
        info.getString("id"));
    // just do simple verification of fields - not data is correct
    // in the fields
    JSONArray counterGroups = info.getJSONArray("counterGroup");
    for (int i = 0; i < counterGroups.length(); i++) {
      JSONObject counterGroup = counterGroups.getJSONObject(i);
      String name = counterGroup.getString("counterGroupName");
      assertTrue((name != null && !name.isEmpty()), "name not set");
      JSONArray counters = counterGroup.getJSONArray("counter");
      for (int j = 0; j < counters.length(); j++) {
        JSONObject counter = counters.getJSONObject(j);
        String counterName = counter.getString("name");
        assertTrue((counterName != null && !counterName.isEmpty()),
            "counter name not set");

        long mapValue = counter.getLong("mapCounterValue");
        assertTrue(mapValue >= 0, "mapCounterValue  >= 0");

        long reduceValue = counter.getLong("reduceCounterValue");
        assertTrue(reduceValue >= 0, "reduceCounterValue  >= 0");

        long totalValue = counter.getLong("totalCounterValue");
        assertTrue(totalValue >= 0, "totalCounterValue  >= 0");

      }
    }
  }

  public void verifyHsJobCountersXML(NodeList nodes, Job job) {

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      assertNotNull(job, "Job not found - output incorrect");

      WebServicesTestUtils.checkStringMatch("id", MRApps.toString(job.getID()),
          WebServicesTestUtils.getXmlString(element, "id"));
      // just do simple verification of fields - not data is correct
      // in the fields
      NodeList groups = element.getElementsByTagName("counterGroup");

      for (int j = 0; j < groups.getLength(); j++) {
        Element counters = (Element) groups.item(j);
        assertNotNull(counters, "should have counters in the web service info");
        String name = WebServicesTestUtils.getXmlString(counters,
            "counterGroupName");
        assertTrue((name != null && !name.isEmpty()), "name not set");
        NodeList counterArr = counters.getElementsByTagName("counter");
        for (int z = 0; z < counterArr.getLength(); z++) {
          Element counter = (Element) counterArr.item(z);
          String counterName = WebServicesTestUtils.getXmlString(counter,
              "name");
          assertTrue((counterName != null && !counterName.isEmpty()),
              "counter name not set");

          long mapValue = WebServicesTestUtils.getXmlLong(counter,
              "mapCounterValue");
          assertTrue(mapValue >= 0, "mapCounterValue not >= 0");

          long reduceValue = WebServicesTestUtils.getXmlLong(counter,
              "reduceCounterValue");
          assertTrue(reduceValue >= 0, "reduceCounterValue  >= 0");

          long totalValue = WebServicesTestUtils.getXmlLong(counter,
              "totalCounterValue");
          assertTrue(totalValue >= 0, "totalCounterValue  >= 0");
        }
      }
    }
  }

  @Test
  void testJobAttempts() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyHsJobAttempts(info, appContext.getJob(id));
    }
  }

  @Test
  void testJobAttemptsSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts/")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyHsJobAttempts(info, appContext.getJob(id));
    }
  }

  @Test
  void testJobAttemptsDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts")
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyHsJobAttempts(info, appContext.getJob(id));
    }
  }

  @Test
  void testJobAttemptsXML() throws Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts")
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      String xml = response.getEntity(String.class);
      DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      Document dom = db.parse(is);
      NodeList attempts = dom.getElementsByTagName("jobAttempts");
      assertEquals(1, attempts.getLength(), "incorrect number of elements");
      NodeList info = dom.getElementsByTagName("jobAttempt");
      verifyHsJobAttemptsXML(info, appContext.getJob(id));
    }
  }

  public void verifyHsJobAttempts(JSONObject info, Job job)
      throws JSONException {

    JSONArray attempts = info.getJSONArray("jobAttempt");
    assertEquals(2, attempts.length(), "incorrect number of elements");
    for (int i = 0; i < attempts.length(); i++) {
      JSONObject attempt = attempts.getJSONObject(i);
      verifyHsJobAttemptsGeneric(job, attempt.getString("nodeHttpAddress"),
          attempt.getString("nodeId"),
          attempt.getInt("id"),
          attempt.getLong("startTime"), attempt.getString("containerId"),
          attempt.getString("logsLink"));
    }
  }

  public void verifyHsJobAttemptsXML(NodeList nodes, Job job) {

    assertEquals(2, nodes.getLength(), "incorrect number of elements");
    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);
      verifyHsJobAttemptsGeneric(job,
          WebServicesTestUtils.getXmlString(element, "nodeHttpAddress"),
          WebServicesTestUtils.getXmlString(element, "nodeId"),
          WebServicesTestUtils.getXmlInt(element, "id"),
          WebServicesTestUtils.getXmlLong(element, "startTime"),
          WebServicesTestUtils.getXmlString(element, "containerId"),
          WebServicesTestUtils.getXmlString(element, "logsLink"));
    }
  }

  public void verifyHsJobAttemptsGeneric(Job job, String nodeHttpAddress,
      String nodeId, int id, long startTime, String containerId,
      String logsLink) {
    boolean attemptFound = false;
    for (AMInfo amInfo : job.getAMInfos()) {
      if (amInfo.getAppAttemptId().getAttemptId() == id) {
        attemptFound = true;
        String nmHost = amInfo.getNodeManagerHost();
        int nmHttpPort = amInfo.getNodeManagerHttpPort();
        int nmPort = amInfo.getNodeManagerPort();
        WebServicesTestUtils.checkStringMatch("nodeHttpAddress", nmHost + ":"
            + nmHttpPort, nodeHttpAddress);
        assertTrue(startTime > 0, "startime not greater than 0");
        WebServicesTestUtils.checkStringMatch("containerId", amInfo
            .getContainerId().toString(), containerId);

        String localLogsLink = join("hsmockwebapp",
            ujoin("logs", nodeId, containerId, MRApps.toString(job.getID()),
                job.getUserName()));

        assertTrue(logsLink.contains(localLogsLink), "logsLink");
      }
    }
    assertTrue(attemptFound, "attempt: " + id + " was not found");
  }

}
