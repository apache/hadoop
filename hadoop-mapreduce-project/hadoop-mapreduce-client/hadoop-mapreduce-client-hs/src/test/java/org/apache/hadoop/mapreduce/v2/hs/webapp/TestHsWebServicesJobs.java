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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

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
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONArray;
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

  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      appContext = new MockHistoryContext(0, 1, 2, 1);
      webApp = mock(HsWebApp.class);
      when(webApp.name()).thenReturn("hsmockwebapp");

      bind(webApp).to(WebApp.class).named("hsWebApp");
      bind(appContext).to(AppContext.class);
      bind(appContext).to(HistoryContext.class).named("ctx");
      bind(conf).to(Configuration.class).named("conf");
      bind(acp).to(ApplicationClientProtocol.class).named("appClient");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(response).to(HttpServletResponse.class);
      final HttpServletRequest request = mock(HttpServletRequest.class);
      bind(request).to(HttpServletRequest.class);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testJobs() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONObject jobItem = jobs.getJSONObject("job");
    JSONArray arr = new JSONArray();
    arr.put(jobItem);
    assertEquals("incorrect number of elements", 1, arr.length());
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getPartialJob(MRApps.toJobID(info.getString("id")));
    VerifyJobsUtils.verifyHsJobPartial(info, job);
  }

  @Test
  public void testJobsSlash() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs/").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONObject jobItem = jobs.getJSONObject("job");
    JSONArray arr = new JSONArray();
    arr.put(jobItem);
    assertEquals("incorrect number of elements", 1, arr.length());
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getPartialJob(MRApps.toJobID(info.getString("id")));
    VerifyJobsUtils.verifyHsJobPartial(info, job);
  }

  @Test
  public void testJobsDefault() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").request().get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONObject jobItem = jobs.getJSONObject("job");
    JSONArray arr = new JSONArray();
    arr.put(jobItem);
    assertEquals("incorrect number of elements", 1, arr.length());
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getPartialJob(MRApps.toJobID(info.getString("id")));
    VerifyJobsUtils.verifyHsJobPartial(info, job);
  }

  @Test
  public void testJobsXML() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").request(MediaType.APPLICATION_XML)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList jobs = dom.getElementsByTagName("jobs");
    assertEquals("incorrect number of elements", 1, jobs.getLength());
    NodeList job = dom.getElementsByTagName("job");
    assertEquals("incorrect number of elements", 1, job.getLength());
    verifyHsJobPartialXML(job, appContext);
  }

  public void verifyHsJobPartialXML(NodeList nodes, MockHistoryContext appContext) {

    assertEquals("incorrect number of elements", 1, nodes.getLength());

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      Job job = appContext.getPartialJob(MRApps.toJobID(WebServicesTestUtils
          .getXmlString(element, "id")));
      assertNotNull("Job not found - output incorrect", job);

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

    assertEquals("incorrect number of elements", 1, nodes.getLength());

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      Job job = appContext.getJob(MRApps.toJobID(WebServicesTestUtils
          .getXmlString(element, "id")));
      assertNotNull("Job not found - output incorrect", job);

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
  public void testJobId() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      Response response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId)
          .request(MediaType.APPLICATION_JSON).get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String entity = response.readEntity(String.class);
      JSONObject json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("job");
      VerifyJobsUtils.verifyHsJob(info, appContext.getJob(id));
    }
  }

  @Test
  public void testJobIdSlash() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      Response response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId + "/")
          .request(MediaType.APPLICATION_JSON).get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String entity = response.readEntity(String.class);
      JSONObject json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("job");

      VerifyJobsUtils.verifyHsJob(info, appContext.getJob(id));
    }
  }

  @Test
  public void testJobIdDefault() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).request().get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String entity = response.readEntity(String.class);
      JSONObject json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("job");
      VerifyJobsUtils.verifyHsJob(info, appContext.getJob(id));
    }
  }

  @Test
  public void testJobIdNonExist() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
        .path("job_0_1234").request().get();
    assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject msg = new JSONObject(entity);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils.checkStringMatch("exception message",
         "job, job_0_1234, is not found", message);
    WebServicesTestUtils.checkStringMatch("exception type", "NotFoundException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
         "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
  }

  @Test
  public void testJobIdInvalid() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
        .path("job_foo").request(MediaType.APPLICATION_JSON).get();
    assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject msg = new JSONObject(entity);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    verifyJobIdInvalid(message, type, classname);
  }

  // verify the exception output default is JSON
  @Test
  public void testJobIdInvalidDefault() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
        .path("job_foo").request().get();
    assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject msg = new JSONObject(entity);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    verifyJobIdInvalid(message, type, classname);
  }

  // test that the exception output works in XML
  @Test
  public void testJobIdInvalidXML() throws Exception {
    WebTarget r = target();
    Response response = r
        .path("ws")
        .path("v1")
        .path("history")
        .path("mapreduce")
        .path("jobs")
        .path("job_foo")
        .request(MediaType.APPLICATION_XML)
        .get();
    assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String msg = response.readEntity(String.class);
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(msg));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("RemoteException");
    Element element = (Element) nodes.item(0);
    String message = WebServicesTestUtils.getXmlString(element, "message");
    String type = WebServicesTestUtils.getXmlString(element, "exception");
    String classname = WebServicesTestUtils.getXmlString(element, "javaClassName");
    verifyJobIdInvalid(message, type, classname);
  }

  private void verifyJobIdInvalid(String message, String type, String classname) {
    WebServicesTestUtils.checkStringMatch("exception message",
        "JobId string : job_foo is not properly formed", message);
    WebServicesTestUtils.checkStringMatch("exception type", "NotFoundException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
  }

  @Test
  public void testJobIdInvalidBogus() throws Exception {
    WebTarget r = target();
    Response response = r
        .path("ws")
        .path("v1")
        .path("history")
        .path("mapreduce")
        .path("jobs")
        .path("bogusfoo")
        .request(MediaType.APPLICATION_JSON)
        .get();
    assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject msg = new JSONObject(entity);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils.checkStringMatch("exception message",
        "JobId string : bogusfoo is not properly formed", message);
    WebServicesTestUtils.checkStringMatch("exception type", "NotFoundException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
         "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
  }

  @Test
  public void testJobIdXML() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r
          .path("ws")
          .path("v1")
          .path("history")
          .path("mapreduce")
          .path("jobs").path(jobId)
          .request(MediaType.APPLICATION_XML)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String xml = response.readEntity(String.class);
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
  public void testJobCounters() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r
          .path("ws")
          .path("v1")
          .path("history")
          .path("mapreduce")
          .path("jobs")
          .path(jobId).path("counters")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String entity = response.readEntity(String.class);
      JSONObject json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobCounters");
      verifyHsJobCounters(info, appContext.getJob(id));
    }
  }

  @Test
  public void testJobCountersSlash() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r
          .path("ws")
          .path("v1")
          .path("history")
          .path("mapreduce")
          .path("jobs")
          .path(jobId)
          .path("counters/")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String entity = response.readEntity(String.class);
      JSONObject json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobCounters");
      verifyHsJobCounters(info, appContext.getJob(id));
    }
  }
  
  @Test
  public void testJobCountersForKilledJob() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r
          .path("ws")
          .path("v1")
          .path("history")
          .path("mapreduce")
          .path("jobs")
          .path(jobId)
          .path("counters/")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String entity = response.readEntity(String.class);
      JSONObject json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobCounters");
      WebServicesTestUtils.checkStringMatch("id", MRApps.toString(id),
          info.getString("id"));
      // assertTrue("Job shouldn't contain any counters", info.length() == 1);
    }
  }

  @Test
  public void testJobCountersDefault() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r
          .path("ws")
          .path("v1")
          .path("history")
          .path("mapreduce")
          .path("jobs")
          .path(jobId)
          .path("counters/")
          .request()
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String entity = response.readEntity(String.class);
      JSONObject json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobCounters");
      verifyHsJobCounters(info, appContext.getJob(id));
    }
  }

  @Test
  public void testJobCountersXML() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r
          .path("ws")
          .path("v1")
          .path("history")
          .path("mapreduce")
          .path("jobs")
          .path(jobId)
          .path("counters")
          .request(MediaType.APPLICATION_XML)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String xml = response.readEntity(String.class);
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

    assertEquals("incorrect number of elements", 2, info.length());

    WebServicesTestUtils.checkStringMatch("id", MRApps.toString(job.getID()),
        info.getString("id"));
    // just do simple verification of fields - not data is correct
    // in the fields
    JSONArray counterGroups = info.getJSONArray("counterGroup");
    for (int i = 0; i < counterGroups.length(); i++) {
      JSONObject counterGroup = counterGroups.getJSONObject(i);
      String name = counterGroup.getString("counterGroupName");
      assertTrue("name not set", (name != null && !name.isEmpty()));
      JSONArray counters = counterGroup.getJSONArray("counter");
      for (int j = 0; j < counters.length(); j++) {
        JSONObject counter = counters.getJSONObject(j);
        String counterName = counter.getString("name");
        assertTrue("counter name not set",
            (counterName != null && !counterName.isEmpty()));

        long mapValue = counter.getLong("mapCounterValue");
        assertTrue("mapCounterValue  >= 0", mapValue >= 0);

        long reduceValue = counter.getLong("reduceCounterValue");
        assertTrue("reduceCounterValue  >= 0", reduceValue >= 0);

        long totalValue = counter.getLong("totalCounterValue");
        assertTrue("totalCounterValue  >= 0", totalValue >= 0);

      }
    }
  }

  public void verifyHsJobCountersXML(NodeList nodes, Job job) {

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      assertNotNull("Job not found - output incorrect", job);

      WebServicesTestUtils.checkStringMatch("id", MRApps.toString(job.getID()),
          WebServicesTestUtils.getXmlString(element, "id"));
      // just do simple verification of fields - not data is correct
      // in the fields
      NodeList groups = element.getElementsByTagName("counterGroup");

      for (int j = 0; j < groups.getLength(); j++) {
        Element counters = (Element) groups.item(j);
        assertNotNull("should have counters in the web service info", counters);
        String name = WebServicesTestUtils.getXmlString(counters,
            "counterGroupName");
        assertTrue("name not set", (name != null && !name.isEmpty()));
        NodeList counterArr = counters.getElementsByTagName("counter");
        for (int z = 0; z < counterArr.getLength(); z++) {
          Element counter = (Element) counterArr.item(z);
          String counterName = WebServicesTestUtils.getXmlString(counter,
              "name");
          assertTrue("counter name not set",
              (counterName != null && !counterName.isEmpty()));

          long mapValue = WebServicesTestUtils.getXmlLong(counter,
              "mapCounterValue");
          assertTrue("mapCounterValue not >= 0", mapValue >= 0);

          long reduceValue = WebServicesTestUtils.getXmlLong(counter,
              "reduceCounterValue");
          assertTrue("reduceCounterValue  >= 0", reduceValue >= 0);

          long totalValue = WebServicesTestUtils.getXmlLong(counter,
              "totalCounterValue");
          assertTrue("totalCounterValue  >= 0", totalValue >= 0);
        }
      }
    }
  }

  @Test
  public void testJobAttempts() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r
          .path("ws")
          .path("v1")
          .path("history")
          .path("mapreduce")
          .path("jobs")
          .path(jobId)
          .path("jobattempts")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String entity = response.readEntity(String.class);
      JSONObject json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyHsJobAttempts(info, appContext.getJob(id));
    }
  }

  @Test
  public void testJobAttemptsSlash() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r
          .path("ws")
          .path("v1")
          .path("history")
          .path("mapreduce")
          .path("jobs")
          .path(jobId)
          .path("jobattempts/")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String entity = response.readEntity(String.class);
      JSONObject json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyHsJobAttempts(info, appContext.getJob(id));
    }
  }

  @Test
  public void testJobAttemptsDefault() throws JSONException, Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r
          .path("ws")
          .path("v1")
          .path("history")
          .path("mapreduce")
          .path("jobs")
          .path(jobId)
          .path("jobattempts")
          .request()
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String entity = response.readEntity(String.class);
      JSONObject json = new JSONObject(entity);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyHsJobAttempts(info, appContext.getJob(id));
    }
  }

  @Test
  public void testJobAttemptsXML() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      Response response = r
          .path("ws")
          .path("v1")
          .path("history")
          .path("mapreduce")
          .path("jobs")
          .path(jobId)
          .path("jobattempts")
          .request(MediaType.APPLICATION_XML)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String xml = response.readEntity(String.class);
      DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      Document dom = db.parse(is);
      NodeList attempts = dom.getElementsByTagName("jobAttempts");
      assertEquals("incorrect number of elements", 1, attempts.getLength());
      NodeList info = dom.getElementsByTagName("jobAttempt");
      verifyHsJobAttemptsXML(info, appContext.getJob(id));
    }
  }

  public void verifyHsJobAttempts(JSONObject info, Job job)
      throws JSONException {

    JSONArray attempts = info.getJSONArray("jobAttempt");
    assertEquals("incorrect number of elements", 2, attempts.length());
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

    assertEquals("incorrect number of elements", 2, nodes.getLength());
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
        assertTrue("startime not greater than 0", startTime > 0);
        WebServicesTestUtils.checkStringMatch("containerId", amInfo
            .getContainerId().toString(), containerId);

        String localLogsLink = join("hsmockwebapp",
            ujoin("logs", nodeId, containerId, MRApps.toString(job.getID()),
                job.getUserName()));

        assertTrue("logsLink", logsLink.contains(localLogsLink));
      }
    }
    assertTrue("attempt: " + id + " was not found", attemptFound);
  }

}
