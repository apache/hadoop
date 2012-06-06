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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MockJobs;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.CachedHistoryStorage;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.MockHistoryJobs;
import org.apache.hadoop.mapreduce.v2.hs.MockHistoryJobs.JobsPair;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;

/**
 * Test the history server Rest API for getting jobs, a specific job, job
 * counters, and job attempts.
 *
 * /ws/v1/history/mapreduce/jobs /ws/v1/history/mapreduce/jobs/{jobid}
 * /ws/v1/history/mapreduce/jobs/{jobid}/counters
 * /ws/v1/history/mapreduce/jobs/{jobid}/jobattempts
 */
public class TestHsWebServicesJobs extends JerseyTest {

  private static Configuration conf = new Configuration();
  private static TestAppContext appContext;
  private static HsWebApp webApp;

  static class TestAppContext implements HistoryContext {
    final ApplicationAttemptId appAttemptID;
    final ApplicationId appID;
    final String user = MockJobs.newUserName();
    final Map<JobId, Job> partialJobs;
    final Map<JobId, Job> fullJobs;
    final long startTime = System.currentTimeMillis();
    
    TestAppContext(int appid, int numJobs, int numTasks, int numAttempts,
        boolean hasFailedTasks) {
      appID = MockJobs.newAppID(appid);
      appAttemptID = MockJobs.newAppAttemptID(appID, 0);
      JobsPair jobs;
      try {
        jobs = MockHistoryJobs.newHistoryJobs(appID, numJobs, numTasks,
            numAttempts, hasFailedTasks);
      } catch (IOException e) {
        throw new YarnException(e);
      }
      partialJobs = jobs.partial;
      fullJobs = jobs.full;
    }

    TestAppContext(int appid, int numJobs, int numTasks, int numAttempts) {
      this(appid, numJobs, numTasks, numAttempts, false);
    }

    TestAppContext() {
      this(0, 1, 2, 1);
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptID;
    }

    @Override
    public ApplicationId getApplicationID() {
      return appID;
    }

    @Override
    public CharSequence getUser() {
      return user;
    }

    @Override
    public Job getJob(JobId jobID) {
      return fullJobs.get(jobID);
    }

    public Job getPartialJob(JobId jobID) {
      return partialJobs.get(jobID);
    }
    
    @Override
    public Map<JobId, Job> getAllJobs() {
      return partialJobs; // OK
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EventHandler getEventHandler() {
      return null;
    }

    @Override
    public Clock getClock() {
      return null;
    }

    @Override
    public String getApplicationName() {
      return "TestApp";
    }

    @Override
    public long getStartTime() {
      return startTime;
    }

    @Override
    public ClusterInfo getClusterInfo() {
      return null;
    }

    @Override
    public Map<JobId, Job> getAllJobs(ApplicationId appID) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public JobsInfo getPartialJobs(Long offset, Long count, String user,
        String queue, Long sBegin, Long sEnd, Long fBegin, Long fEnd,
        JobState jobState) {
      return CachedHistoryStorage.getPartialJobs(this.partialJobs.values(), 
          offset, count, user, queue, sBegin, sEnd, fBegin, fEnd, jobState);
    }
  }

  private Injector injector = Guice.createInjector(new ServletModule() {
    @Override
    protected void configureServlets() {

      appContext = new TestAppContext();
      webApp = mock(HsWebApp.class);
      when(webApp.name()).thenReturn("hsmockwebapp");

      bind(JAXBContextResolver.class);
      bind(HsWebServices.class);
      bind(GenericExceptionHandler.class);
      bind(WebApp.class).toInstance(webApp);
      bind(AppContext.class).toInstance(appContext);
      bind(HistoryContext.class).toInstance(appContext);
      bind(Configuration.class).toInstance(conf);

      serve("/*").with(GuiceContainer.class);
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

  public TestHsWebServicesJobs() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.mapreduce.v2.hs.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testJobs() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    assertEquals("incorrect number of elements", 1, arr.length());
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getPartialJob(MRApps.toJobID(info.getString("id")));
    VerifyJobsUtils.verifyHsJobPartial(info, job);

  }

  @Test
  public void testJobsSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    assertEquals("incorrect number of elements", 1, arr.length());
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getPartialJob(MRApps.toJobID(info.getString("id")));
    VerifyJobsUtils.verifyHsJobPartial(info, job);

  }

  @Test
  public void testJobsDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    assertEquals("incorrect number of elements", 1, arr.length());
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getPartialJob(MRApps.toJobID(info.getString("id")));
    VerifyJobsUtils.verifyHsJobPartial(info, job);

  }

  @Test
  public void testJobsXML() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
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

  public void verifyHsJobPartialXML(NodeList nodes, TestAppContext appContext) {

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
  
  public void verifyHsJobXML(NodeList nodes, TestAppContext appContext) {

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
  public void testJobId() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId)
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("job");
      VerifyJobsUtils.verifyHsJob(info, appContext.getJob(id));
    }

  }

  @Test
  public void testJobIdSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId + "/")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("job");
      VerifyJobsUtils.verifyHsJob(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobIdDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("job");
      VerifyJobsUtils.verifyHsJob(info, appContext.getJob(id));
    }

  }

  @Test
  public void testJobIdNonExist() throws JSONException, Exception {
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
          .path("job_0_1234").get(JSONObject.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
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
  public void testJobIdInvalid() throws JSONException, Exception {
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
          .path("job_foo").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      verifyJobIdInvalid(message, type, classname);

    }
  }

  // verify the exception output default is JSON
  @Test
  public void testJobIdInvalidDefault() throws JSONException, Exception {
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
          .path("job_foo").get(JSONObject.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      verifyJobIdInvalid(message, type, classname);
    }
  }

  // test that the exception output works in XML
  @Test
  public void testJobIdInvalidXML() throws JSONException, Exception {
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
          .path("job_foo").accept(MediaType.APPLICATION_XML)
          .get(JSONObject.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
      String msg = response.getEntity(String.class);
      System.out.println(msg);
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
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
  public void testJobIdInvalidBogus() throws JSONException, Exception {
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
          .path("bogusfoo").get(JSONObject.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
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
  public void testJobIdXML() throws Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId)
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
      String xml = response.getEntity(String.class);
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      Document dom = db.parse(is);
      NodeList job = dom.getElementsByTagName("job");
      verifyHsJobXML(job, appContext);
    }

  }

  @Test
  public void testJobCounters() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("counters")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobCounters");
      verifyHsJobCounters(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobCountersSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("counters/")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobCounters");
      verifyHsJobCounters(info, jobsMap.get(id));
    }
  }
  
  @Test
  public void testJobCountersForKilledJob() throws Exception {
    WebResource r = resource();
    appContext = new TestAppContext(0, 1, 1, 1, true);
    injector = Guice.createInjector(new ServletModule() {
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

        serve("/*").with(GuiceContainer.class);
      }
    });
    
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("counters/")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobCounters");
      WebServicesTestUtils.checkStringMatch("id", MRApps.toString(id),
          info.getString("id"));
      assertTrue("Job shouldn't contain any counters", info.length() == 1);
    }
  }

  @Test
  public void testJobCountersDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("counters/")
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobCounters");
      verifyHsJobCounters(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobCountersXML() throws Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("counters")
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
      String xml = response.getEntity(String.class);
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      Document dom = db.parse(is);
      NodeList info = dom.getElementsByTagName("jobCounters");
      verifyHsJobCountersXML(info, jobsMap.get(id));
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
  public void testJobAttempts() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyHsJobAttempts(info, appContext.getJob(id));
    }
  }

  @Test
  public void testJobAttemptsSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts/")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyHsJobAttempts(info, appContext.getJob(id));
    }
  }

  @Test
  public void testJobAttemptsDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts")
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyHsJobAttempts(info, appContext.getJob(id));
    }
  }

  @Test
  public void testJobAttemptsXML() throws Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts")
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
      String xml = response.getEntity(String.class);
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
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
          attempt.getString("nodeId"), attempt.getInt("id"),
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
      String nodeId, int id, long startTime, String containerId, String logsLink) {
    boolean attemptFound = false;
    for (AMInfo amInfo : job.getAMInfos()) {
      if (amInfo.getAppAttemptId().getAttemptId() == id) {
        attemptFound = true;
        String nmHost = amInfo.getNodeManagerHost();
        int nmHttpPort = amInfo.getNodeManagerHttpPort();
        int nmPort = amInfo.getNodeManagerPort();
        WebServicesTestUtils.checkStringMatch("nodeHttpAddress", nmHost + ":"
            + nmHttpPort, nodeHttpAddress);
        WebServicesTestUtils.checkStringMatch("nodeId",
            BuilderUtils.newNodeId(nmHost, nmPort).toString(), nodeId);
        assertTrue("startime not greater than 0", startTime > 0);
        WebServicesTestUtils.checkStringMatch("containerId", amInfo
            .getContainerId().toString(), containerId);

        String localLogsLink = join(
            "hsmockwebapp",
            ujoin("logs", nodeId, containerId, MRApps.toString(job.getID()),
                job.getUserName()));

        assertTrue("logsLink", logsLink.contains(localLogsLink));
      }
    }
    assertTrue("attempt: " + id + " was not found", attemptFound);
  }

}
