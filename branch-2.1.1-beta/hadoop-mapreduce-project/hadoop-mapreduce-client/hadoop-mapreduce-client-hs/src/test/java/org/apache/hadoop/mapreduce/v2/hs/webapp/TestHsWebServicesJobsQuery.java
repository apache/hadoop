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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.MockHistoryContext;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;

/**
 * Test the history server Rest API for getting jobs with various query
 * parameters.
 *
 * /ws/v1/history/mapreduce/jobs?{query=value}
 */
public class TestHsWebServicesJobsQuery extends JerseyTest {

  private static Configuration conf = new Configuration();
  private static MockHistoryContext appContext;
  private static HsWebApp webApp;

  private Injector injector = Guice.createInjector(new ServletModule() {
    @Override
    protected void configureServlets() {

      appContext = new MockHistoryContext(3, 2, 1);
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

  public TestHsWebServicesJobsQuery() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.mapreduce.v2.hs.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testJobsQueryStateNone() throws JSONException, Exception {
    WebResource r = resource();

     ArrayList<JobState> JOB_STATES = 
         new ArrayList<JobState>(Arrays.asList(JobState.values()));

      // find a state that isn't in use
      Map<JobId, Job> jobsMap = appContext.getAllJobs();
      for (Map.Entry<JobId, Job> entry : jobsMap.entrySet()) {
        JOB_STATES.remove(entry.getValue().getState());
      }

    assertTrue("No unused job states", JOB_STATES.size() > 0);
    JobState notInUse = JOB_STATES.get(0);

    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").queryParam("state", notInUse.toString())
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("jobs is not null", JSONObject.NULL, json.get("jobs"));
  }

  @Test
  public void testJobsQueryState() throws JSONException, Exception {
    WebResource r = resource();
    // we only create 3 jobs and it cycles through states so we should have 3 unique states
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    String queryState = "BOGUS";
    JobId jid = null;
    for (Map.Entry<JobId, Job> entry : jobsMap.entrySet()) {
      jid = entry.getValue().getID();
      queryState = entry.getValue().getState().toString();
      break;
    }
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").queryParam("state", queryState)
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    assertEquals("incorrect number of elements", 1, arr.length());
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getPartialJob(jid);
    VerifyJobsUtils.verifyHsJobPartial(info, job);
  }

  @Test
  public void testJobsQueryStateInvalid() throws JSONException, Exception {
    WebResource r = resource();

    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").queryParam("state", "InvalidState")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils
        .checkStringContains(
            "exception message",
            "org.apache.hadoop.mapreduce.v2.api.records.JobState.InvalidState",
            message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "IllegalArgumentException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "java.lang.IllegalArgumentException", classname);
  }


  @Test
  public void testJobsQueryUserNone() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").queryParam("user", "bogus")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("jobs is not null", JSONObject.NULL, json.get("jobs"));
  }

  @Test
  public void testJobsQueryUser() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").queryParam("user", "mock")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    System.out.println(json.toString());

    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    assertEquals("incorrect number of elements", 3, arr.length());
    // just verify one of them.
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getPartialJob(MRApps.toJobID(info.getString("id")));
    VerifyJobsUtils.verifyHsJobPartial(info, job);
  }

  @Test
  public void testJobsQueryLimit() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").queryParam("limit", "2")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    // make sure we get 2 back
    assertEquals("incorrect number of elements", 2, arr.length());
  }

  @Test
  public void testJobsQueryLimitInvalid() throws JSONException, Exception {
    WebResource r = resource();

    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").queryParam("limit", "-1")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils.checkStringMatch("exception message",
        "java.lang.Exception: limit value must be greater then 0", message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
  }

  @Test
  public void testJobsQueryQueue() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").queryParam("queue", "mockqueue")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    assertEquals("incorrect number of elements", 3, arr.length());
  }

  @Test
  public void testJobsQueryQueueNonExist() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").queryParam("queue", "bogus")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("jobs is not null", JSONObject.NULL, json.get("jobs"));
  }

  @Test
  public void testJobsQueryStartTimeEnd() throws JSONException, Exception {
    WebResource r = resource();
    // the mockJobs start time is the current time - some random amount
    Long now = System.currentTimeMillis();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs")
        .queryParam("startedTimeEnd", String.valueOf(now))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    assertEquals("incorrect number of elements", 3, arr.length());
  }

  @Test
  public void testJobsQueryStartTimeBegin() throws JSONException, Exception {
    WebResource r = resource();
    // the mockJobs start time is the current time - some random amount
    Long now = System.currentTimeMillis();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs")
        .queryParam("startedTimeBegin", String.valueOf(now))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("jobs is not null", JSONObject.NULL, json.get("jobs"));
  }

  @Test
  public void testJobsQueryStartTimeBeginEnd() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    int size = jobsMap.size();
    ArrayList<Long> startTime = new ArrayList<Long>(size);
    // figure out the middle start Time
    for (Map.Entry<JobId, Job> entry : jobsMap.entrySet()) {
      startTime.add(entry.getValue().getReport().getStartTime());
    }
    Collections.sort(startTime);

    assertTrue("Error we must have atleast 3 jobs", size >= 3);
    long midStartTime = startTime.get(size - 2);

    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs")
        .queryParam("startedTimeBegin", String.valueOf(40000))
        .queryParam("startedTimeEnd", String.valueOf(midStartTime))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    assertEquals("incorrect number of elements", size - 1, arr.length());
  }

  @Test
  public void testJobsQueryStartTimeBeginEndInvalid() throws JSONException,
      Exception {
    WebResource r = resource();
    Long now = System.currentTimeMillis();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs")
        .queryParam("startedTimeBegin", String.valueOf(now))
        .queryParam("startedTimeEnd", String.valueOf(40000))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils
        .checkStringMatch(
            "exception message",
            "java.lang.Exception: startedTimeEnd must be greater than startTimeBegin",
            message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
  }

  @Test
  public void testJobsQueryStartTimeInvalidformat() throws JSONException,
      Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").queryParam("startedTimeBegin", "efsd")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils
        .checkStringMatch(
            "exception message",
            "java.lang.Exception: Invalid number format: For input string: \"efsd\"",
            message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
  }

  @Test
  public void testJobsQueryStartTimeEndInvalidformat() throws JSONException,
      Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").queryParam("startedTimeEnd", "efsd")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils
        .checkStringMatch(
            "exception message",
            "java.lang.Exception: Invalid number format: For input string: \"efsd\"",
            message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
  }

  @Test
  public void testJobsQueryStartTimeNegative() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs")
        .queryParam("startedTimeBegin", String.valueOf(-1000))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils
        .checkStringMatch("exception message",
            "java.lang.Exception: startedTimeBegin must be greater than 0",
            message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
  }

  @Test
  public void testJobsQueryStartTimeEndNegative() throws JSONException,
      Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs")
        .queryParam("startedTimeEnd", String.valueOf(-1000))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils.checkStringMatch("exception message",
        "java.lang.Exception: startedTimeEnd must be greater than 0", message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
  }

  @Test
  public void testJobsQueryFinishTimeEndNegative() throws JSONException,
      Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs")
        .queryParam("finishedTimeEnd", String.valueOf(-1000))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils.checkStringMatch("exception message",
        "java.lang.Exception: finishedTimeEnd must be greater than 0", message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
  }

  @Test
  public void testJobsQueryFinishTimeBeginNegative() throws JSONException,
      Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs")
        .queryParam("finishedTimeBegin", String.valueOf(-1000))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils.checkStringMatch("exception message",
        "java.lang.Exception: finishedTimeBegin must be greater than 0",
        message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
  }

  @Test
  public void testJobsQueryFinishTimeBeginEndInvalid() throws JSONException,
      Exception {
    WebResource r = resource();
    Long now = System.currentTimeMillis();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs")
        .queryParam("finishedTimeBegin", String.valueOf(now))
        .queryParam("finishedTimeEnd", String.valueOf(40000))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils
        .checkStringMatch(
            "exception message",
            "java.lang.Exception: finishedTimeEnd must be greater than finishedTimeBegin",
            message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
  }

  @Test
  public void testJobsQueryFinishTimeInvalidformat() throws JSONException,
      Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").queryParam("finishedTimeBegin", "efsd")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils
        .checkStringMatch(
            "exception message",
            "java.lang.Exception: Invalid number format: For input string: \"efsd\"",
            message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
  }

  @Test
  public void testJobsQueryFinishTimeEndInvalidformat() throws JSONException,
      Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs").queryParam("finishedTimeEnd", "efsd")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    assertEquals("incorrect number of elements", 3, exception.length());
    String message = exception.getString("message");
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils
        .checkStringMatch(
            "exception message",
            "java.lang.Exception: Invalid number format: For input string: \"efsd\"",
            message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
  }

  @Test
  public void testJobsQueryFinishTimeBegin() throws JSONException, Exception {
    WebResource r = resource();
    // the mockJobs finish time is the current time + some random amount
    Long now = System.currentTimeMillis();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs")
        .queryParam("finishedTimeBegin", String.valueOf(now))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    assertEquals("incorrect number of elements", 3, arr.length());
  }

  @Test
  public void testJobsQueryFinishTimeEnd() throws JSONException, Exception {
    WebResource r = resource();
    // the mockJobs finish time is the current time + some random amount
    Long now = System.currentTimeMillis();
    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs")
        .queryParam("finishedTimeEnd", String.valueOf(now))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("jobs is not null", JSONObject.NULL, json.get("jobs"));
  }

  @Test
  public void testJobsQueryFinishTimeBeginEnd() throws JSONException, Exception {
    WebResource r = resource();

    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    int size = jobsMap.size();
    // figure out the mid end time - we expect atleast 3 jobs
    ArrayList<Long> finishTime = new ArrayList<Long>(size);
    for (Map.Entry<JobId, Job> entry : jobsMap.entrySet()) {
      finishTime.add(entry.getValue().getReport().getFinishTime());
    }
    Collections.sort(finishTime);

    assertTrue("Error we must have atleast 3 jobs", size >= 3);
    long midFinishTime = finishTime.get(size - 2);

    ClientResponse response = r.path("ws").path("v1").path("history")
        .path("mapreduce").path("jobs")
        .queryParam("finishedTimeBegin", String.valueOf(40000))
        .queryParam("finishedTimeEnd", String.valueOf(midFinishTime))
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    assertEquals("incorrect number of elements", size - 1, arr.length());
  }

}
