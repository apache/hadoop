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
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import com.google.inject.util.Providers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
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
 * Test the history server Rest API for getting task attempts, a
 * specific task attempt, and task attempt counters
 *
 * /ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts
 * /ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}
 * /ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/
 * counters
 */
public class TestHsWebServicesAttempts extends JerseyTestBase {

  private static Configuration conf = new Configuration();
  private static HistoryContext appContext;
  private static HsWebApp webApp;

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      appContext = new MockHistoryContext(0, 1, 2, 1);
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

  public TestHsWebServicesAttempts() {
    super(
        new WebAppDescriptor.Builder("org.apache.hadoop.mapreduce.v2.hs.webapp")
            .contextListenerClass(GuiceServletConfig.class)
            .filterClass(com.google.inject.servlet.GuiceFilter.class)
            .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  void testTaskAttempts() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        ClientResponse response = r.path("ws").path("v1").path("history")
            .path("mapreduce").path("jobs").path(jobId).path("tasks").path(tid)
            .path("attempts").accept(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
            response.getType().toString());
        JSONObject json = response.getEntity(JSONObject.class);
        verifyHsTaskAttempts(json, task);
      }
    }
  }

  @Test
  void testTaskAttemptsSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        ClientResponse response = r.path("ws").path("v1").path("history")
            .path("mapreduce").path("jobs").path(jobId).path("tasks").path(tid)
            .path("attempts/").accept(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
            response.getType().toString());
        JSONObject json = response.getEntity(JSONObject.class);
        verifyHsTaskAttempts(json, task);
      }
    }
  }

  @Test
  void testTaskAttemptsDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        ClientResponse response = r.path("ws").path("v1").path("history")
            .path("mapreduce").path("jobs").path(jobId).path("tasks").path(tid)
            .path("attempts").get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
            response.getType().toString());
        JSONObject json = response.getEntity(JSONObject.class);
        verifyHsTaskAttempts(json, task);
      }
    }
  }

  @Test
  void testTaskAttemptsXML() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        ClientResponse response = r.path("ws").path("v1").path("history")
            .path("mapreduce").path("jobs").path(jobId).path("tasks").path(tid)
            .path("attempts").accept(MediaType.APPLICATION_XML)
            .get(ClientResponse.class);

        assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
            response.getType().toString());
        String xml = response.getEntity(String.class);
        DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputSource is = new InputSource();
        is.setCharacterStream(new StringReader(xml));
        Document dom = db.parse(is);
        NodeList attempts = dom.getElementsByTagName("taskAttempts");
        assertEquals(1, attempts.getLength(), "incorrect number of elements");

        NodeList nodes = dom.getElementsByTagName("taskAttempt");
        verifyHsTaskAttemptsXML(nodes, task);
      }
    }
  }

  @Test
  void testTaskAttemptId() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();

    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      for (Task task : jobsMap.get(id).getTasks().values()) {
        String tid = MRApps.toString(task.getID());

        for (TaskAttempt att : task.getAttempts().values()) {
          TaskAttemptId attemptid = att.getID();
          String attid = MRApps.toString(attemptid);

          ClientResponse response = r.path("ws").path("v1").path("history")
              .path("mapreduce").path("jobs").path(jobId).path("tasks")
              .path(tid).path("attempts").path(attid)
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
          assertEquals(MediaType.APPLICATION_JSON_TYPE + "; "
              + JettyUtils.UTF_8, response.getType().toString());
          JSONObject json = response.getEntity(JSONObject.class);
          assertEquals(1, json.length(), "incorrect number of elements");
          JSONObject info = json.getJSONObject("taskAttempt");
          verifyHsTaskAttempt(info, att, task.getType());
        }
      }
    }
  }

  @Test
  void testTaskAttemptIdSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();

    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      for (Task task : jobsMap.get(id).getTasks().values()) {
        String tid = MRApps.toString(task.getID());

        for (TaskAttempt att : task.getAttempts().values()) {
          TaskAttemptId attemptid = att.getID();
          String attid = MRApps.toString(attemptid);

          ClientResponse response = r.path("ws").path("v1").path("history")
              .path("mapreduce").path("jobs").path(jobId).path("tasks")
              .path(tid).path("attempts").path(attid + "/")
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
          assertEquals(MediaType.APPLICATION_JSON_TYPE + "; "
              + JettyUtils.UTF_8, response.getType().toString());
          JSONObject json = response.getEntity(JSONObject.class);
          assertEquals(1, json.length(), "incorrect number of elements");
          JSONObject info = json.getJSONObject("taskAttempt");
          verifyHsTaskAttempt(info, att, task.getType());
        }
      }
    }
  }

  @Test
  void testTaskAttemptIdDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();

    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      for (Task task : jobsMap.get(id).getTasks().values()) {
        String tid = MRApps.toString(task.getID());

        for (TaskAttempt att : task.getAttempts().values()) {
          TaskAttemptId attemptid = att.getID();
          String attid = MRApps.toString(attemptid);

          ClientResponse response = r.path("ws").path("v1").path("history")
              .path("mapreduce").path("jobs").path(jobId).path("tasks")
              .path(tid).path("attempts").path(attid).get(ClientResponse.class);
          assertEquals(MediaType.APPLICATION_JSON_TYPE + "; "
              + JettyUtils.UTF_8, response.getType().toString());
          JSONObject json = response.getEntity(JSONObject.class);
          assertEquals(1, json.length(), "incorrect number of elements");
          JSONObject info = json.getJSONObject("taskAttempt");
          verifyHsTaskAttempt(info, att, task.getType());
        }
      }
    }
  }

  @Test
  void testTaskAttemptIdXML() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        for (TaskAttempt att : task.getAttempts().values()) {
          TaskAttemptId attemptid = att.getID();
          String attid = MRApps.toString(attemptid);

          ClientResponse response = r.path("ws").path("v1").path("history")
              .path("mapreduce").path("jobs").path(jobId).path("tasks")
              .path(tid).path("attempts").path(attid)
              .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);

          assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
              response.getType().toString());
          String xml = response.getEntity(String.class);
          DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
          DocumentBuilder db = dbf.newDocumentBuilder();
          InputSource is = new InputSource();
          is.setCharacterStream(new StringReader(xml));
          Document dom = db.parse(is);
          NodeList nodes = dom.getElementsByTagName("taskAttempt");
          for (int i = 0; i < nodes.getLength(); i++) {
            Element element = (Element) nodes.item(i);
            verifyHsTaskAttemptXML(element, att, task.getType());
          }
        }
      }
    }
  }

  @Test
  void testTaskAttemptIdBogus() throws JSONException, Exception {

    testTaskAttemptIdErrorGeneric("bogusid",
        "java.lang.Exception: TaskAttemptId string : "
            + "bogusid is not properly formed");
  }

  @Test
  void testTaskAttemptIdNonExist() throws JSONException, Exception {

    testTaskAttemptIdErrorGeneric(
        "attempt_0_1234_m_000000_0",
        "java.lang.Exception: Error getting info on task attempt id attempt_0_1234_m_000000_0");
  }

  @Test
  void testTaskAttemptIdInvalid() throws JSONException, Exception {

    testTaskAttemptIdErrorGeneric("attempt_0_1234_d_000000_0",
        "java.lang.Exception: Bad TaskType identifier. TaskAttemptId string : "
            + "attempt_0_1234_d_000000_0 is not properly formed.");
  }

  @Test
  void testTaskAttemptIdInvalid2() throws JSONException, Exception {

    testTaskAttemptIdErrorGeneric("attempt_1234_m_000000_0",
        "java.lang.Exception: TaskAttemptId string : "
            + "attempt_1234_m_000000_0 is not properly formed");
  }

  @Test
  void testTaskAttemptIdInvalid3() throws JSONException, Exception {

    testTaskAttemptIdErrorGeneric("attempt_0_1234_m_000000",
        "java.lang.Exception: TaskAttemptId string : "
            + "attempt_0_1234_m_000000 is not properly formed");
  }

  private void testTaskAttemptIdErrorGeneric(String attid, String error)
      throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();

    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      for (Task task : jobsMap.get(id).getTasks().values()) {
        String tid = MRApps.toString(task.getID());

        try {
          r.path("ws").path("v1").path("history").path("mapreduce")
              .path("jobs").path(jobId).path("tasks").path(tid)
              .path("attempts").path(attid).accept(MediaType.APPLICATION_JSON)
              .get(JSONObject.class);
          fail("should have thrown exception on invalid uri");
        } catch (UniformInterfaceException ue) {
          ClientResponse response = ue.getResponse();
          assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
          assertEquals(MediaType.APPLICATION_JSON_TYPE + "; "
                  + JettyUtils.UTF_8, response.getType().toString());
          JSONObject msg = response.getEntity(JSONObject.class);
          JSONObject exception = msg.getJSONObject("RemoteException");
          assertEquals(3, exception.length(), "incorrect number of elements");
          String message = exception.getString("message");
          String type = exception.getString("exception");
          String classname = exception.getString("javaClassName");
          WebServicesTestUtils.checkStringMatch("exception message", error,
              message);
          WebServicesTestUtils.checkStringMatch("exception type",
              "NotFoundException", type);
          WebServicesTestUtils.checkStringMatch("exception classname",
              "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
        }
      }
    }
  }

  public void verifyHsTaskAttemptXML(Element element, TaskAttempt att,
      TaskType ttype) {
    verifyTaskAttemptGeneric(att, ttype,
        WebServicesTestUtils.getXmlString(element, "id"),
        WebServicesTestUtils.getXmlString(element, "state"),
        WebServicesTestUtils.getXmlString(element, "type"),
        WebServicesTestUtils.getXmlString(element, "rack"),
        WebServicesTestUtils.getXmlString(element, "nodeHttpAddress"),
        WebServicesTestUtils.getXmlString(element, "diagnostics"),
        WebServicesTestUtils.getXmlString(element, "assignedContainerId"),
        WebServicesTestUtils.getXmlLong(element, "startTime"),
        WebServicesTestUtils.getXmlLong(element, "finishTime"),
        WebServicesTestUtils.getXmlLong(element, "elapsedTime"),
        WebServicesTestUtils.getXmlFloat(element, "progress"));

    if (ttype == TaskType.REDUCE) {
      verifyReduceTaskAttemptGeneric(att,
          WebServicesTestUtils.getXmlLong(element, "shuffleFinishTime"),
          WebServicesTestUtils.getXmlLong(element, "mergeFinishTime"),
          WebServicesTestUtils.getXmlLong(element, "elapsedShuffleTime"),
          WebServicesTestUtils.getXmlLong(element, "elapsedMergeTime"),
          WebServicesTestUtils.getXmlLong(element, "elapsedReduceTime"));
    }
  }

  public void verifyHsTaskAttempt(JSONObject info, TaskAttempt att,
      TaskType ttype) throws JSONException {
    if (ttype == TaskType.REDUCE) {
      assertEquals(17, info.length(), "incorrect number of elements");
    } else {
      assertEquals(12, info.length(), "incorrect number of elements");
    }

    verifyTaskAttemptGeneric(att, ttype, info.getString("id"),
        info.getString("state"), info.getString("type"),
        info.getString("rack"), info.getString("nodeHttpAddress"),
        info.getString("diagnostics"), info.getString("assignedContainerId"),
        info.getLong("startTime"), info.getLong("finishTime"),
        info.getLong("elapsedTime"), (float) info.getDouble("progress"));

    if (ttype == TaskType.REDUCE) {
      verifyReduceTaskAttemptGeneric(att, info.getLong("shuffleFinishTime"),
          info.getLong("mergeFinishTime"), info.getLong("elapsedShuffleTime"),
          info.getLong("elapsedMergeTime"), info.getLong("elapsedReduceTime"));
    }
  }

  public void verifyHsTaskAttempts(JSONObject json, Task task)
      throws JSONException {
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject attempts = json.getJSONObject("taskAttempts");
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONArray arr = attempts.getJSONArray("taskAttempt");
    for (TaskAttempt att : task.getAttempts().values()) {
      TaskAttemptId id = att.getID();
      String attid = MRApps.toString(id);
      Boolean found = false;

      for (int i = 0; i < arr.length(); i++) {
        JSONObject info = arr.getJSONObject(i);
        if (attid.matches(info.getString("id"))) {
          found = true;
          verifyHsTaskAttempt(info, att, task.getType());
        }
      }
      assertTrue(found, "task attempt with id: " + attid
          + " not in web service output");
    }
  }

  public void verifyHsTaskAttemptsXML(NodeList nodes, Task task) {
    assertEquals(1, nodes.getLength(), "incorrect number of elements");

    for (TaskAttempt att : task.getAttempts().values()) {
      TaskAttemptId id = att.getID();
      String attid = MRApps.toString(id);
      Boolean found = false;
      for (int i = 0; i < nodes.getLength(); i++) {
        Element element = (Element) nodes.item(i);

        if (attid.matches(WebServicesTestUtils.getXmlString(element, "id"))) {
          found = true;
          verifyHsTaskAttemptXML(element, att, task.getType());
        }
      }
      assertTrue(found, "task with id: " + attid + " not in web service output");
    }
  }

  public void verifyTaskAttemptGeneric(TaskAttempt ta, TaskType ttype,
      String id, String state, String type, String rack,
      String nodeHttpAddress, String diagnostics, String assignedContainerId,
      long startTime, long finishTime, long elapsedTime, float progress) {

    TaskAttemptId attid = ta.getID();
    String attemptId = MRApps.toString(attid);

    WebServicesTestUtils.checkStringMatch("id", attemptId, id);
    WebServicesTestUtils.checkStringMatch("type", ttype.toString(), type);
    WebServicesTestUtils.checkStringMatch("state", ta.getState().toString(),
        state);
    WebServicesTestUtils.checkStringMatch("rack", ta.getNodeRackName(), rack);
    WebServicesTestUtils.checkStringMatch("nodeHttpAddress",
        ta.getNodeHttpAddress(), nodeHttpAddress);

    String expectDiag = "";
    List<String> diagnosticsList = ta.getDiagnostics();
    if (diagnosticsList != null && !diagnostics.isEmpty()) {
      StringBuilder b = new StringBuilder();
      for (String diag : diagnosticsList) {
        b.append(diag);
      }
      expectDiag = b.toString();
    }
    WebServicesTestUtils.checkStringMatch("diagnostics", expectDiag,
        diagnostics);
    WebServicesTestUtils.checkStringMatch("assignedContainerId",
        ta.getAssignedContainerID().toString(),
        assignedContainerId);

    assertEquals(ta.getLaunchTime(), startTime, "startTime wrong");
    assertEquals(ta.getFinishTime(), finishTime, "finishTime wrong");
    assertEquals(finishTime - startTime, elapsedTime, "elapsedTime wrong");
    assertEquals(ta.getProgress() * 100, progress, 1e-3f, "progress wrong");
  }

  public void verifyReduceTaskAttemptGeneric(TaskAttempt ta,
      long shuffleFinishTime, long mergeFinishTime, long elapsedShuffleTime,
      long elapsedMergeTime, long elapsedReduceTime) {

    assertEquals(ta.getShuffleFinishTime(),
        shuffleFinishTime,
        "shuffleFinishTime wrong");
    assertEquals(ta.getSortFinishTime(),
        mergeFinishTime,
        "mergeFinishTime wrong");
    assertEquals(ta.getShuffleFinishTime() - ta.getLaunchTime(), elapsedShuffleTime,
        "elapsedShuffleTime wrong");
    assertEquals(ta.getSortFinishTime() - ta.getShuffleFinishTime(), elapsedMergeTime,
        "elapsedMergeTime wrong");
    assertEquals(ta.getFinishTime() - ta.getSortFinishTime(), elapsedReduceTime,
        "elapsedReduceTime wrong");
  }

  @Test
  void testTaskAttemptIdCounters() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();

    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      for (Task task : jobsMap.get(id).getTasks().values()) {
        String tid = MRApps.toString(task.getID());

        for (TaskAttempt att : task.getAttempts().values()) {
          TaskAttemptId attemptid = att.getID();
          String attid = MRApps.toString(attemptid);

          ClientResponse response = r.path("ws").path("v1").path("history")
              .path("mapreduce").path("jobs").path(jobId).path("tasks")
              .path(tid).path("attempts").path(attid).path("counters")
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
          assertEquals(MediaType.APPLICATION_JSON_TYPE + "; "
              + JettyUtils.UTF_8, response.getType().toString());
          JSONObject json = response.getEntity(JSONObject.class);
          assertEquals(1, json.length(), "incorrect number of elements");
          JSONObject info = json.getJSONObject("jobTaskAttemptCounters");
          verifyHsJobTaskAttemptCounters(info, att);
        }
      }
    }
  }

  @Test
  void testTaskAttemptIdXMLCounters() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        for (TaskAttempt att : task.getAttempts().values()) {
          TaskAttemptId attemptid = att.getID();
          String attid = MRApps.toString(attemptid);

          ClientResponse response = r.path("ws").path("v1").path("history")
              .path("mapreduce").path("jobs").path(jobId).path("tasks")
              .path(tid).path("attempts").path(attid).path("counters")
              .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);

          assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
              response.getType().toString());
          String xml = response.getEntity(String.class);
          DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
          DocumentBuilder db = dbf.newDocumentBuilder();
          InputSource is = new InputSource();
          is.setCharacterStream(new StringReader(xml));
          Document dom = db.parse(is);
          NodeList nodes = dom.getElementsByTagName("jobTaskAttemptCounters");

          verifyHsTaskCountersXML(nodes, att);
        }
      }
    }
  }

  public void verifyHsJobTaskAttemptCounters(JSONObject info, TaskAttempt att)
      throws JSONException {

    assertEquals(2, info.length(), "incorrect number of elements");

    WebServicesTestUtils.checkStringMatch("id", MRApps.toString(att.getID()),
        info.getString("id"));

    // just do simple verification of fields - not data is correct
    // in the fields
    JSONArray counterGroups = info.getJSONArray("taskAttemptCounterGroup");
    for (int i = 0; i < counterGroups.length(); i++) {
      JSONObject counterGroup = counterGroups.getJSONObject(i);
      String name = counterGroup.getString("counterGroupName");
      assertTrue((name != null && !name.isEmpty()), "name not set");
      JSONArray counters = counterGroup.getJSONArray("counter");
      for (int j = 0; j < counters.length(); j++) {
        JSONObject counter = counters.getJSONObject(j);
        String counterName = counter.getString("name");
        assertTrue((counterName != null && !counterName.isEmpty()),
            "name not set");
        long value = counter.getLong("value");
        assertTrue(value >= 0, "value  >= 0");
      }
    }
  }

  public void verifyHsTaskCountersXML(NodeList nodes, TaskAttempt att) {

    for (int i = 0; i < nodes.getLength(); i++) {

      Element element = (Element) nodes.item(i);
      WebServicesTestUtils.checkStringMatch("id", MRApps.toString(att.getID()),
          WebServicesTestUtils.getXmlString(element, "id"));
      // just do simple verification of fields - not data is correct
      // in the fields
      NodeList groups = element.getElementsByTagName("taskAttemptCounterGroup");

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

          long value = WebServicesTestUtils.getXmlLong(counter, "value");
          assertTrue(value >= 0, "value not >= 0");

        }
      }
    }
  }

}
