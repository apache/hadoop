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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MockAppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
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
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;

/**
 * Test the app master web service Rest API for getting tasks, a specific task,
 * and task counters.
 *
 * /ws/v1/mapreduce/jobs/{jobid}/tasks
 * /ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}
 * /ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/counters
 */
public class TestAMWebServicesTasks extends JerseyTest {

  private static Configuration conf = new Configuration();
  private static AppContext appContext;

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {

      appContext = new MockAppContext(0, 1, 2, 1);
      bind(JAXBContextResolver.class);
      bind(AMWebServices.class);
      bind(GenericExceptionHandler.class);
      bind(AppContext.class).toInstance(appContext);
      bind(Configuration.class).toInstance(conf);

      serve("/*").with(GuiceContainer.class);
    }
  }

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  public TestAMWebServicesTasks() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.mapreduce.v2.app.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testTasks() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("tasks")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject tasks = json.getJSONObject("tasks");
      JSONArray arr = tasks.getJSONArray("task");
      assertEquals("incorrect number of elements", 2, arr.length());

      verifyAMTask(arr, jobsMap.get(id), null);
    }
  }

  @Test
  public void testTasksDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("tasks").get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject tasks = json.getJSONObject("tasks");
      JSONArray arr = tasks.getJSONArray("task");
      assertEquals("incorrect number of elements", 2, arr.length());

      verifyAMTask(arr, jobsMap.get(id), null);
    }
  }

  @Test
  public void testTasksSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("tasks/")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject tasks = json.getJSONObject("tasks");
      JSONArray arr = tasks.getJSONArray("task");
      assertEquals("incorrect number of elements", 2, arr.length());

      verifyAMTask(arr, jobsMap.get(id), null);
    }
  }

  @Test
  public void testTasksXML() throws JSONException, Exception {

    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("tasks")
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
      String xml = response.getEntity(String.class);
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      Document dom = db.parse(is);
      NodeList tasks = dom.getElementsByTagName("tasks");
      assertEquals("incorrect number of elements", 1, tasks.getLength());
      NodeList task = dom.getElementsByTagName("task");
      verifyAMTaskXML(task, jobsMap.get(id));
    }
  }

  @Test
  public void testTasksQueryMap() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String type = "m";
      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("tasks").queryParam("type", type)
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject tasks = json.getJSONObject("tasks");
      JSONArray arr = tasks.getJSONArray("task");
      assertEquals("incorrect number of elements", 1, arr.length());
      verifyAMTask(arr, jobsMap.get(id), type);
    }
  }

  @Test
  public void testTasksQueryReduce() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String type = "r";
      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("tasks").queryParam("type", type)
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject tasks = json.getJSONObject("tasks");
      JSONArray arr = tasks.getJSONArray("task");
      assertEquals("incorrect number of elements", 1, arr.length());
      verifyAMTask(arr, jobsMap.get(id), type);
    }
  }

  @Test
  public void testTasksQueryInvalid() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      // tasktype must be exactly either "m" or "r"
      String tasktype = "reduce";

      try {
        r.path("ws").path("v1").path("mapreduce").path("jobs").path(jobId)
            .path("tasks").queryParam("type", tasktype)
            .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
        fail("should have thrown exception on invalid uri");
      } catch (UniformInterfaceException ue) {
        ClientResponse response = ue.getResponse();
        assertResponseStatusCode(Status.BAD_REQUEST, response.getStatusInfo());
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        JSONObject msg = response.getEntity(JSONObject.class);
        JSONObject exception = msg.getJSONObject("RemoteException");
        assertEquals("incorrect number of elements", 3, exception.length());
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringMatch("exception message",
            "java.lang.Exception: tasktype must be either m or r", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "BadRequestException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
      }
    }
  }

  @Test
  public void testTaskId() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        ClientResponse response = r.path("ws").path("v1").path("mapreduce")
            .path("jobs").path(jobId).path("tasks").path(tid)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        JSONObject json = response.getEntity(JSONObject.class);
        assertEquals("incorrect number of elements", 1, json.length());
        JSONObject info = json.getJSONObject("task");
        verifyAMSingleTask(info, task);
      }
    }
  }

  @Test
  public void testTaskIdSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        ClientResponse response = r.path("ws").path("v1").path("mapreduce")
            .path("jobs").path(jobId).path("tasks").path(tid + "/")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        JSONObject json = response.getEntity(JSONObject.class);
        assertEquals("incorrect number of elements", 1, json.length());
        JSONObject info = json.getJSONObject("task");
        verifyAMSingleTask(info, task);
      }
    }
  }

  @Test
  public void testTaskIdDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        ClientResponse response = r.path("ws").path("v1").path("mapreduce")
            .path("jobs").path(jobId).path("tasks").path(tid)
            .get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        JSONObject json = response.getEntity(JSONObject.class);
        assertEquals("incorrect number of elements", 1, json.length());
        JSONObject info = json.getJSONObject("task");
        verifyAMSingleTask(info, task);
      }
    }
  }

  @Test
  public void testTaskIdBogus() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String tid = "bogustaskid";
      try {
        r.path("ws").path("v1").path("mapreduce").path("jobs").path(jobId)
            .path("tasks").path(tid).get(JSONObject.class);
        fail("should have thrown exception on invalid uri");
      } catch (UniformInterfaceException ue) {
        ClientResponse response = ue.getResponse();
        assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        JSONObject msg = response.getEntity(JSONObject.class);
        JSONObject exception = msg.getJSONObject("RemoteException");
        assertEquals("incorrect number of elements", 3, exception.length());
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "java.lang.Exception: TaskId string : "
                + "bogustaskid is not properly formed"
                + "\nReason: java.util.regex.Matcher[pattern=" +
                TaskID.TASK_ID_REGEX + " region=0,11 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
      }
    }
  }

  @Test
  public void testTaskIdNonExist() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String tid = "task_0_0000_m_000000";
      try {
        r.path("ws").path("v1").path("mapreduce").path("jobs").path(jobId)
            .path("tasks").path(tid).get(JSONObject.class);
        fail("should have thrown exception on invalid uri");
      } catch (UniformInterfaceException ue) {
        ClientResponse response = ue.getResponse();
        assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        JSONObject msg = response.getEntity(JSONObject.class);
        JSONObject exception = msg.getJSONObject("RemoteException");
        assertEquals("incorrect number of elements", 3, exception.length());
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringMatch("exception message",
            "java.lang.Exception: task not found with id task_0_0000_m_000000",
            message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
      }
    }
  }

  @Test
  public void testTaskIdInvalid() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String tid = "task_0_0000_d_000000";
      try {
        r.path("ws").path("v1").path("mapreduce").path("jobs").path(jobId)
            .path("tasks").path(tid).get(JSONObject.class);
        fail("should have thrown exception on invalid uri");
      } catch (UniformInterfaceException ue) {
        ClientResponse response = ue.getResponse();
        assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        JSONObject msg = response.getEntity(JSONObject.class);
        JSONObject exception = msg.getJSONObject("RemoteException");
        assertEquals("incorrect number of elements", 3, exception.length());
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "java.lang.Exception: TaskId string : "
                + "task_0_0000_d_000000 is not properly formed"
                + "\nReason: java.util.regex.Matcher[pattern=" +
                TaskID.TASK_ID_REGEX + " region=0,20 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
      }
    }
  }

  @Test
  public void testTaskIdInvalid2() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String tid = "task_0_m_000000";
      try {
        r.path("ws").path("v1").path("mapreduce").path("jobs").path(jobId)
            .path("tasks").path(tid).get(JSONObject.class);
        fail("should have thrown exception on invalid uri");
      } catch (UniformInterfaceException ue) {
        ClientResponse response = ue.getResponse();
        assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        JSONObject msg = response.getEntity(JSONObject.class);
        JSONObject exception = msg.getJSONObject("RemoteException");
        assertEquals("incorrect number of elements", 3, exception.length());
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "java.lang.Exception: TaskId string : "
                + "task_0_m_000000 is not properly formed"
                + "\nReason: java.util.regex.Matcher[pattern=" +
                TaskID.TASK_ID_REGEX + " region=0,15 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
      }
    }
  }

  @Test
  public void testTaskIdInvalid3() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String tid = "task_0_0000_m";
      try {
        r.path("ws").path("v1").path("mapreduce").path("jobs").path(jobId)
            .path("tasks").path(tid).get(JSONObject.class);
        fail("should have thrown exception on invalid uri");
      } catch (UniformInterfaceException ue) {
        ClientResponse response = ue.getResponse();
        assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        JSONObject msg = response.getEntity(JSONObject.class);
        JSONObject exception = msg.getJSONObject("RemoteException");
        assertEquals("incorrect number of elements", 3, exception.length());
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "java.lang.Exception: TaskId string : "
                + "task_0_0000_m is not properly formed"
                + "\nReason: java.util.regex.Matcher[pattern=" +
                TaskID.TASK_ID_REGEX + " region=0,13 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
      }
    }
  }

  @Test
  public void testTaskIdXML() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        ClientResponse response = r.path("ws").path("v1").path("mapreduce")
            .path("jobs").path(jobId).path("tasks").path(tid)
            .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);

        assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
        String xml = response.getEntity(String.class);
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputSource is = new InputSource();
        is.setCharacterStream(new StringReader(xml));
        Document dom = db.parse(is);
        NodeList nodes = dom.getElementsByTagName("task");
        for (int i = 0; i < nodes.getLength(); i++) {
          Element element = (Element) nodes.item(i);
          verifyAMSingleTaskXML(element, task);
        }
      }
    }
  }

  public void verifyAMSingleTask(JSONObject info, Task task)
      throws JSONException {
    assertEquals("incorrect number of elements", 9, info.length());

    verifyTaskGeneric(task, info.getString("id"), info.getString("state"),
        info.getString("type"), info.getString("successfulAttempt"),
        info.getLong("startTime"), info.getLong("finishTime"),
        info.getLong("elapsedTime"), (float) info.getDouble("progress"),
        info.getString("status"));
  }

  public void verifyAMTask(JSONArray arr, Job job, String type)
      throws JSONException {
    for (Task task : job.getTasks().values()) {
      TaskId id = task.getID();
      String tid = MRApps.toString(id);
      Boolean found = false;
      if (type != null && task.getType() == MRApps.taskType(type)) {

        for (int i = 0; i < arr.length(); i++) {
          JSONObject info = arr.getJSONObject(i);
          if (tid.matches(info.getString("id"))) {
            found = true;
            verifyAMSingleTask(info, task);
          }
        }
        assertTrue("task with id: " + tid + " not in web service output", found);
      }
    }
  }

  public void verifyTaskGeneric(Task task, String id, String state,
      String type, String successfulAttempt, long startTime, long finishTime,
      long elapsedTime, float progress, String status) {

    TaskId taskid = task.getID();
    String tid = MRApps.toString(taskid);
    TaskReport report = task.getReport();

    WebServicesTestUtils.checkStringMatch("id", tid, id);
    WebServicesTestUtils.checkStringMatch("type", task.getType().toString(),
        type);
    WebServicesTestUtils.checkStringMatch("state", report.getTaskState()
        .toString(), state);
    // not easily checked without duplicating logic, just make sure its here
    assertNotNull("successfulAttempt null", successfulAttempt);
    assertEquals("startTime wrong", report.getStartTime(), startTime);
    assertEquals("finishTime wrong", report.getFinishTime(), finishTime);
    assertEquals("elapsedTime wrong", finishTime - startTime, elapsedTime);
    assertEquals("progress wrong", report.getProgress() * 100, progress, 1e-3f);
    assertEquals("status wrong", report.getStatus(), status);
  }

  public void verifyAMSingleTaskXML(Element element, Task task) {
    verifyTaskGeneric(task, WebServicesTestUtils.getXmlString(element, "id"),
        WebServicesTestUtils.getXmlString(element, "state"),
        WebServicesTestUtils.getXmlString(element, "type"),
        WebServicesTestUtils.getXmlString(element, "successfulAttempt"),
        WebServicesTestUtils.getXmlLong(element, "startTime"),
        WebServicesTestUtils.getXmlLong(element, "finishTime"),
        WebServicesTestUtils.getXmlLong(element, "elapsedTime"),
        WebServicesTestUtils.getXmlFloat(element, "progress"),
        WebServicesTestUtils.getXmlString(element, "status"));
  }

  public void verifyAMTaskXML(NodeList nodes, Job job) {

    assertEquals("incorrect number of elements", 2, nodes.getLength());

    for (Task task : job.getTasks().values()) {
      TaskId id = task.getID();
      String tid = MRApps.toString(id);
      Boolean found = false;
      for (int i = 0; i < nodes.getLength(); i++) {
        Element element = (Element) nodes.item(i);

        if (tid.matches(WebServicesTestUtils.getXmlString(element, "id"))) {
          found = true;
          verifyAMSingleTaskXML(element, task);
        }
      }
      assertTrue("task with id: " + tid + " not in web service output", found);
    }
  }

  @Test
  public void testTaskIdCounters() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        ClientResponse response = r.path("ws").path("v1").path("mapreduce")
            .path("jobs").path(jobId).path("tasks").path(tid).path("counters")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        JSONObject json = response.getEntity(JSONObject.class);
        assertEquals("incorrect number of elements", 1, json.length());
        JSONObject info = json.getJSONObject("jobTaskCounters");
        verifyAMJobTaskCounters(info, task);
      }
    }
  }

  @Test
  public void testTaskIdCountersSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        ClientResponse response = r.path("ws").path("v1").path("mapreduce")
            .path("jobs").path(jobId).path("tasks").path(tid).path("counters/")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        JSONObject json = response.getEntity(JSONObject.class);
        assertEquals("incorrect number of elements", 1, json.length());
        JSONObject info = json.getJSONObject("jobTaskCounters");
        verifyAMJobTaskCounters(info, task);
      }
    }
  }

  @Test
  public void testTaskIdCountersDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        ClientResponse response = r.path("ws").path("v1").path("mapreduce")
            .path("jobs").path(jobId).path("tasks").path(tid).path("counters")
            .get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        JSONObject json = response.getEntity(JSONObject.class);
        assertEquals("incorrect number of elements", 1, json.length());
        JSONObject info = json.getJSONObject("jobTaskCounters");
        verifyAMJobTaskCounters(info, task);
      }
    }
  }

  @Test
  public void testJobTaskCountersXML() throws Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        ClientResponse response = r.path("ws").path("v1").path("mapreduce")
            .path("jobs").path(jobId).path("tasks").path(tid).path("counters")
            .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
        String xml = response.getEntity(String.class);
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputSource is = new InputSource();
        is.setCharacterStream(new StringReader(xml));
        Document dom = db.parse(is);
        NodeList info = dom.getElementsByTagName("jobTaskCounters");
        verifyAMTaskCountersXML(info, task);
      }
    }
  }

  public void verifyAMJobTaskCounters(JSONObject info, Task task)
      throws JSONException {

    assertEquals("incorrect number of elements", 2, info.length());

    WebServicesTestUtils.checkStringMatch("id", MRApps.toString(task.getID()),
        info.getString("id"));
    // just do simple verification of fields - not data is correct
    // in the fields
    JSONArray counterGroups = info.getJSONArray("taskCounterGroup");
    for (int i = 0; i < counterGroups.length(); i++) {
      JSONObject counterGroup = counterGroups.getJSONObject(i);
      String name = counterGroup.getString("counterGroupName");
      assertTrue("name not set", (name != null && !name.isEmpty()));
      JSONArray counters = counterGroup.getJSONArray("counter");
      for (int j = 0; j < counters.length(); j++) {
        JSONObject counter = counters.getJSONObject(j);
        String counterName = counter.getString("name");
        assertTrue("name not set",
            (counterName != null && !counterName.isEmpty()));
        long value = counter.getLong("value");
        assertTrue("value  >= 0", value >= 0);
      }
    }
  }

  public void verifyAMTaskCountersXML(NodeList nodes, Task task) {

    for (int i = 0; i < nodes.getLength(); i++) {

      Element element = (Element) nodes.item(i);
      WebServicesTestUtils.checkStringMatch("id",
          MRApps.toString(task.getID()),
          WebServicesTestUtils.getXmlString(element, "id"));
      // just do simple verification of fields - not data is correct
      // in the fields
      NodeList groups = element.getElementsByTagName("taskCounterGroup");

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

          long value = WebServicesTestUtils.getXmlLong(counter, "value");
          assertTrue("value not >= 0", value >= 0);

        }
      }
    }
  }

}
