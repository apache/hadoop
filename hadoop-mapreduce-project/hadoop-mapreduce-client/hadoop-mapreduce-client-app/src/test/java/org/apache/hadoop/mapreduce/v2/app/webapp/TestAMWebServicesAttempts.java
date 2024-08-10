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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MockAppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
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

/**
 * Test the app master web service Rest API for getting task attempts, a
 * specific task attempt, and task attempt counters
 *
 * /ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts
 * /ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}
 * /ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/counters
 */
public class TestAMWebServicesAttempts {

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

      // serve("/*").with(GuiceContainer.class);
    }
  }

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  /*@Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  public TestAMWebServicesAttempts() {
  }*/

  @Test
  public void testTaskAttempts() throws JSONException, Exception {
    /*WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        Response response = r.path("ws").path("v1").path("mapreduce")
            .path("jobs").path(jobId).path("tasks").path(tid).path("attempts")
            .request(MediaType.APPLICATION_JSON).get(Response.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject json = response.readEntity(JSONObject.class);
        verifyAMTaskAttempts(json, task);
      }
    }*/
  }

  @Test
  public void testTaskAttemptsSlash() throws JSONException, Exception {
    /*WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        Response response = r.path("ws").path("v1").path("mapreduce")
            .path("jobs").path(jobId).path("tasks").path(tid).path("attempts/")
            .request(MediaType.APPLICATION_JSON).get(Response.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject json = response.readEntity(JSONObject.class);
        verifyAMTaskAttempts(json, task);
      }
    }*/
  }

  @Test
  public void testTaskAttemptsDefault() throws JSONException, Exception {
    /*WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        Response response = r.path("ws").path("v1").path("mapreduce")
            .path("jobs").path(jobId).path("tasks").path(tid).path("attempts").request()
            .get(Response.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject json = response.readEntity(JSONObject.class);
        verifyAMTaskAttempts(json, task);
      }
    }*/
  }

  @Test
  public void testTaskAttemptsXML() throws JSONException, Exception {
    /*WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        Response response = r.path("ws").path("v1").path("mapreduce")
            .path("jobs").path(jobId).path("tasks").path(tid).path("attempts")
            .request(MediaType.APPLICATION_XML).get(Response.class);

        assertEquals(MediaType.APPLICATION_XML_TYPE + "; " + JettyUtils.UTF_8,
            response.getMediaType().toString());
        String xml = response.readEntity(String.class);
        DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputSource is = new InputSource();
        is.setCharacterStream(new StringReader(xml));
        Document dom = db.parse(is);
        NodeList attempts = dom.getElementsByTagName("taskAttempts");
        assertEquals("incorrect number of elements", 1, attempts.getLength());

        NodeList nodes = dom.getElementsByTagName("taskAttempt");
        verifyAMTaskAttemptsXML(nodes, task);
      }
    }*/
  }

  @Test
  public void testTaskAttemptId() throws JSONException, Exception {
    /*WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();

    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      for (Task task : jobsMap.get(id).getTasks().values()) {
        String tid = MRApps.toString(task.getID());

        for (TaskAttempt att : task.getAttempts().values()) {
          TaskAttemptId attemptid = att.getID();
          String attid = MRApps.toString(attemptid);

          Response response = r.path("ws").path("v1").path("mapreduce")
              .path("jobs").path(jobId).path("tasks").path(tid)
              .path("attempts").path(attid).request(MediaType.APPLICATION_JSON)
              .get(Response.class);
          assertEquals(MediaType.APPLICATION_JSON_TYPE + "; "
                  + JettyUtils.UTF_8, response.getMediaType().toString());
          JSONObject json = response.readEntity(JSONObject.class);
          assertEquals("incorrect number of elements", 1, json.length());
          JSONObject info = json.getJSONObject("taskAttempt");
          verifyAMTaskAttempt(info, att, task.getType());
        }
      }
    }*/
  }

  @Test
  public void testTaskAttemptIdSlash() throws JSONException, Exception {
    /*WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();

    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      for (Task task : jobsMap.get(id).getTasks().values()) {
        String tid = MRApps.toString(task.getID());

        for (TaskAttempt att : task.getAttempts().values()) {
          TaskAttemptId attemptid = att.getID();
          String attid = MRApps.toString(attemptid);

          Response response = r.path("ws").path("v1").path("mapreduce")
              .path("jobs").path(jobId).path("tasks").path(tid)
              .path("attempts").path(attid + "/")
              .request(MediaType.APPLICATION_JSON).get(Response.class);
          assertEquals(MediaType.APPLICATION_JSON_TYPE + "; "
                  + JettyUtils.UTF_8, response.getMediaType().toString());
          JSONObject json = response.readEntity(JSONObject.class);
          assertEquals("incorrect number of elements", 1, json.length());
          JSONObject info = json.getJSONObject("taskAttempt");
          verifyAMTaskAttempt(info, att, task.getType());
        }
      }
    }*/
  }

  @Test
  public void testTaskAttemptIdDefault() throws JSONException, Exception {
    /*WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();

    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      for (Task task : jobsMap.get(id).getTasks().values()) {
        String tid = MRApps.toString(task.getID());

        for (TaskAttempt att : task.getAttempts().values()) {
          TaskAttemptId attemptid = att.getID();
          String attid = MRApps.toString(attemptid);

          Response response = r.path("ws").path("v1").path("mapreduce")
              .path("jobs").path(jobId).path("tasks").path(tid)
              .path("attempts").path(attid).request().get(Response.class);
          assertEquals(MediaType.APPLICATION_JSON_TYPE + "; "
                  + JettyUtils.UTF_8, response.getMediaType().toString());
          JSONObject json = response.readEntity(JSONObject.class);
          assertEquals("incorrect number of elements", 1, json.length());
          JSONObject info = json.getJSONObject("taskAttempt");
          verifyAMTaskAttempt(info, att, task.getType());
        }
      }*/
    }
}


