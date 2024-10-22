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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MockAppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.inject.Singleton;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.io.StringReader;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Test the app master web service Rest API for getting task attempts, a
 * specific task attempt, and task attempt counters
 * /ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/state
 */
public class TestAMWebServicesAttempt extends JerseyTestBase {

  private final static Configuration CONF = new Configuration();
  private static AppContext appContext;
  private final static String WEB_SERVICE_USER_NAME = "testuser";

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(AMWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    config.register(new TestRMCustomAuthFilter());
    return config;
  }

  private static class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      appContext = new MockAppContext(0, 1, 1, 1);
      App app = new App(appContext);
      bind(appContext).to(AppContext.class).named("am");
      bind(app).to(App.class).named("app");
      bind(CONF).to(Configuration.class).named("conf");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      final HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(WEB_SERVICE_USER_NAME);
      bind(response).to(HttpServletResponse.class);
      bind(request).to(HttpServletRequest.class);
    }
  }

  @Singleton
  public static class TestRMCustomAuthFilter extends AuthenticationFilter
      implements ContainerRequestFilter {
    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) throws ServletException {
      Properties props = new Properties();
      Enumeration<?> names = filterConfig.getInitParameterNames();
      while (names.hasMoreElements()) {
        String name = (String) names.nextElement();
        if (name.startsWith(configPrefix)) {
          String value = filterConfig.getInitParameter(name);
          props.put(name.substring(configPrefix.length()), value);
        }
      }
      props.put(AuthenticationFilter.AUTH_TYPE, "simple");
      props.put(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
      return props;
    }

    @Override
    public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestAMWebServicesAttempt() {
  }

  @Test
  public void testGetTaskAttemptIdState() throws Exception {
    WebTarget r = target();
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
              .path("attempts").path(attid).path("state")
              .queryParam("user.name", WEB_SERVICE_USER_NAME)
              .request(MediaType.APPLICATION_JSON).get(Response.class);
          assertEquals(MediaType.APPLICATION_JSON_TYPE + ";"
              + JettyUtils.UTF_8, response.getMediaType().toString());
          String entity = response.readEntity(String.class);
          JSONObject json = new JSONObject(entity);
          JSONObject jobState = json.getJSONObject("jobTaskAttemptState");
          assertEquals(1, json.length(), "incorrect number of elements");
          assertEquals(att.getState().toString(), jobState.get("state"));
        }
      }
    }
  }

  @Test
  public void testGetTaskAttemptIdXMLState() throws Exception {
    WebTarget r = target();
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
              .path("attempts").path(attid).path("state")
              .queryParam("user.name", WEB_SERVICE_USER_NAME)
              .request(MediaType.APPLICATION_XML).get(Response.class);

          assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
              response.getMediaType().toString());
          String xml = response.readEntity(String.class);
          DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
          DocumentBuilder db = dbf.newDocumentBuilder();
          InputSource is = new InputSource();
          is.setCharacterStream(new StringReader(xml));
          Document dom = db.parse(is);
          NodeList nodes = dom.getElementsByTagName("jobTaskAttemptState");
          assertEquals(1, nodes.getLength());
          String state = WebServicesTestUtils.getXmlString(
              (Element) nodes.item(0), "state");
          assertEquals(att.getState().toString(), state);
        }
      }
    }
  }

  @Test
  public void testPutTaskAttemptIdState() throws Exception {
    WebTarget r = target();
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
              .path("attempts").path(attid).path("state")
              .queryParam("user.name", WEB_SERVICE_USER_NAME)
              .request(MediaType.APPLICATION_JSON)
              .put(Entity.json("{\"jobTaskAttemptState\":{\"state\":\"KILLED\"}}"), Response.class);
          assertEquals(MediaType.APPLICATION_JSON_TYPE + ";"
              + JettyUtils.UTF_8, response.getMediaType().toString());
          String entity = response.readEntity(String.class);
          JSONObject json = new JSONObject(entity);
          JSONObject jobState = json.getJSONObject("jobTaskAttemptState");
          assertEquals(1, json.length(), "incorrect number of elements");
          assertEquals(TaskAttemptState.KILLED.toString(), jobState.get("state"));
        }
      }
    }
  }

  @Test
  public void testPutTaskAttemptIdXMLState() throws Exception {
    WebTarget r = target();
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
              .path("attempts").path(attid).path("state")
              .queryParam("user.name", WEB_SERVICE_USER_NAME)
              .request(MediaType.APPLICATION_XML_TYPE)
              .put(Entity.xml("<jobTaskAttemptState><state>KILLED" +
                      "</state></jobTaskAttemptState>"));
          assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
              response.getMediaType().toString());
          String xml = response.readEntity(String.class);
          DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
          DocumentBuilder db = dbf.newDocumentBuilder();
          InputSource is = new InputSource();
          is.setCharacterStream(new StringReader(xml));
          Document dom = db.parse(is);
          NodeList nodes = dom.getElementsByTagName("jobTaskAttemptState");
          assertEquals(1, nodes.getLength());
          String state = WebServicesTestUtils.getXmlString(
              (Element) nodes.item(0), "state");
          assertEquals(TaskAttemptState.KILLED.toString(), state);
        }
      }
    }
  }
}
