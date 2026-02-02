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

import static org.apache.hadoop.yarn.util.StringHelper.ujoin;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MockAppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * Test the app master web service Rest API for getting jobs, a specific job,
 * and job counters.
 *
 * /ws/v1/mapreduce/jobs
 * /ws/v1/mapreduce/jobs/{jobid}
 * /ws/v1/mapreduce/jobs/{jobid}/counters
 * /ws/v1/mapreduce/jobs/{jobid}/jobattempts
 */
public class TestAMWebServicesJobs extends JerseyTestBase {

  private static Configuration conf = new Configuration();
  private static AppContext appContext;

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(AMWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature());
    config.register(JAXBContextResolver.class);
    return config;
  }

  private static class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      appContext = new MockAppContext(0, 1, 2, 1);
      App app = new App(appContext);
      bind(appContext).to(AppContext.class).named("am");
      bind(app).to(App.class).named("app");
      bind(conf).to(Configuration.class).named("conf");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      final HttpServletRequest request = mock(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
      bind(request).to(HttpServletRequest.class);
    }
  }

  @Test
  public void testJobs() throws Exception {
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("mapreduce")
        .path("jobs").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject jobs = json.getJSONObject("jobs");
    JSONObject jobObject = jobs.getJSONObject("job");
    JSONArray arr = new JSONArray();
    arr.put(jobObject);
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getJob(MRApps.toJobID(info.getString("id")));
    verifyAMJob(info, job);

  }

  @Test
  public void testJobsSlash() throws Exception {
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("mapreduce")
        .path("jobs/").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject jobs = json.getJSONObject("jobs");
    JSONObject jobObject = jobs.getJSONObject("job");
    JSONArray arr = new JSONArray();
    arr.put(jobObject);
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getJob(MRApps.toJobID(info.getString("id")));
    verifyAMJob(info, job);

  }

  @Test
  public void testJobsDefault() throws Exception {
    WebTarget r = targetWithJsonObject();
    Response response = r.path("ws").path("v1").path("mapreduce")
        .path("jobs").request().get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals(1, json.length(), "incorrect number of elements");
    JSONObject jobs = json.getJSONObject("jobs");
    JSONObject jobObject = jobs.getJSONObject("job");
    JSONArray arr = new JSONArray();
    arr.put(jobObject);
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getJob(MRApps.toJobID(info.getString("id")));
    verifyAMJob(info, job);

  }

  @Test
  public void testJobsXML() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("mapreduce")
        .path("jobs").request(MediaType.APPLICATION_XML)
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
    assertEquals(1, jobs.getLength(), "incorrect number of elements");
    NodeList job = dom.getElementsByTagName("job");
    assertEquals(1, job.getLength(), "incorrect number of elements");
    verifyAMJobXML(job, appContext);

  }

  @Test
  public void testJobId() throws Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      Response response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json = response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("job");
      verifyAMJob(info, jobsMap.get(id));
    }

  }

  @Test
  public void testJobIdSlash() throws Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      Response response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId + "/").request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json = response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("job");
      verifyAMJob(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobIdDefault() throws Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      Response response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).request().get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json = response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("job");
      verifyAMJob(info, jobsMap.get(id));
    }

  }

  @Test
  public void testJobIdNonExist() throws Exception {
    WebTarget r = targetWithJsonObject();

    try {
      Response response = r.path("ws").path("v1").path("mapreduce").path("jobs")
          .path("job_0_1234").request().get();
      throw new NotFoundException(response);
    } catch (NotFoundException ue) {
      Response response = ue.getResponse();
      assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject msg = response.readEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals(3, exception.length(), "incorrect number of elements");
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "job, job_0_1234, is not found", message);
      WebServicesTestUtils.checkStringMatch("exception type",
          "NotFoundException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    }
  }

  @Test
  public void testJobIdInvalid() throws Exception {
    WebTarget r = targetWithJsonObject();

    try {
      Response response = r.path("ws").path("v1").path("mapreduce").path("jobs").path("job_foo")
          .request(MediaType.APPLICATION_JSON).get();
      throw new NotFoundException(response);
    } catch (NotFoundException ue) {
      Response response = ue.getResponse();
      assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject msg = response.readEntity(JSONObject.class);
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
  public void testJobIdInvalidDefault() throws Exception {
    WebTarget r = targetWithJsonObject();

    try {
      Response response =
          r.path("ws").path("v1").path("mapreduce").path("jobs").path("job_foo").request().get();
      throw new NotFoundException(response);
    } catch (NotFoundException ue) {
      Response response = ue.getResponse();
      assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject msg = response.readEntity(JSONObject.class);
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
  public void testJobIdInvalidXML() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();

    try {
      Response response = r.path("ws").path("v1").path("mapreduce").path("jobs").path("job_foo")
          .request(MediaType.APPLICATION_XML).get();
      throw new NotFoundException(response);
    } catch (NotFoundException ue) {
      Response response = ue.getResponse();
      assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String msg = response.readEntity(String.class);
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
      String classname = WebServicesTestUtils.getXmlString(element, "javaClassName");
      verifyJobIdInvalid(message, type, classname);
    }
  }

  private void verifyJobIdInvalid(String message, String type, String classname) {
    WebServicesTestUtils.checkStringMatch("exception message",
        "JobId string : job_foo is not properly formed",
        message);
    WebServicesTestUtils.checkStringMatch("exception type",
        "NotFoundException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
  }

  @Test
  public void testJobIdInvalidBogus() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();

    try {
      Response response =
          r.path("ws").path("v1").path("mapreduce").path("jobs").path("bogusfoo").request().get();
      throw new NotFoundException(response);
    } catch (NotFoundException ue) {
      Response response = ue.getResponse();
      assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject msg = response.readEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals(3, exception.length(), "incorrect number of elements");
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch(
          "exception message",
          "JobId string : bogusfoo is not properly formed",
          message);
      WebServicesTestUtils.checkStringMatch("exception type", "NotFoundException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
    }
  }

  @Test
  public void testJobIdXML() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      Response response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).request(MediaType.APPLICATION_XML)
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
      verifyAMJobXML(job, appContext);
    }

  }

  public void verifyAMJob(JSONObject info, Job job) throws JSONException {

    assertEquals(31, info.length(), "incorrect number of elements");

    // everyone access fields
    verifyAMJobGeneric(job, info.getString("id"), info.getString("user"),
        info.getString("name"), info.getString("state"),
        info.getLong("startTime"), info.getLong("finishTime"),
        info.getLong("elapsedTime"), info.getInt("mapsTotal"),
        info.getInt("mapsCompleted"), info.getInt("reducesTotal"),
        info.getInt("reducesCompleted"),
        (float) info.getDouble("reduceProgress"),
        (float) info.getDouble("mapProgress"));

    String diagnostics = "";
    if (info.has("diagnostics")) {
      diagnostics = info.getString("diagnostics");
    }

    // restricted access fields - if security and acls set
    verifyAMJobGenericSecure(job, info.getInt("mapsPending"),
        info.getInt("mapsRunning"), info.getInt("reducesPending"),
        info.getInt("reducesRunning"), info.getBoolean("uberized"),
        diagnostics, info.getInt("newReduceAttempts"),
        info.getInt("runningReduceAttempts"),
        info.getInt("failedReduceAttempts"),
        info.getInt("killedReduceAttempts"),
        info.getInt("successfulReduceAttempts"), info.getInt("newMapAttempts"),
        info.getInt("runningMapAttempts"), info.getInt("failedMapAttempts"),
        info.getInt("killedMapAttempts"), info.getInt("successfulMapAttempts"));

    Map<JobACL, AccessControlList> allacls = job.getJobACLs();
    if (allacls != null) {

      for (Map.Entry<JobACL, AccessControlList> entry : allacls.entrySet()) {
        String expectName = entry.getKey().getAclName();
        String expectValue = entry.getValue().getAclString();
        Boolean found = false;
        // make sure ws includes it
        if (info.has("acls")) {
          JSONArray arr = info.getJSONArray("acls");

          for (int i = 0; i < arr.length(); i++) {
            JSONObject aclInfo = arr.getJSONObject(i);
            if (expectName.matches(aclInfo.getString("name"))) {
              found = true;
              WebServicesTestUtils.checkStringMatch("value", expectValue,
                  aclInfo.getString("value"));
            }
          }
        } else {
          fail("should have acls in the web service info");
        }
        assertTrue(found, "acl: " + expectName + " not found in webservice output");
      }
    }

  }

  public void verifyAMJobXML(NodeList nodes, AppContext appContext) {

    assertEquals(1, nodes.getLength(), "incorrect number of elements");

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      Job job = appContext.getJob(MRApps.toJobID(WebServicesTestUtils
          .getXmlString(element, "id")));
      assertNotNull(job, "Job not found - output incorrect");

      verifyAMJobGeneric(job, WebServicesTestUtils.getXmlString(element, "id"),
          WebServicesTestUtils.getXmlString(element, "user"),
          WebServicesTestUtils.getXmlString(element, "name"),
          WebServicesTestUtils.getXmlString(element, "state"),
          WebServicesTestUtils.getXmlLong(element, "startTime"),
          WebServicesTestUtils.getXmlLong(element, "finishTime"),
          WebServicesTestUtils.getXmlLong(element, "elapsedTime"),
          WebServicesTestUtils.getXmlInt(element, "mapsTotal"),
          WebServicesTestUtils.getXmlInt(element, "mapsCompleted"),
          WebServicesTestUtils.getXmlInt(element, "reducesTotal"),
          WebServicesTestUtils.getXmlInt(element, "reducesCompleted"),
          WebServicesTestUtils.getXmlFloat(element, "reduceProgress"),
          WebServicesTestUtils.getXmlFloat(element, "mapProgress"));

      // restricted access fields - if security and acls set
      verifyAMJobGenericSecure(job,
          WebServicesTestUtils.getXmlInt(element, "mapsPending"),
          WebServicesTestUtils.getXmlInt(element, "mapsRunning"),
          WebServicesTestUtils.getXmlInt(element, "reducesPending"),
          WebServicesTestUtils.getXmlInt(element, "reducesRunning"),
          WebServicesTestUtils.getXmlBoolean(element, "uberized"),
          WebServicesTestUtils.getXmlString(element, "diagnostics"),
          WebServicesTestUtils.getXmlInt(element, "newReduceAttempts"),
          WebServicesTestUtils.getXmlInt(element, "runningReduceAttempts"),
          WebServicesTestUtils.getXmlInt(element, "failedReduceAttempts"),
          WebServicesTestUtils.getXmlInt(element, "killedReduceAttempts"),
          WebServicesTestUtils.getXmlInt(element, "successfulReduceAttempts"),
          WebServicesTestUtils.getXmlInt(element, "newMapAttempts"),
          WebServicesTestUtils.getXmlInt(element, "runningMapAttempts"),
          WebServicesTestUtils.getXmlInt(element, "failedMapAttempts"),
          WebServicesTestUtils.getXmlInt(element, "killedMapAttempts"),
          WebServicesTestUtils.getXmlInt(element, "successfulMapAttempts"));

      Map<JobACL, AccessControlList> allacls = job.getJobACLs();
      if (allacls != null) {
        for (Map.Entry<JobACL, AccessControlList> entry : allacls.entrySet()) {
          String expectName = entry.getKey().getAclName();
          String expectValue = entry.getValue().getAclString();
          Boolean found = false;
          // make sure ws includes it
          NodeList id = element.getElementsByTagName("acls");
          if (id != null) {
            for (int j = 0; j < id.getLength(); j++) {
              Element aclElem = (Element) id.item(j);
              if (aclElem == null) {
                fail("should have acls in the web service info");
              }
              if (expectName.matches(WebServicesTestUtils.getXmlString(aclElem,
                  "name"))) {
                found = true;
                WebServicesTestUtils.checkStringMatch("value", expectValue,
                    WebServicesTestUtils.getXmlString(aclElem, "value"));
              }
            }
          } else {
            fail("should have acls in the web service info");
          }
          assertTrue(found, "acl: " + expectName + " not found in webservice output");
        }
      }
    }
  }

  public void verifyAMJobGeneric(Job job, String id, String user, String name,
      String state, long startTime, long finishTime, long elapsedTime,
      int mapsTotal, int mapsCompleted, int reducesTotal, int reducesCompleted,
      float reduceProgress, float mapProgress) {
    JobReport report = job.getReport();

    WebServicesTestUtils.checkStringMatch("id", MRApps.toString(job.getID()),
        id);
    WebServicesTestUtils.checkStringMatch("user", job.getUserName(),
        user);
    WebServicesTestUtils.checkStringMatch("name", job.getName(), name);
    WebServicesTestUtils.checkStringMatch("state", job.getState().toString(),
        state);

    assertEquals(report.getStartTime(), startTime, "startTime incorrect");
    assertEquals(report.getFinishTime(), finishTime, "finishTime incorrect");
    assertEquals(Times.elapsed(report.getStartTime(), report.getFinishTime()),
        elapsedTime, "elapsedTime incorrect");
    assertEquals(job.getTotalMaps(), mapsTotal, "mapsTotal incorrect");
    assertEquals(job.getCompletedMaps(),
        mapsCompleted, "mapsCompleted incorrect");
    assertEquals(job.getTotalReduces(), reducesTotal, "reducesTotal incorrect");
    assertEquals(job.getCompletedReduces(),
        reducesCompleted, "reducesCompleted incorrect");
    assertEquals(report.getMapProgress() * 100, mapProgress, 0, "mapProgress incorrect");
    assertEquals(report.getReduceProgress() * 100,
        reduceProgress, 0, "reduceProgress incorrect");
  }

  public void verifyAMJobGenericSecure(Job job, int mapsPending,
      int mapsRunning, int reducesPending, int reducesRunning,
      Boolean uberized, String diagnostics, int newReduceAttempts,
      int runningReduceAttempts, int failedReduceAttempts,
      int killedReduceAttempts, int successfulReduceAttempts,
      int newMapAttempts, int runningMapAttempts, int failedMapAttempts,
      int killedMapAttempts, int successfulMapAttempts) {

    String diagString = "";
    List<String> diagList = job.getDiagnostics();
    if (diagList != null && !diagList.isEmpty()) {
      StringBuilder b = new StringBuilder();
      for (String diag : diagList) {
        b.append(diag);
      }
      diagString = b.toString();
    }
    WebServicesTestUtils.checkStringMatch("diagnostics", diagString,
        diagnostics);

    assertEquals(job.isUber(), uberized, "isUber incorrect");

    // unfortunately the following fields are all calculated in JobInfo
    // so not easily accessible without doing all the calculations again.
    // For now just make sure they are present.
    assertTrue(mapsPending >= 0, "mapsPending not >= 0");
    assertTrue(mapsRunning >= 0, "mapsRunning not >= 0");
    assertTrue(reducesPending >= 0, "reducesPending not >= 0");
    assertTrue(reducesRunning >= 0, "reducesRunning not >= 0");

    assertTrue(newReduceAttempts >= 0, "newReduceAttempts not >= 0");
    assertTrue(runningReduceAttempts >= 0, "runningReduceAttempts not >= 0");
    assertTrue(failedReduceAttempts >= 0, "failedReduceAttempts not >= 0");
    assertTrue(killedReduceAttempts >= 0, "killedReduceAttempts not >= 0");
    assertTrue(successfulReduceAttempts >= 0, "successfulReduceAttempts not >= 0");

    assertTrue(newMapAttempts >= 0, "newMapAttempts not >= 0");
    assertTrue(runningMapAttempts >= 0, "runningMapAttempts not >= 0");
    assertTrue(failedMapAttempts >= 0, "failedMapAttempts not >= 0");
    assertTrue(killedMapAttempts >= 0, "killedMapAttempts not >= 0");
    assertTrue(successfulMapAttempts >= 0, "successfulMapAttempts not >= 0");

  }

  @Test
  public void testJobCounters() throws Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("counters")
          .request(MediaType.APPLICATION_JSON).get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json = response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("jobCounters");
      verifyAMJobCounters(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobCountersSlash() throws Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      Response response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("counters/")
          .request(MediaType.APPLICATION_JSON).get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json = response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("jobCounters");
      verifyAMJobCounters(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobCountersDefault() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      Response response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("counters/").request().get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json = response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("jobCounters");
      verifyAMJobCounters(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobCountersXML() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      Response response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("counters")
          .request(MediaType.APPLICATION_XML).get(Response.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String xml = response.readEntity(String.class);
      DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      Document dom = db.parse(is);
      NodeList info = dom.getElementsByTagName("jobCounters");
      verifyAMJobCountersXML(info, jobsMap.get(id));
    }
  }

  public void verifyAMJobCounters(JSONObject info, Job job)
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

  public void verifyAMJobCountersXML(NodeList nodes, Job job) {

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
  public void testJobAttempts() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      Response response = r.path("ws").path("v1")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts")
          .request(MediaType.APPLICATION_JSON).get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json = response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyJobAttempts(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobAttemptsSlash() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r.path("ws").path("v1")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts/")
          .request(MediaType.APPLICATION_JSON).get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json = response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyJobAttempts(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobAttemptsDefault() throws Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      Response response = r.path("ws").path("v1")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts").request()
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json = response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyJobAttempts(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobAttemptsXML() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      Response response = r.path("ws").path("v1")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts")
          .request(MediaType.APPLICATION_XML).get(Response.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String xml = response.readEntity(String.class);
      DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      Document dom = db.parse(is);
      NodeList attempts = dom.getElementsByTagName("jobAttempts");
      assertEquals(1, attempts.getLength(), "incorrect number of elements");
      NodeList info = dom.getElementsByTagName("jobAttempt");
      verifyJobAttemptsXML(info, jobsMap.get(id));
    }
  }

  public void verifyJobAttempts(JSONObject info, Job job)
      throws JSONException {

    JSONArray attempts = info.getJSONArray("jobAttempt");
    assertEquals(2, attempts.length(), "incorrect number of elements");
    for (int i = 0; i < attempts.length(); i++) {
      JSONObject attempt = attempts.getJSONObject(i);
      verifyJobAttemptsGeneric(job, attempt.getString("nodeHttpAddress"),
          attempt.getString("nodeId"), attempt.getInt("id"),
          attempt.getLong("startTime"), attempt.getString("containerId"),
          attempt.getString("logsLink"));
    }
  }

  public void verifyJobAttemptsXML(NodeList nodes, Job job) {

    assertEquals(2, nodes.getLength(), "incorrect number of elements");
    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);
      verifyJobAttemptsGeneric(job,
          WebServicesTestUtils.getXmlString(element, "nodeHttpAddress"),
          WebServicesTestUtils.getXmlString(element, "nodeId"),
          WebServicesTestUtils.getXmlInt(element, "id"),
          WebServicesTestUtils.getXmlLong(element, "startTime"),
          WebServicesTestUtils.getXmlString(element, "containerId"),
          WebServicesTestUtils.getXmlString(element, "logsLink"));
    }
  }

  public void verifyJobAttemptsGeneric(Job job, String nodeHttpAddress,
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
            NodeId.newInstance(nmHost, nmPort).toString(), nodeId);
        assertTrue(startTime > 0, "start time not greater than 0");
        WebServicesTestUtils.checkStringMatch("containerId", amInfo
            .getContainerId().toString(), containerId);

        String localLogsLink =ujoin("node", "containerlogs", containerId,
            job.getUserName());

        assertTrue(logsLink.contains(localLogsLink), "logsLink");
      }
    }
    assertTrue(attemptFound, "attempt: " + id + " was not found");
  }

}
