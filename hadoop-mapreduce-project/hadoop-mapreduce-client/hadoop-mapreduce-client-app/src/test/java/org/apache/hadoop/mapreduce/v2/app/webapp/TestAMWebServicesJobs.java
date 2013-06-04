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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MockAppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
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
 * Test the app master web service Rest API for getting jobs, a specific job,
 * and job counters.
 *
 * /ws/v1/mapreduce/jobs
 * /ws/v1/mapreduce/jobs/{jobid}
 * /ws/v1/mapreduce/jobs/{jobid}/counters
 * /ws/v1/mapreduce/jobs/{jobid}/jobattempts
 */
public class TestAMWebServicesJobs extends JerseyTest {

  private static Configuration conf = new Configuration();
  private static AppContext appContext;

  private Injector injector = Guice.createInjector(new ServletModule() {
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

  public TestAMWebServicesJobs() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.mapreduce.v2.app.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testJobs() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("mapreduce")
        .path("jobs").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getJob(MRApps.toJobID(info.getString("id")));
    verifyAMJob(info, job);

  }

  @Test
  public void testJobsSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("mapreduce")
        .path("jobs/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getJob(MRApps.toJobID(info.getString("id")));
    verifyAMJob(info, job);

  }

  @Test
  public void testJobsDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("mapreduce")
        .path("jobs").get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject jobs = json.getJSONObject("jobs");
    JSONArray arr = jobs.getJSONArray("job");
    JSONObject info = arr.getJSONObject(0);
    Job job = appContext.getJob(MRApps.toJobID(info.getString("id")));
    verifyAMJob(info, job);

  }

  @Test
  public void testJobsXML() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("mapreduce")
        .path("jobs").accept(MediaType.APPLICATION_XML)
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
    verifyAMJobXML(job, appContext);

  }

  @Test
  public void testJobId() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("job");
      verifyAMJob(info, jobsMap.get(id));
    }

  }

  @Test
  public void testJobIdSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId + "/").accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("job");
      verifyAMJob(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobIdDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("job");
      verifyAMJob(info, jobsMap.get(id));
    }

  }

  @Test
  public void testJobIdNonExist() throws JSONException, Exception {
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("mapreduce").path("jobs")
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
      r.path("ws").path("v1").path("mapreduce").path("jobs").path("job_foo")
          .accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
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
      r.path("ws").path("v1").path("mapreduce").path("jobs").path("job_foo")
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

  // test that the exception output works in XML
  @Test
  public void testJobIdInvalidXML() throws JSONException, Exception {
    WebResource r = resource();

    try {
      r.path("ws").path("v1").path("mapreduce").path("jobs").path("job_foo")
          .accept(MediaType.APPLICATION_XML).get(JSONObject.class);
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
      r.path("ws").path("v1").path("mapreduce").path("jobs").path("bogusfoo")
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
      WebServicesTestUtils
          .checkStringMatch(
              "exception message",
              "java.lang.Exception: JobId string : bogusfoo is not properly formed",
              message);
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

      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).accept(MediaType.APPLICATION_XML)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
      String xml = response.getEntity(String.class);
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      Document dom = db.parse(is);
      NodeList job = dom.getElementsByTagName("job");
      verifyAMJobXML(job, appContext);
    }

  }

  public void verifyAMJob(JSONObject info, Job job) throws JSONException {

    assertEquals("incorrect number of elements", 30, info.length());

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
        assertTrue("acl: " + expectName + " not found in webservice output",
            found);
      }
    }

  }

  public void verifyAMJobXML(NodeList nodes, AppContext appContext) {

    assertEquals("incorrect number of elements", 1, nodes.getLength());

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);

      Job job = appContext.getJob(MRApps.toJobID(WebServicesTestUtils
          .getXmlString(element, "id")));
      assertNotNull("Job not found - output incorrect", job);

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
          assertTrue("acl: " + expectName + " not found in webservice output",
              found);
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
    WebServicesTestUtils.checkStringMatch("user", job.getUserName().toString(),
        user);
    WebServicesTestUtils.checkStringMatch("name", job.getName(), name);
    WebServicesTestUtils.checkStringMatch("state", job.getState().toString(),
        state);

    assertEquals("startTime incorrect", report.getStartTime(), startTime);
    assertEquals("finishTime incorrect", report.getFinishTime(), finishTime);
    assertEquals("elapsedTime incorrect",
        Times.elapsed(report.getStartTime(), report.getFinishTime()),
        elapsedTime);
    assertEquals("mapsTotal incorrect", job.getTotalMaps(), mapsTotal);
    assertEquals("mapsCompleted incorrect", job.getCompletedMaps(),
        mapsCompleted);
    assertEquals("reducesTotal incorrect", job.getTotalReduces(), reducesTotal);
    assertEquals("reducesCompleted incorrect", job.getCompletedReduces(),
        reducesCompleted);
    assertEquals("mapProgress incorrect", report.getMapProgress() * 100,
        mapProgress, 0);
    assertEquals("reduceProgress incorrect", report.getReduceProgress() * 100,
        reduceProgress, 0);
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
      StringBuffer b = new StringBuffer();
      for (String diag : diagList) {
        b.append(diag);
      }
      diagString = b.toString();
    }
    WebServicesTestUtils.checkStringMatch("diagnostics", diagString,
        diagnostics);

    assertEquals("isUber incorrect", job.isUber(), uberized);

    // unfortunately the following fields are all calculated in JobInfo
    // so not easily accessible without doing all the calculations again.
    // For now just make sure they are present.
    assertTrue("mapsPending not >= 0", mapsPending >= 0);
    assertTrue("mapsRunning not >= 0", mapsRunning >= 0);
    assertTrue("reducesPending not >= 0", reducesPending >= 0);
    assertTrue("reducesRunning not >= 0", reducesRunning >= 0);

    assertTrue("newReduceAttempts not >= 0", newReduceAttempts >= 0);
    assertTrue("runningReduceAttempts not >= 0", runningReduceAttempts >= 0);
    assertTrue("failedReduceAttempts not >= 0", failedReduceAttempts >= 0);
    assertTrue("killedReduceAttempts not >= 0", killedReduceAttempts >= 0);
    assertTrue("successfulReduceAttempts not >= 0",
        successfulReduceAttempts >= 0);

    assertTrue("newMapAttempts not >= 0", newMapAttempts >= 0);
    assertTrue("runningMapAttempts not >= 0", runningMapAttempts >= 0);
    assertTrue("failedMapAttempts not >= 0", failedMapAttempts >= 0);
    assertTrue("killedMapAttempts not >= 0", killedMapAttempts >= 0);
    assertTrue("successfulMapAttempts not >= 0", successfulMapAttempts >= 0);

  }

  @Test
  public void testJobCounters() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("counters")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobCounters");
      verifyAMJobCounters(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobCountersSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("counters/")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobCounters");
      verifyAMJobCounters(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobCountersDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("counters/").get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobCounters");
      verifyAMJobCounters(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobCountersXML() throws Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1").path("mapreduce")
          .path("jobs").path(jobId).path("counters")
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
      String xml = response.getEntity(String.class);
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
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

  public void verifyAMJobCountersXML(NodeList nodes, Job job) {

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

      ClientResponse response = r.path("ws").path("v1")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyJobAttempts(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobAttemptsSlash() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts/")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyJobAttempts(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobAttemptsDefault() throws JSONException, Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1")
          .path("mapreduce").path("jobs").path(jobId).path("jobattempts")
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject json = response.getEntity(JSONObject.class);
      assertEquals("incorrect number of elements", 1, json.length());
      JSONObject info = json.getJSONObject("jobAttempts");
      verifyJobAttempts(info, jobsMap.get(id));
    }
  }

  @Test
  public void testJobAttemptsXML() throws Exception {
    WebResource r = resource();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);

      ClientResponse response = r.path("ws").path("v1")
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
      verifyJobAttemptsXML(info, jobsMap.get(id));
    }
  }

  public void verifyJobAttempts(JSONObject info, Job job)
      throws JSONException {

    JSONArray attempts = info.getJSONArray("jobAttempt");
    assertEquals("incorrect number of elements", 2, attempts.length());
    for (int i = 0; i < attempts.length(); i++) {
      JSONObject attempt = attempts.getJSONObject(i);
      verifyJobAttemptsGeneric(job, attempt.getString("nodeHttpAddress"),
          attempt.getString("nodeId"), attempt.getInt("id"),
          attempt.getLong("startTime"), attempt.getString("containerId"),
          attempt.getString("logsLink"));
    }
  }

  public void verifyJobAttemptsXML(NodeList nodes, Job job) {

    assertEquals("incorrect number of elements", 2, nodes.getLength());
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
        assertTrue("startime not greater than 0", startTime > 0);
        WebServicesTestUtils.checkStringMatch("containerId", amInfo
            .getContainerId().toString(), containerId);

        String localLogsLink =ujoin("node", "containerlogs", containerId,
            job.getUserName());

        assertTrue("logsLink", logsLink.contains(localLogsLink));
      }
    }
    assertTrue("attempt: " + id + " was not found", attemptFound);
  }

}
