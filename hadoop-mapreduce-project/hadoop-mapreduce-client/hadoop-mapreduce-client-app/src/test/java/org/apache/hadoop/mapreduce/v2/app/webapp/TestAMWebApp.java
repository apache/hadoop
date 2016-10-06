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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.APP_ID;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.net.ssl.SSLException;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig.Policy;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.MockAppContext;
import org.apache.hadoop.mapreduce.v2.app.MockJobs;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.client.MRClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.http.HttpStatus;
import org.junit.Test;

import com.google.common.net.HttpHeaders;
import com.google.inject.Injector;

public class TestAMWebApp {

  @Test public void testAppControllerIndex() {
    AppContext ctx = new MockAppContext(0, 1, 1, 1);
    Injector injector = WebAppTests.createMockInjector(AppContext.class, ctx);
    AppController controller = injector.getInstance(AppController.class);
    controller.index();
    assertEquals(ctx.getApplicationID().toString(), controller.get(APP_ID,""));
  }

  @Test public void testAppView() {
    WebAppTests.testPage(AppView.class, AppContext.class, new MockAppContext(0, 1, 1, 1));
  }


  
  @Test public void testJobView() {
    AppContext appContext = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = getJobParams(appContext);
    WebAppTests.testPage(JobPage.class, AppContext.class, appContext, params);
  }

  @Test public void testTasksView() {
    AppContext appContext = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = getTaskParams(appContext);
    WebAppTests.testPage(TasksPage.class, AppContext.class, appContext, params);
  }

  @Test public void testTaskView() {
    AppContext appContext = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = getTaskParams(appContext);
    App app = new App(appContext);
    app.setJob(appContext.getAllJobs().values().iterator().next());
    app.setTask(app.getJob().getTasks().values().iterator().next());
    WebAppTests.testPage(TaskPage.class, App.class, app, params);
  }

  public static Map<String, String> getJobParams(AppContext appContext) {
    JobId jobId = appContext.getAllJobs().entrySet().iterator().next().getKey();
    Map<String, String> params = new HashMap<String, String>();
    params.put(AMParams.JOB_ID, MRApps.toString(jobId));
    return params;
  }
  
  public static Map<String, String> getTaskParams(AppContext appContext) {
    JobId jobId = appContext.getAllJobs().entrySet().iterator().next().getKey();
    Entry<TaskId, Task> e = appContext.getJob(jobId).getTasks().entrySet().iterator().next();
    e.getValue().getType();
    Map<String, String> params = new HashMap<String, String>();
    params.put(AMParams.JOB_ID, MRApps.toString(jobId));
    params.put(AMParams.TASK_ID, MRApps.toString(e.getKey()));
    params.put(AMParams.TASK_TYPE, MRApps.taskSymbol(e.getValue().getType()));
    return params;
  }

  @Test public void testConfView() {
    WebAppTests.testPage(JobConfPage.class, AppContext.class,
                         new MockAppContext(0, 1, 1, 1));
  }

  @Test public void testCountersView() {
    AppContext appContext = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = getJobParams(appContext);
    WebAppTests.testPage(CountersPage.class, AppContext.class,
                         appContext, params);
  }
  
  @Test public void testSingleCounterView() {
    AppContext appContext = new MockAppContext(0, 1, 1, 1);
    Job job = appContext.getAllJobs().values().iterator().next();
    // add a failed task to the job without any counters
    Task failedTask = MockJobs.newTask(job.getID(), 2, 1, true);
    Map<TaskId,Task> tasks = job.getTasks();
    tasks.put(failedTask.getID(), failedTask);
    Map<String, String> params = getJobParams(appContext);
    params.put(AMParams.COUNTER_GROUP, 
        "org.apache.hadoop.mapreduce.FileSystemCounter");
    params.put(AMParams.COUNTER_NAME, "HDFS_WRITE_OPS");
    WebAppTests.testPage(SingleCounterPage.class, AppContext.class,
                         appContext, params);
  }

  @Test public void testTaskCountersView() {
    AppContext appContext = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = getTaskParams(appContext);
    WebAppTests.testPage(CountersPage.class, AppContext.class,
                         appContext, params);
  }

  @Test public void testSingleTaskCounterView() {
    AppContext appContext = new MockAppContext(0, 1, 1, 2);
    Map<String, String> params = getTaskParams(appContext);
    params.put(AMParams.COUNTER_GROUP, 
        "org.apache.hadoop.mapreduce.FileSystemCounter");
    params.put(AMParams.COUNTER_NAME, "HDFS_WRITE_OPS");
    
    // remove counters from one task attempt
    // to test handling of missing counters
    TaskId taskID = MRApps.toTaskID(params.get(AMParams.TASK_ID));
    Job job = appContext.getJob(taskID.getJobId());
    Task task = job.getTask(taskID);
    TaskAttempt attempt = task.getAttempts().values().iterator().next();
    attempt.getReport().setCounters(null);
    
    WebAppTests.testPage(SingleCounterPage.class, AppContext.class,
                         appContext, params);
  }

  @Test
  public void testMRWebAppSSLDisabled() throws Exception {
    MRApp app = new MRApp(2, 2, true, this.getClass().getName(), true) {
      @Override
      protected ClientService createClientService(AppContext context) {
        return new MRClientService(context);
      }
    };
    Configuration conf = new Configuration();
    // MR is explicitly disabling SSL, even though setting as HTTPS_ONLY
    conf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY, Policy.HTTPS_ONLY.name());
    Job job = app.submit(conf);

    String hostPort =
        NetUtils.getHostPortString(((MRClientService) app.getClientService())
          .getWebApp().getListenerAddress());
    // http:// should be accessible
    URL httpUrl = new URL("http://" + hostPort);
    HttpURLConnection conn = (HttpURLConnection) httpUrl.openConnection();
    InputStream in = conn.getInputStream();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(in, out, 1024);
    Assert.assertTrue(out.toString().contains("MapReduce Application"));

    // https:// is not accessible.
    URL httpsUrl = new URL("https://" + hostPort);
    try {
      HttpURLConnection httpsConn =
          (HttpURLConnection) httpsUrl.openConnection();
      httpsConn.getInputStream();
      Assert.fail("https:// is not accessible, expected to fail");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SSLException);
    }

    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
  }

  static String webProxyBase = null;
  public static class TestAMFilterInitializer extends AmFilterInitializer {

    @Override
    protected String getApplicationWebProxyBase() {
      return webProxyBase;
    }
  }

  @Test
  public void testMRWebAppRedirection() throws Exception {

    String[] schemePrefix =
        { WebAppUtils.HTTP_PREFIX, WebAppUtils.HTTPS_PREFIX };
    for (String scheme : schemePrefix) {
      MRApp app = new MRApp(2, 2, true, this.getClass().getName(), true) {
        @Override
        protected ClientService createClientService(AppContext context) {
          return new MRClientService(context);
        }
      };
      Configuration conf = new Configuration();
      conf.set(YarnConfiguration.PROXY_ADDRESS, "9.9.9.9");
      conf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY, scheme
        .equals(WebAppUtils.HTTPS_PREFIX) ? Policy.HTTPS_ONLY.name()
          : Policy.HTTP_ONLY.name());
      webProxyBase = "/proxy/" + app.getAppID();
      conf.set("hadoop.http.filter.initializers",
        TestAMFilterInitializer.class.getName());
      Job job = app.submit(conf);
      String hostPort =
          NetUtils.getHostPortString(((MRClientService) app.getClientService())
            .getWebApp().getListenerAddress());
      URL httpUrl = new URL("http://" + hostPort + "/mapreduce");

      HttpURLConnection conn = (HttpURLConnection) httpUrl.openConnection();
      conn.setInstanceFollowRedirects(false);
      conn.connect();

      // Because we're not calling from the proxy's address, we'll be redirected
      String expectedURL = scheme + conf.get(YarnConfiguration.PROXY_ADDRESS)
          + ProxyUriUtils.getPath(app.getAppID(), "/mapreduce", true);

      Assert.assertEquals(expectedURL,
        conn.getHeaderField(HttpHeaders.LOCATION));
      Assert.assertEquals(HttpStatus.SC_MOVED_TEMPORARILY,
        conn.getResponseCode());
      app.waitForState(job, JobState.SUCCEEDED);
      app.verifyCompleted();
    }
  }

  public static void main(String[] args) {
    WebApps.$for("yarn", AppContext.class, new MockAppContext(0, 8, 88, 4)).
        at(58888).inDevMode().start(new AMWebApp()).joinThread();
  }
}
