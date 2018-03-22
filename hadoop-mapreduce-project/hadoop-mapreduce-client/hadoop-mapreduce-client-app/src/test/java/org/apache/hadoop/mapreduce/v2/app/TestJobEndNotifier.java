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

package org.apache.hadoop.mapreduce.v2.app;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMCommunicator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests job end notification
 *
 */
@SuppressWarnings("unchecked")
public class TestJobEndNotifier extends JobEndNotifier {

  //Test maximum retries is capped by MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS
  private void testNumRetries(Configuration conf) {
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, "0");
    conf.set(MRJobConfig.MR_JOB_END_RETRY_ATTEMPTS, "10");
    setConf(conf);
    Assert.assertTrue("Expected numTries to be 0, but was " + numTries,
      numTries == 0 );

    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, "1");
    setConf(conf);
    Assert.assertTrue("Expected numTries to be 1, but was " + numTries,
      numTries == 1 );

    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, "20");
    setConf(conf);
    Assert.assertTrue("Expected numTries to be 11, but was " + numTries,
      numTries == 11 ); //11 because number of _retries_ is 10
  }

  //Test maximum retry interval is capped by
  //MR_JOB_END_NOTIFICATION_MAX_RETRY_INTERVAL
  private void testWaitInterval(Configuration conf) {
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_RETRY_INTERVAL, "5000");
    conf.set(MRJobConfig.MR_JOB_END_RETRY_INTERVAL, "1000");
    setConf(conf);
    Assert.assertTrue("Expected waitInterval to be 1000, but was "
      + waitInterval, waitInterval == 1000);

    conf.set(MRJobConfig.MR_JOB_END_RETRY_INTERVAL, "10000");
    setConf(conf);
    Assert.assertTrue("Expected waitInterval to be 5000, but was "
      + waitInterval, waitInterval == 5000);

    //Test negative numbers are set to default
    conf.set(MRJobConfig.MR_JOB_END_RETRY_INTERVAL, "-10");
    setConf(conf);
    Assert.assertTrue("Expected waitInterval to be 5000, but was "
      + waitInterval, waitInterval == 5000);
  }

  private void testTimeout(Configuration conf) {
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_TIMEOUT, "1000");
    setConf(conf);
    Assert.assertTrue("Expected timeout to be 1000, but was "
      + timeout, timeout == 1000);
  }

  private void testProxyConfiguration(Configuration conf) {
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY, "somehost");
    setConf(conf);
    Assert.assertTrue("Proxy shouldn't be set because port wasn't specified",
      proxyToUse.type() == Proxy.Type.DIRECT);
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY, "somehost:someport");
    setConf(conf);
    Assert.assertTrue("Proxy shouldn't be set because port wasn't numeric",
      proxyToUse.type() == Proxy.Type.DIRECT);
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY, "somehost:1000");
    setConf(conf);
    Assert.assertEquals("Proxy should have been set but wasn't ",
      "HTTP @ somehost:1000", proxyToUse.toString());
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY, "socks@somehost:1000");
    setConf(conf);
    Assert.assertEquals("Proxy should have been socks but wasn't ",
      "SOCKS @ somehost:1000", proxyToUse.toString());
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY, "SOCKS@somehost:1000");
    setConf(conf);
    Assert.assertEquals("Proxy should have been socks but wasn't ",
      "SOCKS @ somehost:1000", proxyToUse.toString());
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY, "sfafn@somehost:1000");
    setConf(conf);
    Assert.assertEquals("Proxy should have been http but wasn't ",
      "HTTP @ somehost:1000", proxyToUse.toString());
    
  }

  /**
   * Test that setting parameters has the desired effect
   */
  @Test
  public void checkConfiguration() {
    Configuration conf = new Configuration();
    testNumRetries(conf);
    testWaitInterval(conf);
    testTimeout(conf);
    testProxyConfiguration(conf);
  }

  protected int notificationCount = 0;
  @Override
  protected boolean notifyURLOnce() {
    boolean success = super.notifyURLOnce();
    notificationCount++;
    return success;
  }

  //Check retries happen as intended
  @Test
  public void testNotifyRetries() throws InterruptedException {
    JobConf conf = new JobConf();
    conf.set(MRJobConfig.MR_JOB_END_RETRY_ATTEMPTS, "0");
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, "1");
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_URL, "http://nonexistent");
    conf.set(MRJobConfig.MR_JOB_END_RETRY_INTERVAL, "5000");
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_RETRY_INTERVAL, "5000");

    JobReport jobReport = mock(JobReport.class);
 
    long startTime = System.currentTimeMillis();
    this.notificationCount = 0;
    this.setConf(conf);
    this.notify(jobReport);
    long endTime = System.currentTimeMillis();
    Assert.assertEquals("Only 1 try was expected but was : "
      + this.notificationCount, 1, this.notificationCount);
    Assert.assertTrue("Should have taken more than 5 seconds it took "
      + (endTime - startTime), endTime - startTime > 5000);

    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, "3");
    conf.set(MRJobConfig.MR_JOB_END_RETRY_ATTEMPTS, "3");
    conf.set(MRJobConfig.MR_JOB_END_RETRY_INTERVAL, "3000");
    conf.set(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_RETRY_INTERVAL, "3000");

    startTime = System.currentTimeMillis();
    this.notificationCount = 0;
    this.setConf(conf);
    this.notify(jobReport);
    endTime = System.currentTimeMillis();
    Assert.assertEquals("Only 3 retries were expected but was : "
      + this.notificationCount, 3, this.notificationCount);
    Assert.assertTrue("Should have taken more than 9 seconds it took "
      + (endTime - startTime), endTime - startTime > 9000);

  }

  private void testNotificationOnLastRetry(boolean withRuntimeException)
      throws Exception {
    HttpServer2 server = startHttpServer();
    // Act like it is the second attempt. Default max attempts is 2
    MRApp app = spy(new MRAppWithCustomContainerAllocator(
        2, 2, true, this.getClass().getName(), true, 2, true));
    doNothing().when(app).sysexit();
    JobConf conf = new JobConf();
    conf.set(JobContext.MR_JOB_END_NOTIFICATION_URL,
        JobEndServlet.baseUrl + "jobend?jobid=$jobId&status=$jobStatus");
    JobImpl job = (JobImpl)app.submit(conf);
    app.waitForInternalState(job, JobStateInternal.SUCCEEDED);
    // Unregistration succeeds: successfullyUnregistered is set
    if (withRuntimeException) {
      YarnRuntimeException runtimeException = new YarnRuntimeException(
          new ClosedChannelException());
      doThrow(runtimeException).when(app).stop();
    }
    app.shutDownJob();
    Assert.assertTrue(app.isLastAMRetry());
    Assert.assertEquals(1, JobEndServlet.calledTimes);
    Assert.assertEquals("jobid=" + job.getID() + "&status=SUCCEEDED",
        JobEndServlet.requestUri.getQuery());
    Assert.assertEquals(JobState.SUCCEEDED.toString(),
        JobEndServlet.foundJobState);
    server.stop();
  }

  @Test
  public void testNotificationOnLastRetryNormalShutdown() throws Exception {
    testNotificationOnLastRetry(false);
  }

  @Test
  public void testNotificationOnLastRetryShutdownWithRuntimeException()
      throws Exception {
    testNotificationOnLastRetry(true);
  }

  @Test
  public void testAbsentNotificationOnNotLastRetryUnregistrationFailure()
      throws Exception {
    HttpServer2 server = startHttpServer();
    MRApp app = spy(new MRAppWithCustomContainerAllocator(2, 2, false,
        this.getClass().getName(), true, 1, false));
    doNothing().when(app).sysexit();
    JobConf conf = new JobConf();
    conf.set(JobContext.MR_JOB_END_NOTIFICATION_URL,
        JobEndServlet.baseUrl + "jobend?jobid=$jobId&status=$jobStatus");
    JobImpl job = (JobImpl)app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    app.getContext().getEventHandler()
      .handle(new JobEvent(app.getJobId(), JobEventType.JOB_AM_REBOOT));
    app.waitForInternalState(job, JobStateInternal.REBOOT);
    // Now shutdown.
    // Unregistration fails: isLastAMRetry is recalculated, this is not
    app.shutDownJob();
    // Not the last AM attempt. So user should that the job is still running.
    app.waitForState(job, JobState.RUNNING);
    Assert.assertFalse(app.isLastAMRetry());
    Assert.assertEquals(0, JobEndServlet.calledTimes);
    Assert.assertNull(JobEndServlet.requestUri);
    Assert.assertNull(JobEndServlet.foundJobState);
    server.stop();
  }

  @Test
  public void testNotificationOnLastRetryUnregistrationFailure()
      throws Exception {
    HttpServer2 server = startHttpServer();
    MRApp app = spy(new MRAppWithCustomContainerAllocator(2, 2, false,
        this.getClass().getName(), true, 2, false));
    // Currently, we will have isLastRetry always equals to false at beginning
    // of MRAppMaster, except staging area exists or commit already started at 
    // the beginning.
    // Now manually set isLastRetry to true and this should reset to false when
    // unregister failed.
    app.isLastAMRetry = true;
    doNothing().when(app).sysexit();
    JobConf conf = new JobConf();
    conf.set(JobContext.MR_JOB_END_NOTIFICATION_URL,
        JobEndServlet.baseUrl + "jobend?jobid=$jobId&status=$jobStatus");
    JobImpl job = (JobImpl)app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    app.getContext().getEventHandler()
      .handle(new JobEvent(app.getJobId(), JobEventType.JOB_AM_REBOOT));
    app.waitForInternalState(job, JobStateInternal.REBOOT);
    // Now shutdown. User should see FAILED state.
    // Unregistration fails: isLastAMRetry is recalculated, this is
    ///reboot will stop service internally, we don't need to shutdown twice
    app.waitForServiceToStop(10000);
    Assert.assertFalse(app.isLastAMRetry());
    // Since it's not last retry, JobEndServlet didn't called
    Assert.assertEquals(0, JobEndServlet.calledTimes);
    Assert.assertNull(JobEndServlet.requestUri);
    Assert.assertNull(JobEndServlet.foundJobState);
    server.stop();
  }

  private static HttpServer2 startHttpServer() throws Exception {
    new File(System.getProperty(
        "build.webapps", "build/webapps") + "/test").mkdirs();
    HttpServer2 server = new HttpServer2.Builder().setName("test")
        .addEndpoint(URI.create("http://localhost:0"))
        .setFindPort(true).build();
    server.addServlet("jobend", "/jobend", JobEndServlet.class);
    server.start();

    JobEndServlet.calledTimes = 0;
    JobEndServlet.requestUri = null;
    JobEndServlet.baseUrl = "http://localhost:"
        + server.getConnectorAddress(0).getPort() + "/";
    JobEndServlet.foundJobState = null;
    return server;
  }

  @SuppressWarnings("serial")
  public static class JobEndServlet extends HttpServlet {
    public static volatile int calledTimes = 0;
    public static URI requestUri;
    public static String baseUrl;
    public static String foundJobState;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
      InputStreamReader in = new InputStreamReader(request.getInputStream());
      PrintStream out = new PrintStream(response.getOutputStream());

      calledTimes++;
      try {
        requestUri = new URI(null, null,
            request.getRequestURI(), request.getQueryString(), null);
        foundJobState = request.getParameter("status");
      } catch (URISyntaxException e) {
      }

      in.close();
      out.close();
    }
  }

  private class MRAppWithCustomContainerAllocator extends MRApp {

    private boolean crushUnregistration;

    public MRAppWithCustomContainerAllocator(int maps, int reduces,
        boolean autoComplete, String testName, boolean cleanOnStart,
        int startCount, boolean crushUnregistration) {
      super(maps, reduces, autoComplete, testName, cleanOnStart, startCount,
          false);
      this.crushUnregistration = crushUnregistration;
    }

    @Override
    protected ContainerAllocator createContainerAllocator(
        ClientService clientService, AppContext context) {
      context = spy(context);
      when(context.getEventHandler()).thenReturn(null);
      when(context.getApplicationID()).thenReturn(null);
      return new CustomContainerAllocator(this, context);
    }

    private class CustomContainerAllocator
        extends RMCommunicator
        implements ContainerAllocator, RMHeartbeatHandler {
      private MRAppWithCustomContainerAllocator app;
      private MRAppContainerAllocator allocator =
          new MRAppContainerAllocator();

      public CustomContainerAllocator(
          MRAppWithCustomContainerAllocator app, AppContext context) {
        super(null, context);
        this.app = app;
      }

      @Override
      public void serviceInit(Configuration conf) {
      }

      @Override
      public void serviceStart() {
      }

      @Override
      public void serviceStop() {
        unregister();
      }

      @Override
      protected void doUnregistration()
          throws YarnException, IOException, InterruptedException {
        if (crushUnregistration) {
          app.successfullyUnregistered.set(true);
        } else {
          throw new YarnException("test exception");
        }
      }

      @Override
      public void handle(ContainerAllocatorEvent event) {
        allocator.handle(event);
      }

      @Override
      public long getLastHeartbeatTime() {
        return allocator.getLastHeartbeatTime();
      }

      @Override
      public void runOnNextHeartbeat(Runnable callback) {
        allocator.runOnNextHeartbeat(callback);
      }

      @Override
      protected void heartbeat() throws Exception {
      }
    }

  }

}
