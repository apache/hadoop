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

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;

public class TestJobEndNotifier extends TestCase {
  HttpServer server;
  URL baseUrl;

  @SuppressWarnings("serial")
  public static class JobEndServlet extends HttpServlet {
    public static volatile int calledTimes = 0;
    public static URI requestUri;

    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      InputStreamReader in = new InputStreamReader(request.getInputStream());
      PrintStream out = new PrintStream(response.getOutputStream());

      calledTimes++;
      try {
        requestUri = new URI(null, null,
            request.getRequestURI(), request.getQueryString(), null);
      } catch (URISyntaxException e) {
      }

      in.close();
      out.close();
    }
  }

  // Servlet that delays requests for a long time
  @SuppressWarnings("serial")
  public static class DelayServlet extends HttpServlet {
    public static volatile int calledTimes = 0;

    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      boolean timedOut = false;
      calledTimes++;
      try {
        // Sleep for a long time
        Thread.sleep(1000000);
      } catch (InterruptedException e) {
        timedOut = true;
      }
      assertTrue("DelayServlet should be interrupted", timedOut);
    }
  }

  // Servlet that fails all requests into it
  @SuppressWarnings("serial")
  public static class FailServlet extends HttpServlet {
    public static volatile int calledTimes = 0;

    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      calledTimes++;
      throw new IOException("I am failing!");
    }
  }

  public void setUp() throws Exception {
    new File(System.getProperty("build.webapps", "build/webapps") + "/test"
        ).mkdirs();
    server = new HttpServer("test", "0.0.0.0", 0, true);
    server.addServlet("delay", "/delay", DelayServlet.class);
    server.addServlet("jobend", "/jobend", JobEndServlet.class);
    server.addServlet("fail", "/fail", FailServlet.class);
    server.start();
    int port = server.getPort();
    baseUrl = new URL("http://localhost:" + port + "/");

    JobEndServlet.calledTimes = 0;
    JobEndServlet.requestUri = null;
    DelayServlet.calledTimes = 0;
    FailServlet.calledTimes = 0;
  }

  public void tearDown() throws Exception {
    server.stop();
  }

  /**
   * Validate that $jobId and $jobStatus fields are properly substituted
   * in the output URI
   */
  public void testUriSubstitution() throws InterruptedException {
    try {
      JobEndNotifier.startNotifier();

      JobStatus jobStatus = createTestJobStatus(
          "job_20130313155005308_0001", JobStatus.SUCCEEDED);
      JobConf jobConf = createTestJobConf(
          new Configuration(), 0,
          baseUrl + "jobend?jobid=$jobId&status=$jobStatus");
      JobEndNotifier.registerNotification(jobConf, jobStatus);

      int maxLoop = 100;
      while (JobEndServlet.calledTimes != 1 && maxLoop-- > 0) {
        Thread.sleep(100);
      }

      // Validate params
      assertEquals(1, JobEndServlet.calledTimes);
      assertEquals("jobid=job_20130313155005308_0001&status=SUCCEEDED",
          JobEndServlet.requestUri.getQuery());
    } finally {
      JobEndNotifier.stopNotifier();
    }
  }

  /**
   * Validate job.end.retry.attempts logic.
   */
  public void testRetryCount() throws InterruptedException {
    try {
      JobEndNotifier.startNotifier();

      int retryAttempts = 3;
      JobStatus jobStatus = createTestJobStatus(
          "job_20130313155005308_0001", JobStatus.SUCCEEDED);
      JobConf jobConf = createTestJobConf(
          new Configuration(), retryAttempts, baseUrl + "fail");
      JobEndNotifier.registerNotification(jobConf, jobStatus);

      int maxLoop = 100;
      while (FailServlet.calledTimes != (retryAttempts + 1) && maxLoop-- > 0) {
        Thread.sleep(100);
      }

      // Validate params
      assertEquals(retryAttempts + 1, FailServlet.calledTimes);
    } finally {
      JobEndNotifier.stopNotifier();
    }
  }

  /**
   * Validate that the notification times out after reaching
   * mapreduce.job.end-notification.timeout.
   */
  public void testNotificationTimeout() throws InterruptedException {
    try {
      Configuration conf = new Configuration();
      // Reduce the timeout to 1 second
      conf.setInt("mapreduce.job.end-notification.timeout", 1000);
      JobEndNotifier.startNotifier();

      // Submit one notification that will delay infinitely
      JobStatus jobStatus = createTestJobStatus(
          "job_20130313155005308_0001", JobStatus.SUCCEEDED);
      JobConf jobConf = createTestJobConf(
          conf, 0, baseUrl + "delay");
      JobEndNotifier.registerNotification(jobConf, jobStatus);

      // Submit another notification that will return promptly
      jobConf.setJobEndNotificationURI(baseUrl + "jobend");
      JobEndNotifier.registerNotification(jobConf, jobStatus);

      // Make sure the notification passed thru
      int maxLoop = 100;
      while (JobEndServlet.calledTimes != 1 && maxLoop-- > 0) {
        Thread.sleep(100);
      }
      assertEquals("JobEnd notification should have been received by now",
          1, JobEndServlet.calledTimes);
      assertEquals(1, DelayServlet.calledTimes);
      assertEquals("/jobend", JobEndServlet.requestUri.getPath());
    } finally {
      JobEndNotifier.stopNotifier();
    }
  }

  /**
   * Basic validation for localRunnerNotification.
   */
  public void testLocalJobRunnerUriSubstitution() throws InterruptedException {
    JobStatus jobStatus = createTestJobStatus(
        "job_20130313155005308_0001", JobStatus.SUCCEEDED);
    JobConf jobConf = createTestJobConf(
        new Configuration(), 0,
        baseUrl + "jobend?jobid=$jobId&status=$jobStatus");
    JobEndNotifier.localRunnerNotification(jobConf, jobStatus);

    // No need to wait for the notification to go thru since calls are
    // synchronous

    // Validate params
    assertEquals(1, JobEndServlet.calledTimes);
    assertEquals("jobid=job_20130313155005308_0001&status=SUCCEEDED",
        JobEndServlet.requestUri.getQuery());
  }

  /**
   * Validate job.end.retry.attempts for the localJobRunner.
   */
  public void testLocalJobRunnerRetryCount() throws InterruptedException {
    int retryAttempts = 3;
    JobStatus jobStatus = createTestJobStatus(
        "job_20130313155005308_0001", JobStatus.SUCCEEDED);
    JobConf jobConf = createTestJobConf(
        new Configuration(), retryAttempts, baseUrl + "fail");
    JobEndNotifier.localRunnerNotification(jobConf, jobStatus);

    // Validate params
    assertEquals(retryAttempts + 1, FailServlet.calledTimes);
  }

  private static JobStatus createTestJobStatus(String jobId, int state) {
    return new JobStatus(
        JobID.forName(jobId), 0.5f, 0.0f,
        state);
  }

  private static JobConf createTestJobConf(
      Configuration conf, int retryAttempts, String notificationUri) {
    JobConf jobConf = new JobConf(conf);
    jobConf.setInt("job.end.retry.attempts", retryAttempts);
    jobConf.set("job.end.retry.interval", "0");
    jobConf.setJobEndNotificationURI(notificationUri);
    return jobConf;
  }
}
