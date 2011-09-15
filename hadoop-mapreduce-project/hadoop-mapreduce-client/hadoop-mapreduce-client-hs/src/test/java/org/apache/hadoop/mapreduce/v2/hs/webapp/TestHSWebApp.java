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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.APP_ID;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.ATTEMPT_STATE;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MockJobs;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.webapp.AMParams;
import org.apache.hadoop.mapreduce.v2.app.webapp.TestAMWebApp;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Test;

import com.google.inject.Injector;

public class TestHSWebApp {
  private static final Log LOG = LogFactory.getLog(TestHSWebApp.class);

  static class TestAppContext implements AppContext {
    final ApplicationAttemptId appAttemptID;
    final ApplicationId appID;
    final String user = MockJobs.newUserName();
    final Map<JobId, Job> jobs;
    final long startTime = System.currentTimeMillis();

    TestAppContext(int appid, int numJobs, int numTasks, int numAttempts) {
      appID = MockJobs.newAppID(appid);
      appAttemptID = MockJobs.newAppAttemptID(appID, 0);
      jobs = MockJobs.newJobs(appID, numJobs, numTasks, numAttempts);
    }

    TestAppContext() {
      this(0, 1, 1, 1);
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptID;
    }

    @Override
    public ApplicationId getApplicationID() {
      return appID;
    }

    @Override
    public CharSequence getUser() {
      return user;
    }

    @Override
    public Job getJob(JobId jobID) {
      return jobs.get(jobID);
    }

    @Override
    public Map<JobId, Job> getAllJobs() {
      return jobs; // OK
    }

    @Override
    public EventHandler getEventHandler() {
      return null;
    }

    @Override
    public Clock getClock() {
      return null;
    }

    @Override
    public String getApplicationName() {
      return "TestApp";
    }

    @Override
    public long getStartTime() {
      return startTime;
    }
  }

  @Test public void testAppControllerIndex() {
    TestAppContext ctx = new TestAppContext();
    Injector injector = WebAppTests.createMockInjector(AppContext.class, ctx);
    HsController controller = injector.getInstance(HsController.class);
    controller.index();
    assertEquals(ctx.appID.toString(), controller.get(APP_ID,""));
  }

  @Test public void testJobView() {
    LOG.info("HsJobPage");
    AppContext appContext = new TestAppContext();
    Map<String, String> params = TestAMWebApp.getJobParams(appContext);
    WebAppTests.testPage(HsJobPage.class, AppContext.class, appContext, params);
  }

  @Test
  public void testTasksView() {
    LOG.info("HsTasksPage");
    AppContext appContext = new TestAppContext();
    Map<String, String> params = TestAMWebApp.getTaskParams(appContext);
    WebAppTests.testPage(HsTasksPage.class, AppContext.class, appContext,
        params);
  }

  @Test
  public void testTaskView() {
    LOG.info("HsTaskPage");
    AppContext appContext = new TestAppContext();
    Map<String, String> params = TestAMWebApp.getTaskParams(appContext);
    WebAppTests
        .testPage(HsTaskPage.class, AppContext.class, appContext, params);
  }

  @Test public void testAttemptsWithJobView() {
    LOG.info("HsAttemptsPage with data");
    TestAppContext ctx = new TestAppContext();
    JobId id = ctx.getAllJobs().keySet().iterator().next();
    Map<String, String> params = new HashMap<String,String>();
    params.put(JOB_ID, id.toString());
    params.put(TASK_TYPE, "m");
    params.put(ATTEMPT_STATE, "SUCCESSFUL");
    WebAppTests.testPage(HsAttemptsPage.class, AppContext.class,
        ctx, params);
  }
  
  @Test public void testAttemptsView() {
    LOG.info("HsAttemptsPage");
    AppContext appContext = new TestAppContext();
    Map<String, String> params = TestAMWebApp.getTaskParams(appContext);
    WebAppTests.testPage(HsAttemptsPage.class, AppContext.class,
                         appContext, params);
  }
  
  @Test public void testConfView() {
    LOG.info("HsConfPage");
    WebAppTests.testPage(HsConfPage.class, AppContext.class,
                         new TestAppContext());
  }
}
