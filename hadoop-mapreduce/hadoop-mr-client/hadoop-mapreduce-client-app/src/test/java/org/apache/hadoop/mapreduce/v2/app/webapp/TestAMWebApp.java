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

import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MockJobs;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Test;

import com.google.inject.Injector;

public class TestAMWebApp {

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
    AppController controller = injector.getInstance(AppController.class);
    controller.index();
    assertEquals(Apps.toString(ctx.appID), controller.get(APP_ID,""));
  }

  @Test public void testAppView() {
    WebAppTests.testPage(AppView.class, AppContext.class, new TestAppContext());
  }

  @Test public void testJobView() {
    WebAppTests.testPage(JobPage.class, AppContext.class, new TestAppContext());
  }

  @Test public void testTasksView() {
    WebAppTests.testPage(TasksPage.class, AppContext.class,
                         new TestAppContext());
  }

  @Test public void testTaskView() {
    WebAppTests.testPage(TaskPage.class, AppContext.class,
                         new TestAppContext());
  }

  public static void main(String[] args) {
    WebApps.$for("yarn", AppContext.class, new TestAppContext(0, 8, 88, 4)).
        at(58888).inDevMode().start(new AMWebApp()).joinThread();
  }
}
