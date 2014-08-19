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

import java.io.IOException;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.client.MRClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.junit.Test;

public class TestMRAppComponentDependencies {

  @Test(timeout = 20000)
  public void testComponentStopOrder() throws Exception {
    @SuppressWarnings("resource")
    TestMRApp app = new TestMRApp(1, 1, true, this.getClass().getName(), true);
    JobImpl job = (JobImpl) app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();

    int waitTime = 20 * 1000;
    while (waitTime > 0 && app.numStops < 2) {
      Thread.sleep(100);
      waitTime -= 100;
    }

    // assert JobHistoryEventHandlerStopped and then clientServiceStopped
    Assert.assertEquals(1, app.JobHistoryEventHandlerStopped);
    Assert.assertEquals(2, app.clientServiceStopped);
  }

  private final class TestMRApp extends MRApp {
    int JobHistoryEventHandlerStopped;
    int clientServiceStopped;
    int numStops;

    public TestMRApp(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
      JobHistoryEventHandlerStopped = 0;
      clientServiceStopped = 0;
      numStops = 0;
    }

    @Override
    protected Job createJob(Configuration conf, JobStateInternal forcedState,
        String diagnostic) {
      UserGroupInformation currentUser = null;
      try {
        currentUser = UserGroupInformation.getCurrentUser();
      } catch (IOException e) {
        throw new YarnRuntimeException(e);
      }
      Job newJob =
          new TestJob(getJobId(), getAttemptID(), conf, getDispatcher()
            .getEventHandler(), getTaskAttemptListener(), getContext()
            .getClock(), getCommitter(), isNewApiCommitter(),
            currentUser.getUserName(), getContext(), forcedState, diagnostic);
      ((AppContext) getContext()).getAllJobs().put(newJob.getID(), newJob);

      getDispatcher().register(JobFinishEvent.Type.class,
        createJobFinishEventHandler());

      return newJob;
    }

    @Override
    protected ClientService createClientService(AppContext context) {
      return new MRClientService(context) {
        @Override
        public void serviceStop() throws Exception {
          numStops++;
          clientServiceStopped = numStops;
          super.serviceStop();
        }
      };
    }

    @Override
    protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
        AppContext context) {
      return new JobHistoryEventHandler(context, getStartCount()) {
        @Override
        public void serviceStop() throws Exception {
          numStops++;
          JobHistoryEventHandlerStopped = numStops;
          super.serviceStop();
        }
      };
    }
  }
}
