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

package org.apache.hadoop.mapreduce.v2.app.job.impl;

import java.util.Iterator;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletion;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.yarn.event.EventHandler;
import org.junit.Test;

public class TestTaskAttempt{

  @Test
  public void testMRAppHistoryForMap() throws Exception {
    MRApp app = new FailingAttemptsMRApp(1, 0);
    testMRAppHistory(app);
  }

  @Test
  public void testMRAppHistoryForReduce() throws Exception {
    MRApp app = new FailingAttemptsMRApp(0, 1);
    testMRAppHistory(app);
  }

  private void testMRAppHistory(MRApp app) throws Exception {
    Configuration conf = new Configuration();
    Job job = app.submit(conf);
    app.waitForState(job, JobState.FAILED);
    Map<TaskId, Task> tasks = job.getTasks();

    Assert.assertEquals("Num tasks is not correct", 1, tasks.size());
    Task task = tasks.values().iterator().next();
    Assert.assertEquals("Task state not correct", TaskState.FAILED, task
        .getReport().getTaskState());
    Map<TaskAttemptId, TaskAttempt> attempts = tasks.values().iterator().next()
        .getAttempts();
    Assert.assertEquals("Num attempts is not correct", 4, attempts.size());

    Iterator<TaskAttempt> it = attempts.values().iterator();
    TaskAttemptReport report = it.next().getReport();
    Assert.assertEquals("Attempt state not correct", TaskAttemptState.FAILED,
        report.getTaskAttemptState());
    Assert.assertEquals("Diagnostic Information is not Correct",
        "Test Diagnostic Event", report.getDiagnosticInfo());
    report = it.next().getReport();
    Assert.assertEquals("Attempt state not correct", TaskAttemptState.FAILED,
        report.getTaskAttemptState());
  }

  static class FailingAttemptsMRApp extends MRApp {
    FailingAttemptsMRApp(int maps, int reduces) {
      super(maps, reduces, true, "FailingAttemptsMRApp", true);
    }

    @Override
    protected void attemptLaunched(TaskAttemptId attemptID) {
      getContext().getEventHandler().handle(
          new TaskAttemptDiagnosticsUpdateEvent(attemptID,
              "Test Diagnostic Event"));
      getContext().getEventHandler().handle(
          new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_FAILMSG));
    }

    protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
        AppContext context) {
      return new EventHandler<JobHistoryEvent>() {
        @Override
        public void handle(JobHistoryEvent event) {
          if (event.getType() == org.apache.hadoop.mapreduce.jobhistory.EventType.MAP_ATTEMPT_FAILED) {
            TaskAttemptUnsuccessfulCompletion datum = (TaskAttemptUnsuccessfulCompletion) event
                .getHistoryEvent().getDatum();
            Assert.assertEquals("Diagnostic Information is not Correct",
                "Test Diagnostic Event", datum.get(6).toString());
          }
        }
      };
    }
  }
}
