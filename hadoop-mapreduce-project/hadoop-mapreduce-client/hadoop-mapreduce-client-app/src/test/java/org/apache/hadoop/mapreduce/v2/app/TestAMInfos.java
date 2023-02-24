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

import java.util.Iterator;
import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.TestRecovery.MRAppWithHistory;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.junit.Test;

public class TestAMInfos {

  @Test
  public void testAMInfosWithoutRecoveryEnabled() throws Exception {
    int runCount = 0;
    MRApp app =
        new MRAppWithHistory(1, 0, false, this.getClass().getName(), true,
          ++runCount);
    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);

    long am1StartTime = app.getAllAMInfos().get(0).getStartTime();

    Assert.assertEquals("No of tasks not correct", 1, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask = it.next();
    app.waitForState(mapTask, TaskState.RUNNING);
    TaskAttempt taskAttempt = mapTask.getAttempts().values().iterator().next();
    app.waitForState(taskAttempt, TaskAttemptState.RUNNING);

    // stop the app
    app.stop();

    // rerun
    app =
        new MRAppWithHistory(1, 0, false, this.getClass().getName(), false,
          ++runCount);
    conf = new Configuration();
    // in rerun the AMInfo will be recovered from previous run even if recovery
    // is not enabled.
    conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, false);
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    Assert.assertEquals("No of tasks not correct", 1, job.getTasks().size());
    it = job.getTasks().values().iterator();
    mapTask = it.next();
    // There should be two AMInfos
    List<AMInfo> amInfos = app.getAllAMInfos();
    Assert.assertEquals(2, amInfos.size());
    AMInfo amInfoOne = amInfos.get(0);
    Assert.assertEquals(am1StartTime, amInfoOne.getStartTime());
    app.stop();
  }
}
