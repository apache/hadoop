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

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.yarn.event.EventHandler;
import org.junit.Test;

public class TestRecovery {

  private static final Log LOG = LogFactory.getLog(TestRecovery.class);

  @Test
  public void testCrashed() throws Exception {
    int runCount = 0;
    MRApp app = new MRAppWithHistory(2, 1, false, this.getClass().getName(), true, ++runCount);
    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    long jobStartTime = job.getReport().getStartTime();
    //all maps would be running
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    Iterator<Task> it = job.getTasks().values().iterator();
    Task mapTask1 = it.next();
    Task mapTask2 = it.next();
    Task reduceTask = it.next();
    
    // all maps must be running
    app.waitForState(mapTask1, TaskState.RUNNING);
    app.waitForState(mapTask2, TaskState.RUNNING);
    
    TaskAttempt task1Attempt1 = mapTask1.getAttempts().values().iterator().next();
    TaskAttempt task2Attempt = mapTask2.getAttempts().values().iterator().next();
    
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task1Attempt1, TaskAttemptState.RUNNING);
    app.waitForState(task2Attempt, TaskAttemptState.RUNNING);
    
    // reduces must be in NEW state
    Assert.assertEquals("Reduce Task state not correct",
        TaskState.RUNNING, reduceTask.getReport().getTaskState());
    
  //send the fail signal to the 1st map task attempt
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task1Attempt1.getID(),
            TaskAttemptEventType.TA_FAILMSG));
    
    app.waitForState(task1Attempt1, TaskAttemptState.FAILED);
    
    while (mapTask1.getAttempts().size() != 2) {
      Thread.sleep(2000);
      LOG.info("Waiting for next attempt to start");
    }
    Iterator<TaskAttempt> itr = mapTask1.getAttempts().values().iterator();
    itr.next();
    TaskAttempt task1Attempt2 = itr.next();
    
    app.waitForState(task1Attempt2, TaskAttemptState.RUNNING);

    //send the kill signal to the 1st map 2nd attempt
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task1Attempt2.getID(),
            TaskAttemptEventType.TA_KILL));
    
    app.waitForState(task1Attempt2, TaskAttemptState.KILLED);
    
    while (mapTask1.getAttempts().size() != 3) {
      Thread.sleep(2000);
      LOG.info("Waiting for next attempt to start");
    }
    itr = mapTask1.getAttempts().values().iterator();
    itr.next();
    itr.next();
    TaskAttempt task1Attempt3 = itr.next();
    
    app.waitForState(task1Attempt3, TaskAttemptState.RUNNING);

    //send the done signal to the 1st map 3rd attempt
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            task1Attempt3.getID(),
            TaskAttemptEventType.TA_DONE));

    //wait for first map task to complete
    app.waitForState(mapTask1, TaskState.SUCCEEDED);
    long task1StartTime = mapTask1.getReport().getStartTime();
    long task1FinishTime = mapTask1.getReport().getFinishTime();
    
    //stop the app
    app.stop();
    
    //rerun
    //in rerun the 1st map will be recovered from previous run
    app = new MRAppWithHistory(2, 1, false, this.getClass().getName(), false, ++runCount);
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, true);
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);
    //all maps would be running
    Assert.assertEquals("No of tasks not correct",
       3, job.getTasks().size());
    it = job.getTasks().values().iterator();
    mapTask1 = it.next();
    mapTask2 = it.next();
    reduceTask = it.next();
    
    // first map will be recovered, no need to send done
    app.waitForState(mapTask1, TaskState.SUCCEEDED);
    
    app.waitForState(mapTask2, TaskState.RUNNING);
    
    task2Attempt = mapTask2.getAttempts().values().iterator().next();
    //before sending the TA_DONE, event make sure attempt has come to 
    //RUNNING state
    app.waitForState(task2Attempt, TaskAttemptState.RUNNING);
    
  //send the done signal to the 2nd map task
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            mapTask2.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));
    
    //wait to get it completed
    app.waitForState(mapTask2, TaskState.SUCCEEDED);
    
    //wait for reduce to be running before sending done
    app.waitForState(reduceTask, TaskState.RUNNING);
    //send the done signal to the reduce
    app.getContext().getEventHandler().handle(
        new TaskAttemptEvent(
            reduceTask.getAttempts().values().iterator().next().getID(),
            TaskAttemptEventType.TA_DONE));
    
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
    Assert.assertEquals("Job Start time not correct",
        jobStartTime, job.getReport().getStartTime());
    Assert.assertEquals("Task Start time not correct",
        task1StartTime, mapTask1.getReport().getStartTime());
    Assert.assertEquals("Task Finish time not correct",
        task1FinishTime, mapTask1.getReport().getFinishTime());
  }

  class MRAppWithHistory extends MRApp {
    public MRAppWithHistory(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart, int startCount) {
      super(maps, reduces, autoComplete, testName, cleanOnStart, startCount);
    }

    @Override
    protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
        AppContext context) {
      JobHistoryEventHandler eventHandler = new JobHistoryEventHandler(context, 
          getStartCount());
      return eventHandler;
    }
  }
  
  public static void main(String[] arg) throws Exception {
    TestRecovery test = new TestRecovery();
    test.testCrashed();
  }
}
