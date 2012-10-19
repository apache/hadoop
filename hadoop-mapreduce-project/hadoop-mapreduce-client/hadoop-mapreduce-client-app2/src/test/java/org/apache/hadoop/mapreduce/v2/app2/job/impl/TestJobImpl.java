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

package org.apache.hadoop.mapreduce.v2.app2.job.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app2.MRApp;
import org.apache.hadoop.mapreduce.v2.app2.job.Job;
import org.apache.hadoop.mapreduce.v2.app2.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app2.job.Task;
import org.apache.hadoop.mapreduce.v2.app2.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.impl.JobImpl.InitTransition;
import org.apache.hadoop.mapreduce.v2.app2.job.impl.JobImpl.JobNoTasksCompletedTransition;
import org.apache.hadoop.mapreduce.v2.app2.metrics.MRAppMetrics;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests various functions of the JobImpl class
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestJobImpl {
  
  @Test
  public void testJobNoTasksTransition() { 
    JobNoTasksCompletedTransition trans = new JobNoTasksCompletedTransition();
    JobImpl mockJob = mock(JobImpl.class);

    // Force checkJobCompleteSuccess to return null
    Task mockTask = mock(Task.class);
    Map<TaskId, Task> tasks = new HashMap<TaskId, Task>();
    tasks.put(mockTask.getID(), mockTask);
    mockJob.tasks = tasks;

    when(mockJob.getInternalState()).thenReturn(JobStateInternal.ERROR);
    JobEvent mockJobEvent = mock(JobEvent.class);
    JobStateInternal state = trans.transition(mockJob, mockJobEvent);
    Assert.assertEquals("Incorrect state returned from JobNoTasksCompletedTransition",
        JobStateInternal.ERROR, state);
  }

  @Test
  public void testCommitJobFailsJob() {

    JobImpl mockJob = mock(JobImpl.class);
    mockJob.tasks = new HashMap<TaskId, Task>();
    OutputCommitter mockCommitter = mock(OutputCommitter.class);
    EventHandler mockEventHandler = mock(EventHandler.class);
    JobContext mockJobContext = mock(JobContext.class);

    when(mockJob.getCommitter()).thenReturn(mockCommitter);
    when(mockJob.getEventHandler()).thenReturn(mockEventHandler);
    when(mockJob.getJobContext()).thenReturn(mockJobContext);
    doNothing().when(mockJob).setFinishTime();
    doNothing().when(mockJob).logJobHistoryFinishedEvent();
    when(mockJob.finished(JobStateInternal.KILLED)).thenReturn(JobStateInternal.KILLED);
    when(mockJob.finished(JobStateInternal.FAILED)).thenReturn(JobStateInternal.FAILED);
    when(mockJob.finished(JobStateInternal.SUCCEEDED)).thenReturn(JobStateInternal.SUCCEEDED);

    try {
      doThrow(new IOException()).when(mockCommitter).commitJob(any(JobContext.class));
    } catch (IOException e) {
      // commitJob stubbed out, so this can't happen
    }
    doNothing().when(mockEventHandler).handle(any(JobHistoryEvent.class));
    Assert.assertNotNull("checkJobCompleteSuccess incorrectly returns null " +
      "for successful job",
      JobImpl.checkJobCompleteSuccess(mockJob));
    Assert.assertEquals("checkJobCompleteSuccess returns incorrect state",
        JobStateInternal.FAILED, JobImpl.checkJobCompleteSuccess(mockJob));
  }

  @Test
  public void testCheckJobCompleteSuccess() {
    
    JobImpl mockJob = mock(JobImpl.class);
    mockJob.tasks = new HashMap<TaskId, Task>();
    OutputCommitter mockCommitter = mock(OutputCommitter.class);
    EventHandler mockEventHandler = mock(EventHandler.class);
    JobContext mockJobContext = mock(JobContext.class);
    
    when(mockJob.getCommitter()).thenReturn(mockCommitter);
    when(mockJob.getEventHandler()).thenReturn(mockEventHandler);
    when(mockJob.getJobContext()).thenReturn(mockJobContext);
    doNothing().when(mockJob).setFinishTime();
    doNothing().when(mockJob).logJobHistoryFinishedEvent();
    when(mockJob.finished(any(JobStateInternal.class))).thenReturn(JobStateInternal.SUCCEEDED);

    try {
      doNothing().when(mockCommitter).commitJob(any(JobContext.class));
    } catch (IOException e) {
      // commitJob stubbed out, so this can't happen
    }
    doNothing().when(mockEventHandler).handle(any(JobHistoryEvent.class));
    Assert.assertNotNull("checkJobCompleteSuccess incorrectly returns null " +
      "for successful job",
      JobImpl.checkJobCompleteSuccess(mockJob));
    Assert.assertEquals("checkJobCompleteSuccess returns incorrect state",
        JobStateInternal.SUCCEEDED, JobImpl.checkJobCompleteSuccess(mockJob));
  }

  @Test
  public void testCheckJobCompleteSuccessFailed() {
    JobImpl mockJob = mock(JobImpl.class);

    // Make the completedTasks not equal the getTasks()
    Task mockTask = mock(Task.class);
    Map<TaskId, Task> tasks = new HashMap<TaskId, Task>();
    tasks.put(mockTask.getID(), mockTask);
    mockJob.tasks = tasks;
    
    try {
      // Just in case the code breaks and reaches these calls
      OutputCommitter mockCommitter = mock(OutputCommitter.class);
      EventHandler mockEventHandler = mock(EventHandler.class);
      doNothing().when(mockCommitter).commitJob(any(JobContext.class));
      doNothing().when(mockEventHandler).handle(any(JobHistoryEvent.class));
    } catch (IOException e) {
      e.printStackTrace();    
    }
    Assert.assertNull("checkJobCompleteSuccess incorrectly returns not-null " +
      "for unsuccessful job",
      JobImpl.checkJobCompleteSuccess(mockJob));
  }


  public static void main(String[] args) throws Exception {
    TestJobImpl t = new TestJobImpl();
    t.testJobNoTasksTransition();
    t.testCheckJobCompleteSuccess();
    t.testCheckJobCompleteSuccessFailed();
    t.testCheckAccess();
  }

  @Test
  public void testCheckAccess() {
    // Create two unique users
    String user1 = System.getProperty("user.name");
    String user2 = user1 + "1234";
    UserGroupInformation ugi1 = UserGroupInformation.createRemoteUser(user1);
    UserGroupInformation ugi2 = UserGroupInformation.createRemoteUser(user2);

    // Create the job
    JobID jobID = JobID.forName("job_1234567890000_0001");
    JobId jobId = TypeConverter.toYarn(jobID);

    // Setup configuration access only to user1 (owner)
    Configuration conf1 = new Configuration();
    conf1.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    conf1.set(MRJobConfig.JOB_ACL_VIEW_JOB, "");

    // Verify access
    JobImpl job1 = new JobImpl(jobId, null, conf1, null, null, null, null, null,
        null, null, null, true, null, 0, null, null, null);
    Assert.assertTrue(job1.checkAccess(ugi1, JobACL.VIEW_JOB));
    Assert.assertFalse(job1.checkAccess(ugi2, JobACL.VIEW_JOB));

    // Setup configuration access to the user1 (owner) and user2
    Configuration conf2 = new Configuration();
    conf2.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    conf2.set(MRJobConfig.JOB_ACL_VIEW_JOB, user2);

    // Verify access
    JobImpl job2 = new JobImpl(jobId, null, conf2, null, null, null, null, null,
        null, null, null, true, null, 0, null, null, null);
    Assert.assertTrue(job2.checkAccess(ugi1, JobACL.VIEW_JOB));
    Assert.assertTrue(job2.checkAccess(ugi2, JobACL.VIEW_JOB));

    // Setup configuration access with security enabled and access to all
    Configuration conf3 = new Configuration();
    conf3.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    conf3.set(MRJobConfig.JOB_ACL_VIEW_JOB, "*");

    // Verify access
    JobImpl job3 = new JobImpl(jobId, null, conf3, null, null, null, null, null,
        null, null, null, true, null, 0, null, null, null);
    Assert.assertTrue(job3.checkAccess(ugi1, JobACL.VIEW_JOB));
    Assert.assertTrue(job3.checkAccess(ugi2, JobACL.VIEW_JOB));

    // Setup configuration access without security enabled
    Configuration conf4 = new Configuration();
    conf4.setBoolean(MRConfig.MR_ACLS_ENABLED, false);
    conf4.set(MRJobConfig.JOB_ACL_VIEW_JOB, "");

    // Verify access
    JobImpl job4 = new JobImpl(jobId, null, conf4, null, null, null, null, null,
        null, null, null, true, null, 0, null, null, null);
    Assert.assertTrue(job4.checkAccess(ugi1, JobACL.VIEW_JOB));
    Assert.assertTrue(job4.checkAccess(ugi2, JobACL.VIEW_JOB));

    // Setup configuration access without security enabled
    Configuration conf5 = new Configuration();
    conf5.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    conf5.set(MRJobConfig.JOB_ACL_VIEW_JOB, "");

    // Verify access
    JobImpl job5 = new JobImpl(jobId, null, conf5, null, null, null, null, null,
        null, null, null, true, null, 0, null, null, null);
    Assert.assertTrue(job5.checkAccess(ugi1, null));
    Assert.assertTrue(job5.checkAccess(ugi2, null));
  }
  @Test
  public void testUberDecision() throws Exception {

    // with default values, no of maps is 2
    Configuration conf = new Configuration();
    boolean isUber = testUberDecision(conf);
    Assert.assertFalse(isUber);

    // enable uber mode, no of maps is 2
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, true);
    isUber = testUberDecision(conf);
    Assert.assertTrue(isUber);

    // enable uber mode, no of maps is 2, no of reduces is 1 and uber task max
    // reduces is 0
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, true);
    conf.setInt(MRJobConfig.JOB_UBERTASK_MAXREDUCES, 0);
    conf.setInt(MRJobConfig.NUM_REDUCES, 1);
    isUber = testUberDecision(conf);
    Assert.assertFalse(isUber);

    // enable uber mode, no of maps is 2, no of reduces is 1 and uber task max
    // reduces is 1
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, true);
    conf.setInt(MRJobConfig.JOB_UBERTASK_MAXREDUCES, 1);
    conf.setInt(MRJobConfig.NUM_REDUCES, 1);
    isUber = testUberDecision(conf);
    Assert.assertTrue(isUber);

    // enable uber mode, no of maps is 2 and uber task max maps is 0
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, true);
    conf.setInt(MRJobConfig.JOB_UBERTASK_MAXMAPS, 1);
    isUber = testUberDecision(conf);
    Assert.assertFalse(isUber);
  }
  
  @Test
  public void testReportedAppProgress() throws Exception {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(appId, 1);
    
    int numMaps = 10;
    int numReduces = 10;
    int numTasks = numMaps + numReduces;
    Configuration conf = new Configuration();
    MRApp mrApp = new MRApp(appAttemptId, BuilderUtils.newContainerId(
        appAttemptId, 0), numMaps, numReduces, false,
        this.getClass().getName(), true, 1) {
      @Override
      protected Dispatcher createDispatcher() {
        return new DrainDispatcher();
      }
    };
    Job job = mrApp.submit(conf);
    DrainDispatcher dispatcher = (DrainDispatcher) mrApp.getDispatcher();
    
    mrApp.waitForState(job, JobState.RUNNING);
    
    // Empty the queue. All Attempts in RUNNING state.
    // Using waitForState can be slow.

    dispatcher.await();
    // At this point, setup is complete. Tasks may be running.
    float expected = 0.05f;
    Assert.assertEquals(expected, job.getProgress(), 0.001f);
    
    Iterator<Task> it = job.getTasks().values().iterator();

    // finish 1 map.
    int toFinish = 1;
    finishNextNTasks(mrApp, it, toFinish, dispatcher);
    expected += toFinish * 0.9/numTasks;
    Assert.assertEquals(expected, job.getProgress(), 0.001f);
      
    // finish 7 more maps.
    toFinish = 7;
    finishNextNTasks(mrApp, it, toFinish, dispatcher);
    expected += toFinish * 0.9/numTasks;
    Assert.assertEquals(expected, job.getProgress(), 0.001f);
    
    // finish remaining 2 maps.
    toFinish = 2;
    finishNextNTasks(mrApp, it, toFinish, dispatcher);
    expected += toFinish * 0.9/numTasks;
    Assert.assertEquals(expected, job.getProgress(), 0.001f);
    
    // finish 2 reduces
    toFinish = 2;
    finishNextNTasks(mrApp, it, toFinish, dispatcher);
    expected += toFinish * 0.9/numTasks;
    Assert.assertEquals(expected, job.getProgress(), 0.001f);
    
    // finish remaining 8 reduces.
    toFinish = 8;
    finishNextNTasks(mrApp, it, toFinish, dispatcher);
    expected += toFinish * 0.9/numTasks;
    Assert.assertEquals(expected, job.getProgress(), 0.001f);
    
    mrApp.waitForState(job, JobState.SUCCEEDED);
  }
  
  @Test
  // Refer to comments for the previous test.
  public void testReportedAppProgressWithOnlyMaps() throws Exception {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(appId, 1);
    
    int numMaps = 10;
    int numReduces = 0;
    int numTasks = numMaps + numReduces;
    Configuration conf = new Configuration();
    MRApp mrApp = new MRApp(appAttemptId, BuilderUtils.newContainerId(
        appAttemptId, 0), numMaps, numReduces, false,
        this.getClass().getName(), true, 1) {
      @Override
      protected Dispatcher createDispatcher() {
        return new DrainDispatcher();
      }
    };
    Job job = mrApp.submit(conf);
    DrainDispatcher dispatcher = (DrainDispatcher) mrApp.getDispatcher();
    
    mrApp.waitForState(job, JobState.RUNNING);
    
    // Empty the queue. All Attempts in RUNNING state.
    // Using waitForState can be slow.

    dispatcher.await();
    // At this point, setup is complete. Tasks may be running.
    float expected = 0.05f;
    Assert.assertEquals(expected, job.getProgress(), 0.001f);
    
    Iterator<Task> it = job.getTasks().values().iterator();

    // finish 1 map.
    int toFinish = 1;
    finishNextNTasks(mrApp, it, toFinish, dispatcher);
    expected += toFinish * 0.9/numTasks;
    Assert.assertEquals(expected, job.getProgress(), 0.001f);

    // finish 5 more maps.
    toFinish = 5;
    finishNextNTasks(mrApp, it, toFinish, dispatcher);
    expected += toFinish * 0.9/numTasks;
    Assert.assertEquals(expected, job.getProgress(), 0.001f);

    // finish the rest.
    toFinish = 4;
    finishNextNTasks(mrApp, it, toFinish, dispatcher);
    expected += toFinish * 0.9/numTasks;
    Assert.assertEquals(expected, job.getProgress(), 0.001f);
    // TODO This last verification should've been a race. Since the AM never
    // goes beyond 0.95f, this is ok for now.
    
    // TODO. Ideally MRApp should be provide a way to signal job completion.
    // i.e. Do not auto complete a job after all tasks completed. Use in prev
    // test as well. 
    mrApp.waitForState(job, JobState.SUCCEEDED);
  }
  
  private void finishNextNTasks(MRApp mrApp, Iterator<Task> it, int n,
      DrainDispatcher dispatcher) throws Exception {
    finishNextNTasks(mrApp, it, n);
    dispatcher.await();
  }
  
  private void finishNextNTasks(MRApp mrApp, Iterator<Task> it, int n)
      throws Exception {
    for (int i = 0; i < n; i++) {
      if (!it.hasNext()) {
        throw new RuntimeException("Attempt to finish a non-existing task");
      }
      Task task = it.next();
      finishTask(mrApp, task);
    }
  }

  private void finishTask(MRApp mrApp, Task task) throws Exception {
    TaskAttempt attempt = task.getAttempts().values().iterator().next();
    mrApp.sendFinishToTaskAttempt(attempt.getID(), TaskAttemptState.SUCCEEDED,
        false);
  }
  private boolean testUberDecision(Configuration conf) {
    JobID jobID = JobID.forName("job_1234567890000_0001");
    JobId jobId = TypeConverter.toYarn(jobID);
    MRAppMetrics mrAppMetrics = MRAppMetrics.create();
    JobImpl job = new JobImpl(jobId, Records
        .newRecord(ApplicationAttemptId.class), conf, mock(EventHandler.class),
        null, mock(JobTokenSecretManager.class), null, null, null,
        mrAppMetrics, mock(OutputCommitter.class), true, null, 0, null, null, null);
    InitTransition initTransition = getInitTransition();
    JobEvent mockJobEvent = mock(JobEvent.class);
    initTransition.transition(job, mockJobEvent);
    boolean isUber = job.isUber();
    return isUber;
  }

  private InitTransition getInitTransition() {
    InitTransition initTransition = new InitTransition() {
      @Override
      protected TaskSplitMetaInfo[] createSplits(JobImpl job, JobId jobId) {
        return new TaskSplitMetaInfo[] { new TaskSplitMetaInfo(),
            new TaskSplitMetaInfo() };
      }
    };
    return initTransition;
  }
}
