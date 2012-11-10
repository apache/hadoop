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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl.InitTransition;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl.JobNoTasksCompletedTransition;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.InlineDispatcher;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
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
    when(mockJob.finished(JobStateInternal.KILLED)).thenReturn(
        JobStateInternal.KILLED);
    when(mockJob.finished(JobStateInternal.FAILED)).thenReturn(
        JobStateInternal.FAILED);
    when(mockJob.finished(JobStateInternal.SUCCEEDED)).thenReturn(
        JobStateInternal.SUCCEEDED);

    try {
      doThrow(new IOException()).when(mockCommitter).commitJob(any(JobContext.class));
    } catch (IOException e) {
      // commitJob stubbed out, so this can't happen
    }
    doNothing().when(mockEventHandler).handle(any(JobHistoryEvent.class));
    JobStateInternal jobState = JobImpl.checkJobCompleteSuccess(mockJob);
    Assert.assertNotNull("checkJobCompleteSuccess incorrectly returns null " +
      "for successful job", jobState);
    Assert.assertEquals("checkJobCompleteSuccess returns incorrect state",
        JobStateInternal.FAILED, jobState);
    verify(mockJob).abortJob(
        eq(org.apache.hadoop.mapreduce.JobStatus.State.FAILED));
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
    when(mockJob.finished(any(JobStateInternal.class))).thenReturn(
        JobStateInternal.SUCCEEDED);

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
    t.testReportDiagnostics();
    t.testUberDecision();
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
        null, null, null, true, null, 0, null, null);
    Assert.assertTrue(job1.checkAccess(ugi1, JobACL.VIEW_JOB));
    Assert.assertFalse(job1.checkAccess(ugi2, JobACL.VIEW_JOB));

    // Setup configuration access to the user1 (owner) and user2
    Configuration conf2 = new Configuration();
    conf2.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    conf2.set(MRJobConfig.JOB_ACL_VIEW_JOB, user2);

    // Verify access
    JobImpl job2 = new JobImpl(jobId, null, conf2, null, null, null, null, null,
        null, null, null, true, null, 0, null, null);
    Assert.assertTrue(job2.checkAccess(ugi1, JobACL.VIEW_JOB));
    Assert.assertTrue(job2.checkAccess(ugi2, JobACL.VIEW_JOB));

    // Setup configuration access with security enabled and access to all
    Configuration conf3 = new Configuration();
    conf3.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    conf3.set(MRJobConfig.JOB_ACL_VIEW_JOB, "*");

    // Verify access
    JobImpl job3 = new JobImpl(jobId, null, conf3, null, null, null, null, null,
        null, null, null, true, null, 0, null, null);
    Assert.assertTrue(job3.checkAccess(ugi1, JobACL.VIEW_JOB));
    Assert.assertTrue(job3.checkAccess(ugi2, JobACL.VIEW_JOB));

    // Setup configuration access without security enabled
    Configuration conf4 = new Configuration();
    conf4.setBoolean(MRConfig.MR_ACLS_ENABLED, false);
    conf4.set(MRJobConfig.JOB_ACL_VIEW_JOB, "");

    // Verify access
    JobImpl job4 = new JobImpl(jobId, null, conf4, null, null, null, null, null,
        null, null, null, true, null, 0, null, null);
    Assert.assertTrue(job4.checkAccess(ugi1, JobACL.VIEW_JOB));
    Assert.assertTrue(job4.checkAccess(ugi2, JobACL.VIEW_JOB));

    // Setup configuration access without security enabled
    Configuration conf5 = new Configuration();
    conf5.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    conf5.set(MRJobConfig.JOB_ACL_VIEW_JOB, "");

    // Verify access
    JobImpl job5 = new JobImpl(jobId, null, conf5, null, null, null, null, null,
        null, null, null, true, null, 0, null, null);
    Assert.assertTrue(job5.checkAccess(ugi1, null));
    Assert.assertTrue(job5.checkAccess(ugi2, null));
  }

  @Test
  public void testReportDiagnostics() throws Exception {
    JobID jobID = JobID.forName("job_1234567890000_0001");
    JobId jobId = TypeConverter.toYarn(jobID);
    final String diagMsg = "some diagnostic message";
    final JobDiagnosticsUpdateEvent diagUpdateEvent =
        new JobDiagnosticsUpdateEvent(jobId, diagMsg);
    MRAppMetrics mrAppMetrics = MRAppMetrics.create();
    JobImpl job = new JobImpl(jobId, Records
        .newRecord(ApplicationAttemptId.class), new Configuration(),
        mock(EventHandler.class),
        null, mock(JobTokenSecretManager.class), null,
        new SystemClock(), null,
        mrAppMetrics, mock(OutputCommitter.class),
        true, null, 0, null, null);
    job.handle(diagUpdateEvent);
    String diagnostics = job.getReport().getDiagnostics();
    Assert.assertNotNull(diagnostics);
    Assert.assertTrue(diagnostics.contains(diagMsg));

    job = new JobImpl(jobId, Records
        .newRecord(ApplicationAttemptId.class), new Configuration(),
        mock(EventHandler.class),
        null, mock(JobTokenSecretManager.class), null,
        new SystemClock(), null,
        mrAppMetrics, mock(OutputCommitter.class),
        true, null, 0, null, null);
    job.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
    job.handle(diagUpdateEvent);
    diagnostics = job.getReport().getDiagnostics();
    Assert.assertNotNull(diagnostics);
    Assert.assertTrue(diagnostics.contains(diagMsg));
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

  private boolean testUberDecision(Configuration conf) {
    JobID jobID = JobID.forName("job_1234567890000_0001");
    JobId jobId = TypeConverter.toYarn(jobID);
    MRAppMetrics mrAppMetrics = MRAppMetrics.create();
    JobImpl job = new JobImpl(jobId, Records
        .newRecord(ApplicationAttemptId.class), conf, mock(EventHandler.class),
        null, mock(JobTokenSecretManager.class), null, null, null,
        mrAppMetrics, mock(OutputCommitter.class), true, null, 0, null, null);
    InitTransition initTransition = getInitTransition();
    JobEvent mockJobEvent = mock(JobEvent.class);
    initTransition.transition(job, mockJobEvent);
    boolean isUber = job.isUber();
    return isUber;
  }

  private static InitTransition getInitTransition() {
    InitTransition initTransition = new InitTransition() {
      @Override
      protected TaskSplitMetaInfo[] createSplits(JobImpl job, JobId jobId) {
        return new TaskSplitMetaInfo[] { new TaskSplitMetaInfo(),
            new TaskSplitMetaInfo() };
      }
    };
    return initTransition;
  }

  @Test
  public void testTransitionsAtFailed() throws IOException {
    Configuration conf = new Configuration();
    JobID jobID = JobID.forName("job_1234567890000_0001");
    JobId jobId = TypeConverter.toYarn(jobID);
    OutputCommitter committer = mock(OutputCommitter.class);
    doThrow(new IOException("forcefail"))
      .when(committer).setupJob(any(JobContext.class));
    InlineDispatcher dispatcher = new InlineDispatcher();
    JobImpl job = new StubbedJob(jobId, Records
        .newRecord(ApplicationAttemptId.class), conf,
        dispatcher.getEventHandler(), committer, true, null);

    dispatcher.register(JobEventType.class, job);
    job.handle(new JobEvent(jobId, JobEventType.JOB_INIT));
    Assert.assertEquals(JobState.FAILED, job.getState());

    job.handle(new JobEvent(jobId, JobEventType.JOB_TASK_COMPLETED));
    Assert.assertEquals(JobState.FAILED, job.getState());
    job.handle(new JobEvent(jobId, JobEventType.JOB_TASK_ATTEMPT_COMPLETED));
    Assert.assertEquals(JobState.FAILED, job.getState());
    job.handle(new JobEvent(jobId, JobEventType.JOB_MAP_TASK_RESCHEDULED));
    Assert.assertEquals(JobState.FAILED, job.getState());
    job.handle(new JobEvent(jobId, JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE));
    Assert.assertEquals(JobState.FAILED, job.getState());
  }

  private static class StubbedJob extends JobImpl {
    //override the init transition
    private final InitTransition initTransition = getInitTransition();
    StateMachineFactory<JobImpl, JobStateInternal, JobEventType, JobEvent> localFactory
        = stateMachineFactory.addTransition(JobStateInternal.NEW,
            EnumSet.of(JobStateInternal.INITED, JobStateInternal.FAILED),
            JobEventType.JOB_INIT,
            // This is abusive.
            initTransition);

    private final StateMachine<JobStateInternal, JobEventType, JobEvent>
        localStateMachine;

    @Override
    protected StateMachine<JobStateInternal, JobEventType, JobEvent> getStateMachine() {
      return localStateMachine;
    }

    public StubbedJob(JobId jobId, ApplicationAttemptId applicationAttemptId,
        Configuration conf, EventHandler eventHandler,
        OutputCommitter committer, boolean newApiCommitter, String user) {
      super(jobId, applicationAttemptId, conf, eventHandler,
          null, new JobTokenSecretManager(), new Credentials(),
          new SystemClock(), null, MRAppMetrics.create(), committer,
          newApiCommitter, user, System.currentTimeMillis(), null, null);

      // This "this leak" is okay because the retained pointer is in an
      //  instance variable.
      localStateMachine = localFactory.make(this);
    }
  }
}
