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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.EventType;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobSubmittedEvent;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEventHandler;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEventType;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobStartEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobUpdatedNodesEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl.InitTransition;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Tests various functions of the JobImpl class
 */
@SuppressWarnings({"rawtypes"})
public class TestJobImpl {
  
  static String stagingDir = "target/test-staging/";

  @BeforeClass
  public static void setup() {    
    File dir = new File(stagingDir);
    stagingDir = dir.getAbsolutePath();
  }

  @Before
  public void cleanup() throws IOException {
    File dir = new File(stagingDir);
    if(dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
    dir.mkdirs();
  }
  
  @Test
  public void testJobNoTasks() {
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.NUM_REDUCES, 0);
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    conf.set(MRJobConfig.WORKFLOW_ID, "testId");
    conf.set(MRJobConfig.WORKFLOW_NAME, "testName");
    conf.set(MRJobConfig.WORKFLOW_NODE_NAME, "testNodeName");
    conf.set(MRJobConfig.WORKFLOW_ADJACENCY_PREFIX_STRING + "key1", "value1");
    conf.set(MRJobConfig.WORKFLOW_ADJACENCY_PREFIX_STRING + "key2", "value2");
    conf.set(MRJobConfig.WORKFLOW_TAGS, "tag1,tag2");
    
 
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    OutputCommitter committer = mock(OutputCommitter.class);
    CommitterEventHandler commitHandler =
        createCommitterEventHandler(dispatcher, committer);
    commitHandler.init(conf);
    commitHandler.start();

    JobSubmittedEventHandler jseHandler = new JobSubmittedEventHandler("testId",
        "testName", "testNodeName", "\"key2\"=\"value2\" \"key1\"=\"value1\" ",
        "tag1,tag2");
    dispatcher.register(EventType.class, jseHandler);
    JobImpl job = createStubbedJob(conf, dispatcher, 0, null);
    job.handle(new JobEvent(job.getID(), JobEventType.JOB_INIT));
    assertJobState(job, JobStateInternal.INITED);
    job.handle(new JobStartEvent(job.getID()));
    assertJobState(job, JobStateInternal.SUCCEEDED);
    dispatcher.stop();
    commitHandler.stop();
    try {
      Assert.assertTrue(jseHandler.getAssertValue());
    } catch (InterruptedException e) {
      Assert.fail("Workflow related attributes are not tested properly");
    }
  }

  @Test(timeout=20000)
  public void testCommitJobFailsJob() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    CyclicBarrier syncBarrier = new CyclicBarrier(2);
    OutputCommitter committer = new TestingOutputCommitter(syncBarrier, false);
    CommitterEventHandler commitHandler =
        createCommitterEventHandler(dispatcher, committer);
    commitHandler.init(conf);
    commitHandler.start();

    JobImpl job = createRunningStubbedJob(conf, dispatcher, 2, null);
    completeJobTasks(job);
    assertJobState(job, JobStateInternal.COMMITTING);

    // let the committer fail and verify the job fails
    syncBarrier.await();
    assertJobState(job, JobStateInternal.FAILED);
    dispatcher.stop();
    commitHandler.stop();
  }

  @Test(timeout=20000)
  public void testCheckJobCompleteSuccess() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    CyclicBarrier syncBarrier = new CyclicBarrier(2);
    OutputCommitter committer = new TestingOutputCommitter(syncBarrier, true);
    CommitterEventHandler commitHandler =
        createCommitterEventHandler(dispatcher, committer);
    commitHandler.init(conf);
    commitHandler.start();

    JobImpl job = createRunningStubbedJob(conf, dispatcher, 2, null);
    completeJobTasks(job);
    assertJobState(job, JobStateInternal.COMMITTING);

    job.handle(new JobEvent(job.getID(),
        JobEventType.JOB_TASK_ATTEMPT_COMPLETED));
    assertJobState(job, JobStateInternal.COMMITTING);

    job.handle(new JobEvent(job.getID(),
        JobEventType.JOB_MAP_TASK_RESCHEDULED));
    assertJobState(job, JobStateInternal.COMMITTING);

    // let the committer complete and verify the job succeeds
    syncBarrier.await();
    assertJobState(job, JobStateInternal.SUCCEEDED);
    
    job.handle(new JobEvent(job.getID(),
        JobEventType.JOB_TASK_ATTEMPT_COMPLETED));
    assertJobState(job, JobStateInternal.SUCCEEDED);

    job.handle(new JobEvent(job.getID(), 
        JobEventType.JOB_MAP_TASK_RESCHEDULED));
    assertJobState(job, JobStateInternal.SUCCEEDED);
    
    dispatcher.stop();
    commitHandler.stop();
  }

  @Test(timeout=20000)
  public void testRebootedDuringSetup() throws Exception{
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    OutputCommitter committer = new StubbedOutputCommitter() {
      @Override
      public synchronized void setupJob(JobContext jobContext)
          throws IOException {
        while(!Thread.interrupted()){
          try{
            wait();
          }catch (InterruptedException e) {
          }
        }
      }
    };
    CommitterEventHandler commitHandler =
        createCommitterEventHandler(dispatcher, committer);
    commitHandler.init(conf);
    commitHandler.start();

    AppContext mockContext = mock(AppContext.class);
    when(mockContext.isLastAMRetry()).thenReturn(false);
    JobImpl job = createStubbedJob(conf, dispatcher, 2, mockContext);
    JobId jobId = job.getID();
    job.handle(new JobEvent(jobId, JobEventType.JOB_INIT));
    assertJobState(job, JobStateInternal.INITED);
    job.handle(new JobStartEvent(jobId));
    assertJobState(job, JobStateInternal.SETUP);

    job.handle(new JobEvent(job.getID(), JobEventType.JOB_AM_REBOOT));
    assertJobState(job, JobStateInternal.REBOOT);
    // return the external state as RUNNING since otherwise JobClient will
    // exit when it polls the AM for job state
    Assert.assertEquals(JobState.RUNNING, job.getState());

    dispatcher.stop();
    commitHandler.stop();
  }

  @Test(timeout=20000)
  public void testRebootedDuringCommit() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    conf.setInt(MRJobConfig.MR_AM_MAX_ATTEMPTS, 2);
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    CyclicBarrier syncBarrier = new CyclicBarrier(2);
    OutputCommitter committer = new WaitingOutputCommitter(syncBarrier, true);
    CommitterEventHandler commitHandler =
        createCommitterEventHandler(dispatcher, committer);
    commitHandler.init(conf);
    commitHandler.start();

    AppContext mockContext = mock(AppContext.class);
    when(mockContext.isLastAMRetry()).thenReturn(true);
    when(mockContext.hasSuccessfullyUnregistered()).thenReturn(false);
    JobImpl job = createRunningStubbedJob(conf, dispatcher, 2, mockContext);
    completeJobTasks(job);
    assertJobState(job, JobStateInternal.COMMITTING);

    syncBarrier.await();
    job.handle(new JobEvent(job.getID(), JobEventType.JOB_AM_REBOOT));
    assertJobState(job, JobStateInternal.REBOOT);
    // return the external state as ERROR since this is last retry.
    Assert.assertEquals(JobState.RUNNING, job.getState());
    when(mockContext.hasSuccessfullyUnregistered()).thenReturn(true);
    Assert.assertEquals(JobState.ERROR, job.getState());

    dispatcher.stop();
    commitHandler.stop();
  }

  @Test(timeout=20000)
  public void testKilledDuringSetup() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    OutputCommitter committer = new StubbedOutputCommitter() {
      @Override
      public synchronized void setupJob(JobContext jobContext)
          throws IOException {
        while (!Thread.interrupted()) {
          try {
            wait();
          } catch (InterruptedException e) {
          }
        }
      }
    };
    CommitterEventHandler commitHandler =
        createCommitterEventHandler(dispatcher, committer);
    commitHandler.init(conf);
    commitHandler.start();

    JobImpl job = createStubbedJob(conf, dispatcher, 2, null);
    JobId jobId = job.getID();
    job.handle(new JobEvent(jobId, JobEventType.JOB_INIT));
    assertJobState(job, JobStateInternal.INITED);
    job.handle(new JobStartEvent(jobId));
    assertJobState(job, JobStateInternal.SETUP);

    job.handle(new JobEvent(job.getID(), JobEventType.JOB_KILL));
    assertJobState(job, JobStateInternal.KILLED);
    dispatcher.stop();
    commitHandler.stop();
  }

  @Test(timeout=20000)
  public void testKilledDuringCommit() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    CyclicBarrier syncBarrier = new CyclicBarrier(2);
    OutputCommitter committer = new WaitingOutputCommitter(syncBarrier, true);
    CommitterEventHandler commitHandler =
        createCommitterEventHandler(dispatcher, committer);
    commitHandler.init(conf);
    commitHandler.start();

    JobImpl job = createRunningStubbedJob(conf, dispatcher, 2, null);
    completeJobTasks(job);
    assertJobState(job, JobStateInternal.COMMITTING);

    syncBarrier.await();
    job.handle(new JobEvent(job.getID(), JobEventType.JOB_KILL));
    assertJobState(job, JobStateInternal.KILLED);
    dispatcher.stop();
    commitHandler.stop();
  }

  @Test
  public void testAbortJobCalledAfterKillingTasks() throws IOException {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    conf.set(MRJobConfig.MR_AM_COMMITTER_CANCEL_TIMEOUT_MS, "1000");
    InlineDispatcher dispatcher = new InlineDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    OutputCommitter committer = Mockito.mock(OutputCommitter.class);
    CommitterEventHandler commitHandler =
        createCommitterEventHandler(dispatcher, committer);
    commitHandler.init(conf);
    commitHandler.start();
    JobImpl job = createRunningStubbedJob(conf, dispatcher, 2, null);

    //Fail one task. This should land the JobImpl in the FAIL_WAIT state
    job.handle(new JobTaskEvent(
      MRBuilderUtils.newTaskId(job.getID(), 1, TaskType.MAP),
      TaskState.FAILED));
    //Verify abort job hasn't been called
    Mockito.verify(committer, Mockito.never())
      .abortJob((JobContext) Mockito.any(), (State) Mockito.any());
    assertJobState(job, JobStateInternal.FAIL_WAIT);

    //Verify abortJob is called once and the job failed
    Mockito.verify(committer, Mockito.timeout(2000).times(1))
      .abortJob((JobContext) Mockito.any(), (State) Mockito.any());
    assertJobState(job, JobStateInternal.FAILED);

    dispatcher.stop();
  }

  @Test (timeout=10000)
  public void testFailAbortDoesntHang() throws IOException {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    conf.set(MRJobConfig.MR_AM_COMMITTER_CANCEL_TIMEOUT_MS, "1000");
    
    DrainDispatcher dispatcher = new DrainDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    OutputCommitter committer = Mockito.mock(OutputCommitter.class);
    CommitterEventHandler commitHandler =
        createCommitterEventHandler(dispatcher, committer);
    commitHandler.init(conf);
    commitHandler.start();
    //Job has only 1 mapper task. No reducers
    conf.setInt(MRJobConfig.NUM_REDUCES, 0);
    conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);
    JobImpl job = createRunningStubbedJob(conf, dispatcher, 1, null);

    //Fail / finish all the tasks. This should land the JobImpl directly in the
    //FAIL_ABORT state
    for(Task t: job.tasks.values()) {
      TaskImpl task = (TaskImpl) t;
      task.handle(new TaskEvent(task.getID(), TaskEventType.T_SCHEDULE));
      for(TaskAttempt ta: task.getAttempts().values()) {
        task.handle(new TaskTAttemptEvent(ta.getID(),
          TaskEventType.T_ATTEMPT_FAILED));
      }
    }

    dispatcher.await();
    //Verify abortJob is called once and the job failed
    Mockito.verify(committer, Mockito.timeout(2000).times(1))
      .abortJob((JobContext) Mockito.any(), (State) Mockito.any());
    assertJobState(job, JobStateInternal.FAILED);

    dispatcher.stop();
  }

  @Test(timeout=20000)
  public void testKilledDuringFailAbort() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    OutputCommitter committer = new StubbedOutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) throws IOException {
        throw new IOException("forced failure");
      }

      @Override
      public synchronized void abortJob(JobContext jobContext, State state)
          throws IOException {
        while (!Thread.interrupted()) {
          try {
            wait();
          } catch (InterruptedException e) {
          }
        }
      }
    };
    CommitterEventHandler commitHandler =
        createCommitterEventHandler(dispatcher, committer);
    commitHandler.init(conf);
    commitHandler.start();

    JobImpl job = createStubbedJob(conf, dispatcher, 2, null);
    JobId jobId = job.getID();
    job.handle(new JobEvent(jobId, JobEventType.JOB_INIT));
    assertJobState(job, JobStateInternal.INITED);
    job.handle(new JobStartEvent(jobId));
    assertJobState(job, JobStateInternal.FAIL_ABORT);

    job.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
    assertJobState(job, JobStateInternal.KILLED);
    dispatcher.stop();
    commitHandler.stop();
  }

  @Test(timeout=20000)
  public void testKilledDuringKillAbort() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    OutputCommitter committer = new StubbedOutputCommitter() {
      @Override
      public synchronized void abortJob(JobContext jobContext, State state)
          throws IOException {
        while (!Thread.interrupted()) {
          try {
            wait();
          } catch (InterruptedException e) {
          }
        }
      }
    };
    CommitterEventHandler commitHandler =
        createCommitterEventHandler(dispatcher, committer);
    commitHandler.init(conf);
    commitHandler.start();

    JobImpl job = createStubbedJob(conf, dispatcher, 2, null);
    JobId jobId = job.getID();
    job.handle(new JobEvent(jobId, JobEventType.JOB_INIT));
    assertJobState(job, JobStateInternal.INITED);
    job.handle(new JobStartEvent(jobId));
    assertJobState(job, JobStateInternal.SETUP);

    job.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
    assertJobState(job, JobStateInternal.KILL_ABORT);

    job.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
    assertJobState(job, JobStateInternal.KILLED);
    dispatcher.stop();
    commitHandler.stop();
  }

  @Test(timeout=20000)
  public void testUnusableNodeTransition() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    conf.setInt(MRJobConfig.NUM_REDUCES, 1);
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    CyclicBarrier syncBarrier = new CyclicBarrier(2);
    OutputCommitter committer = new TestingOutputCommitter(syncBarrier, true);
    CommitterEventHandler commitHandler =
        createCommitterEventHandler(dispatcher, committer);
    commitHandler.init(conf);
    commitHandler.start();

    final JobImpl job = createRunningStubbedJob(conf, dispatcher, 2, null);
    // add a special task event handler to put the task back to running in case
    // of task rescheduling/killing
    EventHandler<TaskAttemptEvent> taskAttemptEventHandler =
        new EventHandler<TaskAttemptEvent>() {
      @Override
      public void handle(TaskAttemptEvent event) {
        if (event.getType() == TaskAttemptEventType.TA_KILL) {
          job.decrementSucceededMapperCount();
        }
      }
    };
    dispatcher.register(TaskAttemptEventType.class, taskAttemptEventHandler);

    // replace the tasks with spied versions to return the right attempts
    Map<TaskId,Task> spiedTasks = new HashMap<TaskId,Task>();
    List<NodeReport> nodeReports = new ArrayList<NodeReport>();
    Map<NodeReport,TaskId> nodeReportsToTaskIds =
        new HashMap<NodeReport,TaskId>();
    for (Map.Entry<TaskId,Task> e: job.tasks.entrySet()) {
      TaskId taskId = e.getKey();
      Task task = e.getValue();
      if (taskId.getTaskType() == TaskType.MAP) {
        // add an attempt to the task to simulate nodes
        NodeId nodeId = mock(NodeId.class);
        TaskAttempt attempt = mock(TaskAttempt.class);
        when(attempt.getNodeId()).thenReturn(nodeId);
        TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
        when(attempt.getID()).thenReturn(attemptId);
        // create a spied task
        Task spied = spy(task);
        doReturn(attempt).when(spied).getAttempt(any(TaskAttemptId.class));
        spiedTasks.put(taskId, spied);

        // create a NodeReport based on the node id
        NodeReport report = mock(NodeReport.class);
        when(report.getNodeState()).thenReturn(NodeState.UNHEALTHY);
        when(report.getNodeId()).thenReturn(nodeId);
        nodeReports.add(report);
        nodeReportsToTaskIds.put(report, taskId);
      }
    }
    // replace the tasks with the spied tasks
    job.tasks.putAll(spiedTasks);

    // complete all mappers first
    for (TaskId taskId: job.tasks.keySet()) {
      if (taskId.getTaskType() == TaskType.MAP) {
        // generate a task attempt completed event first to populate the
        // nodes-to-succeeded-attempts map
        TaskAttemptCompletionEvent tce =
            Records.newRecord(TaskAttemptCompletionEvent.class);
        TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
        tce.setAttemptId(attemptId);
        tce.setStatus(TaskAttemptCompletionEventStatus.SUCCEEDED);
        job.handle(new JobTaskAttemptCompletedEvent(tce));
        // complete the task itself
        job.handle(new JobTaskEvent(taskId, TaskState.SUCCEEDED));
        Assert.assertEquals(JobState.RUNNING, job.getState());
      }
    }

    // add an event for a node transition
    NodeReport firstMapperNodeReport = nodeReports.get(0);
    NodeReport secondMapperNodeReport = nodeReports.get(1);
    job.handle(new JobUpdatedNodesEvent(job.getID(),
        Collections.singletonList(firstMapperNodeReport)));
    // complete the reducer
    for (TaskId taskId: job.tasks.keySet()) {
      if (taskId.getTaskType() == TaskType.REDUCE) {
        job.handle(new JobTaskEvent(taskId, TaskState.SUCCEEDED));
      }
    }
    // add another event for a node transition for the other mapper
    // this should not trigger rescheduling
    job.handle(new JobUpdatedNodesEvent(job.getID(),
        Collections.singletonList(secondMapperNodeReport)));
    // complete the first mapper that was rescheduled
    TaskId firstMapper = nodeReportsToTaskIds.get(firstMapperNodeReport);
    job.handle(new JobTaskEvent(firstMapper, TaskState.SUCCEEDED));
    // verify the state is moving to committing
    assertJobState(job, JobStateInternal.COMMITTING);

    // let the committer complete and verify the job succeeds
    syncBarrier.await();
    assertJobState(job, JobStateInternal.SUCCEEDED);

    dispatcher.stop();
    commitHandler.stop();
  }

  public static void main(String[] args) throws Exception {
    TestJobImpl t = new TestJobImpl();
    t.testJobNoTasks();
    t.testCheckJobCompleteSuccess();
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
        null, null, null, true, user1, 0, null, null, null, null);
    Assert.assertTrue(job1.checkAccess(ugi1, JobACL.VIEW_JOB));
    Assert.assertFalse(job1.checkAccess(ugi2, JobACL.VIEW_JOB));

    // Setup configuration access to the user1 (owner) and user2
    Configuration conf2 = new Configuration();
    conf2.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    conf2.set(MRJobConfig.JOB_ACL_VIEW_JOB, user2);

    // Verify access
    JobImpl job2 = new JobImpl(jobId, null, conf2, null, null, null, null, null,
        null, null, null, true, user1, 0, null, null, null, null);
    Assert.assertTrue(job2.checkAccess(ugi1, JobACL.VIEW_JOB));
    Assert.assertTrue(job2.checkAccess(ugi2, JobACL.VIEW_JOB));

    // Setup configuration access with security enabled and access to all
    Configuration conf3 = new Configuration();
    conf3.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    conf3.set(MRJobConfig.JOB_ACL_VIEW_JOB, "*");

    // Verify access
    JobImpl job3 = new JobImpl(jobId, null, conf3, null, null, null, null, null,
        null, null, null, true, user1, 0, null, null, null, null);
    Assert.assertTrue(job3.checkAccess(ugi1, JobACL.VIEW_JOB));
    Assert.assertTrue(job3.checkAccess(ugi2, JobACL.VIEW_JOB));

    // Setup configuration access without security enabled
    Configuration conf4 = new Configuration();
    conf4.setBoolean(MRConfig.MR_ACLS_ENABLED, false);
    conf4.set(MRJobConfig.JOB_ACL_VIEW_JOB, "");

    // Verify access
    JobImpl job4 = new JobImpl(jobId, null, conf4, null, null, null, null, null,
        null, null, null, true, user1, 0, null, null, null, null);
    Assert.assertTrue(job4.checkAccess(ugi1, JobACL.VIEW_JOB));
    Assert.assertTrue(job4.checkAccess(ugi2, JobACL.VIEW_JOB));

    // Setup configuration access without security enabled
    Configuration conf5 = new Configuration();
    conf5.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
    conf5.set(MRJobConfig.JOB_ACL_VIEW_JOB, "");

    // Verify access
    JobImpl job5 = new JobImpl(jobId, null, conf5, null, null, null, null, null,
        null, null, null, true, user1, 0, null, null, null, null);
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
    AppContext mockContext = mock(AppContext.class);
    when(mockContext.hasSuccessfullyUnregistered()).thenReturn(true);
    JobImpl job = new JobImpl(jobId, Records
        .newRecord(ApplicationAttemptId.class), new Configuration(),
        mock(EventHandler.class),
        null, mock(JobTokenSecretManager.class), null,
        new SystemClock(), null,
        mrAppMetrics, null, true, null, 0, null, mockContext, null, null);
    job.handle(diagUpdateEvent);
    String diagnostics = job.getReport().getDiagnostics();
    Assert.assertNotNull(diagnostics);
    Assert.assertTrue(diagnostics.contains(diagMsg));

    job = new JobImpl(jobId, Records
        .newRecord(ApplicationAttemptId.class), new Configuration(),
        mock(EventHandler.class),
        null, mock(JobTokenSecretManager.class), null,
        new SystemClock(), null,
        mrAppMetrics, null, true, null, 0, null, mockContext, null, null);
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
    
 // enable uber mode of 0 reducer no matter how much memory assigned to reducer
    conf = new Configuration();
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, true);  
    conf.setInt(MRJobConfig.NUM_REDUCES, 0);           
    conf.setInt(MRJobConfig.REDUCE_MEMORY_MB, 2048);
    conf.setInt(MRJobConfig.REDUCE_CPU_VCORES, 10);
    isUber = testUberDecision(conf);
    Assert.assertTrue(isUber);
  }

  private boolean testUberDecision(Configuration conf) {
    JobID jobID = JobID.forName("job_1234567890000_0001");
    JobId jobId = TypeConverter.toYarn(jobID);
    MRAppMetrics mrAppMetrics = MRAppMetrics.create();
    JobImpl job =
        new JobImpl(jobId, ApplicationAttemptId.newInstance(
          ApplicationId.newInstance(0, 0), 0), conf, mock(EventHandler.class),
          null, new JobTokenSecretManager(), new Credentials(), null, null,
          mrAppMetrics, null, true, null, 0, null, null, null, null);
    InitTransition initTransition = getInitTransition(2);
    JobEvent mockJobEvent = mock(JobEvent.class);
    initTransition.transition(job, mockJobEvent);
    boolean isUber = job.isUber();
    return isUber;
  }

  private static InitTransition getInitTransition(final int numSplits) {
    InitTransition initTransition = new InitTransition() {
      @Override
      protected TaskSplitMetaInfo[] createSplits(JobImpl job, JobId jobId) {
        TaskSplitMetaInfo[] splits = new TaskSplitMetaInfo[numSplits];
        for (int i = 0; i < numSplits; ++i) {
          splits[i] = new TaskSplitMetaInfo();
        }
        return splits;
      }
    };
    return initTransition;
  }

  @Test
  public void testTransitionsAtFailed() throws IOException {
    Configuration conf = new Configuration();
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();

    OutputCommitter committer = mock(OutputCommitter.class);
    doThrow(new IOException("forcefail"))
      .when(committer).setupJob(any(JobContext.class));
    CommitterEventHandler commitHandler =
        createCommitterEventHandler(dispatcher, committer);
    commitHandler.init(conf);
    commitHandler.start();

    AppContext mockContext = mock(AppContext.class);
    when(mockContext.hasSuccessfullyUnregistered()).thenReturn(false);
    JobImpl job = createStubbedJob(conf, dispatcher, 2, mockContext);
    JobId jobId = job.getID();
    job.handle(new JobEvent(jobId, JobEventType.JOB_INIT));
    assertJobState(job, JobStateInternal.INITED);
    job.handle(new JobStartEvent(jobId));
    assertJobState(job, JobStateInternal.FAILED);

    job.handle(new JobEvent(jobId, JobEventType.JOB_TASK_COMPLETED));
    assertJobState(job, JobStateInternal.FAILED);
    job.handle(new JobEvent(jobId, JobEventType.JOB_TASK_ATTEMPT_COMPLETED));
    assertJobState(job, JobStateInternal.FAILED);
    job.handle(new JobEvent(jobId, JobEventType.JOB_MAP_TASK_RESCHEDULED));
    assertJobState(job, JobStateInternal.FAILED);
    job.handle(new JobEvent(jobId, JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE));
    assertJobState(job, JobStateInternal.FAILED);
    Assert.assertEquals(JobState.RUNNING, job.getState());
    when(mockContext.hasSuccessfullyUnregistered()).thenReturn(true);
    Assert.assertEquals(JobState.FAILED, job.getState());

    dispatcher.stop();
    commitHandler.stop();
  }

  static final String EXCEPTIONMSG = "Splits max exceeded";
  @Test
  public void testMetaInfoSizeOverMax() throws Exception {
    Configuration conf = new Configuration();
    JobID jobID = JobID.forName("job_1234567890000_0001");
    JobId jobId = TypeConverter.toYarn(jobID);
    MRAppMetrics mrAppMetrics = MRAppMetrics.create();
    JobImpl job =
        new JobImpl(jobId, ApplicationAttemptId.newInstance(
          ApplicationId.newInstance(0, 0), 0), conf, mock(EventHandler.class),
          null, new JobTokenSecretManager(), new Credentials(), null, null,
          mrAppMetrics, null, true, null, 0, null, null, null, null);
    InitTransition initTransition = new InitTransition() {
        @Override
        protected TaskSplitMetaInfo[] createSplits(JobImpl job, JobId jobId) {
          throw new YarnRuntimeException(EXCEPTIONMSG);
        }
      };
    JobEvent mockJobEvent = mock(JobEvent.class);

    JobStateInternal jobSI = initTransition.transition(job, mockJobEvent);
    Assert.assertTrue("When init fails, return value from InitTransition.transition should equal NEW.",
                      jobSI.equals(JobStateInternal.NEW));
    Assert.assertTrue("Job diagnostics should contain YarnRuntimeException",
                      job.getDiagnostics().toString().contains("YarnRuntimeException"));
    Assert.assertTrue("Job diagnostics should contain " + EXCEPTIONMSG,
                      job.getDiagnostics().toString().contains(EXCEPTIONMSG));
  }

  private static CommitterEventHandler createCommitterEventHandler(
      Dispatcher dispatcher, OutputCommitter committer) {
    final SystemClock clock = new SystemClock();
    AppContext appContext = mock(AppContext.class);
    when(appContext.getEventHandler()).thenReturn(
        dispatcher.getEventHandler());
    when(appContext.getClock()).thenReturn(clock);
    RMHeartbeatHandler heartbeatHandler = new RMHeartbeatHandler() {
      @Override
      public long getLastHeartbeatTime() {
        return clock.getTime();
      }
      @Override
      public void runOnNextHeartbeat(Runnable callback) {
        callback.run();
      }
    };
    ApplicationAttemptId id = 
      ConverterUtils.toApplicationAttemptId("appattempt_1234567890000_0001_0");
    when(appContext.getApplicationID()).thenReturn(id.getApplicationId());
    when(appContext.getApplicationAttemptId()).thenReturn(id);
    CommitterEventHandler handler =
        new CommitterEventHandler(appContext, committer, heartbeatHandler);
    dispatcher.register(CommitterEventType.class, handler);
    return handler;
  }

  private static StubbedJob createStubbedJob(Configuration conf,
      Dispatcher dispatcher, int numSplits, AppContext appContext) {
    JobID jobID = JobID.forName("job_1234567890000_0001");
    JobId jobId = TypeConverter.toYarn(jobID);
    if (appContext == null) {
      appContext = mock(AppContext.class);
      when(appContext.hasSuccessfullyUnregistered()).thenReturn(true);
    }
    StubbedJob job = new StubbedJob(jobId,
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 0), 0),
        conf,dispatcher.getEventHandler(), true, "somebody", numSplits, appContext);
    dispatcher.register(JobEventType.class, job);
    EventHandler mockHandler = mock(EventHandler.class);
    dispatcher.register(TaskEventType.class, mockHandler);
    dispatcher.register(org.apache.hadoop.mapreduce.jobhistory.EventType.class,
        mockHandler);
    dispatcher.register(JobFinishEvent.Type.class, mockHandler);
    return job;
  }

  private static StubbedJob createRunningStubbedJob(Configuration conf,
      Dispatcher dispatcher, int numSplits, AppContext appContext) {
    StubbedJob job = createStubbedJob(conf, dispatcher, numSplits, appContext);
    job.handle(new JobEvent(job.getID(), JobEventType.JOB_INIT));
    assertJobState(job, JobStateInternal.INITED);
    job.handle(new JobStartEvent(job.getID()));
    assertJobState(job, JobStateInternal.RUNNING);
    return job;
  }

  private static void completeJobTasks(JobImpl job) {
    // complete the map tasks and the reduce tasks so we start committing
    int numMaps = job.getTotalMaps();
    for (int i = 0; i < numMaps; ++i) {
      job.handle(new JobTaskEvent(
          MRBuilderUtils.newTaskId(job.getID(), 1, TaskType.MAP),
          TaskState.SUCCEEDED));
      Assert.assertEquals(JobState.RUNNING, job.getState());
    }
    int numReduces = job.getTotalReduces();
    for (int i = 0; i < numReduces; ++i) {
      job.handle(new JobTaskEvent(
          MRBuilderUtils.newTaskId(job.getID(), 1, TaskType.MAP),
          TaskState.SUCCEEDED));
      Assert.assertEquals(JobState.RUNNING, job.getState());
    }
  }

  private static void assertJobState(JobImpl job, JobStateInternal state) {
    int timeToWaitMsec = 5 * 1000;
    while (timeToWaitMsec > 0 && job.getInternalState() != state) {
      try {
        Thread.sleep(10);
        timeToWaitMsec -= 10;
      } catch (InterruptedException e) {
        break;
      }
    }
    Assert.assertEquals(state, job.getInternalState());
  }

  private static class JobSubmittedEventHandler implements
      EventHandler<JobHistoryEvent> {

    private String workflowId;
    
    private String workflowName;
    
    private String workflowNodeName;
    
    private String workflowAdjacencies;
    
    private String workflowTags;
    
    private Boolean assertBoolean;

    public JobSubmittedEventHandler(String workflowId, String workflowName,
        String workflowNodeName, String workflowAdjacencies,
        String workflowTags) {
      this.workflowId = workflowId;
      this.workflowName = workflowName;
      this.workflowNodeName = workflowNodeName;
      this.workflowAdjacencies = workflowAdjacencies;
      this.workflowTags = workflowTags;
      assertBoolean = null;
    }

    @Override
    public void handle(JobHistoryEvent jhEvent) {
      if (jhEvent.getType() != EventType.JOB_SUBMITTED) {
        return;
      }
      JobSubmittedEvent jsEvent = (JobSubmittedEvent) jhEvent.getHistoryEvent();
      if (!workflowId.equals(jsEvent.getWorkflowId())) {
        setAssertValue(false);
        return;
      }
      if (!workflowName.equals(jsEvent.getWorkflowName())) {
        setAssertValue(false);
        return;
      }
      if (!workflowNodeName.equals(jsEvent.getWorkflowNodeName())) {
        setAssertValue(false);
        return;
      }
     
      String[] wrkflowAdj = workflowAdjacencies.split(" ");
      String[] jswrkflowAdj = jsEvent.getWorkflowAdjacencies().split(" ");
      Arrays.sort(wrkflowAdj);
      Arrays.sort(jswrkflowAdj);
      if (!Arrays.equals(wrkflowAdj, jswrkflowAdj)) {
        setAssertValue(false);
        return;
      }
      if (!workflowTags.equals(jsEvent.getWorkflowTags())) {
        setAssertValue(false);
        return;
      }
      setAssertValue(true);
    }
    
    private synchronized void setAssertValue(Boolean bool) {
      assertBoolean = bool;
      notify();
    }
    
    public synchronized boolean getAssertValue() throws InterruptedException {
      while (assertBoolean == null) {
        wait();
      }
      return assertBoolean;
    }

  }

  private static class StubbedJob extends JobImpl {
    //override the init transition
    private final InitTransition initTransition;
    StateMachineFactory<JobImpl, JobStateInternal, JobEventType, JobEvent>
        localFactory;

    private final StateMachine<JobStateInternal, JobEventType, JobEvent>
        localStateMachine;

    @Override
    protected StateMachine<JobStateInternal, JobEventType, JobEvent> getStateMachine() {
      return localStateMachine;
    }

    public StubbedJob(JobId jobId, ApplicationAttemptId applicationAttemptId,
        Configuration conf, EventHandler eventHandler, boolean newApiCommitter,
        String user, int numSplits, AppContext appContext) {
      super(jobId, applicationAttemptId, conf, eventHandler,
          null, new JobTokenSecretManager(), new Credentials(),
          new SystemClock(), Collections.<TaskId, TaskInfo> emptyMap(),
          MRAppMetrics.create(), null, newApiCommitter, user,
          System.currentTimeMillis(), null, appContext, null, null);

      initTransition = getInitTransition(numSplits);
      localFactory = stateMachineFactory.addTransition(JobStateInternal.NEW,
            EnumSet.of(JobStateInternal.INITED, JobStateInternal.FAILED),
            JobEventType.JOB_INIT,
            // This is abusive.
            initTransition);

      // This "this leak" is okay because the retained pointer is in an
      //  instance variable.
      localStateMachine = localFactory.make(this);
    }
  }

  private static class StubbedOutputCommitter extends OutputCommitter {

    public StubbedOutputCommitter() {
      super();
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext)
        throws IOException {
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {
    }
  }

  private static class TestingOutputCommitter extends StubbedOutputCommitter {
    CyclicBarrier syncBarrier;
    boolean shouldSucceed;

    public TestingOutputCommitter(CyclicBarrier syncBarrier,
        boolean shouldSucceed) {
      super();
      this.syncBarrier = syncBarrier;
      this.shouldSucceed = shouldSucceed;
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      try {
        syncBarrier.await();
      } catch (BrokenBarrierException e) {
      } catch (InterruptedException e) {
      }

      if (!shouldSucceed) {
        throw new IOException("forced failure");
      }
    }
  }

  private static class WaitingOutputCommitter extends TestingOutputCommitter {
    public WaitingOutputCommitter(CyclicBarrier syncBarrier,
        boolean shouldSucceed) {
      super(syncBarrier, shouldSucceed);
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      try {
        syncBarrier.await();
      } catch (BrokenBarrierException e) {
      } catch (InterruptedException e) {
      }

      while (!Thread.interrupted()) {
        try {
          synchronized (this) {
            wait();
          }
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }
}
