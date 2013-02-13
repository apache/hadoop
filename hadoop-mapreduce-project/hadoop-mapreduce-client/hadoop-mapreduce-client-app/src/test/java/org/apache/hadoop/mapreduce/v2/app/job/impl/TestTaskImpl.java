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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.Avataar;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.TaskStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class TestTaskImpl {

  private static final Log LOG = LogFactory.getLog(TestTaskImpl.class);    
  
  private JobConf conf;
  private TaskAttemptListener taskAttemptListener;
  private Token<JobTokenIdentifier> jobToken;
  private JobId jobId;
  private Path remoteJobConfFile;
  private Credentials credentials;
  private Clock clock;
  private Map<TaskId, TaskInfo> completedTasksFromPreviousRun;
  private MRAppMetrics metrics;
  private TaskImpl mockTask;
  private ApplicationId appId;
  private TaskSplitMetaInfo taskSplitMetaInfo;  
  private String[] dataLocations = new String[0]; 
  private AppContext appContext;
  
  private int startCount = 0;
  private int taskCounter = 0;
  private final int partition = 1;
  
  private InlineDispatcher dispatcher;   
  private List<MockTaskAttemptImpl> taskAttempts;
  
  private class MockTaskImpl extends TaskImpl {
        
    private int taskAttemptCounter = 0;
    TaskType taskType;

    public MockTaskImpl(JobId jobId, int partition,
        EventHandler eventHandler, Path remoteJobConfFile, JobConf conf,
        TaskAttemptListener taskAttemptListener,
        Token<JobTokenIdentifier> jobToken,
        Credentials credentials, Clock clock,
        Map<TaskId, TaskInfo> completedTasksFromPreviousRun, int startCount,
        MRAppMetrics metrics, AppContext appContext, TaskType taskType) {
      super(jobId, taskType , partition, eventHandler,
          remoteJobConfFile, conf, taskAttemptListener,
          jobToken, credentials, clock,
          completedTasksFromPreviousRun, startCount, metrics, appContext);
      this.taskType = taskType;
    }

    @Override
    public TaskType getType() {
      return taskType;
    }

    @Override
    protected TaskAttemptImpl createAttempt() {
      MockTaskAttemptImpl attempt = new MockTaskAttemptImpl(getID(), ++taskAttemptCounter, 
          eventHandler, taskAttemptListener, remoteJobConfFile, partition,
          conf, jobToken, credentials, clock, appContext, taskType);
      taskAttempts.add(attempt);
      return attempt;
    }

    @Override
    protected int getMaxAttempts() {
      return 100;
    }

    @Override
    protected void internalError(TaskEventType type) {
      super.internalError(type);
      fail("Internal error: " + type);
    }
  }
  
  private class MockTaskAttemptImpl extends TaskAttemptImpl {

    private float progress = 0;
    private TaskAttemptState state = TaskAttemptState.NEW;
    private TaskType taskType;
    private Counters attemptCounters = TaskAttemptImpl.EMPTY_COUNTERS;

    public MockTaskAttemptImpl(TaskId taskId, int id, EventHandler eventHandler,
        TaskAttemptListener taskAttemptListener, Path jobFile, int partition,
        JobConf conf, Token<JobTokenIdentifier> jobToken,
        Credentials credentials, Clock clock,
        AppContext appContext, TaskType taskType) {
      super(taskId, id, eventHandler, taskAttemptListener, jobFile, partition, conf,
          dataLocations, jobToken, credentials, clock, appContext);
      this.taskType = taskType;
    }

    public TaskAttemptId getAttemptId() {
      return getID();
    }
    
    @Override
    protected Task createRemoteTask() {
      return new MockTask(taskType);
    }    
    
    public float getProgress() {
      return progress ;
    }
    
    public void setProgress(float progress) {
      this.progress = progress;
    }
    
    public void setState(TaskAttemptState state) {
      this.state = state;
    }
    
    public TaskAttemptState getState() {
      return state;
    }

    @Override
    public Counters getCounters() {
      return attemptCounters;
    }

    public void setCounters(Counters counters) {
      attemptCounters = counters;
    }
  }
  
  private class MockTask extends Task {

    private TaskType taskType;
    MockTask(TaskType taskType) {
      this.taskType = taskType;
    }
    
    @Override
    public void run(JobConf job, TaskUmbilicalProtocol umbilical)
        throws IOException, ClassNotFoundException, InterruptedException {
      return;
    }

    @Override
    public boolean isMapTask() {
      return (taskType == TaskType.MAP);
    }    
    
  }
  
  @Before 
  @SuppressWarnings("unchecked")
  public void setup() {
     dispatcher = new InlineDispatcher();
    
    ++startCount;
    
    conf = new JobConf();
    taskAttemptListener = mock(TaskAttemptListener.class);
    jobToken = (Token<JobTokenIdentifier>) mock(Token.class);
    remoteJobConfFile = mock(Path.class);
    credentials = null;
    clock = new SystemClock();
    metrics = mock(MRAppMetrics.class);  
    dataLocations = new String[1];
    
    appId = Records.newRecord(ApplicationId.class);
    appId.setClusterTimestamp(System.currentTimeMillis());
    appId.setId(1);

    jobId = Records.newRecord(JobId.class);
    jobId.setId(1);
    jobId.setAppId(appId);
    appContext = mock(AppContext.class);

    taskSplitMetaInfo = mock(TaskSplitMetaInfo.class);
    when(taskSplitMetaInfo.getLocations()).thenReturn(dataLocations); 
    
    taskAttempts = new ArrayList<MockTaskAttemptImpl>();    
  }
  
  private MockTaskImpl createMockTask(TaskType taskType) {
    return new MockTaskImpl(jobId, partition, dispatcher.getEventHandler(),
        remoteJobConfFile, conf, taskAttemptListener, jobToken,
        credentials, clock,
        completedTasksFromPreviousRun, startCount,
        metrics, appContext, taskType);
  }

  @After 
  public void teardown() {
    taskAttempts.clear();
  }
  
  private TaskId getNewTaskID() {
    TaskId taskId = Records.newRecord(TaskId.class);
    taskId.setId(++taskCounter);
    taskId.setJobId(jobId);
    taskId.setTaskType(mockTask.getType());    
    return taskId;
  }
  
  private void scheduleTaskAttempt(TaskId taskId) {
    mockTask.handle(new TaskEvent(taskId, 
        TaskEventType.T_SCHEDULE));
    assertTaskScheduledState();
    assertTaskAttemptAvataar(Avataar.VIRGIN);
  }
  
  private void killTask(TaskId taskId) {
    mockTask.handle(new TaskEvent(taskId, 
        TaskEventType.T_KILL));
    assertTaskKillWaitState();
  }
  
  private void killScheduledTaskAttempt(TaskAttemptId attemptId) {
    mockTask.handle(new TaskTAttemptEvent(attemptId, 
        TaskEventType.T_ATTEMPT_KILLED));
    assertTaskScheduledState();
  }

  private void launchTaskAttempt(TaskAttemptId attemptId) {
    mockTask.handle(new TaskTAttemptEvent(attemptId, 
        TaskEventType.T_ATTEMPT_LAUNCHED));
    assertTaskRunningState();    
  }
  
  private void commitTaskAttempt(TaskAttemptId attemptId) {
    mockTask.handle(new TaskTAttemptEvent(attemptId, 
        TaskEventType.T_ATTEMPT_COMMIT_PENDING));
    assertTaskRunningState();    
  }
  
  private MockTaskAttemptImpl getLastAttempt() {
    return taskAttempts.get(taskAttempts.size()-1);
  }
  
  private void updateLastAttemptProgress(float p) {    
    getLastAttempt().setProgress(p);
  }

  private void updateLastAttemptState(TaskAttemptState s) {
    getLastAttempt().setState(s);
  }
  
  private void killRunningTaskAttempt(TaskAttemptId attemptId) {
    mockTask.handle(new TaskTAttemptEvent(attemptId, 
        TaskEventType.T_ATTEMPT_KILLED));
    assertTaskRunningState();  
  }
  
  private void failRunningTaskAttempt(TaskAttemptId attemptId) {
    mockTask.handle(new TaskTAttemptEvent(attemptId, 
        TaskEventType.T_ATTEMPT_FAILED));
    assertTaskRunningState();
  }
  
  /**
   * {@link TaskState#NEW}
   */
  private void assertTaskNewState() {
    assertEquals(TaskState.NEW, mockTask.getState());
  }
  
  /**
   * {@link TaskState#SCHEDULED}
   */
  private void assertTaskScheduledState() {
    assertEquals(TaskState.SCHEDULED, mockTask.getState());
  }

  /**
   * {@link TaskState#RUNNING}
   */
  private void assertTaskRunningState() {
    assertEquals(TaskState.RUNNING, mockTask.getState());
  }
    
  /**
   * {@link TaskState#KILL_WAIT}
   */
  private void assertTaskKillWaitState() {
    assertEquals(TaskStateInternal.KILL_WAIT, mockTask.getInternalState());
  }
  
  /**
   * {@link TaskState#SUCCEEDED}
   */
  private void assertTaskSucceededState() {
    assertEquals(TaskState.SUCCEEDED, mockTask.getState());
  }

  /**
   * {@link Avataar}
   */
  private void assertTaskAttemptAvataar(Avataar avataar) {
    for (TaskAttempt taskAttempt : mockTask.getAttempts().values()) {
      if (((TaskAttemptImpl) taskAttempt).getAvataar() == avataar) {
        return;
      }
    }
    fail("There is no " + (avataar == Avataar.VIRGIN ? "virgin" : "speculative")
        + "task attempt");
  }
  
  @Test
  public void testInit() {
    LOG.info("--- START: testInit ---");
    mockTask = createMockTask(TaskType.MAP);        
    assertTaskNewState();
    assert(taskAttempts.size() == 0);
  }

  @Test
  /**
   * {@link TaskState#NEW}->{@link TaskState#SCHEDULED}
   */
  public void testScheduleTask() {
    LOG.info("--- START: testScheduleTask ---");
    mockTask = createMockTask(TaskType.MAP);        
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
  }
  
  @Test 
  /**
   * {@link TaskState#SCHEDULED}->{@link TaskState#KILL_WAIT}
   */
  public void testKillScheduledTask() {
    LOG.info("--- START: testKillScheduledTask ---");
    mockTask = createMockTask(TaskType.MAP);        
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    killTask(taskId);
  }
  
  @Test 
  /**
   * Kill attempt
   * {@link TaskState#SCHEDULED}->{@link TaskState#SCHEDULED}
   */
  public void testKillScheduledTaskAttempt() {
    LOG.info("--- START: testKillScheduledTaskAttempt ---");
    mockTask = createMockTask(TaskType.MAP);        
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    killScheduledTaskAttempt(getLastAttempt().getAttemptId());
  }
  
  @Test 
  /**
   * Launch attempt
   * {@link TaskState#SCHEDULED}->{@link TaskState#RUNNING}
   */
  public void testLaunchTaskAttempt() {
    LOG.info("--- START: testLaunchTaskAttempt ---");
    mockTask = createMockTask(TaskType.MAP);        
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(getLastAttempt().getAttemptId());
  }

  @Test
  /**
   * Kill running attempt
   * {@link TaskState#RUNNING}->{@link TaskState#RUNNING} 
   */
  public void testKillRunningTaskAttempt() {
    LOG.info("--- START: testKillRunningTaskAttempt ---");
    mockTask = createMockTask(TaskType.MAP);        
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(getLastAttempt().getAttemptId());
    killRunningTaskAttempt(getLastAttempt().getAttemptId());    
  }

  @Test
  public void testKillSuccessfulTask() {
    LOG.info("--- START: testKillSuccesfulTask ---");
    mockTask = createMockTask(TaskType.MAP);
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(getLastAttempt().getAttemptId());
    commitTaskAttempt(getLastAttempt().getAttemptId());
    mockTask.handle(new TaskTAttemptEvent(getLastAttempt().getAttemptId(),
        TaskEventType.T_ATTEMPT_SUCCEEDED));
    assertTaskSucceededState();
    mockTask.handle(new TaskEvent(taskId, TaskEventType.T_KILL));
    assertTaskSucceededState();
  }

  @Test 
  public void testTaskProgress() {
    LOG.info("--- START: testTaskProgress ---");
    mockTask = createMockTask(TaskType.MAP);        
        
    // launch task
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    float progress = 0f;
    assert(mockTask.getProgress() == progress);
    launchTaskAttempt(getLastAttempt().getAttemptId());    
    
    // update attempt1 
    progress = 50f;
    updateLastAttemptProgress(progress);
    assert(mockTask.getProgress() == progress);
    progress = 100f;
    updateLastAttemptProgress(progress);
    assert(mockTask.getProgress() == progress);
    
    progress = 0f;
    // mark first attempt as killed
    updateLastAttemptState(TaskAttemptState.KILLED);
    assert(mockTask.getProgress() == progress);

    // kill first attempt 
    // should trigger a new attempt
    // as no successful attempts 
    killRunningTaskAttempt(getLastAttempt().getAttemptId());
    assert(taskAttempts.size() == 2);
    
    assert(mockTask.getProgress() == 0f);
    launchTaskAttempt(getLastAttempt().getAttemptId());
    progress = 50f;
    updateLastAttemptProgress(progress);
    assert(mockTask.getProgress() == progress);
        
  }
  
  @Test
  public void testFailureDuringTaskAttemptCommit() {
    mockTask = createMockTask(TaskType.MAP);        
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(getLastAttempt().getAttemptId());
    updateLastAttemptState(TaskAttemptState.COMMIT_PENDING);
    commitTaskAttempt(getLastAttempt().getAttemptId());

    // During the task attempt commit there is an exception which causes
    // the attempt to fail
    updateLastAttemptState(TaskAttemptState.FAILED);
    failRunningTaskAttempt(getLastAttempt().getAttemptId());

    assertEquals(2, taskAttempts.size());
    updateLastAttemptState(TaskAttemptState.SUCCEEDED);
    commitTaskAttempt(getLastAttempt().getAttemptId());
    mockTask.handle(new TaskTAttemptEvent(getLastAttempt().getAttemptId(), 
        TaskEventType.T_ATTEMPT_SUCCEEDED));
    
    assertFalse("First attempt should not commit",
        mockTask.canCommit(taskAttempts.get(0).getAttemptId()));
    assertTrue("Second attempt should commit",
        mockTask.canCommit(getLastAttempt().getAttemptId()));

    assertTaskSucceededState();
  }
  
  private void runSpeculativeTaskAttemptSucceeds(
      TaskEventType firstAttemptFinishEvent) {
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(getLastAttempt().getAttemptId());
    updateLastAttemptState(TaskAttemptState.RUNNING);

    // Add a speculative task attempt that succeeds
    mockTask.handle(new TaskTAttemptEvent(getLastAttempt().getAttemptId(), 
        TaskEventType.T_ADD_SPEC_ATTEMPT));
    launchTaskAttempt(getLastAttempt().getAttemptId());
    commitTaskAttempt(getLastAttempt().getAttemptId());
    mockTask.handle(new TaskTAttemptEvent(getLastAttempt().getAttemptId(), 
        TaskEventType.T_ATTEMPT_SUCCEEDED));
    
    // The task should now have succeeded
    assertTaskSucceededState();
    
    // Now complete the first task attempt, after the second has succeeded
    mockTask.handle(new TaskTAttemptEvent(taskAttempts.get(0).getAttemptId(), 
        firstAttemptFinishEvent));
    
    // The task should still be in the succeeded state
    assertTaskSucceededState();
    
    // The task should contain speculative a task attempt
    assertTaskAttemptAvataar(Avataar.SPECULATIVE);
  }
  
  @Test
  public void testMapSpeculativeTaskAttemptSucceedsEvenIfFirstFails() {
    mockTask = createMockTask(TaskType.MAP);        
    runSpeculativeTaskAttemptSucceeds(TaskEventType.T_ATTEMPT_FAILED);
  }

  @Test
  public void testReduceSpeculativeTaskAttemptSucceedsEvenIfFirstFails() {
    mockTask = createMockTask(TaskType.REDUCE);        
    runSpeculativeTaskAttemptSucceeds(TaskEventType.T_ATTEMPT_FAILED);
  }
  
  @Test
  public void testMapSpeculativeTaskAttemptSucceedsEvenIfFirstIsKilled() {
    mockTask = createMockTask(TaskType.MAP);        
    runSpeculativeTaskAttemptSucceeds(TaskEventType.T_ATTEMPT_KILLED);
  }

  @Test
  public void testReduceSpeculativeTaskAttemptSucceedsEvenIfFirstIsKilled() {
    mockTask = createMockTask(TaskType.REDUCE);        
    runSpeculativeTaskAttemptSucceeds(TaskEventType.T_ATTEMPT_KILLED);
  }

  @Test
  public void testMultipleTaskAttemptsSucceed() {
    mockTask = createMockTask(TaskType.MAP);
    runSpeculativeTaskAttemptSucceeds(TaskEventType.T_ATTEMPT_SUCCEEDED);
  }

  @Test
  public void testCommitAfterSucceeds() {
    mockTask = createMockTask(TaskType.REDUCE);
    runSpeculativeTaskAttemptSucceeds(TaskEventType.T_ATTEMPT_COMMIT_PENDING);
  }

  @Test
  public void testSpeculativeMapFetchFailure() {
    // Setup a scenario where speculative task wins, first attempt killed
    mockTask = createMockTask(TaskType.MAP);
    runSpeculativeTaskAttemptSucceeds(TaskEventType.T_ATTEMPT_KILLED);
    assertEquals(2, taskAttempts.size());

    // speculative attempt retroactively fails from fetch failures
    mockTask.handle(new TaskTAttemptEvent(taskAttempts.get(1).getAttemptId(),
        TaskEventType.T_ATTEMPT_FAILED));

    assertTaskScheduledState();
    assertEquals(3, taskAttempts.size());
  }

  @Test
  public void testSpeculativeMapMultipleSucceedFetchFailure() {
    // Setup a scenario where speculative task wins, first attempt succeeds
    mockTask = createMockTask(TaskType.MAP);
    runSpeculativeTaskAttemptSucceeds(TaskEventType.T_ATTEMPT_SUCCEEDED);
    assertEquals(2, taskAttempts.size());

    // speculative attempt retroactively fails from fetch failures
    mockTask.handle(new TaskTAttemptEvent(taskAttempts.get(1).getAttemptId(),
        TaskEventType.T_ATTEMPT_FAILED));

    assertTaskScheduledState();
    assertEquals(3, taskAttempts.size());
  }

  @Test
  public void testSpeculativeMapFailedFetchFailure() {
    // Setup a scenario where speculative task wins, first attempt succeeds
    mockTask = createMockTask(TaskType.MAP);
    runSpeculativeTaskAttemptSucceeds(TaskEventType.T_ATTEMPT_FAILED);
    assertEquals(2, taskAttempts.size());

    // speculative attempt retroactively fails from fetch failures
    mockTask.handle(new TaskTAttemptEvent(taskAttempts.get(1).getAttemptId(),
        TaskEventType.T_ATTEMPT_FAILED));

    assertTaskScheduledState();
    assertEquals(3, taskAttempts.size());
  }

  @Test
  public void testFailedTransitions() {
    mockTask = new MockTaskImpl(jobId, partition, dispatcher.getEventHandler(),
        remoteJobConfFile, conf, taskAttemptListener, jobToken,
        credentials, clock,
        completedTasksFromPreviousRun, startCount,
        metrics, appContext, TaskType.MAP) {
          @Override
          protected int getMaxAttempts() {
            return 1;
          }
    };
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(getLastAttempt().getAttemptId());

    // add three more speculative attempts
    mockTask.handle(new TaskTAttemptEvent(getLastAttempt().getAttemptId(),
        TaskEventType.T_ADD_SPEC_ATTEMPT));
    launchTaskAttempt(getLastAttempt().getAttemptId());
    mockTask.handle(new TaskTAttemptEvent(getLastAttempt().getAttemptId(),
        TaskEventType.T_ADD_SPEC_ATTEMPT));
    launchTaskAttempt(getLastAttempt().getAttemptId());
    mockTask.handle(new TaskTAttemptEvent(getLastAttempt().getAttemptId(),
        TaskEventType.T_ADD_SPEC_ATTEMPT));
    launchTaskAttempt(getLastAttempt().getAttemptId());
    assertEquals(4, taskAttempts.size());

    // have the first attempt fail, verify task failed due to no retries
    MockTaskAttemptImpl taskAttempt = taskAttempts.get(0);
    taskAttempt.setState(TaskAttemptState.FAILED);
    mockTask.handle(new TaskTAttemptEvent(taskAttempt.getAttemptId(),
        TaskEventType.T_ATTEMPT_FAILED));
    assertEquals(TaskState.FAILED, mockTask.getState());

    // verify task can no longer be killed
    mockTask.handle(new TaskEvent(taskId, TaskEventType.T_KILL));
    assertEquals(TaskState.FAILED, mockTask.getState());

    // verify speculative doesn't launch new tasks
    mockTask.handle(new TaskTAttemptEvent(getLastAttempt().getAttemptId(),
        TaskEventType.T_ADD_SPEC_ATTEMPT));
    mockTask.handle(new TaskTAttemptEvent(getLastAttempt().getAttemptId(),
        TaskEventType.T_ATTEMPT_LAUNCHED));
    assertEquals(TaskState.FAILED, mockTask.getState());
    assertEquals(4, taskAttempts.size());

    // verify attempt events from active tasks don't knock task out of FAILED
    taskAttempt = taskAttempts.get(1);
    taskAttempt.setState(TaskAttemptState.COMMIT_PENDING);
    mockTask.handle(new TaskTAttemptEvent(taskAttempt.getAttemptId(),
        TaskEventType.T_ATTEMPT_COMMIT_PENDING));
    assertEquals(TaskState.FAILED, mockTask.getState());
    taskAttempt.setState(TaskAttemptState.FAILED);
    mockTask.handle(new TaskTAttemptEvent(taskAttempt.getAttemptId(),
        TaskEventType.T_ATTEMPT_FAILED));
    assertEquals(TaskState.FAILED, mockTask.getState());
    taskAttempt = taskAttempts.get(2);
    taskAttempt.setState(TaskAttemptState.SUCCEEDED);
    mockTask.handle(new TaskTAttemptEvent(taskAttempt.getAttemptId(),
        TaskEventType.T_ATTEMPT_SUCCEEDED));
    assertEquals(TaskState.FAILED, mockTask.getState());
    taskAttempt = taskAttempts.get(3);
    taskAttempt.setState(TaskAttemptState.KILLED);
    mockTask.handle(new TaskTAttemptEvent(taskAttempt.getAttemptId(),
        TaskEventType.T_ATTEMPT_KILLED));
    assertEquals(TaskState.FAILED, mockTask.getState());
  }

  @Test
  public void testCountersWithSpeculation() {
    mockTask = new MockTaskImpl(jobId, partition, dispatcher.getEventHandler(),
        remoteJobConfFile, conf, taskAttemptListener, jobToken,
        credentials, clock,
        completedTasksFromPreviousRun, startCount,
        metrics, appContext, TaskType.MAP) {
          @Override
          protected int getMaxAttempts() {
            return 1;
          }
    };
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(getLastAttempt().getAttemptId());
    updateLastAttemptState(TaskAttemptState.RUNNING);
    MockTaskAttemptImpl baseAttempt = getLastAttempt();

    // add a speculative attempt
    mockTask.handle(new TaskTAttemptEvent(getLastAttempt().getAttemptId(),
        TaskEventType.T_ADD_SPEC_ATTEMPT));
    launchTaskAttempt(getLastAttempt().getAttemptId());
    updateLastAttemptState(TaskAttemptState.RUNNING);
    MockTaskAttemptImpl specAttempt = getLastAttempt();
    assertEquals(2, taskAttempts.size());

    Counters specAttemptCounters = new Counters();
    Counter cpuCounter = specAttemptCounters.findCounter(
        TaskCounter.CPU_MILLISECONDS);
    cpuCounter.setValue(1000);
    specAttempt.setCounters(specAttemptCounters);

    // have the spec attempt succeed but second attempt at 1.0 progress as well
    commitTaskAttempt(specAttempt.getAttemptId());
    specAttempt.setProgress(1.0f);
    specAttempt.setState(TaskAttemptState.SUCCEEDED);
    mockTask.handle(new TaskTAttemptEvent(specAttempt.getAttemptId(),
        TaskEventType.T_ATTEMPT_SUCCEEDED));
    assertEquals(TaskState.SUCCEEDED, mockTask.getState());
    baseAttempt.setProgress(1.0f);

    Counters taskCounters = mockTask.getCounters();
    assertEquals("wrong counters for task", specAttemptCounters, taskCounters);
  }
}
