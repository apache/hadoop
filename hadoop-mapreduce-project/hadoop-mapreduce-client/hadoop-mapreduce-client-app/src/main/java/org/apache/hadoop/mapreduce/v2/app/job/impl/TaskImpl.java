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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.jobhistory.TaskFailedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskStartedEvent;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.Avataar;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.TaskStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobMapTaskRescheduledEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerFailedEvent;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implementation of Task interface.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class TaskImpl implements Task, EventHandler<TaskEvent> {

  private static final Log LOG = LogFactory.getLog(TaskImpl.class);

  protected final JobConf conf;
  protected final Path jobFile;
  protected final int partition;
  protected final TaskAttemptListener taskAttemptListener;
  protected final EventHandler eventHandler;
  private final TaskId taskId;
  private Map<TaskAttemptId, TaskAttempt> attempts;
  private final int maxAttempts;
  protected final Clock clock;
  private final Lock readLock;
  private final Lock writeLock;
  private final MRAppMetrics metrics;
  protected final AppContext appContext;
  private long scheduledTime;
  
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  protected boolean encryptedShuffle;
  protected Credentials credentials;
  protected Token<JobTokenIdentifier> jobToken;
  
  //should be set to one which comes first
  //saying COMMIT_PENDING
  private TaskAttemptId commitAttempt;

  private TaskAttemptId successfulAttempt;

  private final Set<TaskAttemptId> failedAttempts;
  // Track the finished attempts - successful, failed and killed
  private final Set<TaskAttemptId> finishedAttempts;
  // counts the number of attempts that are either running or in a state where
  //  they will come to be running when they get a Container
  private final Set<TaskAttemptId> inProgressAttempts;

  private boolean historyTaskStartGenerated = false;
  
  private static final SingleArcTransition<TaskImpl, TaskEvent> 
     ATTEMPT_KILLED_TRANSITION = new AttemptKilledTransition();
  private static final SingleArcTransition<TaskImpl, TaskEvent> 
     KILL_TRANSITION = new KillTransition();

  private static final StateMachineFactory
               <TaskImpl, TaskStateInternal, TaskEventType, TaskEvent> 
            stateMachineFactory 
           = new StateMachineFactory<TaskImpl, TaskStateInternal, TaskEventType, TaskEvent>
               (TaskStateInternal.NEW)

    // define the state machine of Task

    // Transitions from NEW state
    .addTransition(TaskStateInternal.NEW, TaskStateInternal.SCHEDULED, 
        TaskEventType.T_SCHEDULE, new InitialScheduleTransition())
    .addTransition(TaskStateInternal.NEW, TaskStateInternal.KILLED, 
        TaskEventType.T_KILL, new KillNewTransition())

    // Transitions from SCHEDULED state
      //when the first attempt is launched, the task state is set to RUNNING
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.RUNNING, 
         TaskEventType.T_ATTEMPT_LAUNCHED, new LaunchTransition())
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.KILL_WAIT, 
         TaskEventType.T_KILL, KILL_TRANSITION)
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.SCHEDULED, 
         TaskEventType.T_ATTEMPT_KILLED, ATTEMPT_KILLED_TRANSITION)
     .addTransition(TaskStateInternal.SCHEDULED, 
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.FAILED), 
        TaskEventType.T_ATTEMPT_FAILED, 
        new AttemptFailedTransition())
 
    // Transitions from RUNNING state
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING, 
        TaskEventType.T_ATTEMPT_LAUNCHED) //more attempts may start later
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING, 
        TaskEventType.T_ATTEMPT_COMMIT_PENDING,
        new AttemptCommitPendingTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING,
        TaskEventType.T_ADD_SPEC_ATTEMPT, new RedundantScheduleTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.SUCCEEDED, 
        TaskEventType.T_ATTEMPT_SUCCEEDED,
        new AttemptSucceededTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING, 
        TaskEventType.T_ATTEMPT_KILLED,
        ATTEMPT_KILLED_TRANSITION)
    .addTransition(TaskStateInternal.RUNNING, 
        EnumSet.of(TaskStateInternal.RUNNING, TaskStateInternal.FAILED), 
        TaskEventType.T_ATTEMPT_FAILED,
        new AttemptFailedTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.KILL_WAIT, 
        TaskEventType.T_KILL, KILL_TRANSITION)

    // Transitions from KILL_WAIT state
    .addTransition(TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskStateInternal.KILL_WAIT, TaskStateInternal.KILLED),
        TaskEventType.T_ATTEMPT_KILLED,
        new KillWaitAttemptKilledTransition())
    .addTransition(TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskStateInternal.KILL_WAIT, TaskStateInternal.KILLED),
        TaskEventType.T_ATTEMPT_SUCCEEDED,
        new KillWaitAttemptSucceededTransition())
    .addTransition(TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskStateInternal.KILL_WAIT, TaskStateInternal.KILLED),
        TaskEventType.T_ATTEMPT_FAILED,
        new KillWaitAttemptFailedTransition())
    // Ignore-able transitions.
    .addTransition(
        TaskStateInternal.KILL_WAIT,
        TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskEventType.T_KILL,
            TaskEventType.T_ATTEMPT_LAUNCHED,
            TaskEventType.T_ATTEMPT_COMMIT_PENDING,
            TaskEventType.T_ADD_SPEC_ATTEMPT))

    // Transitions from SUCCEEDED state
    .addTransition(TaskStateInternal.SUCCEEDED,
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.SUCCEEDED, TaskStateInternal.FAILED),
        TaskEventType.T_ATTEMPT_FAILED, new RetroactiveFailureTransition())
    .addTransition(TaskStateInternal.SUCCEEDED,
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.SUCCEEDED),
        TaskEventType.T_ATTEMPT_KILLED, new RetroactiveKilledTransition())
    .addTransition(TaskStateInternal.SUCCEEDED, TaskStateInternal.SUCCEEDED,
        TaskEventType.T_ATTEMPT_SUCCEEDED,
        new AttemptSucceededAtSucceededTransition())
    // Ignore-able transitions.
    .addTransition(
        TaskStateInternal.SUCCEEDED, TaskStateInternal.SUCCEEDED,
        EnumSet.of(TaskEventType.T_ADD_SPEC_ATTEMPT,
            TaskEventType.T_ATTEMPT_COMMIT_PENDING,
            TaskEventType.T_ATTEMPT_LAUNCHED,
            TaskEventType.T_KILL))

    // Transitions from FAILED state        
    .addTransition(TaskStateInternal.FAILED, TaskStateInternal.FAILED,
        EnumSet.of(TaskEventType.T_KILL,
                   TaskEventType.T_ADD_SPEC_ATTEMPT,
                   TaskEventType.T_ATTEMPT_COMMIT_PENDING,
                   TaskEventType.T_ATTEMPT_FAILED,
                   TaskEventType.T_ATTEMPT_KILLED,
                   TaskEventType.T_ATTEMPT_LAUNCHED,
                   TaskEventType.T_ATTEMPT_SUCCEEDED))

    // Transitions from KILLED state
    .addTransition(TaskStateInternal.KILLED, TaskStateInternal.KILLED,
        EnumSet.of(TaskEventType.T_KILL,
                   TaskEventType.T_ADD_SPEC_ATTEMPT))

    // create the topology tables
    .installTopology();

  private final StateMachine<TaskStateInternal, TaskEventType, TaskEvent>
    stateMachine;

  // By default, the next TaskAttempt number is zero. Changes during recovery  
  protected int nextAttemptNumber = 0;
  private List<TaskAttemptInfo> taskAttemptsFromPreviousGeneration =
      new ArrayList<TaskAttemptInfo>();

  private static final class RecoverdAttemptsComparator implements
      Comparator<TaskAttemptInfo> {
    @Override
    public int compare(TaskAttemptInfo attempt1, TaskAttemptInfo attempt2) {
      long diff = attempt1.getStartTime() - attempt2.getStartTime();
      return diff == 0 ? 0 : (diff < 0 ? -1 : 1);
    }
  }

  private static final RecoverdAttemptsComparator RECOVERED_ATTEMPTS_COMPARATOR =
      new RecoverdAttemptsComparator();

  @Override
  public TaskState getState() {
    readLock.lock();
    try {
      return getExternalState(getInternalState());
    } finally {
      readLock.unlock();
    }
  }

  public TaskImpl(JobId jobId, TaskType taskType, int partition,
      EventHandler eventHandler, Path remoteJobConfFile, JobConf conf,
      TaskAttemptListener taskAttemptListener,
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      Map<TaskId, TaskInfo> completedTasksFromPreviousRun, int startCount,
      MRAppMetrics metrics, AppContext appContext) {
    this.conf = conf;
    this.clock = clock;
    this.jobFile = remoteJobConfFile;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    this.attempts = Collections.emptyMap();
    this.finishedAttempts = new HashSet<TaskAttemptId>(2);
    this.failedAttempts = new HashSet<TaskAttemptId>(2);
    this.inProgressAttempts = new HashSet<TaskAttemptId>(2);
    // This overridable method call is okay in a constructor because we
    //  have a convention that none of the overrides depends on any
    //  fields that need initialization.
    maxAttempts = getMaxAttempts();
    taskId = MRBuilderUtils.newTaskId(jobId, partition, taskType);
    this.partition = partition;
    this.taskAttemptListener = taskAttemptListener;
    this.eventHandler = eventHandler;
    this.credentials = credentials;
    this.jobToken = jobToken;
    this.metrics = metrics;
    this.appContext = appContext;
    this.encryptedShuffle = conf.getBoolean(MRConfig.SHUFFLE_SSL_ENABLED_KEY,
                                            MRConfig.SHUFFLE_SSL_ENABLED_DEFAULT);

    // See if this is from a previous generation.
    if (completedTasksFromPreviousRun != null
        && completedTasksFromPreviousRun.containsKey(taskId)) {
      // This task has TaskAttempts from previous generation. We have to replay
      // them.
      LOG.info("Task is from previous run " + taskId);
      TaskInfo taskInfo = completedTasksFromPreviousRun.get(taskId);
      Map<TaskAttemptID, TaskAttemptInfo> allAttempts =
          taskInfo.getAllTaskAttempts();
      taskAttemptsFromPreviousGeneration = new ArrayList<TaskAttemptInfo>();
      taskAttemptsFromPreviousGeneration.addAll(allAttempts.values());
      Collections.sort(taskAttemptsFromPreviousGeneration,
        RECOVERED_ATTEMPTS_COMPARATOR);
    }

    if (taskAttemptsFromPreviousGeneration.isEmpty()) {
      // All the previous attempts are exhausted, now start with a new
      // generation.

      // All the new TaskAttemptIDs are generated based on MR
      // ApplicationAttemptID so that attempts from previous lives don't
      // over-step the current one. This assumes that a task won't have more
      // than 1000 attempts in its single generation, which is very reasonable.
      // Someone is nuts if he/she thinks he/she can live with 1000 TaskAttempts
      // and requires serious medical attention.
      nextAttemptNumber = (startCount - 1) * 1000;
    } else {
      // There are still some TaskAttempts from previous generation, use them
      nextAttemptNumber =
          taskAttemptsFromPreviousGeneration.remove(0).getAttemptId().getId();
    }

    // This "this leak" is okay because the retained pointer is in an
    //  instance variable.
    stateMachine = stateMachineFactory.make(this);
  }

  @Override
  public Map<TaskAttemptId, TaskAttempt> getAttempts() {
    readLock.lock();

    try {
      if (attempts.size() <= 1) {
        return attempts;
      }
      
      Map<TaskAttemptId, TaskAttempt> result
          = new LinkedHashMap<TaskAttemptId, TaskAttempt>();
      result.putAll(attempts);

      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskAttempt getAttempt(TaskAttemptId attemptID) {
    readLock.lock();
    try {
      return attempts.get(attemptID);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskId getID() {
    return taskId;
  }

  @Override
  public boolean isFinished() {
    readLock.lock();
    try {
     // TODO: Use stateMachine level method?
      return (getInternalState() == TaskStateInternal.SUCCEEDED ||
              getInternalState() == TaskStateInternal.FAILED ||
              getInternalState() == TaskStateInternal.KILLED);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskReport getReport() {
    TaskReport report = recordFactory.newRecordInstance(TaskReport.class);
    readLock.lock();
    try {
      report.setTaskId(taskId);
      report.setStartTime(getLaunchTime());
      report.setFinishTime(getFinishTime());
      report.setTaskState(getState());
      report.setProgress(getProgress());

      for (TaskAttempt attempt : attempts.values()) {
        if (TaskAttemptState.RUNNING.equals(attempt.getState())) {
          report.addRunningAttempt(attempt.getID());
        }
      }

      report.setSuccessfulAttempt(successfulAttempt);
      
      for (TaskAttempt att : attempts.values()) {
        String prefix = "AttemptID:" + att.getID() + " Info:";
        for (CharSequence cs : att.getDiagnostics()) {
          report.addDiagnostics(prefix + cs);
          
        }
      }

      // Add a copy of counters as the last step so that their lifetime on heap
      // is as small as possible.
      report.setCounters(TypeConverter.toYarn(getCounters()));

      return report;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Counters getCounters() {
    Counters counters = null;
    readLock.lock();
    try {
      TaskAttempt bestAttempt = selectBestAttempt();
      if (bestAttempt != null) {
        counters = bestAttempt.getCounters();
      } else {
        counters = TaskAttemptImpl.EMPTY_COUNTERS;
//        counters.groups = new HashMap<CharSequence, CounterGroup>();
      }
      return counters;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public float getProgress() {
    readLock.lock();
    try {
      TaskAttempt bestAttempt = selectBestAttempt();
      if (bestAttempt == null) {
        return 0f;
      }
      return bestAttempt.getProgress();
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  public TaskStateInternal getInternalState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  private static TaskState getExternalState(TaskStateInternal smState) {
    if (smState == TaskStateInternal.KILL_WAIT) {
      return TaskState.KILLED;
    } else {
      return TaskState.valueOf(smState.name());
    }
  }

  //this is always called in read/write lock
  private long getLaunchTime() {
    long taskLaunchTime = 0;
    boolean launchTimeSet = false;
    for (TaskAttempt at : attempts.values()) {
      // select the least launch time of all attempts
      long attemptLaunchTime = at.getLaunchTime();
      if (attemptLaunchTime != 0 && !launchTimeSet) {
        // For the first non-zero launch time
        launchTimeSet = true;
        taskLaunchTime = attemptLaunchTime;
      } else if (attemptLaunchTime != 0 && taskLaunchTime > attemptLaunchTime) {
        taskLaunchTime = attemptLaunchTime;
      }
    }
    if (!launchTimeSet) {
      return this.scheduledTime;
    }
    return taskLaunchTime;
  }

  //this is always called in read/write lock
  //TODO Verify behaviour is Task is killed (no finished attempt)
  private long getFinishTime() {
    if (!isFinished()) {
      return 0;
    }
    long finishTime = 0;
    for (TaskAttempt at : attempts.values()) {
      //select the max finish time of all attempts
      if (finishTime < at.getFinishTime()) {
        finishTime = at.getFinishTime();
      }
    }
    return finishTime;
  }

  private long getFinishTime(TaskAttemptId taId) {
    if (taId == null) {
      return clock.getTime();
    }
    long finishTime = 0;
    for (TaskAttempt at : attempts.values()) {
      //select the max finish time of all attempts
      if (at.getID().equals(taId)) {
        return at.getFinishTime();
      }
    }
    return finishTime;
  }
  
  private TaskStateInternal finished(TaskStateInternal finalState) {
    if (getInternalState() == TaskStateInternal.RUNNING) {
      metrics.endRunningTask(this);
    }
    return finalState;
  }

  //select the nextAttemptNumber with best progress
  // always called inside the Read Lock
  private TaskAttempt selectBestAttempt() {
    if (successfulAttempt != null) {
      return attempts.get(successfulAttempt);
    }

    float progress = 0f;
    TaskAttempt result = null;
    for (TaskAttempt at : attempts.values()) {
      switch (at.getState()) {
      
      // ignore all failed task attempts
      case FAILED: 
      case KILLED:
        continue;      
      }      
      if (result == null) {
        result = at; //The first time around
      }
      // calculate the best progress
      float attemptProgress = at.getProgress();
      if (attemptProgress > progress) {
        result = at;
        progress = attemptProgress;
      }
    }
    return result;
  }

  @Override
  public boolean canCommit(TaskAttemptId taskAttemptID) {
    readLock.lock();
    boolean canCommit = false;
    try {
      if (commitAttempt != null) {
        canCommit = taskAttemptID.equals(commitAttempt);
        LOG.info("Result of canCommit for " + taskAttemptID + ":" + canCommit);
      }
    } finally {
      readLock.unlock();
    }
    return canCommit;
  }

  protected abstract TaskAttemptImpl createAttempt();

  // No override of this method may require that the subclass be initialized.
  protected abstract int getMaxAttempts();

  protected TaskAttempt getSuccessfulAttempt() {
    readLock.lock();
    try {
      if (null == successfulAttempt) {
        return null;
      }
      return attempts.get(successfulAttempt);
    } finally {
      readLock.unlock();
    }
  }

  // This is always called in the Write Lock
  private void addAndScheduleAttempt(Avataar avataar) {
    TaskAttempt attempt = createAttempt();
    ((TaskAttemptImpl) attempt).setAvataar(avataar);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created attempt " + attempt.getID());
    }
    switch (attempts.size()) {
      case 0:
        attempts = Collections.singletonMap(attempt.getID(), attempt);
        break;
        
      case 1:
        Map<TaskAttemptId, TaskAttempt> newAttempts
            = new LinkedHashMap<TaskAttemptId, TaskAttempt>(maxAttempts);
        newAttempts.putAll(attempts);
        attempts = newAttempts;
        attempts.put(attempt.getID(), attempt);
        break;

      default:
        attempts.put(attempt.getID(), attempt);
        break;
    }

    // Update nextATtemptNumber
    if (taskAttemptsFromPreviousGeneration.isEmpty()) {
      ++nextAttemptNumber;
    } else {
      // There are still some TaskAttempts from previous generation, use them
      nextAttemptNumber =
          taskAttemptsFromPreviousGeneration.remove(0).getAttemptId().getId();
    }

    inProgressAttempts.add(attempt.getID());
    //schedule the nextAttemptNumber
    if (failedAttempts.size() > 0) {
      eventHandler.handle(new TaskAttemptEvent(attempt.getID(),
        TaskAttemptEventType.TA_RESCHEDULE));
    } else {
      eventHandler.handle(new TaskAttemptEvent(attempt.getID(),
          TaskAttemptEventType.TA_SCHEDULE));
    }
  }

  @Override
  public void handle(TaskEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getTaskID() + " of type "
          + event.getType());
    }
    try {
      writeLock.lock();
      TaskStateInternal oldState = getInternalState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state for "
            + this.taskId, e);
        internalError(event.getType());
      }
      if (oldState != getInternalState()) {
        LOG.info(taskId + " Task Transitioned from " + oldState + " to "
            + getInternalState());
      }

    } finally {
      writeLock.unlock();
    }
  }

  protected void internalError(TaskEventType type) {
    LOG.error("Invalid event " + type + " on Task " + this.taskId);
    eventHandler.handle(new JobDiagnosticsUpdateEvent(
        this.taskId.getJobId(), "Invalid event " + type + 
        " on Task " + this.taskId));
    eventHandler.handle(new JobEvent(this.taskId.getJobId(),
        JobEventType.INTERNAL_ERROR));
  }

  // always called inside a transition, in turn inside the Write Lock
  private void handleTaskAttemptCompletion(TaskAttemptId attemptId,
      TaskAttemptCompletionEventStatus status) {
    TaskAttempt attempt = attempts.get(attemptId);
    //raise the completion event only if the container is assigned
    // to nextAttemptNumber
    if (attempt.getNodeHttpAddress() != null) {
      TaskAttemptCompletionEvent tce = recordFactory
          .newRecordInstance(TaskAttemptCompletionEvent.class);
      tce.setEventId(-1);
      String scheme = (encryptedShuffle) ? "https://" : "http://";
      tce.setMapOutputServerAddress(StringInterner.weakIntern(scheme
         + attempt.getNodeHttpAddress().split(":")[0] + ":"
         + attempt.getShufflePort()));
      tce.setStatus(status);
      tce.setAttemptId(attempt.getID());
      int runTime = 0;
      if (attempt.getFinishTime() != 0 && attempt.getLaunchTime() !=0)
        runTime = (int)(attempt.getFinishTime() - attempt.getLaunchTime());
      tce.setAttemptRunTime(runTime);
      
      //raise the event to job so that it adds the completion event to its
      //data structures
      eventHandler.handle(new JobTaskAttemptCompletedEvent(tce));
    }
  }

  private static TaskFinishedEvent createTaskFinishedEvent(TaskImpl task, TaskStateInternal taskState) {
    TaskFinishedEvent tfe =
      new TaskFinishedEvent(TypeConverter.fromYarn(task.taskId),
        TypeConverter.fromYarn(task.successfulAttempt),
        task.getFinishTime(task.successfulAttempt),
        TypeConverter.fromYarn(task.taskId.getTaskType()),
        taskState.toString(),
        task.getCounters());
    return tfe;
  }
  
  private static TaskFailedEvent createTaskFailedEvent(TaskImpl task, List<String> diag, TaskStateInternal taskState, TaskAttemptId taId) {
    StringBuilder errorSb = new StringBuilder();
    if (diag != null) {
      for (String d : diag) {
        errorSb.append(", ").append(d);
      }
    }
    TaskFailedEvent taskFailedEvent = new TaskFailedEvent(
        TypeConverter.fromYarn(task.taskId),
     // Hack since getFinishTime needs isFinished to be true and that doesn't happen till after the transition.
        task.getFinishTime(taId),
        TypeConverter.fromYarn(task.getType()),
        errorSb.toString(),
        taskState.toString(),
        taId == null ? null : TypeConverter.fromYarn(taId));
    return taskFailedEvent;
  }
  
  private static void unSucceed(TaskImpl task) {
    task.commitAttempt = null;
    task.successfulAttempt = null;
  }

  /**
  * @return a String representation of the splits.
  *
  * Subclasses can override this method to provide their own representations
  * of splits (if any).
  *
  */
  protected String getSplitsAsString(){
	  return "";
  }

  private static class InitialScheduleTransition
    implements SingleArcTransition<TaskImpl, TaskEvent> {

    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      task.addAndScheduleAttempt(Avataar.VIRGIN);
      task.scheduledTime = task.clock.getTime();
      TaskStartedEvent tse = new TaskStartedEvent(
          TypeConverter.fromYarn(task.taskId), task.getLaunchTime(),
          TypeConverter.fromYarn(task.taskId.getTaskType()),
          task.getSplitsAsString());
      task.eventHandler
          .handle(new JobHistoryEvent(task.taskId.getJobId(), tse));
      task.historyTaskStartGenerated = true;
    }
  }

  // Used when creating a new attempt while one is already running.
  //  Currently we do this for speculation.  In the future we may do this
  //  for tasks that failed in a way that might indicate application code
  //  problems, so we can take later failures in parallel and flush the
  //  job quickly when this happens.
  private static class RedundantScheduleTransition
    implements SingleArcTransition<TaskImpl, TaskEvent> {

    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      LOG.info("Scheduling a redundant attempt for task " + task.taskId);
      task.addAndScheduleAttempt(Avataar.SPECULATIVE);
    }
  }

  private static class AttemptCommitPendingTransition 
          implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskTAttemptEvent ev = (TaskTAttemptEvent) event;
      // The nextAttemptNumber is commit pending, decide on set the commitAttempt
      TaskAttemptId attemptID = ev.getTaskAttemptID();
      if (task.commitAttempt == null) {
        // TODO: validate attemptID
        task.commitAttempt = attemptID;
        LOG.info(attemptID + " given a go for committing the task output.");
      } else {
        // Don't think this can be a pluggable decision, so simply raise an
        // event for the TaskAttempt to delete its output.
        LOG.info(task.commitAttempt
            + " already given a go for committing the task output, so killing "
            + attemptID);
        task.eventHandler.handle(new TaskAttemptEvent(
            attemptID, TaskAttemptEventType.TA_KILL));
      }
    }
  }

  private static class AttemptSucceededTransition 
      implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskTAttemptEvent taskTAttemptEvent = (TaskTAttemptEvent) event;
      TaskAttemptId taskAttemptId = taskTAttemptEvent.getTaskAttemptID();
      task.handleTaskAttemptCompletion(
          taskAttemptId, 
          TaskAttemptCompletionEventStatus.SUCCEEDED);
      task.finishedAttempts.add(taskAttemptId);
      task.inProgressAttempts.remove(taskAttemptId);
      task.successfulAttempt = taskAttemptId;
      task.eventHandler.handle(new JobTaskEvent(
          task.taskId, TaskState.SUCCEEDED));
      LOG.info("Task succeeded with attempt " + task.successfulAttempt);
      // issue kill to all other attempts
      if (task.historyTaskStartGenerated) {
        TaskFinishedEvent tfe = createTaskFinishedEvent(task,
            TaskStateInternal.SUCCEEDED);
        task.eventHandler.handle(new JobHistoryEvent(task.taskId.getJobId(),
            tfe));
      }
      for (TaskAttempt attempt : task.attempts.values()) {
        if (attempt.getID() != task.successfulAttempt &&
            // This is okay because it can only talk us out of sending a
            //  TA_KILL message to an attempt that doesn't need one for
            //  other reasons.
            !attempt.isFinished()) {
          LOG.info("Issuing kill to other attempt " + attempt.getID());
          task.eventHandler.handle(
              new TaskAttemptEvent(attempt.getID(), 
                  TaskAttemptEventType.TA_KILL));
        }
      }
      task.finished(TaskStateInternal.SUCCEEDED);
    }
  }

  private static class AttemptKilledTransition implements
      SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskAttemptId taskAttemptId =
          ((TaskTAttemptEvent) event).getTaskAttemptID();
      task.handleTaskAttemptCompletion(
          taskAttemptId, 
          TaskAttemptCompletionEventStatus.KILLED);
      task.finishedAttempts.add(taskAttemptId);
      task.inProgressAttempts.remove(taskAttemptId);
      if (task.successfulAttempt == null) {
        task.addAndScheduleAttempt(Avataar.VIRGIN);
      }
      if ((task.commitAttempt != null) && (task.commitAttempt == taskAttemptId)) {
    	task.commitAttempt = null;
      }
    }
  }


  private static class KillWaitAttemptKilledTransition implements
      MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    protected TaskStateInternal finalState = TaskStateInternal.KILLED;
    protected final TaskAttemptCompletionEventStatus taCompletionEventStatus;

    public KillWaitAttemptKilledTransition() {
      this(TaskAttemptCompletionEventStatus.KILLED);
    }

    public KillWaitAttemptKilledTransition(
        TaskAttemptCompletionEventStatus taCompletionEventStatus) {
      this.taCompletionEventStatus = taCompletionEventStatus;
    }

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      TaskAttemptId taskAttemptId =
          ((TaskTAttemptEvent) event).getTaskAttemptID();
      task.handleTaskAttemptCompletion(taskAttemptId, taCompletionEventStatus);
      task.finishedAttempts.add(taskAttemptId);
      // check whether all attempts are finished
      if (task.finishedAttempts.size() == task.attempts.size()) {
        if (task.historyTaskStartGenerated) {
        TaskFailedEvent taskFailedEvent = createTaskFailedEvent(task, null,
              finalState, null); // TODO JH verify failedAttempt null
        task.eventHandler.handle(new JobHistoryEvent(task.taskId.getJobId(),
            taskFailedEvent)); 
        } else {
          LOG.debug("Not generating HistoryFinish event since start event not" +
          		" generated for task: " + task.getID());
        }

        task.eventHandler.handle(
            new JobTaskEvent(task.taskId, getExternalState(finalState)));
        return finalState;
      }
      return task.getInternalState();
    }
  }

  private static class KillWaitAttemptSucceededTransition extends
      KillWaitAttemptKilledTransition {
    public KillWaitAttemptSucceededTransition() {
      super(TaskAttemptCompletionEventStatus.SUCCEEDED);
    }
  }

  private static class KillWaitAttemptFailedTransition extends
      KillWaitAttemptKilledTransition {
    public KillWaitAttemptFailedTransition() {
      super(TaskAttemptCompletionEventStatus.FAILED);
    }
  }

  private static class AttemptFailedTransition implements
    MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      TaskTAttemptEvent castEvent = (TaskTAttemptEvent) event;
      TaskAttemptId taskAttemptId = castEvent.getTaskAttemptID();
      task.failedAttempts.add(taskAttemptId); 
      if (taskAttemptId.equals(task.commitAttempt)) {
        task.commitAttempt = null;
      }
      TaskAttempt attempt = task.attempts.get(taskAttemptId);
      if (attempt.getAssignedContainerMgrAddress() != null) {
        //container was assigned
        task.eventHandler.handle(new ContainerFailedEvent(attempt.getID(), 
            attempt.getAssignedContainerMgrAddress()));
      }
      
      task.finishedAttempts.add(taskAttemptId);
      if (task.failedAttempts.size() < task.maxAttempts) {
        task.handleTaskAttemptCompletion(
            taskAttemptId, 
            TaskAttemptCompletionEventStatus.FAILED);
        // we don't need a new event if we already have a spare
        task.inProgressAttempts.remove(taskAttemptId);
        if (task.inProgressAttempts.size() == 0
            && task.successfulAttempt == null) {
          task.addAndScheduleAttempt(Avataar.VIRGIN);
        }
      } else {
        task.handleTaskAttemptCompletion(
            taskAttemptId, 
            TaskAttemptCompletionEventStatus.TIPFAILED);

        // issue kill to all non finished attempts
        for (TaskAttempt taskAttempt : task.attempts.values()) {
          task.killUnfinishedAttempt
            (taskAttempt, "Task has failed. Killing attempt!");
        }
        task.inProgressAttempts.clear();
        
        if (task.historyTaskStartGenerated) {
        TaskFailedEvent taskFailedEvent = createTaskFailedEvent(task, attempt.getDiagnostics(),
            TaskStateInternal.FAILED, taskAttemptId);
        task.eventHandler.handle(new JobHistoryEvent(task.taskId.getJobId(),
            taskFailedEvent));
        } else {
          LOG.debug("Not generating HistoryFinish event since start event not" +
          		" generated for task: " + task.getID());
        }
        task.eventHandler.handle(
            new JobTaskEvent(task.taskId, TaskState.FAILED));
        return task.finished(TaskStateInternal.FAILED);
      }
      return getDefaultState(task);
    }

    protected TaskStateInternal getDefaultState(TaskImpl task) {
      return task.getInternalState();
    }
  }

  private static class RetroactiveFailureTransition
      extends AttemptFailedTransition {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      TaskTAttemptEvent castEvent = (TaskTAttemptEvent) event;
      if (task.getInternalState() == TaskStateInternal.SUCCEEDED &&
          !castEvent.getTaskAttemptID().equals(task.successfulAttempt)) {
        // don't allow a different task attempt to override a previous
        // succeeded state
        task.finishedAttempts.add(castEvent.getTaskAttemptID());
        task.inProgressAttempts.remove(castEvent.getTaskAttemptID());
        return TaskStateInternal.SUCCEEDED;
      }

      // a successful REDUCE task should not be overridden
      //TODO: consider moving it to MapTaskImpl
      if (!TaskType.MAP.equals(task.getType())) {
        LOG.error("Unexpected event for REDUCE task " + event.getType());
        task.internalError(event.getType());
      }
      
      // tell the job about the rescheduling
      task.eventHandler.handle(
          new JobMapTaskRescheduledEvent(task.taskId));
      // super.transition is mostly coded for the case where an
      //  UNcompleted task failed.  When a COMPLETED task retroactively
      //  fails, we have to let AttemptFailedTransition.transition
      //  believe that there's no redundancy.
      unSucceed(task);
      // fake increase in Uncomplete attempts for super.transition
      task.inProgressAttempts.add(castEvent.getTaskAttemptID());
      return super.transition(task, event);
    }

    @Override
    protected TaskStateInternal getDefaultState(TaskImpl task) {
      return TaskStateInternal.SCHEDULED;
    }
  }

  private static class RetroactiveKilledTransition implements
    MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      TaskAttemptId attemptId = null;
      if (event instanceof TaskTAttemptEvent) {
        TaskTAttemptEvent castEvent = (TaskTAttemptEvent) event;
        attemptId = castEvent.getTaskAttemptID(); 
        if (task.getInternalState() == TaskStateInternal.SUCCEEDED &&
            !attemptId.equals(task.successfulAttempt)) {
          // don't allow a different task attempt to override a previous
          // succeeded state
          task.finishedAttempts.add(castEvent.getTaskAttemptID());
          task.inProgressAttempts.remove(castEvent.getTaskAttemptID());
          return TaskStateInternal.SUCCEEDED;
        }
      }

      // a successful REDUCE task should not be overridden
      // TODO: consider moving it to MapTaskImpl
      if (!TaskType.MAP.equals(task.getType())) {
        LOG.error("Unexpected event for REDUCE task " + event.getType());
        task.internalError(event.getType());
      }

      // successful attempt is now killed. reschedule
      // tell the job about the rescheduling
      unSucceed(task);
      task.handleTaskAttemptCompletion(attemptId,
          TaskAttemptCompletionEventStatus.KILLED);
      task.eventHandler.handle(new JobMapTaskRescheduledEvent(task.taskId));
      // typically we are here because this map task was run on a bad node and
      // we want to reschedule it on a different node.
      // Depending on whether there are previous failed attempts or not this
      // can SCHEDULE or RESCHEDULE the container allocate request. If this
      // SCHEDULE's then the dataLocal hosts of this taskAttempt will be used
      // from the map splitInfo. So the bad node might be sent as a location
      // to the RM. But the RM would ignore that just like it would ignore
      // currently pending container requests affinitized to bad nodes.
      task.addAndScheduleAttempt(Avataar.VIRGIN);
      return TaskStateInternal.SCHEDULED;
    }
  }

  private static class AttemptSucceededAtSucceededTransition
    implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskTAttemptEvent castEvent = (TaskTAttemptEvent) event;
      task.finishedAttempts.add(castEvent.getTaskAttemptID());
      task.inProgressAttempts.remove(castEvent.getTaskAttemptID());
    }
  }

  private static class KillNewTransition 
    implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      
      if (task.historyTaskStartGenerated) {
      TaskFailedEvent taskFailedEvent = createTaskFailedEvent(task, null,
            TaskStateInternal.KILLED, null); // TODO Verify failedAttemptId is null
      task.eventHandler.handle(new JobHistoryEvent(task.taskId.getJobId(),
          taskFailedEvent));
      }else {
        LOG.debug("Not generating HistoryFinish event since start event not" +
        		" generated for task: " + task.getID());
      }

      task.eventHandler.handle(new JobTaskEvent(task.taskId,
          getExternalState(TaskStateInternal.KILLED)));
      task.metrics.endWaitingTask(task);
    }
  }

  private void killUnfinishedAttempt(TaskAttempt attempt, String logMsg) {
    if (attempt != null && !attempt.isFinished()) {
      eventHandler.handle(
          new TaskAttemptEvent(attempt.getID(),
              TaskAttemptEventType.TA_KILL));
    }
  }

  private static class KillTransition 
    implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      // issue kill to all non finished attempts
      for (TaskAttempt attempt : task.attempts.values()) {
        task.killUnfinishedAttempt
            (attempt, "Task KILL is received. Killing attempt!");
      }

      task.inProgressAttempts.clear();
    }
  }

  static class LaunchTransition
      implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      task.metrics.launchedTask(task);
      task.metrics.runningTask(task);
      
    }
  }
}
