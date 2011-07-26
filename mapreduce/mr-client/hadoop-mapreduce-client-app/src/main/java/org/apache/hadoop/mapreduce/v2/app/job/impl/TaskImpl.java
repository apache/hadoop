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

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskFailedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskStartedEvent;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerFailedEvent;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
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
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

/**
 * Implementation of Task interface.
 */
public abstract class TaskImpl implements Task, EventHandler<TaskEvent> {

  private static final Log LOG = LogFactory.getLog(TaskImpl.class);

  protected final Configuration conf;
  protected final Path jobFile;
  protected final OutputCommitter committer;
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
  private long scheduledTime;
  
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  protected Collection<Token<? extends TokenIdentifier>> fsTokens;
  protected Token<JobTokenIdentifier> jobToken;
  
  // counts the number of attempts that are either running or in a state where
  //  they will come to be running when they get a Container
  private int numberUncompletedAttempts = 0;

  private boolean historyTaskStartGenerated = false;
  
  private static final SingleArcTransition<TaskImpl, TaskEvent> 
     ATTEMPT_KILLED_TRANSITION = new AttemptKilledTransition();
  private static final SingleArcTransition<TaskImpl, TaskEvent> 
     KILL_TRANSITION = new KillTransition();

  private static final StateMachineFactory
               <TaskImpl, TaskState, TaskEventType, TaskEvent> 
            stateMachineFactory 
           = new StateMachineFactory<TaskImpl, TaskState, TaskEventType, TaskEvent>
               (TaskState.NEW)

    // define the state machine of Task

    // Transitions from NEW state
    .addTransition(TaskState.NEW, TaskState.SCHEDULED, 
        TaskEventType.T_SCHEDULE, new InitialScheduleTransition())
    .addTransition(TaskState.NEW, TaskState.KILLED, 
        TaskEventType.T_KILL, new KillNewTransition())

    // Transitions from SCHEDULED state
      //when the first attempt is launched, the task state is set to RUNNING
     .addTransition(TaskState.SCHEDULED, TaskState.RUNNING, 
         TaskEventType.T_ATTEMPT_LAUNCHED, new LaunchTransition())
     .addTransition(TaskState.SCHEDULED, TaskState.KILL_WAIT, 
         TaskEventType.T_KILL, KILL_TRANSITION)
     .addTransition(TaskState.SCHEDULED, TaskState.SCHEDULED, 
         TaskEventType.T_ATTEMPT_KILLED, ATTEMPT_KILLED_TRANSITION)
     .addTransition(TaskState.SCHEDULED, 
        EnumSet.of(TaskState.SCHEDULED, TaskState.FAILED), 
        TaskEventType.T_ATTEMPT_FAILED, 
        new AttemptFailedTransition())
 
    // Transitions from RUNNING state
    .addTransition(TaskState.RUNNING, TaskState.RUNNING, 
        TaskEventType.T_ATTEMPT_LAUNCHED) //more attempts may start later
    .addTransition(TaskState.RUNNING, TaskState.RUNNING, 
        TaskEventType.T_ATTEMPT_COMMIT_PENDING,
        new AttemptCommitPendingTransition())
    .addTransition(TaskState.RUNNING, TaskState.RUNNING,
        TaskEventType.T_ADD_SPEC_ATTEMPT, new RedundantScheduleTransition())
    .addTransition(TaskState.RUNNING, TaskState.SUCCEEDED, 
        TaskEventType.T_ATTEMPT_SUCCEEDED,
        new AttemptSucceededTransition())
    .addTransition(TaskState.RUNNING, TaskState.RUNNING, 
        TaskEventType.T_ATTEMPT_KILLED,
        ATTEMPT_KILLED_TRANSITION)
    .addTransition(TaskState.RUNNING, 
        EnumSet.of(TaskState.RUNNING, TaskState.FAILED), 
        TaskEventType.T_ATTEMPT_FAILED,
        new AttemptFailedTransition())
    .addTransition(TaskState.RUNNING, TaskState.KILL_WAIT, 
        TaskEventType.T_KILL, KILL_TRANSITION)

    // Transitions from KILL_WAIT state
    .addTransition(TaskState.KILL_WAIT,
        EnumSet.of(TaskState.KILL_WAIT, TaskState.KILLED),
        TaskEventType.T_ATTEMPT_KILLED,
        new KillWaitAttemptKilledTransition())
    // Ignore-able transitions.
    .addTransition(
        TaskState.KILL_WAIT,
        TaskState.KILL_WAIT,
        EnumSet.of(TaskEventType.T_KILL,
            TaskEventType.T_ATTEMPT_LAUNCHED,
            TaskEventType.T_ATTEMPT_COMMIT_PENDING,
            TaskEventType.T_ATTEMPT_FAILED,
            TaskEventType.T_ATTEMPT_SUCCEEDED,
            TaskEventType.T_ADD_SPEC_ATTEMPT))

    // Transitions from SUCCEEDED state
    .addTransition(TaskState.SUCCEEDED, //only possible for map tasks
        EnumSet.of(TaskState.SCHEDULED, TaskState.FAILED),
        TaskEventType.T_ATTEMPT_FAILED, new MapRetroactiveFailureTransition())
    // Ignore-able transitions.
    .addTransition(
        TaskState.SUCCEEDED, TaskState.SUCCEEDED,
        EnumSet.of(TaskEventType.T_KILL,
            TaskEventType.T_ADD_SPEC_ATTEMPT,
            TaskEventType.T_ATTEMPT_LAUNCHED,
            TaskEventType.T_ATTEMPT_KILLED))

    // Transitions from FAILED state        
    .addTransition(TaskState.FAILED, TaskState.FAILED,
        EnumSet.of(TaskEventType.T_KILL,
                   TaskEventType.T_ADD_SPEC_ATTEMPT))

    // Transitions from KILLED state
    .addTransition(TaskState.KILLED, TaskState.KILLED,
        EnumSet.of(TaskEventType.T_KILL,
                   TaskEventType.T_ADD_SPEC_ATTEMPT))

    // create the topology tables
    .installTopology();

  private final StateMachine<TaskState, TaskEventType, TaskEvent>
    stateMachine;
  
  protected int nextAttemptNumber;

  //should be set to one which comes first
  //saying COMMIT_PENDING
  private TaskAttemptId commitAttempt;

  private TaskAttemptId successfulAttempt;

  private int failedAttempts;
  private int finishedAttempts;//finish are total of success, failed and killed

  @Override
  public TaskState getState() {
    return stateMachine.getCurrentState();
  }

  public TaskImpl(JobId jobId, TaskType taskType, int partition,
      EventHandler eventHandler, Path remoteJobConfFile, Configuration conf,
      TaskAttemptListener taskAttemptListener, OutputCommitter committer,
      Token<JobTokenIdentifier> jobToken,
      Collection<Token<? extends TokenIdentifier>> fsTokens, Clock clock,
      Set<TaskId> completedTasksFromPreviousRun, int startCount,
      MRAppMetrics metrics) {
    this.conf = conf;
    this.clock = clock;
    this.jobFile = remoteJobConfFile;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    this.attempts = Collections.emptyMap();
    // This overridable method call is okay in a constructor because we
    //  have a convention that none of the overrides depends on any
    //  fields that need initialization.
    maxAttempts = getMaxAttempts();
    taskId = recordFactory.newRecordInstance(TaskId.class);
    taskId.setJobId(jobId);
    taskId.setId(partition);
    taskId.setTaskType(taskType);
    this.partition = partition;
    this.taskAttemptListener = taskAttemptListener;
    this.eventHandler = eventHandler;
    this.committer = committer;
    this.fsTokens = fsTokens;
    this.jobToken = jobToken;
    this.metrics = metrics;

    if (completedTasksFromPreviousRun != null
        && completedTasksFromPreviousRun.contains(taskId)) {
      LOG.info("Task is from previous run " + taskId);
      startCount = startCount - 1;
    }

    //attempt ids are generated based on MR app startCount so that attempts
    //from previous lives don't overstep the current one.
    //this assumes that a task won't have more than 1000 attempts in its single 
    //life
    nextAttemptNumber = (startCount - 1) * 1000;

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
      return (getState() == TaskState.SUCCEEDED ||
          getState() == TaskState.FAILED ||
          getState() == TaskState.KILLED);
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
      report.setCounters(getCounters());

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
        counters = recordFactory.newRecordInstance(Counters.class);
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
        return 0;
      }
      return bestAttempt.getProgress();
    } finally {
      readLock.unlock();
    }
  }

  //this is always called in read/write lock
  private long getLaunchTime() {
    long launchTime = 0;
    for (TaskAttempt at : attempts.values()) {
      //select the least launch time of all attempts
      if (launchTime == 0  || launchTime > at.getLaunchTime()) {
        launchTime = at.getLaunchTime();
      }
    }
    if (launchTime == 0) {
      return this.scheduledTime;
    }
    return launchTime;
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
  
  private TaskState finished(TaskState finalState) {
    if (getState() == TaskState.RUNNING) {
      metrics.endRunningTask(this);
    }
    return finalState;
  }

  //select the nextAttemptNumber with best progress
  // always called inside the Read Lock
  private TaskAttempt selectBestAttempt() {
    float progress = 0f;
    TaskAttempt result = null;
    for (TaskAttempt at : attempts.values()) {
      if (result == null) {
        result = at; //The first time around
      }
      //TODO: consider the nextAttemptNumber only if it is not failed/killed ?
      // calculate the best progress
      if (at.getProgress() > progress) {
        result = at;
        progress = at.getProgress();
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
  private void addAndScheduleAttempt() {
    TaskAttempt attempt = createAttempt();
    LOG.info("Created attempt " + attempt.getID());
    switch (attempts.size()) {
      case 0:
        attempts = Collections.singletonMap(attempt.getID(), attempt);
        break;
        
      case 1:
        Map newAttempts
            = new LinkedHashMap<TaskAttemptId, TaskAttempt>(maxAttempts);
        newAttempts.putAll(attempts);
        attempts = newAttempts;
        attempts.put(attempt.getID(), attempt);
        break;

      default:
        attempts.put(attempt.getID(), attempt);
        break;
    }
    ++nextAttemptNumber;
    ++numberUncompletedAttempts;
    //schedule the nextAttemptNumber
    if (failedAttempts > 0) {
      eventHandler.handle(new TaskAttemptEvent(attempt.getID(),
        TaskAttemptEventType.TA_RESCHEDULE));
    } else {
      eventHandler.handle(new TaskAttemptEvent(attempt.getID(),
          TaskAttemptEventType.TA_SCHEDULE));
    }
  }

  @Override
  public void handle(TaskEvent event) {
    LOG.info("Processing " + event.getTaskID() + " of type " + event.getType());
    try {
      writeLock.lock();
      TaskState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        internalError(event.getType());
      }
      if (oldState != getState()) {
        LOG.info(taskId + " Task Transitioned from " + oldState + " to "
            + getState());
      }

    } finally {
      writeLock.unlock();
    }
  }

  private void internalError(TaskEventType type) {
    eventHandler.handle(new JobDiagnosticsUpdateEvent(
        this.taskId.getJobId(), "Invalid event " + type + 
        " on Task " + this.taskId));
    eventHandler.handle(new JobEvent(this.taskId.getJobId(),
        JobEventType.INTERNAL_ERROR));
  }

  // always called inside a transition, in turn inside the Write Lock
  private void handleTaskAttemptCompletion(TaskAttemptId attemptId,
      TaskAttemptCompletionEventStatus status) {
    finishedAttempts++;
    TaskAttempt attempt = attempts.get(attemptId);
    //raise the completion event only if the container is assigned
    // to nextAttemptNumber
    if (attempt.getNodeHttpAddress() != null) {
      TaskAttemptCompletionEvent tce = recordFactory.newRecordInstance(TaskAttemptCompletionEvent.class);
      tce.setEventId(-1);
      //TODO: XXXXXX  hardcoded port
      tce.setMapOutputServerAddress("http://" + attempt.getNodeHttpAddress().split(":")[0] + ":8080");
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

  private static TaskFinishedEvent createTaskFinishedEvent(TaskImpl task, TaskState taskState) {
    TaskFinishedEvent tfe =
      new TaskFinishedEvent(TypeConverter.fromYarn(task.taskId),
        task.getFinishTime(task.successfulAttempt),
        TypeConverter.fromYarn(task.taskId.getTaskType()),
        taskState.toString(),
        TypeConverter.fromYarn(task.getCounters()));
    return tfe;
  }
  
  private static TaskFailedEvent createTaskFailedEvent(TaskImpl task, List<String> diag, TaskState taskState, TaskAttemptId taId) {
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
      task.addAndScheduleAttempt();
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
      task.addAndScheduleAttempt();
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
      task.handleTaskAttemptCompletion(
          ((TaskTAttemptEvent) event).getTaskAttemptID(), 
          TaskAttemptCompletionEventStatus.SUCCEEDED);
      --task.numberUncompletedAttempts;
      task.successfulAttempt = ((TaskTAttemptEvent) event).getTaskAttemptID();
      task.eventHandler.handle(new JobTaskEvent(
          task.taskId, TaskState.SUCCEEDED));
      LOG.info("Task succeeded with attempt " + task.successfulAttempt);
      // issue kill to all other attempts
      if (task.historyTaskStartGenerated) {
        TaskFinishedEvent tfe = createTaskFinishedEvent(task,
            TaskState.SUCCEEDED);
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
      task.finished(TaskState.SUCCEEDED);
    }
  }

  private static class AttemptKilledTransition implements
      SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      task.handleTaskAttemptCompletion(
          ((TaskTAttemptEvent) event).getTaskAttemptID(), 
          TaskAttemptCompletionEventStatus.KILLED);
      --task.numberUncompletedAttempts;
      if (task.successfulAttempt == null) {
        task.addAndScheduleAttempt();
      }
    }
  }


  private static class KillWaitAttemptKilledTransition implements
      MultipleArcTransition<TaskImpl, TaskEvent, TaskState> {

    protected TaskState finalState = TaskState.KILLED;

    @Override
    public TaskState transition(TaskImpl task, TaskEvent event) {
      task.handleTaskAttemptCompletion(
          ((TaskTAttemptEvent) event).getTaskAttemptID(), 
          TaskAttemptCompletionEventStatus.KILLED);
      // check whether all attempts are finished
      if (task.finishedAttempts == task.attempts.size()) {
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
            new JobTaskEvent(task.taskId, finalState));
        return finalState;
      }
      return task.getState();
    }
  }

  private static class AttemptFailedTransition implements
    MultipleArcTransition<TaskImpl, TaskEvent, TaskState> {

    @Override
    public TaskState transition(TaskImpl task, TaskEvent event) {
      task.failedAttempts++;
      TaskTAttemptEvent castEvent = (TaskTAttemptEvent) event;
      TaskAttempt attempt = task.attempts.get(castEvent.getTaskAttemptID());
      if (attempt.getAssignedContainerMgrAddress() != null) {
        //container was assigned
        task.eventHandler.handle(new ContainerFailedEvent(attempt.getID(), 
            attempt.getAssignedContainerMgrAddress()));
      }
      
      if (task.failedAttempts < task.maxAttempts) {
        task.handleTaskAttemptCompletion(
            ((TaskTAttemptEvent) event).getTaskAttemptID(), 
            TaskAttemptCompletionEventStatus.FAILED);
        // we don't need a new event if we already have a spare
        if (--task.numberUncompletedAttempts == 0
            && task.successfulAttempt == null) {
          task.addAndScheduleAttempt();
        }
      } else {
        task.handleTaskAttemptCompletion(
            ((TaskTAttemptEvent) event).getTaskAttemptID(), 
            TaskAttemptCompletionEventStatus.TIPFAILED);
        TaskTAttemptEvent ev = (TaskTAttemptEvent) event;
        TaskAttemptId taId = ev.getTaskAttemptID();
        
        if (task.historyTaskStartGenerated) {
        TaskFailedEvent taskFailedEvent = createTaskFailedEvent(task, attempt.getDiagnostics(),
            TaskState.FAILED, taId);
        task.eventHandler.handle(new JobHistoryEvent(task.taskId.getJobId(),
            taskFailedEvent));
        } else {
          LOG.debug("Not generating HistoryFinish event since start event not" +
          		" generated for task: " + task.getID());
        }
        task.eventHandler.handle(
            new JobTaskEvent(task.taskId, TaskState.FAILED));
        return task.finished(TaskState.FAILED);
      }
      return getDefaultState(task);
    }

    protected TaskState getDefaultState(Task task) {
      return task.getState();
    }

    protected void unSucceed(TaskImpl task) {
      ++task.numberUncompletedAttempts;
      task.successfulAttempt = null;
    }
  }

  private static class MapRetroactiveFailureTransition
      extends AttemptFailedTransition {

    @Override
    public TaskState transition(TaskImpl task, TaskEvent event) {
      //verify that this occurs only for map task
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
      return super.transition(task, event);
    }

    @Override
    protected TaskState getDefaultState(Task task) {
      return TaskState.SCHEDULED;
    }
  }

  private static class KillNewTransition 
    implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      
      if (task.historyTaskStartGenerated) {
      TaskFailedEvent taskFailedEvent = createTaskFailedEvent(task, null,
            TaskState.KILLED, null); // TODO Verify failedAttemptId is null
      task.eventHandler.handle(new JobHistoryEvent(task.taskId.getJobId(),
          taskFailedEvent));
      }else {
        LOG.debug("Not generating HistoryFinish event since start event not" +
        		" generated for task: " + task.getID());
      }

      task.eventHandler.handle(
          new JobTaskEvent(task.taskId, TaskState.KILLED));
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

      task.numberUncompletedAttempts = 0;
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
