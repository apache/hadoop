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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobInfoChangeEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobInitedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobSubmittedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.MRConstants;
import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

/** Implementation of Job interface. Maintains the state machines of Job.
 * The read and write calls use ReadWriteLock for concurrency.
 */
public class JobImpl implements org.apache.hadoop.mapreduce.v2.app.job.Job, 
  EventHandler<JobEvent> {

  private static final Log LOG = LogFactory.getLog(JobImpl.class);

  //The maximum fraction of fetch failures allowed for a map
  private static final double MAX_ALLOWED_FETCH_FAILURES_FRACTION = 0.5;

  // Maximum no. of fetch-failure notifications after which map task is failed
  private static final int MAX_FETCH_FAILURES_NOTIFICATIONS = 3;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  
  //final fields
  private final Clock clock;
  private final JobACLsManager aclsManager;
  private final String username;
  private final Map<JobACL, AccessControlList> jobACLs;
  private final int startCount;
  private final Set<TaskId> completedTasksFromPreviousRun;
  private final Lock readLock;
  private final Lock writeLock;
  private final JobId jobId;
  private final String jobName;
  private final org.apache.hadoop.mapreduce.JobID oldJobId;
  private final TaskAttemptListener taskAttemptListener;
  private final Object tasksSyncHandle = new Object();
  private final Set<TaskId> mapTasks = new LinkedHashSet<TaskId>();
  private final Set<TaskId> reduceTasks = new LinkedHashSet<TaskId>();
  private final EventHandler eventHandler;
  private final MRAppMetrics metrics;

  private boolean lazyTasksCopyNeeded = false;
  private volatile Map<TaskId, Task> tasks = new LinkedHashMap<TaskId, Task>();
  private Counters jobCounters = newCounters();
    // FIXME:  
    //
    // Can then replace task-level uber counters (MR-2424) with job-level ones
    // sent from LocalContainerLauncher, and eventually including a count of
    // of uber-AM attempts (probably sent from MRAppMaster).
  public Configuration conf;

  //fields initialized in init
  private FileSystem fs;
  private Path remoteJobSubmitDir;
  public Path remoteJobConfFile;
  private JobContext jobContext;
  private OutputCommitter committer;
  private int allowedMapFailuresPercent = 0;
  private int allowedReduceFailuresPercent = 0;
  private List<TaskAttemptCompletionEvent> taskAttemptCompletionEvents;
  private final List<String> diagnostics = new ArrayList<String>();
  
  //task/attempt related datastructures
  private final Map<TaskId, Integer> successAttemptCompletionEventNoMap = 
    new HashMap<TaskId, Integer>();
  private final Map<TaskAttemptId, Integer> fetchFailuresMapping = 
    new HashMap<TaskAttemptId, Integer>();

  private static final DiagnosticsUpdateTransition
      DIAGNOSTIC_UPDATE_TRANSITION = new DiagnosticsUpdateTransition();
  private static final InternalErrorTransition
      INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final TaskAttemptCompletedEventTransition
      TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION =
          new TaskAttemptCompletedEventTransition();
  private static final CounterUpdateTransition COUNTER_UPDATE_TRANSITION =
      new CounterUpdateTransition();

  protected static final
    StateMachineFactory<JobImpl, JobState, JobEventType, JobEvent> 
       stateMachineFactory
     = new StateMachineFactory<JobImpl, JobState, JobEventType, JobEvent>
              (JobState.NEW)

          // Transitions from NEW state
          .addTransition(JobState.NEW, JobState.NEW,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobState.NEW, JobState.NEW,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition
              (JobState.NEW,
              EnumSet.of(JobState.INITED, JobState.FAILED),
              JobEventType.JOB_INIT,
              new InitTransition())
          .addTransition(JobState.NEW, JobState.KILLED,
              JobEventType.JOB_KILL,
              new KillNewJobTransition())
          .addTransition(JobState.NEW, JobState.ERROR,
              JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from INITED state
          .addTransition(JobState.INITED, JobState.INITED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobState.INITED, JobState.INITED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobState.INITED, JobState.RUNNING,
              JobEventType.JOB_START,
              new StartTransition())
          .addTransition(JobState.INITED, JobState.KILLED,
              JobEventType.JOB_KILL,
              new KillInitedJobTransition())
          .addTransition(JobState.INITED, JobState.ERROR,
              JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from RUNNING state
          .addTransition(JobState.RUNNING, JobState.RUNNING,
              JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
              TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition
              (JobState.RUNNING,
              EnumSet.of(JobState.RUNNING, JobState.SUCCEEDED, JobState.FAILED),
              JobEventType.JOB_TASK_COMPLETED,
              new TaskCompletedTransition())
          .addTransition
              (JobState.RUNNING,
              EnumSet.of(JobState.RUNNING, JobState.SUCCEEDED, JobState.FAILED),
              JobEventType.JOB_COMPLETED,
              new JobNoTasksCompletedTransition())
          .addTransition(JobState.RUNNING, JobState.KILL_WAIT,
              JobEventType.JOB_KILL, new KillTasksTransition())
          .addTransition(JobState.RUNNING, JobState.RUNNING,
              JobEventType.JOB_MAP_TASK_RESCHEDULED,
              new MapTaskRescheduledTransition())
          .addTransition(JobState.RUNNING, JobState.RUNNING,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobState.RUNNING, JobState.RUNNING,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobState.RUNNING, JobState.RUNNING,
              JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
              new TaskAttemptFetchFailureTransition())
          .addTransition(
              JobState.RUNNING,
              JobState.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from KILL_WAIT state.
          .addTransition
              (JobState.KILL_WAIT,
              EnumSet.of(JobState.KILL_WAIT, JobState.KILLED),
              JobEventType.JOB_TASK_COMPLETED,
              new KillWaitTaskCompletedTransition())
          .addTransition(JobState.KILL_WAIT, JobState.KILL_WAIT,
              JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
              TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition(JobState.KILL_WAIT, JobState.KILL_WAIT,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobState.KILL_WAIT, JobState.KILL_WAIT,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobState.KILL_WAIT,
              JobState.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobState.KILL_WAIT, JobState.KILL_WAIT,
              EnumSet.of(JobEventType.JOB_KILL,
                         JobEventType.JOB_MAP_TASK_RESCHEDULED,
                         JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE))

          // Transitions from SUCCEEDED state
          .addTransition(JobState.SUCCEEDED, JobState.SUCCEEDED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobState.SUCCEEDED, JobState.SUCCEEDED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobState.SUCCEEDED,
              JobState.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobState.SUCCEEDED, JobState.SUCCEEDED,
              EnumSet.of(JobEventType.JOB_KILL,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE))

          // Transitions from FAILED state
          .addTransition(JobState.FAILED, JobState.FAILED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobState.FAILED, JobState.FAILED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobState.FAILED,
              JobState.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobState.FAILED, JobState.FAILED,
              EnumSet.of(JobEventType.JOB_KILL,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE))

          // Transitions from KILLED state
          .addTransition(JobState.KILLED, JobState.KILLED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobState.KILLED, JobState.KILLED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobState.KILLED,
              JobState.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobState.KILLED, JobState.KILLED,
              EnumSet.of(JobEventType.JOB_KILL,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE))

          // No transitions from INTERNAL_ERROR state. Ignore all.
          .addTransition(
              JobState.ERROR,
              JobState.ERROR,
              EnumSet.of(JobEventType.JOB_INIT,
                  JobEventType.JOB_KILL,
                  JobEventType.JOB_TASK_COMPLETED,
                  JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
                  JobEventType.JOB_MAP_TASK_RESCHEDULED,
                  JobEventType.JOB_DIAGNOSTIC_UPDATE,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
                  JobEventType.INTERNAL_ERROR))

          // create the topology tables
          .installTopology();
 
  private final StateMachine<JobState, JobEventType, JobEvent> stateMachine;

  //changing fields while the job is running
  private int numMapTasks;
  private int numReduceTasks;
  private int completedTaskCount = 0;
  private int succeededMapTaskCount = 0;
  private int succeededReduceTaskCount = 0;
  private int failedMapTaskCount = 0;
  private int failedReduceTaskCount = 0;
  private int killedMapTaskCount = 0;
  private int killedReduceTaskCount = 0;
  private long submitTime;
  private long startTime;
  private long finishTime;
  private float setupProgress;
  private float cleanupProgress;
  private boolean isUber = false;

  private Credentials fsTokens;
  private Token<JobTokenIdentifier> jobToken;
  private JobTokenSecretManager jobTokenSecretManager;

  public JobImpl(ApplicationId appID, Configuration conf,
      EventHandler eventHandler, TaskAttemptListener taskAttemptListener,
      JobTokenSecretManager jobTokenSecretManager,
      Credentials fsTokenCredentials, Clock clock, int startCount, 
      Set<TaskId> completedTasksFromPreviousRun, MRAppMetrics metrics) {

    this.jobId = recordFactory.newRecordInstance(JobId.class);
    this.jobName = conf.get(JobContext.JOB_NAME, "<missing job name>");
    this.conf = conf;
    this.metrics = metrics;
    this.clock = clock;
    this.completedTasksFromPreviousRun = completedTasksFromPreviousRun;
    this.startCount = startCount;
    jobId.setAppId(appID);
    jobId.setId(appID.getId());
    oldJobId = TypeConverter.fromYarn(jobId);
    LOG.info("Job created" +
    		" appId=" + appID + 
    		" jobId=" + jobId + 
    		" oldJobId=" + oldJobId);
    
    this.taskAttemptListener = taskAttemptListener;
    this.eventHandler = eventHandler;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    this.fsTokens = fsTokenCredentials;
    this.jobTokenSecretManager = jobTokenSecretManager;

    this.aclsManager = new JobACLsManager(conf);
    this.username = System.getProperty("user.name");
    this.jobACLs = aclsManager.constructJobACLs(conf);
    // This "this leak" is okay because the retained pointer is in an
    //  instance variable.
    stateMachine = stateMachineFactory.make(this);
  }

  protected StateMachine<JobState, JobEventType, JobEvent> getStateMachine() {
    return stateMachine;
  }

  @Override
  public JobId getID() {
    return jobId;
  }

  // Getter methods that make unit testing easier (package-scoped)
  OutputCommitter getCommitter() {
    return this.committer;
  }

  EventHandler getEventHandler() {
    return this.eventHandler;
  }

  JobContext getJobContext() {
    return this.jobContext;
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, 
      JobACL jobOperation) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return true;
    }
    AccessControlList jobACL = jobACLs.get(jobOperation);
    return aclsManager.checkAccess(callerUGI, jobOperation, username, jobACL);
  }

  @Override
  public Task getTask(TaskId taskID) {
    readLock.lock();
    try {
      return tasks.get(taskID);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getCompletedMaps() {
    readLock.lock();
    try {
      return succeededMapTaskCount + failedMapTaskCount + killedMapTaskCount;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getCompletedReduces() {
    readLock.lock();
    try {
      return succeededReduceTaskCount + failedReduceTaskCount 
                  + killedReduceTaskCount;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean isUber() {
    return isUber;
  }

  @Override
  public Counters getCounters() {
    Counters counters = newCounters();
    readLock.lock();
    try {
      incrAllCounters(counters, jobCounters);
      return incrTaskCounters(counters, tasks.values());
    } finally {
      readLock.unlock();
    }
  }

  private Counters getTypeCounters(Set<TaskId> taskIds) {
    Counters counters = newCounters();
    for (TaskId taskId : taskIds) {
      Task task = tasks.get(taskId);
      incrAllCounters(counters, task.getCounters());
    }
    return counters;
  }

  private Counters getMapCounters() {
    readLock.lock();
    try {
      return getTypeCounters(mapTasks);
    } finally {
      readLock.unlock();
    }
  }
  
  private Counters getReduceCounters() {
    readLock.lock();
    try {
      return getTypeCounters(reduceTasks);
    } finally {
      readLock.unlock();
    }
  }
  
  public static Counters newCounters() {
    Counters counters = RecordFactoryProvider.getRecordFactory(null)
        .newRecordInstance(Counters.class);
    return counters;
  }

  public static Counters incrTaskCounters(Counters counters,
                                          Collection<Task> tasks) {
    for (Task task : tasks) {
      incrAllCounters(counters, task.getCounters());
    }
    return counters;
  }

  public static void incrAllCounters(Counters counters, Counters other) {
    if (other != null) {
      for (CounterGroup otherGroup: other.getAllCounterGroups().values()) {
        CounterGroup group = counters.getCounterGroup(otherGroup.getName());
        if (group == null) {
          group = RecordFactoryProvider.getRecordFactory(null)
              .newRecordInstance(CounterGroup.class);
          group.setName(otherGroup.getName());
          counters.setCounterGroup(group.getName(), group);
        }
        group.setDisplayName(otherGroup.getDisplayName());
        for (Counter otherCounter : otherGroup.getAllCounters().values()) {
          Counter counter = group.getCounter(otherCounter.getName());
          if (counter == null) {
            counter = RecordFactoryProvider.getRecordFactory(null)
                .newRecordInstance(Counter.class);
            counter.setName(otherCounter.getName());
            group.setCounter(counter.getName(), counter);
          }
          counter.setDisplayName(otherCounter.getDisplayName());
          counter.setValue(counter.getValue() + otherCounter.getValue());
        }
      }
    }
  }

  @Override
  public TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(
      int fromEventId, int maxEvents) {
    TaskAttemptCompletionEvent[] events = new TaskAttemptCompletionEvent[0];
    readLock.lock();
    try {
      if (taskAttemptCompletionEvents.size() > fromEventId) {
        int actualMax = Math.min(maxEvents,
            (taskAttemptCompletionEvents.size() - fromEventId));
        events = taskAttemptCompletionEvents.subList(fromEventId,
            actualMax + fromEventId).toArray(events);
      }
      return events;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<String> getDiagnostics() {
    readLock.lock();
    try {
      return diagnostics;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public JobReport getReport() {
    readLock.lock();
    try {
      JobReport report = recordFactory.newRecordInstance(JobReport.class);
      report.setJobId(jobId);
      report.setJobState(getState());
      
      // TODO - Fix to correctly setup report and to check state
      if (report.getJobState() == JobState.NEW) {
        return report;
      }
      
      report.setStartTime(startTime);
      report.setFinishTime(finishTime);
      report.setSetupProgress(setupProgress);
      report.setCleanupProgress(cleanupProgress);
      report.setMapProgress(computeProgress(mapTasks));
      report.setReduceProgress(computeProgress(reduceTasks));

      return report;
    } finally {
      readLock.unlock();
    }
  }

  private float computeProgress(Set<TaskId> taskIds) {
    readLock.lock();
    try {
      float progress = 0;
      for (TaskId taskId : taskIds) {
        Task task = tasks.get(taskId);
        progress += task.getProgress();
      }
      int taskIdsSize = taskIds.size();
      if (taskIdsSize != 0) {
        progress = progress/taskIdsSize;
      }
      return progress;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Map<TaskId, Task> getTasks() {
    synchronized (tasksSyncHandle) {
      lazyTasksCopyNeeded = true;
      return Collections.unmodifiableMap(tasks);
    }
  }

  @Override
  public Map<TaskId,Task> getTasks(TaskType taskType) {
    Map<TaskId, Task> localTasksCopy = tasks;
    Map<TaskId, Task> result = new HashMap<TaskId, Task>();
    Set<TaskId> tasksOfGivenType = null;
    readLock.lock();
    try {
      if (TaskType.MAP == taskType) {
        tasksOfGivenType = mapTasks;
      } else {
        tasksOfGivenType = reduceTasks;
      }
      for (TaskId taskID : tasksOfGivenType)
      result.put(taskID, localTasksCopy.get(taskID));
      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public JobState getState() {
    readLock.lock();
    try {
     return getStateMachine().getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  protected void scheduleTasks(Set<TaskId> taskIDs) {
    for (TaskId taskID : taskIDs) {
      eventHandler.handle(new TaskEvent(taskID, 
          TaskEventType.T_SCHEDULE));
    }
  }

  @Override
  /**
   * The only entry point to change the Job.
   */
  public void handle(JobEvent event) {
    LOG.info("Processing " + event.getJobId() + " of type " + event.getType());
    try {
      writeLock.lock();
      JobState oldState = getState();
      try {
         getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        addDiagnostic("Invalid event " + event.getType() + 
            " on Job " + this.jobId);
        eventHandler.handle(new JobEvent(this.jobId,
            JobEventType.INTERNAL_ERROR));
      }
      //notify the eventhandler of state change
      if (oldState != getState()) {
        LOG.info(jobId + "Job Transitioned from " + oldState + " to "
                 + getState());
      }
    }
    
    finally {
      writeLock.unlock();
    }
  }

  //helpful in testing
  protected void addTask(Task task) {
    synchronized (tasksSyncHandle) {
      if (lazyTasksCopyNeeded) {
        Map<TaskId, Task> newTasks = new LinkedHashMap<TaskId, Task>();
        newTasks.putAll(tasks);
        tasks = newTasks;
        lazyTasksCopyNeeded = false;
      }
    }
    tasks.put(task.getID(), task);
    if (task.getType() == TaskType.MAP) {
      mapTasks.add(task.getID());
    } else if (task.getType() == TaskType.REDUCE) {
      reduceTasks.add(task.getID());
    }
    metrics.waitingTask(task);
  }

  void setFinishTime() {
    finishTime = clock.getTime();
  }

  void logJobHistoryFinishedEvent() {
    this.setFinishTime();
    JobFinishedEvent jfe = createJobFinishedEvent(this);
    LOG.info("Calling handler for JobFinishedEvent ");
    this.getEventHandler().handle(new JobHistoryEvent(this.jobId, jfe));    
  }
  
  static JobState checkJobCompleteSuccess(JobImpl job) {
    // check for Job success
    if (job.completedTaskCount == job.getTasks().size()) {
      try {
        // Commit job & do cleanup
        job.getCommitter().commitJob(job.getJobContext());
      } catch (IOException e) {
        LOG.warn("Could not do commit for Job", e);
      }
      
      job.logJobHistoryFinishedEvent();
      return job.finished(JobState.SUCCEEDED);
    }
    return null;
  }

  JobState finished(JobState finalState) {
    if (getState() == JobState.RUNNING) {
      metrics.endRunningJob(this);
    }
    if (finishTime == 0) setFinishTime();
    eventHandler.handle(new JobFinishEvent(jobId));

    switch (finalState) {
      case KILLED:
        metrics.killedJob(this);
        break;
      case FAILED:
        metrics.failedJob(this);
        break;
      case SUCCEEDED:
        metrics.completedJob(this);
    }
    return finalState;
  }

  @Override
  public String getName() {
    return jobName;
  }

  @Override
  public int getTotalMaps() {
    return mapTasks.size();  //FIXME: why indirection? return numMapTasks...
                             // unless race?  how soon can this get called?
  }

  @Override
  public int getTotalReduces() {
    return reduceTasks.size();  //FIXME: why indirection? return numReduceTasks
  }

  public static class InitTransition 
      implements MultipleArcTransition<JobImpl, JobEvent, JobState> {

    /**
     * Note that this transition method is called directly (and synchronously)
     * by MRAppMaster's init() method (i.e., no RPC, no thread-switching;
     * just plain sequential call within AM context), so we can trigger
     * modifications in AM state from here (at least, if AM is written that
     * way; MR version is).
     */
    @Override
    public JobState transition(JobImpl job, JobEvent event) {
      job.submitTime = job.clock.getTime();
      job.metrics.submittedJob(job);
      job.metrics.preparingJob(job);
      try {
        setup(job);
        job.fs = FileSystem.get(job.conf);

        //log to job history
        JobSubmittedEvent jse = new JobSubmittedEvent(job.oldJobId,
              job.conf.get(MRJobConfig.JOB_NAME, "test"), 
            job.conf.get(MRJobConfig.USER_NAME, "mapred"),
            job.submitTime,
            job.remoteJobConfFile.toString(),
            job.jobACLs, job.conf.get(MRJobConfig.QUEUE_NAME, "test"));
        job.eventHandler.handle(new JobHistoryEvent(job.jobId, jse));
        //TODO JH Verify jobACLs, UserName via UGI?

        TaskSplitMetaInfo[] taskSplitMetaInfo = createSplits(job, job.jobId);
        job.numMapTasks = taskSplitMetaInfo.length;
        job.numReduceTasks = job.conf.getInt(MRJobConfig.NUM_REDUCES, 0);

        if (job.numMapTasks == 0 && job.numReduceTasks == 0) {
          job.addDiagnostic("No of maps and reduces are 0 " + job.jobId);
        }

        checkTaskLimits();

        
        boolean newApiCommitter = false;
        if ((job.numReduceTasks > 0 && 
            job.conf.getBoolean("mapred.reducer.new-api", false)) ||
              (job.numReduceTasks == 0 && 
               job.conf.getBoolean("mapred.mapper.new-api", false)))  {
          newApiCommitter = true;
          LOG.info("Using mapred newApiCommitter.");
        }
        
        LOG.info("OutputCommitter set in config " + job.conf.get("mapred.output.committer.class"));
        
        if (newApiCommitter) {
          job.jobContext = new JobContextImpl(job.conf,
              job.oldJobId);
          org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID = RecordFactoryProvider
              .getRecordFactory(null)
              .newRecordInstance(
                  org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId.class);
          attemptID.setTaskId(RecordFactoryProvider.getRecordFactory(null)
              .newRecordInstance(TaskId.class));
          attemptID.getTaskId().setJobId(job.jobId);
          attemptID.getTaskId().setTaskType(TaskType.MAP);
          TaskAttemptContext taskContext = new TaskAttemptContextImpl(job.conf,
              TypeConverter.fromYarn(attemptID));
          try {
            OutputFormat outputFormat = ReflectionUtils.newInstance(
                taskContext.getOutputFormatClass(), job.conf);
            job.committer = outputFormat.getOutputCommitter(taskContext);
          } catch(Exception e) {
            throw new IOException("Failed to assign outputcommitter", e);
          }
        } else {
          job.jobContext = new org.apache.hadoop.mapred.JobContextImpl(
              new JobConf(job.conf), job.oldJobId);
          job.committer = ReflectionUtils.newInstance(
              job.conf.getClass("mapred.output.committer.class", FileOutputCommitter.class,
              org.apache.hadoop.mapred.OutputCommitter.class), job.conf);
        }
        LOG.info("OutputCommitter is " + job.committer.getClass().getName());
        
        long inputLength = 0;
        for (int i = 0; i < job.numMapTasks; ++i) {
          inputLength += taskSplitMetaInfo[i].getInputDataLength();
        }

//FIXME:  need new memory criterion for uber-decision (oops, too late here; until AM-resizing supported, must depend on job client to pass fat-slot needs)
        // these are no longer "system" settings, necessarily; user may override
        int sysMaxMaps = job.conf.getInt(MRJobConfig.JOB_UBERTASK_MAXMAPS, 9);
        int sysMaxReduces =
            job.conf.getInt(MRJobConfig.JOB_UBERTASK_MAXREDUCES, 1);
        long sysMaxBytes = job.conf.getLong(MRJobConfig.JOB_UBERTASK_MAXBYTES,
            job.conf.getLong("dfs.block.size", 64*1024*1024));  //FIXME: this is wrong; get FS from [File?]InputFormat and default block size from that
        //long sysMemSizeForUberSlot = JobTracker.getMemSizeForReduceSlot(); // FIXME [could use default AM-container memory size...]

        boolean uberEnabled =
            job.conf.getBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
        boolean smallNumMapTasks = (job.numMapTasks <= sysMaxMaps);
        boolean smallNumReduceTasks = (job.numReduceTasks <= sysMaxReduces);
        boolean smallInput = (inputLength <= sysMaxBytes);
        boolean smallMemory = true;  //FIXME (see above)
            // ignoring overhead due to UberTask and statics as negligible here:
//  FIXME   && (Math.max(memoryPerMap, memoryPerReduce) <= sysMemSizeForUberSlot
//              || sysMemSizeForUberSlot == JobConf.DISABLED_MEMORY_LIMIT)
        boolean notChainJob = !isChainJob(job.conf);

        // User has overall veto power over uberization, or user can modify
        // limits (overriding system settings and potentially shooting
        // themselves in the head).  Note that ChainMapper/Reducer are
        // fundamentally incompatible with MR-1220; they employ a blocking

        // User has overall veto power over uberization, or user can modify
        // limits (overriding system settings and potentially shooting
        // themselves in the head).  Note that ChainMapper/Reducer are
        // fundamentally incompatible with MR-1220; they employ a blocking
        // queue between the maps/reduces and thus require parallel execution,
        // while "uber-AM" (MR AM + LocalContainerLauncher) loops over tasks
        // and thus requires sequential execution.
        job.isUber = uberEnabled && smallNumMapTasks && smallNumReduceTasks
            && smallInput && smallMemory && notChainJob;

        if (job.isUber) {
          LOG.info("Uberizing job " + job.jobId + ": " + job.numMapTasks + "m+"
              + job.numReduceTasks + "r tasks (" + inputLength
              + " input bytes) will run sequentially on single node.");
              //TODO: also note which node?

          // make sure reduces are scheduled only after all map are completed
          job.conf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART,
                            1.0f);
          // uber-subtask attempts all get launched on same node; if one fails,
          // probably should retry elsewhere, i.e., move entire uber-AM:  ergo,
          // limit attempts to 1 (or at most 2?  probably not...)
          job.conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);
          job.conf.setInt(MRJobConfig.REDUCE_MAX_ATTEMPTS, 1);

          // disable speculation:  makes no sense to speculate an entire job
//        canSpeculateMaps = canSpeculateReduces = false; // [TODO: in old version, ultimately was from conf.getMapSpeculativeExecution(), conf.getReduceSpeculativeExecution()]
        } else {
          StringBuilder msg = new StringBuilder();
          msg.append("Not uberizing ").append(job.jobId).append(" because:");
          if (!uberEnabled)
            msg.append(" not enabled;");
          if (!smallNumMapTasks)
            msg.append(" too many maps;");
          if (!smallNumReduceTasks)
            msg.append(" too many reduces;");
          if (!smallInput)
            msg.append(" too much input;");
          if (!smallMemory)
            msg.append(" too much RAM;");
          if (!notChainJob)
            msg.append(" chainjob");
          LOG.info(msg.toString());
        }

        job.taskAttemptCompletionEvents =
            new ArrayList<TaskAttemptCompletionEvent>(
                job.numMapTasks + job.numReduceTasks + 10);

        job.allowedMapFailuresPercent =
            job.conf.getInt(MRJobConfig.MAP_FAILURES_MAX_PERCENT, 0);
        job.allowedReduceFailuresPercent =
            job.conf.getInt(MRJobConfig.REDUCE_FAILURES_MAXPERCENT, 0);

        // do the setup
        job.committer.setupJob(job.jobContext);
        job.setupProgress = 1.0f;

        // create the Tasks but don't start them yet
        createMapTasks(job, inputLength, taskSplitMetaInfo);
        createReduceTasks(job);

        job.metrics.endPreparingJob(job);
        return JobState.INITED;
        //TODO XXX Should JobInitedEvent be generated here (instead of in StartTransition)

      } catch (IOException e) {
        LOG.warn("Job init failed", e);
        job.addDiagnostic("Job init failed : "
            + StringUtils.stringifyException(e));
        job.abortJob(org.apache.hadoop.mapreduce.JobStatus.State.FAILED);
        job.metrics.endPreparingJob(job);
        return job.finished(JobState.FAILED);
      }
    }

    protected void setup(JobImpl job) throws IOException {

      String oldJobIDString = job.oldJobId.toString();
      String user = 
        UserGroupInformation.getCurrentUser().getShortUserName();
      Path path = MRApps.getStagingAreaDir(job.conf, user);
      LOG.info("DEBUG --- startJobs:"
          + " parent="
          + path + " child="
          + oldJobIDString);

      job.remoteJobSubmitDir =
          FileSystem.get(job.conf).makeQualified(
              new Path(path, oldJobIDString));
      job.remoteJobConfFile =
          new Path(job.remoteJobSubmitDir, MRConstants.JOB_CONF_FILE);

      // Prepare the TaskAttemptListener server for authentication of Containers
      // TaskAttemptListener gets the information via jobTokenSecretManager.
      JobTokenIdentifier identifier =
          new JobTokenIdentifier(new Text(oldJobIDString));
      job.jobToken =
          new Token<JobTokenIdentifier>(identifier, job.jobTokenSecretManager);
      job.jobToken.setService(identifier.getJobId());
      // Add it to the jobTokenSecretManager so that TaskAttemptListener server
      // can authenticate containers(tasks)
      job.jobTokenSecretManager.addTokenForJob(oldJobIDString, job.jobToken);
      LOG.info("Adding job token for " + oldJobIDString
          + " to jobTokenSecretManager");

      // Upload the jobTokens onto the remote FS so that ContainerManager can
      // localize it to be used by the Containers(tasks)
      Credentials tokenStorage = new Credentials();
      TokenCache.setJobToken(job.jobToken, tokenStorage);

      if (UserGroupInformation.isSecurityEnabled()) {
        tokenStorage.addAll(job.fsTokens);
      }

      Path remoteJobTokenFile =
          new Path(job.remoteJobSubmitDir,
              MRConstants.APPLICATION_TOKENS_FILE);
      tokenStorage.writeTokenStorageFile(remoteJobTokenFile, job.conf);
      LOG.info("Writing back the job-token file on the remote file system:"
          + remoteJobTokenFile.toString());
    }

    /**
     * ChainMapper and ChainReducer must execute in parallel, so they're not
     * compatible with uberization/LocalContainerLauncher (100% sequential).
     */
    boolean isChainJob(Configuration conf) {
      boolean isChainJob = false;
      try {
        String mapClassName = conf.get(MRJobConfig.MAP_CLASS_ATTR);
        if (mapClassName != null) {
          Class<?> mapClass = Class.forName(mapClassName);
          if (ChainMapper.class.isAssignableFrom(mapClass))
            isChainJob = true;
        }
      } catch (ClassNotFoundException cnfe) {
        // don't care; assume it's not derived from ChainMapper
      }
      try {
        String reduceClassName = conf.get(MRJobConfig.REDUCE_CLASS_ATTR);
        if (reduceClassName != null) {
          Class<?> reduceClass = Class.forName(reduceClassName);
          if (ChainReducer.class.isAssignableFrom(reduceClass))
            isChainJob = true;
        }
      } catch (ClassNotFoundException cnfe) {
        // don't care; assume it's not derived from ChainReducer
      }
      return isChainJob;
    }

    private void createMapTasks(JobImpl job, long inputLength,
                                TaskSplitMetaInfo[] splits) {
      for (int i=0; i < job.numMapTasks; ++i) {
        TaskImpl task =
            new MapTaskImpl(job.jobId, i,
                job.eventHandler, 
                job.remoteJobConfFile, 
                job.conf, splits[i], 
                job.taskAttemptListener, 
                job.committer, job.jobToken, job.fsTokens.getAllTokens(), 
                job.clock, job.completedTasksFromPreviousRun, job.startCount,
                job.metrics);
        job.addTask(task);
      }
      LOG.info("Input size for job " + job.jobId + " = " + inputLength
          + ". Number of splits = " + splits.length);
    }

    private void createReduceTasks(JobImpl job) {
      for (int i = 0; i < job.numReduceTasks; i++) {
        TaskImpl task =
            new ReduceTaskImpl(job.jobId, i,
                job.eventHandler, 
                job.remoteJobConfFile, 
                job.conf, job.numMapTasks, 
                job.taskAttemptListener, job.committer, job.jobToken,
                job.fsTokens.getAllTokens(), job.clock, 
                job.completedTasksFromPreviousRun, job.startCount, job.metrics);
        job.addTask(task);
      }
      LOG.info("Number of reduces for job " + job.jobId + " = "
          + job.numReduceTasks);
    }

    protected TaskSplitMetaInfo[] createSplits(JobImpl job, JobId jobId) {
      TaskSplitMetaInfo[] allTaskSplitMetaInfo;
      try {
        allTaskSplitMetaInfo = SplitMetaInfoReader.readSplitMetaInfo(
            job.oldJobId, job.fs, 
            job.conf, 
            job.remoteJobSubmitDir);
      } catch (IOException e) {
        throw new YarnException(e);
      }
      return allTaskSplitMetaInfo;
    }

    /**
     * If the number of tasks are greater than the configured value
     * throw an exception that will fail job initialization
     */
    private void checkTaskLimits() {
      // no code, for now
    }
  } // end of InitTransition

  public static class StartTransition
  implements SingleArcTransition<JobImpl, JobEvent> {
    /**
     * This transition executes in the event-dispatcher thread, though it's
     * triggered in MRAppMaster's startJobs() method.
     */
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.startTime = job.clock.getTime();
      job.scheduleTasks(job.mapTasks);  // schedule (i.e., start) the maps
      job.scheduleTasks(job.reduceTasks);
      JobInitedEvent jie =
        new JobInitedEvent(job.oldJobId,
             job.startTime,
             job.numMapTasks, job.numReduceTasks,
             job.isUber, 0, 0,  // FIXME: lose latter two args again (old-style uber junk:  needs to go along with 98% of other old-style uber junk)
             job.getState().toString()); //Will transition to state running. Currently in INITED
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, jie));
      JobInfoChangeEvent jice = new JobInfoChangeEvent(job.oldJobId,
          job.submitTime, job.startTime);
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, jice));
      job.metrics.runningJob(job);

			// If we have no tasks, just transition to job completed
      if (job.numReduceTasks == 0 && job.numMapTasks == 0) {
        job.eventHandler.handle(new JobEvent(job.jobId, JobEventType.JOB_COMPLETED));
      }
    }
  }

  private void abortJob(
      org.apache.hadoop.mapreduce.JobStatus.State finalState) {
    try {
      committer.abortJob(jobContext, finalState);
    } catch (IOException e) {
      LOG.warn("Could not abortJob", e);
    }
    if (finishTime == 0) setFinishTime();
    cleanupProgress = 1.0f;
    JobUnsuccessfulCompletionEvent unsuccessfulJobEvent =
      new JobUnsuccessfulCompletionEvent(oldJobId,
          finishTime,
          succeededMapTaskCount,
          succeededReduceTaskCount,
          finalState.toString());
    eventHandler.handle(new JobHistoryEvent(jobId, unsuccessfulJobEvent));
  }
    
  // JobFinishedEvent triggers the move of the history file out of the staging
  // area. May need to create a new event type for this if JobFinished should 
  // not be generated for KilledJobs, etc.
  private static JobFinishedEvent createJobFinishedEvent(JobImpl job) {
    JobFinishedEvent jfe = new JobFinishedEvent(
        job.oldJobId, job.finishTime,
        job.succeededMapTaskCount, job.succeededReduceTaskCount,
        job.failedMapTaskCount, job.failedReduceTaskCount,
        TypeConverter.fromYarn(job.getMapCounters()),
        TypeConverter.fromYarn(job.getReduceCounters()),
        TypeConverter.fromYarn(job.getCounters()));
    return jfe;
  }

  // Task-start has been moved out of InitTransition, so this arc simply
  // hardcodes 0 for both map and reduce finished tasks.
  private static class KillNewJobTransition
  implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.setFinishTime();
      JobUnsuccessfulCompletionEvent failedEvent =
          new JobUnsuccessfulCompletionEvent(job.oldJobId,
              job.finishTime, 0, 0,
              JobState.KILLED.toString());
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, failedEvent));
      job.finished(JobState.KILLED);
    }
  }

  private static class KillInitedJobTransition
  implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.abortJob(org.apache.hadoop.mapreduce.JobStatus.State.KILLED);
      job.addDiagnostic("Job received Kill in INITED state.");
      job.finished(JobState.KILLED);
    }
  }

  private static class KillTasksTransition
      implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.addDiagnostic("Job received Kill while in RUNNING state.");
      for (Task task : job.tasks.values()) {
        job.eventHandler.handle(
            new TaskEvent(task.getID(), TaskEventType.T_KILL));
      }
      job.metrics.endRunningJob(job);
    }
  }

  private static class TaskAttemptCompletedEventTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      TaskAttemptCompletionEvent tce = 
        ((JobTaskAttemptCompletedEvent) event).getCompletionEvent();
      // Add the TaskAttemptCompletionEvent
      //eventId is equal to index in the arraylist
      tce.setEventId(job.taskAttemptCompletionEvents.size());
      job.taskAttemptCompletionEvents.add(tce);
      
      //make the previous completion event as obsolete if it exists
      Object successEventNo = 
        job.successAttemptCompletionEventNoMap.remove(tce.getAttemptId().getTaskId());
      if (successEventNo != null) {
        TaskAttemptCompletionEvent successEvent = 
          job.taskAttemptCompletionEvents.get((Integer) successEventNo);
        successEvent.setStatus(TaskAttemptCompletionEventStatus.OBSOLETE);
      }

      if (TaskAttemptCompletionEventStatus.SUCCEEDED.equals(tce.getStatus())) {
        job.successAttemptCompletionEventNoMap.put(tce.getAttemptId().getTaskId(), 
            tce.getEventId());
      }
    }
  }

  private static class TaskAttemptFetchFailureTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      JobTaskAttemptFetchFailureEvent fetchfailureEvent = 
        (JobTaskAttemptFetchFailureEvent) event;
      for (org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId mapId : 
            fetchfailureEvent.getMaps()) {
        Integer fetchFailures = job.fetchFailuresMapping.get(mapId);
        fetchFailures = (fetchFailures == null) ? 1 : (fetchFailures+1);
        job.fetchFailuresMapping.put(mapId, fetchFailures);
        
        //get number of running reduces
        int runningReduceTasks = 0;
        for (TaskId taskId : job.reduceTasks) {
          if (TaskState.RUNNING.equals(job.tasks.get(taskId).getState())) {
            runningReduceTasks++;
          }
        }
        
        float failureRate = (float) fetchFailures / runningReduceTasks;
        // declare faulty if fetch-failures >= max-allowed-failures
        boolean isMapFaulty =
            (failureRate >= MAX_ALLOWED_FETCH_FAILURES_FRACTION);
        if (fetchFailures >= MAX_FETCH_FAILURES_NOTIFICATIONS && isMapFaulty) {
          LOG.info("Too many fetch-failures for output of task attempt: " + 
              mapId + " ... raising fetch failure to map");
          job.eventHandler.handle(new TaskAttemptEvent(mapId, 
              TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE));
          job.fetchFailuresMapping.remove(mapId);
        }
      }
    }
  }

  private static class TaskCompletedTransition implements
      MultipleArcTransition<JobImpl, JobEvent, JobState> {

    @Override
    public JobState transition(JobImpl job, JobEvent event) {
      job.completedTaskCount++;
      LOG.info("Num completed Tasks: " + job.completedTaskCount);
      JobTaskEvent taskEvent = (JobTaskEvent) event;
      Task task = job.tasks.get(taskEvent.getTaskID());
      if (taskEvent.getState() == TaskState.SUCCEEDED) {
        taskSucceeded(job, task);
      } else if (taskEvent.getState() == TaskState.FAILED) {
        taskFailed(job, task);
      } else if (taskEvent.getState() == TaskState.KILLED) {
        taskKilled(job, task);
      }

      return checkJobForCompletion(job);
    }

    protected JobState checkJobForCompletion(JobImpl job) {
      //check for Job failure
      if (job.failedMapTaskCount*100 > 
        job.allowedMapFailuresPercent*job.numMapTasks ||
        job.failedReduceTaskCount*100 > 
        job.allowedReduceFailuresPercent*job.numReduceTasks) {
        job.setFinishTime();

        String diagnosticMsg = "Job failed as tasks failed. " +
            "failedMaps:" + job.failedMapTaskCount + 
            " failedReduces:" + job.failedReduceTaskCount;
        LOG.info(diagnosticMsg);
        job.addDiagnostic(diagnosticMsg);
        job.abortJob(org.apache.hadoop.mapreduce.JobStatus.State.FAILED);
        return job.finished(JobState.FAILED);
      }
      
      JobState jobCompleteSuccess = JobImpl.checkJobCompleteSuccess(job);
      if (jobCompleteSuccess != null) {
        return jobCompleteSuccess;
      }
      
      //return the current state, Job not finished yet
      return job.getState();
    }

    private void taskSucceeded(JobImpl job, Task task) {
      if (task.getType() == TaskType.MAP) {
        job.succeededMapTaskCount++;
      } else {
        job.succeededReduceTaskCount++;
      }
      job.metrics.completedTask(task);
    }
  
    private void taskFailed(JobImpl job, Task task) {
      if (task.getType() == TaskType.MAP) {
        job.failedMapTaskCount++;
      } else if (task.getType() == TaskType.REDUCE) {
        job.failedReduceTaskCount++;
      }
      job.addDiagnostic("Task failed " + task.getID());
      job.metrics.failedTask(task);
    }

    private void taskKilled(JobImpl job, Task task) {
      if (task.getType() == TaskType.MAP) {
        job.killedMapTaskCount++;
      } else if (task.getType() == TaskType.REDUCE) {
        job.killedReduceTaskCount++;
      }
      job.metrics.killedTask(task);
    }
  }

  // Transition class for handling jobs with no tasks
  static class JobNoTasksCompletedTransition implements
  MultipleArcTransition<JobImpl, JobEvent, JobState> {

    @Override
    public JobState transition(JobImpl job, JobEvent event) {
      JobState jobCompleteSuccess = JobImpl.checkJobCompleteSuccess(job);
      if (jobCompleteSuccess != null) {
        return jobCompleteSuccess;
      }
      
      // Return the current state, Job not finished yet
      return job.getState();
    }
  }

  private static class MapTaskRescheduledTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      //succeeded map task is restarted back
      job.completedTaskCount--;
      job.succeededMapTaskCount--;
    }
  }

  private static class KillWaitTaskCompletedTransition extends  
      TaskCompletedTransition {
    @Override
    protected JobState checkJobForCompletion(JobImpl job) {
      if (job.completedTaskCount == job.tasks.size()) {
        job.setFinishTime();
        job.abortJob(org.apache.hadoop.mapreduce.JobStatus.State.KILLED);
        return job.finished(JobState.KILLED);
      }
      //return the current state, Job not finished yet
      return job.getState();
    }
  }

  private void addDiagnostic(String diag) {
    diagnostics.add(diag);
  }
  
  private static class DiagnosticsUpdateTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.addDiagnostic(((JobDiagnosticsUpdateEvent) event)
          .getDiagnosticUpdate());
    }
  }
  
  private static class CounterUpdateTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      JobCounterUpdateEvent jce = (JobCounterUpdateEvent) event;
      for (JobCounterUpdateEvent.CounterIncrementalUpdate ci : jce
          .getCounterUpdates()) {
        job.jobCounters.incrCounter(ci.getCounterKey(), ci.getIncrementValue());
      }
    }
  }

  private static class InternalErrorTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      //TODO Is this JH event required.
      job.setFinishTime();
      JobUnsuccessfulCompletionEvent failedEvent =
          new JobUnsuccessfulCompletionEvent(job.oldJobId,
              job.finishTime, 0, 0,
              JobState.ERROR.toString());
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, failedEvent));
      job.finished(JobState.ERROR);
    }
  }

}
