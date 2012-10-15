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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.WrappedProgressSplitsBlock;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.MapAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.ReduceAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStartedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app2.TaskHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app2.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobTaskAttemptFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventContainerTerminated;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventContainerTerminating;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventNodeFailed;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptRemoteStartEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptScheduleEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerTALaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerEventTAEnded;
import org.apache.hadoop.mapreduce.v2.app2.speculate.SpeculatorEvent;
import org.apache.hadoop.mapreduce.v2.app2.taskclean.TaskCleanupEvent;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.Records;

public abstract class TaskAttemptImpl implements TaskAttempt,
    EventHandler<TaskAttemptEvent> {
  
  private static final Log LOG = LogFactory.getLog(TaskAttemptImpl.class);
  private static final String LINE_SEPARATOR = System
      .getProperty("line.separator");
  
  static final Counters EMPTY_COUNTERS = new Counters();
  private static final long MEMORY_SPLITS_RESOLUTION = 1024; //TODO Make configurable?

  protected final JobConf conf;
  protected final Path jobFile;
  protected final int partition;
  @SuppressWarnings("rawtypes")
  protected EventHandler eventHandler;
  private final TaskAttemptId attemptId;
  private final TaskId taskId;
  private final JobId jobId;
  private final Clock clock;
//  private final TaskAttemptListener taskAttemptListener;
  private final OutputCommitter committer;
  private final Resource resourceCapability;
  private final String[] dataLocalHosts;
  private final List<String> diagnostics = new ArrayList<String>();
  private final Lock readLock;
  private final Lock writeLock;
  private final AppContext appContext;
  private final TaskHeartbeatHandler taskHeartbeatHandler;
  private Credentials credentials;
  private Token<JobTokenIdentifier> jobToken;
  private long launchTime = 0;
  private long finishTime = 0;
  private WrappedProgressSplitsBlock progressSplitBlock;
  private int shufflePort = -1;
  private String trackerName;
  private int httpPort;
  

  // TODO Can these be replaced by the container object ?
  private ContainerId containerId;
  private NodeId containerNodeId;
  private String containerMgrAddress;
  private String nodeHttpAddress;
  private String nodeRackName;

  private TaskAttemptStatus reportedStatus;
  
  private boolean speculatorContainerRequestSent = false;
  
  
  private static StateMachineFactory
      <TaskAttemptImpl, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
      stateMachineFactory
      = new StateMachineFactory
      <TaskAttemptImpl, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
      (TaskAttemptState.NEW);
  
  private static SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION;
  private static boolean stateMachineInited = false;
  private final StateMachine
      <TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent> stateMachine;
  
  // TODO XXX: Ensure MAPREDUCE-4457 is factored in. Also MAPREDUCE-4068.
  // TODO XXX: Rename all CONTAINER_COMPLETED transitions to TERMINATED.
  private void initStateMachine() {
    stateMachineFactory = 
    stateMachineFactory
        .addTransition(TaskAttemptState.NEW, TaskAttemptState.START_WAIT, TaskAttemptEventType.TA_SCHEDULE, createScheduleTransition())
        .addTransition(TaskAttemptState.NEW, TaskAttemptState.NEW, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptState.NEW, TaskAttemptState.FAILED, TaskAttemptEventType.TA_FAIL_REQUEST, createFailRequestTransition())
        .addTransition(TaskAttemptState.NEW, TaskAttemptState.KILLED, TaskAttemptEventType.TA_KILL_REQUEST, createKillRequestTransition())
        
        .addTransition(TaskAttemptState.START_WAIT, TaskAttemptState.RUNNING, TaskAttemptEventType.TA_STARTED_REMOTELY, createStartedTransition())
        .addTransition(TaskAttemptState.START_WAIT, TaskAttemptState.START_WAIT, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptState.START_WAIT, TaskAttemptState.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_FAIL_REQUEST, createFailRequestBeforeRunningTransition())
        .addTransition(TaskAttemptState.START_WAIT, TaskAttemptState.KILL_IN_PROGRESS, TaskAttemptEventType.TA_KILL_REQUEST, createKillRequestBeforeRunningTransition())
        .addTransition(TaskAttemptState.START_WAIT, TaskAttemptState.KILL_IN_PROGRESS, TaskAttemptEventType.TA_NODE_FAILED, createNodeFailedBeforeRunningTransition())
        .addTransition(TaskAttemptState.START_WAIT, TaskAttemptState.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_CONTAINER_TERMINATING, createContainerTerminatingBeforeRunningTransition())
        .addTransition(TaskAttemptState.START_WAIT, TaskAttemptState.FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATED, createContainerCompletedBeforeRunningTransition())
        
        .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.RUNNING, TaskAttemptEventType.TA_STATUS_UPDATE, createStatusUpdateTransition())
        .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.RUNNING, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.COMMIT_PENDING, TaskAttemptEventType.TA_COMMIT_PENDING, createCommitPendingTransition())
        .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.SUCCEEDED, TaskAttemptEventType.TA_DONE, createSucceededTransition())
        .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_FAILED, createFailRequestWhileRunningTransition())
        .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_TIMED_OUT, createFailRequestWhileRunningTransition())
        .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_FAIL_REQUEST, createFailRequestWhileRunningTransition())
        .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.KILL_IN_PROGRESS, TaskAttemptEventType.TA_KILL_REQUEST, createKillRequestWhileRunningTransition())
        .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.KILL_IN_PROGRESS, TaskAttemptEventType.TA_NODE_FAILED, createNodeFailedWhileRunningTransition())
        .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.RUNNING, TaskAttemptEventType.TA_CONTAINER_TERMINATING, createContainerTerminatingWhileRunningTransition())
        .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATED, createContainerCompletedWhileRunningTransition())
        
        // XXX Maybe move getMessage / getDiagnosticInfo into the base TaskAttemptEvent ?
        
        .addTransition(TaskAttemptState.COMMIT_PENDING, TaskAttemptState.COMMIT_PENDING, TaskAttemptEventType.TA_STATUS_UPDATE, createStatusUpdateTransition())
        .addTransition(TaskAttemptState.COMMIT_PENDING, TaskAttemptState.COMMIT_PENDING, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptState.COMMIT_PENDING, TaskAttemptState.COMMIT_PENDING, TaskAttemptEventType.TA_COMMIT_PENDING) // TODO ensure this is an ignorable event.
        .addTransition(TaskAttemptState.COMMIT_PENDING, TaskAttemptState.SUCCEEDED, TaskAttemptEventType.TA_DONE, createSucceededTransition())
        .addTransition(TaskAttemptState.COMMIT_PENDING, TaskAttemptState.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_FAILED, createFailRequestWhileRunningTransition())
        .addTransition(TaskAttemptState.COMMIT_PENDING, TaskAttemptState.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_TIMED_OUT, createFailRequestWhileRunningTransition())
        .addTransition(TaskAttemptState.COMMIT_PENDING, TaskAttemptState.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_FAIL_REQUEST, createFailRequestWhileRunningTransition())
        .addTransition(TaskAttemptState.COMMIT_PENDING, TaskAttemptState.KILL_IN_PROGRESS, TaskAttemptEventType.TA_KILL_REQUEST, createKillRequestWhileRunningTransition())
        .addTransition(TaskAttemptState.COMMIT_PENDING, TaskAttemptState.KILL_IN_PROGRESS, TaskAttemptEventType.TA_NODE_FAILED, createNodeFailedWhileRunningTransition())
        .addTransition(TaskAttemptState.COMMIT_PENDING, TaskAttemptState.RUNNING, TaskAttemptEventType.TA_CONTAINER_TERMINATING, createContainerTerminatingWhileRunningTransition())
        .addTransition(TaskAttemptState.COMMIT_PENDING, TaskAttemptState.FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATED, createContainerCompletedWhileRunningTransition())

        .addTransition(TaskAttemptState.KILL_IN_PROGRESS, TaskAttemptState.KILLED, TaskAttemptEventType.TA_CONTAINER_TERMINATED, createTerminatedTransition())
        .addTransition(TaskAttemptState.KILL_IN_PROGRESS, TaskAttemptState.KILL_IN_PROGRESS, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptState.KILL_IN_PROGRESS, TaskAttemptState.KILL_IN_PROGRESS, EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY, TaskAttemptEventType.TA_STATUS_UPDATE, TaskAttemptEventType.TA_COMMIT_PENDING, TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED, TaskAttemptEventType.TA_TIMED_OUT, TaskAttemptEventType.TA_FAIL_REQUEST, TaskAttemptEventType.TA_KILL_REQUEST, TaskAttemptEventType.TA_NODE_FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATING))
        
        .addTransition(TaskAttemptState.FAIL_IN_PROGRESS, TaskAttemptState.FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATED, createTerminatedTransition())
        .addTransition(TaskAttemptState.FAIL_IN_PROGRESS, TaskAttemptState.FAIL_IN_PROGRESS, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptState.FAIL_IN_PROGRESS, TaskAttemptState.FAIL_IN_PROGRESS, EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY, TaskAttemptEventType.TA_STATUS_UPDATE, TaskAttemptEventType.TA_COMMIT_PENDING, TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED, TaskAttemptEventType.TA_TIMED_OUT, TaskAttemptEventType.TA_FAIL_REQUEST, TaskAttemptEventType.TA_KILL_REQUEST, TaskAttemptEventType.TA_NODE_FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATING))
        
        .addTransition(TaskAttemptState.KILLED, TaskAttemptState.KILLED, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptState.KILLED, TaskAttemptState.KILLED, EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY, TaskAttemptEventType.TA_STATUS_UPDATE, TaskAttemptEventType.TA_COMMIT_PENDING, TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED, TaskAttemptEventType.TA_FAIL_REQUEST, TaskAttemptEventType.TA_KILL_REQUEST, TaskAttemptEventType.TA_NODE_FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATING, TaskAttemptEventType.TA_CONTAINER_TERMINATED))

        .addTransition(TaskAttemptState.FAILED, TaskAttemptState.FAILED, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptState.FAILED, TaskAttemptState.FAILED, EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY, TaskAttemptEventType.TA_STATUS_UPDATE, TaskAttemptEventType.TA_COMMIT_PENDING, TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED, TaskAttemptEventType.TA_FAIL_REQUEST, TaskAttemptEventType.TA_KILL_REQUEST, TaskAttemptEventType.TA_NODE_FAILED, TaskAttemptEventType.TA_CONTAINER_TERMINATING, TaskAttemptEventType.TA_CONTAINER_TERMINATED))
        
        // TODO XXX: FailRequest / KillRequest at SUCCEEDED need to consider Map / Reduce task.
        .addTransition(TaskAttemptState.SUCCEEDED, TaskAttemptState.SUCCEEDED, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        .addTransition(TaskAttemptState.SUCCEEDED, TaskAttemptState.FAILED, TaskAttemptEventType.TA_FAIL_REQUEST, createFailRequestAfterSuccessTransition())
        .addTransition(TaskAttemptState.SUCCEEDED, TaskAttemptState.KILLED, TaskAttemptEventType.TA_KILL_REQUEST, createKillRequestAfterSuccessTransition())
        .addTransition(TaskAttemptState.SUCCEEDED, TaskAttemptState.KILLED, TaskAttemptEventType.TA_NODE_FAILED, createNodeFailedAfterSuccessTransition())
        .addTransition(TaskAttemptState.SUCCEEDED, TaskAttemptState.FAILED, TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURES, createTooManyFetchFailuresTransition())
        .addTransition(TaskAttemptState.SUCCEEDED, TaskAttemptState.SUCCEEDED, EnumSet.of(TaskAttemptEventType.TA_CONTAINER_TERMINATED, TaskAttemptEventType.TA_TIMED_OUT, TaskAttemptEventType.TA_CONTAINER_TERMINATING))
        
        
        .installTopology();
  }

  // TODO Remove TaskAttemptListener from the constructor.
  @SuppressWarnings("rawtypes")
  public TaskAttemptImpl(TaskId taskId, int i, EventHandler eventHandler,
      TaskAttemptListener tal, Path jobFile, int partition, JobConf conf,
      String[] dataLocalHosts, OutputCommitter committer,
      Token<JobTokenIdentifier> jobToken, Credentials credentials, Clock clock,
      TaskHeartbeatHandler taskHeartbeatHandler, AppContext appContext) {
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.taskId = taskId;
    this.jobId = taskId.getJobId();
    this.attemptId = MRBuilderUtils.newTaskAttemptId(taskId, i);
    this.eventHandler = eventHandler;
    //Reported status
    this.jobFile = jobFile;
    this.partition = partition;
    this.conf = conf;
    this.dataLocalHosts = dataLocalHosts;
    this.committer = committer;
    this.jobToken = jobToken;
    this.credentials = credentials;
    this.clock = clock;
    this.taskHeartbeatHandler = taskHeartbeatHandler;
    this.appContext = appContext;
    this.resourceCapability = BuilderUtils.newResource(getMemoryRequired(conf,
        taskId.getTaskType()));
    this.reportedStatus = new TaskAttemptStatus();
    initTaskAttemptStatus(reportedStatus);
    RackResolver.init(conf);
    synchronized(stateMachineFactory) {
      if (!stateMachineInited) {
        LOG.info("XXX: Initializing State Machine Factory");
        DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION = createDiagnosticUpdateTransition();
        initStateMachine();
        stateMachineInited = true;
      }
    }
    this.stateMachine = stateMachineFactory.make(this);
  }
  
  
  

  @Override
  public TaskAttemptId getID() {
    return attemptId;
  }

  protected abstract org.apache.hadoop.mapred.Task createRemoteTask();
  
  @Override
  public TaskAttemptReport getReport() {
    TaskAttemptReport result = Records.newRecord(TaskAttemptReport.class);
    readLock.lock();
    try {
      result.setTaskAttemptId(attemptId);
      //take the LOCAL state of attempt
      //DO NOT take from reportedStatus
      
      result.setTaskAttemptState(getState());
      result.setProgress(reportedStatus.progress);
      result.setStartTime(launchTime);
      result.setFinishTime(finishTime);
      result.setShuffleFinishTime(this.reportedStatus.shuffleFinishTime);
      result.setDiagnosticInfo(StringUtils.join(LINE_SEPARATOR, getDiagnostics()));
      result.setPhase(reportedStatus.phase);
      result.setStateString(reportedStatus.stateString);
      result.setCounters(TypeConverter.toYarn(getCounters()));
      result.setContainerId(this.getAssignedContainerID());
      result.setNodeManagerHost(trackerName);
      result.setNodeManagerHttpPort(httpPort);
      if (this.containerNodeId != null) {
        result.setNodeManagerPort(this.containerNodeId.getPort());
      }
      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<String> getDiagnostics() {
    List<String> result = new ArrayList<String>();
    readLock.lock();
    try {
      result.addAll(diagnostics);
      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Counters getCounters() {
    readLock.lock();
    try {
      Counters counters = reportedStatus.counters;
      if (counters == null) {
        counters = EMPTY_COUNTERS;
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
      return reportedStatus.progress;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskAttemptState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean isFinished() {
    readLock.lock();
    try {
      // TODO: Use stateMachine level method?
      return (getState() == TaskAttemptState.SUCCEEDED || 
          getState() == TaskAttemptState.FAILED ||
          getState() == TaskAttemptState.KILLED);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ContainerId getAssignedContainerID() {
    readLock.lock();
    try {
      return containerId;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String getAssignedContainerMgrAddress() {
    readLock.lock();
    try {
      return containerMgrAddress;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public NodeId getNodeId() {
    readLock.lock();
    try {
      return containerNodeId;
    } finally {
      readLock.unlock();
    }
  }

  /**If container Assigned then return the node's address, otherwise null.
   */
  @Override
  public String getNodeHttpAddress() {
    readLock.lock();
    try {
      return nodeHttpAddress;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * If container Assigned then return the node's rackname, otherwise null.
   */
  @Override
  public String getNodeRackName() {
    this.readLock.lock();
    try {
      return this.nodeRackName;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public long getLaunchTime() {
    readLock.lock();
    try {
      return launchTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getFinishTime() {
    readLock.lock();
    try {
      return finishTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getShuffleFinishTime() {
    readLock.lock();
    try {
      return this.reportedStatus.shuffleFinishTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getSortFinishTime() {
    readLock.lock();
    try {
      return this.reportedStatus.sortFinishTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getShufflePort() {
    readLock.lock();
    try {
      return shufflePort;
    } finally {
      readLock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handle(TaskAttemptEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getTaskAttemptID() + " of type "
          + event.getType());
    }
    LOG.info("XXX: Processing " + event.getTaskAttemptID() + " of type "
        + event.getType() + " while in state: " + getState());
    writeLock.lock();
    try {
      final TaskAttemptState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state for "
            + this.attemptId, e);
        eventHandler.handle(new JobDiagnosticsUpdateEvent(
            this.attemptId.getTaskId().getJobId(), "Invalid event " + event.getType() + 
            " on TaskAttempt " + this.attemptId));
        eventHandler.handle(new JobEvent(this.attemptId.getTaskId().getJobId(),
            JobEventType.INTERNAL_ERROR));
      }
      if (oldState != getState()) {
          LOG.info(attemptId + " TaskAttempt Transitioned from " 
           + oldState + " to "
           + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    this.eventHandler.handle(event);
  }

  private int getMemoryRequired(Configuration conf, TaskType taskType) {
    int memory = 1024;
    if (taskType == TaskType.MAP)  {
      memory =
          conf.getInt(MRJobConfig.MAP_MEMORY_MB,
              MRJobConfig.DEFAULT_MAP_MEMORY_MB);
    } else if (taskType == TaskType.REDUCE) {
      memory =
          conf.getInt(MRJobConfig.REDUCE_MEMORY_MB,
              MRJobConfig.DEFAULT_REDUCE_MEMORY_MB);
    }
    
    return memory;
  }
  
  // always called in write lock
  private void setFinishTime() {
    // set the finish time only if launch time is set
    if (launchTime != 0) {
      finishTime = clock.getTime();
    }
  }

  // TOOD Merge some of these JobCounter events.
  private static JobCounterUpdateEvent createJobCounterUpdateEventTALaunched(
      TaskAttemptImpl ta) {
    JobCounterUpdateEvent jce = new JobCounterUpdateEvent(ta.jobId);
    jce.addCounterUpdate(
        ta.taskId.getTaskType() == TaskType.MAP ? JobCounter.TOTAL_LAUNCHED_MAPS
            : JobCounter.TOTAL_LAUNCHED_REDUCES, 1);
    return jce;
  }

  private static JobCounterUpdateEvent createJobCounterUpdateEventSlotMillis(
      TaskAttemptImpl ta) {
    JobCounterUpdateEvent jce = new JobCounterUpdateEvent(ta.jobId);
    long slotMillis = computeSlotMillis(ta);
    jce.addCounterUpdate(
        ta.taskId.getTaskType() == TaskType.MAP ? JobCounter.SLOTS_MILLIS_MAPS
            : JobCounter.SLOTS_MILLIS_REDUCES, slotMillis);
    return jce;
  }

  private static JobCounterUpdateEvent createJobCounterUpdateEventTATerminated(
      TaskAttemptImpl taskAttempt, boolean taskAlreadyCompleted,
      TaskAttemptState taState) {
    TaskType taskType = taskAttempt.getID().getTaskId().getTaskType();
    JobCounterUpdateEvent jce = new JobCounterUpdateEvent(taskAttempt.getID()
        .getTaskId().getJobId());

    long slotMillisIncrement = computeSlotMillis(taskAttempt);

    if (taskType == TaskType.MAP) {
      if (taState == TaskAttemptState.FAILED) {
        jce.addCounterUpdate(JobCounter.NUM_FAILED_MAPS, 1);
      } else if (taState == TaskAttemptState.KILLED) {
        jce.addCounterUpdate(JobCounter.NUM_KILLED_MAPS, 1);
      }
      if (!taskAlreadyCompleted) {
        // dont double count the elapsed time
        jce.addCounterUpdate(JobCounter.SLOTS_MILLIS_MAPS, slotMillisIncrement);
      }
    } else {
      if (taState == TaskAttemptState.FAILED) {
        jce.addCounterUpdate(JobCounter.NUM_FAILED_REDUCES, 1);
      } else if (taState == TaskAttemptState.KILLED) {
        jce.addCounterUpdate(JobCounter.NUM_KILLED_REDUCES, 1);
      }
      if (!taskAlreadyCompleted) {
        // dont double count the elapsed time
        jce.addCounterUpdate(JobCounter.SLOTS_MILLIS_REDUCES,
            slotMillisIncrement);
      }
    }
    return jce;
  }

  private static long computeSlotMillis(TaskAttemptImpl taskAttempt) {
    TaskType taskType = taskAttempt.getID().getTaskId().getTaskType();
    int slotMemoryReq =
        taskAttempt.getMemoryRequired(taskAttempt.conf, taskType);

    int minSlotMemSize =
        taskAttempt.appContext.getClusterInfo().getMinContainerCapability()
            .getMemory();

    int simSlotsRequired =
        minSlotMemSize == 0 ? 0 : (int) Math.ceil((float) slotMemoryReq
            / minSlotMemSize);

    long slotMillisIncrement =
        simSlotsRequired
            * (taskAttempt.getFinishTime() - taskAttempt.getLaunchTime());
    return slotMillisIncrement;
  }

  // TODO Change to return a JobHistoryEvent.
  private static
      TaskAttemptUnsuccessfulCompletionEvent
  createTaskAttemptUnsuccessfulCompletionEvent(TaskAttemptImpl taskAttempt,
      TaskAttemptState attemptState) {
    TaskAttemptUnsuccessfulCompletionEvent tauce =
    new TaskAttemptUnsuccessfulCompletionEvent(
        TypeConverter.fromYarn(taskAttempt.attemptId),
        TypeConverter.fromYarn(taskAttempt.attemptId.getTaskId()
            .getTaskType()), attemptState.toString(),
        taskAttempt.finishTime,
        taskAttempt.containerNodeId == null ? "UNKNOWN"
            : taskAttempt.containerNodeId.getHost(),
        taskAttempt.containerNodeId == null ? -1 
            : taskAttempt.containerNodeId.getPort(),    
        taskAttempt.nodeRackName == null ? "UNKNOWN" 
            : taskAttempt.nodeRackName,
        StringUtils.join(
            LINE_SEPARATOR, taskAttempt.getDiagnostics()), taskAttempt
            .getProgressSplitBlock().burst());
    return tauce;
  }
  
  private JobHistoryEvent createTaskAttemptStartedEvent() {
    TaskAttemptStartedEvent tase = new TaskAttemptStartedEvent(
        TypeConverter.fromYarn(attemptId), TypeConverter.fromYarn(taskId
            .getTaskType()), launchTime, trackerName, httpPort, shufflePort,
        containerId);
    return new JobHistoryEvent(jobId, tase);

  }

  private WrappedProgressSplitsBlock getProgressSplitBlock() {
    readLock.lock();
    try {
      if (progressSplitBlock == null) {
        progressSplitBlock = new WrappedProgressSplitsBlock(conf.getInt(
            MRJobConfig.MR_AM_NUM_PROGRESS_SPLITS,
            MRJobConfig.DEFAULT_MR_AM_NUM_PROGRESS_SPLITS));
      }
      return progressSplitBlock;
    } finally {
      readLock.unlock();
    }
  }
  
  private void updateProgressSplits() {
    double newProgress = reportedStatus.progress;
    newProgress = Math.max(Math.min(newProgress, 1.0D), 0.0D);
    Counters counters = reportedStatus.counters;
    if (counters == null)
      return;

    WrappedProgressSplitsBlock splitsBlock = getProgressSplitBlock();
    if (splitsBlock != null) {
      long now = clock.getTime();
      long start = getLaunchTime();
      
      if (start == 0)
        return;

      if (start != 0 && now - start <= Integer.MAX_VALUE) {
        splitsBlock.getProgressWallclockTime().extend(newProgress,
            (int) (now - start));
      }

      Counter cpuCounter = counters.findCounter(TaskCounter.CPU_MILLISECONDS);
      if (cpuCounter != null && cpuCounter.getValue() <= Integer.MAX_VALUE) {
        splitsBlock.getProgressCPUTime().extend(newProgress,
            (int) cpuCounter.getValue()); // long to int? TODO: FIX. Same below
      }

      Counter virtualBytes = counters
        .findCounter(TaskCounter.VIRTUAL_MEMORY_BYTES);
      if (virtualBytes != null) {
        splitsBlock.getProgressVirtualMemoryKbytes().extend(newProgress,
            (int) (virtualBytes.getValue() / (MEMORY_SPLITS_RESOLUTION)));
      }

      Counter physicalBytes = counters
        .findCounter(TaskCounter.PHYSICAL_MEMORY_BYTES);
      if (physicalBytes != null) {
        splitsBlock.getProgressPhysicalMemoryKbytes().extend(newProgress,
            (int) (physicalBytes.getValue() / (MEMORY_SPLITS_RESOLUTION)));
      }
    }
  }
  
  @SuppressWarnings({ "unchecked" })
  private void logAttemptFinishedEvent(TaskAttemptState state) {
    //Log finished events only if an attempt started.
    if (getLaunchTime() == 0) return; 
    if (attemptId.getTaskId().getTaskType() == TaskType.MAP) {
      MapAttemptFinishedEvent mfe =
         new MapAttemptFinishedEvent(TypeConverter.fromYarn(attemptId),
         TypeConverter.fromYarn(attemptId.getTaskId().getTaskType()),
         state.toString(),
         this.reportedStatus.mapFinishTime,
         finishTime,
         this.containerNodeId == null ? "UNKNOWN"
             : this.containerNodeId.getHost(),
         this.containerNodeId == null ? -1 : this.containerNodeId.getPort(),
         this.nodeRackName == null ? "UNKNOWN" : this.nodeRackName,
         this.reportedStatus.stateString,
         getCounters(),
         getProgressSplitBlock().burst());
         eventHandler.handle(
           new JobHistoryEvent(attemptId.getTaskId().getJobId(), mfe));
    } else {
       ReduceAttemptFinishedEvent rfe =
         new ReduceAttemptFinishedEvent(TypeConverter.fromYarn(attemptId),
         TypeConverter.fromYarn(attemptId.getTaskId().getTaskType()),
         state.toString(),
         this.reportedStatus.shuffleFinishTime,
         this.reportedStatus.sortFinishTime,
         finishTime,
         this.containerNodeId == null ? "UNKNOWN"
             : this.containerNodeId.getHost(),
         this.containerNodeId == null ? -1 : this.containerNodeId.getPort(),
         this.nodeRackName == null ? "UNKNOWN" : this.nodeRackName,
         this.reportedStatus.stateString,
         getCounters(),
         getProgressSplitBlock().burst());
         eventHandler.handle(
           new JobHistoryEvent(attemptId.getTaskId().getJobId(), rfe));
    }
  }

  private void maybeSendSpeculatorContainerRequest() {
    if (!speculatorContainerRequestSent) {
      sendEvent(new SpeculatorEvent(taskId, +1));
      speculatorContainerRequestSent = true;
    }
  }

  private void maybeSendSpeculatorContainerRelease() {
    if (speculatorContainerRequestSent) {
      sendEvent(new SpeculatorEvent(taskId, -1));
      speculatorContainerRequestSent = false;
    }
  }
  
  private void sendTaskAttemptCleanupEvent() {
    TaskAttemptContext taContext = new TaskAttemptContextImpl(this.conf,
        TypeConverter.fromYarn(this.attemptId));
    sendEvent(new TaskCleanupEvent(this.attemptId, this.committer, taContext));
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> 
      createScheduleTransition() {
    return new ScheduleTaskattempt();
  }

  protected static class ScheduleTaskattempt implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptScheduleEvent scheduleEvent = (TaskAttemptScheduleEvent) event;
      // Event to speculator - containerNeeded++
      // TODO How does the speculator handle this.. should it be going out from
      // the container instead.
      ta.maybeSendSpeculatorContainerRequest();

      // TODO XXX: Creating the remote task here may not be required in case of recovery.
      // Could be solved by having the Scheduler pull the RemoteTask.
      
      // Create the remote task.
      org.apache.hadoop.mapred.Task remoteTask = ta.createRemoteTask();
      // Create startTaskRequest

      String[] hostArray;
      String[] rackArray;
      if (scheduleEvent.isRescheduled()) {
        // No node/rack locality.
        hostArray = new String[0];
        rackArray = new String[0];
      } else {
        // Ask for node / rack locality.
        Set<String> racks = new HashSet<String>();
        for (String host : ta.dataLocalHosts) {
          racks.add(RackResolver.resolve(host).getNetworkLocation());
        }
        hostArray = TaskAttemptImplHelpers.resolveHosts(ta.dataLocalHosts);
        rackArray = racks.toArray(new String[racks.size()]);
      }

      // Send out a launch request to the scheduler.
      AMSchedulerTALaunchRequestEvent launchRequestEvent = new AMSchedulerTALaunchRequestEvent(
          ta.attemptId, scheduleEvent.isRescheduled(), ta.resourceCapability,
          remoteTask, ta, ta.credentials, ta.jobToken, hostArray, rackArray);
      ta.sendEvent(launchRequestEvent);
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> 
      createDiagnosticUpdateTransition() {
    return new DiagnosticInformationUpdater();
  }

  protected static class DiagnosticInformationUpdater implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptDiagnosticsUpdateEvent diagEvent = 
          (TaskAttemptDiagnosticsUpdateEvent) event;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Diagnostics update for " + ta.attemptId + ": "
            + diagEvent.getDiagnosticInfo());
      }
      ta.addDiagnosticInfo(diagEvent.getDiagnosticInfo());
    }
  }

  private void addDiagnosticInfo(String diag) {
    if (diag != null && !diag.equals("")) {
      diagnostics.add(diag);
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> 
      createFailRequestTransition() {
    return new FailRequest();
  }

  // TODO XXX: FailRequest == KillRequest.
  protected static class FailRequest implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      // TODO XXX -> Remove this comment after getting to CompletedEvent processing.
      // TODO XXX move this out into a helper method. CompletedEvents should not
      // be extending Failed events.
      //set the finish time
      ta.setFinishTime();

      ta.sendEvent(createJobCounterUpdateEventTATerminated(ta, false,
          TaskAttemptState.FAILED));
      if (ta.getLaunchTime() != 0) {
        // TODO XXX: For cases like this, recovery goes for a toss, since the the attempt will not exist in the history file.
        ta.sendEvent(new JobHistoryEvent(ta.jobId,
            createTaskAttemptUnsuccessfulCompletionEvent(ta,
                TaskAttemptState.FAILED)));
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Not generating HistoryFinish event since start event not " +
              "generated for taskAttempt: " + ta.getID());
        }
      }
      // Send out events to the Task - indicating TaskAttemptFailure.
      ta.sendEvent(new TaskTAttemptEvent(ta.attemptId,
          TaskEventType.T_ATTEMPT_FAILED));
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> 
      createKillRequestTransition() {
    return new KillRequest();
  }
  
  // TODO: Identical to TAFailRequest except for the states.. Merge together.
  protected static class KillRequest implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
    //set the finish time
      ta.setFinishTime();

      ta.sendEvent(createJobCounterUpdateEventTATerminated(ta, false,
          TaskAttemptState.KILLED));
      if (ta.getLaunchTime() != 0) {
        ta.sendEvent(new JobHistoryEvent(ta.jobId,
            createTaskAttemptUnsuccessfulCompletionEvent(ta,
                TaskAttemptState.KILLED)));
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Not generating HistoryFinish event since start event not " +
              "generated for taskAttempt: " + ta.getID());
        }
      }
      // Send out events to the Task - indicating TaskAttemptFailure.
      ta.sendEvent(new TaskTAttemptEvent(ta.attemptId,
          TaskEventType.T_ATTEMPT_KILLED));
    }
  }
  
  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> 
      createStartedTransition() {
    return new Started();
  }
  
  protected static class Started implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent origEvent) {
      TaskAttemptRemoteStartEvent event = (TaskAttemptRemoteStartEvent) origEvent;

      // TODO XXX What all changes here after CLC construction is done. Remove TODOs after that.
      
      Container container = ta.appContext.getAllContainers()
          .get(event.getContainerId()).getContainer();
      
      ta.containerId = event.getContainerId();
      ta.containerNodeId = container.getNodeId();
      ta.containerMgrAddress = ta.containerNodeId.toString();
      ta.nodeHttpAddress = container.getNodeHttpAddress();
      ta.nodeRackName = RackResolver.resolve(ta.containerNodeId.getHost())
          .getNetworkLocation();
      // TODO ContainerToken not required in TA.
      // TODO assignedCapability not required in TA.
      // TODO jvmId only required if TAL registration happens here.
      // TODO Anything to be done with the TaskAttemptListener ? or is that in
      // the Container.
 
      ta.launchTime = ta.clock.getTime();
      ta.shufflePort = event.getShufflePort();

      // TODO Resolve to host / IP in case of a local address.
      InetSocketAddress nodeHttpInetAddr = NetUtils
          .createSocketAddr(ta.nodeHttpAddress); // TODO: Costly?
      ta.trackerName = nodeHttpInetAddr.getHostName();
      ta.httpPort = nodeHttpInetAddr.getPort();
      ta.sendEvent(createJobCounterUpdateEventTALaunched(ta));
      
      LOG.info("TaskAttempt: [" + ta.attemptId+ "] started."
          + " Is using containerId: [" + ta.containerId + "]"
          + " on NM: [" + ta.containerMgrAddress + "]");

      // JobHistoryEvent
      ta.sendEvent(ta.createTaskAttemptStartedEvent());

      // Inform the speculator about the container assignment.
      ta.maybeSendSpeculatorContainerRelease();
      // Inform speculator about startTime
      ta.sendEvent(new SpeculatorEvent(ta.attemptId, true, ta.launchTime));

      // Inform the Task
      ta.sendEvent(new TaskTAttemptEvent(ta.attemptId,
          TaskEventType.T_ATTEMPT_LAUNCHED));

      ta.taskHeartbeatHandler.register(ta.attemptId);
    }
  }
  
  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      createFailRequestBeforeRunningTransition() {
    return new FailRequestBeforeRunning();
  }

  // TODO XXX: Rename FailRequest / KillRequest*BEFORE*Running
  // TODO Again, can failReqeust and KillRequest be merged ?
  protected static class FailRequestBeforeRunning extends FailRequest {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      // Inform the scheduler
      ta.sendEvent(new AMSchedulerEventTAEnded(ta.attemptId,
          TaskAttemptState.FAILED));
      // Decrement speculator container request.
      ta.maybeSendSpeculatorContainerRelease();
      
      // TODO XXX. AnyCounterUpdates ? KILL/FAIL count. Slot_Millis, etc
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> 
      createKillRequestBeforeRunningTransition() {
    return new KillRequestBeforeRunning();
  }

  // TODO XXX: Identical to FailRequestWhileRunning except for states.
  protected static class KillRequestBeforeRunning extends KillRequest {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      // XXX Remove Comment: Takes care of finish time, history, TaskEvent.
      super.transition(ta, event);
      // Inform the scheduler
      ta.sendEvent(new AMSchedulerEventTAEnded(ta.attemptId,
          TaskAttemptState.KILLED));
      // Decrement speculator container request.
      ta.maybeSendSpeculatorContainerRelease();
      
      // TODO XXX. AnyCounterUpdates ? KILL/FAIL count. Slot_Millis, etc
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      createNodeFailedBeforeRunningTransition() {
    return new NodeFailedBeforeRunning();
  }

  protected static class NodeFailedBeforeRunning extends
      KillRequestBeforeRunning {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptEventNodeFailed nfEvent = (TaskAttemptEventNodeFailed) event;
      ta.addDiagnosticInfo(nfEvent.getDiagnosticInfo());
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      createContainerTerminatingBeforeRunningTransition() {
    return new ContainerTerminatingBeforeRunning();
  }

  protected static class ContainerTerminatingBeforeRunning extends
      FailRequestBeforeRunning {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      TaskAttemptEventContainerTerminating tEvent = 
          (TaskAttemptEventContainerTerminating) event;
      ta.addDiagnosticInfo(tEvent.getDiagnosticInfo());
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> 
      createContainerCompletedBeforeRunningTransition() {
    return new ContainerCompletedBeforeRunning();
  }

  protected static class ContainerCompletedBeforeRunning extends
      FailRequestBeforeRunning {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.sendTaskAttemptCleanupEvent();

      TaskAttemptEventContainerTerminated tEvent =
          (TaskAttemptEventContainerTerminated) event;
      ta.addDiagnosticInfo(tEvent.getDiagnosticInfo());

      // TODO XXX: Maybe other counters: Failed, Killed, etc.
    }
  }

  protected static SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> 
      createStatusUpdateTransition() {
    return new StatusUpdater();
  }

  protected static class StatusUpdater implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptStatus newReportedStatus = ((TaskAttemptStatusUpdateEvent) event)
          .getReportedTaskAttemptStatus();
      ta.reportedStatus = newReportedStatus;
      ta.reportedStatus.taskState = ta.getState();

      // Inform speculator of status.
      ta.sendEvent(new SpeculatorEvent(ta.reportedStatus, ta.clock.getTime()));

      ta.updateProgressSplits();

      // Inform the job about fetch failures if they exist.
      if (ta.reportedStatus.fetchFailedMaps != null
          && ta.reportedStatus.fetchFailedMaps.size() > 0) {
        ta.sendEvent(new JobTaskAttemptFetchFailureEvent(ta.attemptId,
            ta.reportedStatus.fetchFailedMaps));
      }
      // TODO at some point. Nodes may be interested in FetchFailure info.
      // Can be used to blacklist nodes.
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      createCommitPendingTransition() {
    return new CommitPendingHandler();
  }

  protected static class CommitPendingHandler implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      // Inform the task that the attempt wants to commit.
      ta.sendEvent(new TaskTAttemptEvent(ta.attemptId,
          TaskEventType.T_ATTEMPT_COMMIT_PENDING));
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> 
      createSucceededTransition() {
    return new Succeeded();
  }

  protected static class Succeeded implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      
      
      
      //Inform the speculator. Generate history event. Update counters.
      ta.setFinishTime();
      ta.sendEvent(new SpeculatorEvent(ta.reportedStatus, ta.finishTime));
      ta.logAttemptFinishedEvent(TaskAttemptState.SUCCEEDED);
      ta.sendEvent(createJobCounterUpdateEventSlotMillis(ta));

      // Inform the Scheduler.
      ta.sendEvent(new AMSchedulerEventTAEnded(ta.attemptId,
          TaskAttemptState.SUCCEEDED));
      
      // Inform the task.
      ta.sendEvent(new TaskTAttemptEvent(ta.attemptId,
          TaskEventType.T_ATTEMPT_SUCCEEDED));
      
      //Unregister from the TaskHeartbeatHandler.
      ta.taskHeartbeatHandler.unregister(ta.attemptId);

      // TODO maybe. For reuse ... Stacking pulls for a reduce task, even if the
      // TA finishes independently. // Will likely be the Job's responsibility.
      
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> 
    createFailRequestWhileRunningTransition() {
    return new FailRequestWhileRunning();
  }

  protected static class FailRequestWhileRunning extends
      FailRequestBeforeRunning {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.taskHeartbeatHandler.unregister(ta.attemptId);
      // TODO Speculator does not need to go out. maybeSend... will take care of this for now.
    }
  }
  
  // TODO XXX: Remove and merge with FailRequestWhileRunning
  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      createKillRequestWhileRunningTransition() {
    return new KillRequestWhileRunning();
  }

  protected static class KillRequestWhileRunning extends
      KillRequestBeforeRunning {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.taskHeartbeatHandler.unregister(ta.attemptId);
      // TODO Speculator does not need to go out. maybeSend... will take care of this for now.
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      createNodeFailedWhileRunningTransition() {
    return new NodeFailedWhileRunning();
  }

  protected static class NodeFailedWhileRunning extends FailRequestWhileRunning {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptEventNodeFailed nfEvent = (TaskAttemptEventNodeFailed) event;
      ta.addDiagnosticInfo(nfEvent.getDiagnosticInfo());
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      createContainerTerminatingWhileRunningTransition() {
    return new ContainerTerminatingWhileRunning();
  }

  protected static class ContainerTerminatingWhileRunning extends
      FailRequestWhileRunning {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      TaskAttemptEventContainerTerminating tEvent =
          (TaskAttemptEventContainerTerminating) event;
      ta.addDiagnosticInfo(tEvent.getDiagnosticInfo());
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      createContainerCompletedWhileRunningTransition() {
    return new ContaienrCompletedWhileRunning();
  }

  protected static class ContaienrCompletedWhileRunning extends
      FailRequestWhileRunning {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.sendTaskAttemptCleanupEvent();
      TaskAttemptEventContainerTerminated tEvent =
          (TaskAttemptEventContainerTerminated) event;
      ta.addDiagnosticInfo(tEvent.getDiagnosticInfo());
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      createTerminatedTransition() {
    return new Terminated();
  }

  protected static class Terminated implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      ta.sendTaskAttemptCleanupEvent();
      TaskAttemptEventContainerTerminated tEvent =
          (TaskAttemptEventContainerTerminated) event;
      ta.addDiagnosticInfo(tEvent.getDiagnosticInfo());
    }

  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      createFailRequestAfterSuccessTransition() {
    return new FailRequestAfterSuccess();
  }

  protected static class FailRequestAfterSuccess extends FailRequest {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.sendTaskAttemptCleanupEvent();
      // XXX: This may need some additional handling.
      // TaskAttempt failed after SUCCESS -> the container should also be STOPPED if it's still RUNNING.
      // Inform the Scheduler.
      // XXX: Also maybe change the ougoing Task Attempt to be FETCH_FAILURE related.
      // TODO XXX: Any counter updates. 
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> 
      createKillRequestAfterSuccessTransition() {
    return new KillRequestAfterSuccess();
  }

  protected static class KillRequestAfterSuccess extends KillRequest {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      // TODO XXX (Comments Not really required) Check for this being a MAP task only. Otherwise ignore it.
      //... It may be possible for the event to come in for a REDUCE task, since
      // this event and the DONE event are generated in separate threads. Ignore
      // in that case.
      // TODO Handle diagnostics info.
      ta.sendTaskAttemptCleanupEvent();
    }
  }
  
  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      createNodeFailedAfterSuccessTransition() {
    return new NodeFailedAfterSuccess();
  }
  
  protected static class NodeFailedAfterSuccess extends KillRequestAfterSuccess {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptEventNodeFailed nfEvent = (TaskAttemptEventNodeFailed) event;
      ta.addDiagnosticInfo(nfEvent.getDiagnosticInfo());
    }
  }

  protected SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> 
      createTooManyFetchFailuresTransition() {
    return new TooManyFetchFailures();
  }

  protected static class TooManyFetchFailures extends FailRequestAfterSuccess {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      // TODO Maybe change this to send out a TaskEvent.TOO_MANY_FETCH_FAILURES.
      ta.addDiagnosticInfo("Too many fetch failures. Failing the attempt");
      super.transition(ta, event);
    }
  }
  
  private void initTaskAttemptStatus(TaskAttemptStatus result) {
    result.progress = 0.0f;
    result.phase = Phase.STARTING;
    result.stateString = "NEW";
    result.taskState = TaskAttemptState.NEW;
    Counters counters = EMPTY_COUNTERS;
    //    counters.groups = new HashMap<String, CounterGroup>();
    result.counters = counters;
  }

  // TODO Consolidate all the Failed / Killed methods which only differ in state.
  // Move some of the functionality out to helpers, instead of extending non-related event classes.
  
  // TODO. The transition classes / methods may need to be public for testing.
  // Leaving the return type as SingleArcTransition - so that extension is not required, when testing. 
  // Extension doesn't help anything iac.

  
  // TODO Can all these create* methods be made more generic...
}
