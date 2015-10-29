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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobInfoChangeEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobInitedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobQueueChangeEvent;
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
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterJobAbortEvent;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterJobCommitEvent;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterJobSetupEvent;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobAbortCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCommitFailedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobSetupFailedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobStartEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobUpdatedNodesEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptTooManyFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskRecoverEvent;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/** Implementation of Job interface. Maintains the state machines of Job.
 * The read and write calls use ReadWriteLock for concurrency.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class JobImpl implements org.apache.hadoop.mapreduce.v2.app.job.Job, 
  EventHandler<JobEvent> {

  private static final TaskAttemptCompletionEvent[]
    EMPTY_TASK_ATTEMPT_COMPLETION_EVENTS = new TaskAttemptCompletionEvent[0];

  private static final TaskCompletionEvent[]
    EMPTY_TASK_COMPLETION_EVENTS = new TaskCompletionEvent[0];

  private static final Log LOG = LogFactory.getLog(JobImpl.class);

  //The maximum fraction of fetch failures allowed for a map
  private float maxAllowedFetchFailuresFraction;
  
  //Maximum no. of fetch-failure notifications after which map task is failed
  private int maxFetchFailuresNotifications;

  public static final String JOB_KILLED_DIAG =
      "Job received Kill while in RUNNING state.";
  
  //final fields
  private final ApplicationAttemptId applicationAttemptId;
  private final Clock clock;
  private final JobACLsManager aclsManager;
  private final String reporterUserName;
  private final Map<JobACL, AccessControlList> jobACLs;
  private float setupWeight = 0.05f;
  private float cleanupWeight = 0.05f;
  private float mapWeight = 0.0f;
  private float reduceWeight = 0.0f;
  private final Map<TaskId, TaskInfo> completedTasksFromPreviousRun;
  private final List<AMInfo> amInfos;
  private final Lock readLock;
  private final Lock writeLock;
  private final JobId jobId;
  private final String jobName;
  private final OutputCommitter committer;
  private final boolean newApiCommitter;
  private final org.apache.hadoop.mapreduce.JobID oldJobId;
  private final TaskAttemptListener taskAttemptListener;
  private final Object tasksSyncHandle = new Object();
  private final Set<TaskId> mapTasks = new LinkedHashSet<TaskId>();
  private final Set<TaskId> reduceTasks = new LinkedHashSet<TaskId>();
  /**
   * maps nodes to tasks that have run on those nodes
   */
  private final HashMap<NodeId, List<TaskAttemptId>> 
    nodesToSucceededTaskAttempts = new HashMap<NodeId, List<TaskAttemptId>>();

  private final EventHandler eventHandler;
  private final MRAppMetrics metrics;
  private final String userName;
  private String queueName;
  private final long appSubmitTime;
  private final AppContext appContext;

  private boolean lazyTasksCopyNeeded = false;
  volatile Map<TaskId, Task> tasks = new LinkedHashMap<TaskId, Task>();
  private Counters jobCounters = new Counters();
  private Object fullCountersLock = new Object();
  private Counters fullCounters = null;
  private Counters finalMapCounters = null;
  private Counters finalReduceCounters = null;

    // FIXME:  
    //
    // Can then replace task-level uber counters (MR-2424) with job-level ones
    // sent from LocalContainerLauncher, and eventually including a count of
    // of uber-AM attempts (probably sent from MRAppMaster).
  public JobConf conf;

  //fields initialized in init
  private FileSystem fs;
  private Path remoteJobSubmitDir;
  public Path remoteJobConfFile;
  private JobContext jobContext;
  private int allowedMapFailuresPercent = 0;
  private int allowedReduceFailuresPercent = 0;
  private List<TaskAttemptCompletionEvent> taskAttemptCompletionEvents;
  private List<TaskCompletionEvent> mapAttemptCompletionEvents;
  private List<Integer> taskCompletionIdxToMapCompletionIdx;
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
  private static final InternalRebootTransition
      INTERNAL_REBOOT_TRANSITION = new InternalRebootTransition();
  private static final TaskAttemptCompletedEventTransition
      TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION =
          new TaskAttemptCompletedEventTransition();
  private static final CounterUpdateTransition COUNTER_UPDATE_TRANSITION =
      new CounterUpdateTransition();
  private static final UpdatedNodesTransition UPDATED_NODES_TRANSITION =
      new UpdatedNodesTransition();

  protected static final
    StateMachineFactory<JobImpl, JobStateInternal, JobEventType, JobEvent> 
       stateMachineFactory
     = new StateMachineFactory<JobImpl, JobStateInternal, JobEventType, JobEvent>
              (JobStateInternal.NEW)

          // Transitions from NEW state
          .addTransition(JobStateInternal.NEW, JobStateInternal.NEW,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.NEW, JobStateInternal.NEW,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition
              (JobStateInternal.NEW,
              EnumSet.of(JobStateInternal.INITED, JobStateInternal.NEW),
              JobEventType.JOB_INIT,
              new InitTransition())
          .addTransition(JobStateInternal.NEW, JobStateInternal.FAIL_ABORT,
              JobEventType.JOB_INIT_FAILED,
              new InitFailedTransition())
          .addTransition(JobStateInternal.NEW, JobStateInternal.KILLED,
              JobEventType.JOB_KILL,
              new KillNewJobTransition())
          .addTransition(JobStateInternal.NEW, JobStateInternal.ERROR,
              JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(JobStateInternal.NEW, JobStateInternal.REBOOT,
              JobEventType.JOB_AM_REBOOT,
              INTERNAL_REBOOT_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.NEW, JobStateInternal.NEW,
              JobEventType.JOB_UPDATED_NODES)

          // Transitions from INITED state
          .addTransition(JobStateInternal.INITED, JobStateInternal.INITED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.INITED, JobStateInternal.INITED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.INITED, JobStateInternal.SETUP,
              JobEventType.JOB_START,
              new StartTransition())
          .addTransition(JobStateInternal.INITED, JobStateInternal.KILLED,
              JobEventType.JOB_KILL,
              new KillInitedJobTransition())
          .addTransition(JobStateInternal.INITED, JobStateInternal.ERROR,
              JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(JobStateInternal.INITED, JobStateInternal.REBOOT,
              JobEventType.JOB_AM_REBOOT,
              INTERNAL_REBOOT_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.INITED, JobStateInternal.INITED,
              JobEventType.JOB_UPDATED_NODES)

          // Transitions from SETUP state
          .addTransition(JobStateInternal.SETUP, JobStateInternal.SETUP,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.SETUP, JobStateInternal.SETUP,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.SETUP, JobStateInternal.RUNNING,
              JobEventType.JOB_SETUP_COMPLETED,
              new SetupCompletedTransition())
          .addTransition(JobStateInternal.SETUP, JobStateInternal.FAIL_ABORT,
              JobEventType.JOB_SETUP_FAILED,
              new SetupFailedTransition())
          .addTransition(JobStateInternal.SETUP, JobStateInternal.KILL_ABORT,
              JobEventType.JOB_KILL,
              new KilledDuringSetupTransition())
          .addTransition(JobStateInternal.SETUP, JobStateInternal.ERROR,
              JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(JobStateInternal.SETUP, JobStateInternal.REBOOT,
              JobEventType.JOB_AM_REBOOT,
              INTERNAL_REBOOT_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.SETUP, JobStateInternal.SETUP,
              JobEventType.JOB_UPDATED_NODES)

          // Transitions from RUNNING state
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.RUNNING,
              JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
              TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition
              (JobStateInternal.RUNNING,
              EnumSet.of(JobStateInternal.RUNNING,
                  JobStateInternal.COMMITTING, JobStateInternal.FAIL_WAIT,
                  JobStateInternal.FAIL_ABORT),
              JobEventType.JOB_TASK_COMPLETED,
              new TaskCompletedTransition())
          .addTransition
              (JobStateInternal.RUNNING,
              EnumSet.of(JobStateInternal.RUNNING,
                  JobStateInternal.COMMITTING),
              JobEventType.JOB_COMPLETED,
              new JobNoTasksCompletedTransition())
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.KILL_WAIT,
              JobEventType.JOB_KILL, new KillTasksTransition())
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.RUNNING,
              JobEventType.JOB_UPDATED_NODES,
              UPDATED_NODES_TRANSITION)
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.RUNNING,
              JobEventType.JOB_MAP_TASK_RESCHEDULED,
              new MapTaskRescheduledTransition())
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.RUNNING,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.RUNNING,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.RUNNING,
              JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
              new TaskAttemptFetchFailureTransition())
          .addTransition(
              JobStateInternal.RUNNING,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(JobStateInternal.RUNNING, JobStateInternal.REBOOT,
              JobEventType.JOB_AM_REBOOT,
              INTERNAL_REBOOT_TRANSITION)

          // Transitions from KILL_WAIT state.
          .addTransition
              (JobStateInternal.KILL_WAIT,
              EnumSet.of(JobStateInternal.KILL_WAIT,
                  JobStateInternal.KILL_ABORT),
              JobEventType.JOB_TASK_COMPLETED,
              new KillWaitTaskCompletedTransition())
          .addTransition(JobStateInternal.KILL_WAIT, JobStateInternal.KILL_WAIT,
              JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
              TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION)
          .addTransition(JobStateInternal.KILL_WAIT, JobStateInternal.KILL_WAIT,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.KILL_WAIT, JobStateInternal.KILL_WAIT,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobStateInternal.KILL_WAIT,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.KILL_WAIT, JobStateInternal.KILL_WAIT,
              EnumSet.of(JobEventType.JOB_KILL,
                  JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_MAP_TASK_RESCHEDULED,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
                  JobEventType.JOB_AM_REBOOT))

          // Transitions from COMMITTING state
          .addTransition(JobStateInternal.COMMITTING,
              JobStateInternal.SUCCEEDED,
              JobEventType.JOB_COMMIT_COMPLETED,
              new CommitSucceededTransition())
          .addTransition(JobStateInternal.COMMITTING,
              JobStateInternal.FAIL_ABORT,
              JobEventType.JOB_COMMIT_FAILED,
              new CommitFailedTransition())
          .addTransition(JobStateInternal.COMMITTING,
              JobStateInternal.KILL_ABORT,
              JobEventType.JOB_KILL,
              new KilledDuringCommitTransition())
          .addTransition(JobStateInternal.COMMITTING,
              JobStateInternal.COMMITTING,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.COMMITTING,
              JobStateInternal.COMMITTING,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.COMMITTING,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(JobStateInternal.COMMITTING, JobStateInternal.REBOOT,
              JobEventType.JOB_AM_REBOOT,
              INTERNAL_REBOOT_TRANSITION)
              // Ignore-able events
          .addTransition(JobStateInternal.COMMITTING,
              JobStateInternal.COMMITTING,
              EnumSet.of(JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE))

          // Transitions from SUCCEEDED state
          .addTransition(JobStateInternal.SUCCEEDED, JobStateInternal.SUCCEEDED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.SUCCEEDED, JobStateInternal.SUCCEEDED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobStateInternal.SUCCEEDED,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.SUCCEEDED, JobStateInternal.SUCCEEDED,
              EnumSet.of(JobEventType.JOB_KILL, 
                  JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
                  JobEventType.JOB_AM_REBOOT,
                  JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
                  JobEventType.JOB_MAP_TASK_RESCHEDULED))

          // Transitions from FAIL_WAIT state
          .addTransition(JobStateInternal.FAIL_WAIT,
              JobStateInternal.FAIL_WAIT,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.FAIL_WAIT,
              JobStateInternal.FAIL_WAIT,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.FAIL_WAIT,
              EnumSet.of(JobStateInternal.FAIL_WAIT, JobStateInternal.FAIL_ABORT),
              JobEventType.JOB_TASK_COMPLETED, 
              new JobFailWaitTransition())
          .addTransition(JobStateInternal.FAIL_WAIT,
              JobStateInternal.FAIL_ABORT, JobEventType.JOB_FAIL_WAIT_TIMEDOUT, 
              new JobFailWaitTimedOutTransition())
          .addTransition(JobStateInternal.FAIL_WAIT, JobStateInternal.KILLED,
              JobEventType.JOB_KILL,
              new KilledDuringAbortTransition())
          .addTransition(JobStateInternal.FAIL_WAIT,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.FAIL_WAIT,
              JobStateInternal.FAIL_WAIT,
              EnumSet.of(JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
                  JobEventType.JOB_MAP_TASK_RESCHEDULED,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
                  JobEventType.JOB_AM_REBOOT))

          //Transitions from FAIL_ABORT state
          .addTransition(JobStateInternal.FAIL_ABORT,
              JobStateInternal.FAIL_ABORT,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.FAIL_ABORT,
              JobStateInternal.FAIL_ABORT,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.FAIL_ABORT, JobStateInternal.FAILED,
              JobEventType.JOB_ABORT_COMPLETED,
              new JobAbortCompletedTransition())
          .addTransition(JobStateInternal.FAIL_ABORT, JobStateInternal.KILLED,
              JobEventType.JOB_KILL,
              new KilledDuringAbortTransition())
          .addTransition(JobStateInternal.FAIL_ABORT,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.FAIL_ABORT,
              JobStateInternal.FAIL_ABORT,
              EnumSet.of(JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_TASK_COMPLETED,
                  JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
                  JobEventType.JOB_MAP_TASK_RESCHEDULED,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
                  JobEventType.JOB_COMMIT_COMPLETED,
                  JobEventType.JOB_COMMIT_FAILED,
                  JobEventType.JOB_AM_REBOOT,
                  JobEventType.JOB_FAIL_WAIT_TIMEDOUT))

          // Transitions from KILL_ABORT state
          .addTransition(JobStateInternal.KILL_ABORT,
              JobStateInternal.KILL_ABORT,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.KILL_ABORT,
              JobStateInternal.KILL_ABORT,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.KILL_ABORT, JobStateInternal.KILLED,
              JobEventType.JOB_ABORT_COMPLETED,
              new JobAbortCompletedTransition())
          .addTransition(JobStateInternal.KILL_ABORT, JobStateInternal.KILLED,
              JobEventType.JOB_KILL,
              new KilledDuringAbortTransition())
          .addTransition(JobStateInternal.KILL_ABORT,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.KILL_ABORT,
              JobStateInternal.KILL_ABORT,
              EnumSet.of(JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
                  JobEventType.JOB_SETUP_COMPLETED,
                  JobEventType.JOB_SETUP_FAILED,
                  JobEventType.JOB_COMMIT_COMPLETED,
                  JobEventType.JOB_COMMIT_FAILED,
                  JobEventType.JOB_AM_REBOOT))

          // Transitions from FAILED state
          .addTransition(JobStateInternal.FAILED, JobStateInternal.FAILED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.FAILED, JobStateInternal.FAILED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobStateInternal.FAILED,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.FAILED, JobStateInternal.FAILED,
              EnumSet.of(JobEventType.JOB_KILL, 
                  JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_TASK_COMPLETED,
                  JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
                  JobEventType.JOB_MAP_TASK_RESCHEDULED,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
                  JobEventType.JOB_SETUP_COMPLETED,
                  JobEventType.JOB_SETUP_FAILED,
                  JobEventType.JOB_COMMIT_COMPLETED,
                  JobEventType.JOB_COMMIT_FAILED,
                  JobEventType.JOB_ABORT_COMPLETED,
                  JobEventType.JOB_AM_REBOOT))

          // Transitions from KILLED state
          .addTransition(JobStateInternal.KILLED, JobStateInternal.KILLED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.KILLED, JobStateInternal.KILLED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobStateInternal.KILLED,
              JobStateInternal.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.KILLED, JobStateInternal.KILLED,
              EnumSet.of(JobEventType.JOB_KILL, 
                  JobEventType.JOB_START,
                  JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
                  JobEventType.JOB_SETUP_COMPLETED,
                  JobEventType.JOB_SETUP_FAILED,
                  JobEventType.JOB_COMMIT_COMPLETED,
                  JobEventType.JOB_COMMIT_FAILED,
                  JobEventType.JOB_ABORT_COMPLETED,
                  JobEventType.JOB_AM_REBOOT))

          // No transitions from INTERNAL_ERROR state. Ignore all.
          .addTransition(
              JobStateInternal.ERROR,
              JobStateInternal.ERROR,
              EnumSet.of(JobEventType.JOB_INIT,
                  JobEventType.JOB_KILL,
                  JobEventType.JOB_TASK_COMPLETED,
                  JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
                  JobEventType.JOB_MAP_TASK_RESCHEDULED,
                  JobEventType.JOB_DIAGNOSTIC_UPDATE,
                  JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
                  JobEventType.JOB_SETUP_COMPLETED,
                  JobEventType.JOB_SETUP_FAILED,
                  JobEventType.JOB_COMMIT_COMPLETED,
                  JobEventType.JOB_COMMIT_FAILED,
                  JobEventType.JOB_ABORT_COMPLETED,
                  JobEventType.INTERNAL_ERROR,
                  JobEventType.JOB_AM_REBOOT))
          .addTransition(JobStateInternal.ERROR, JobStateInternal.ERROR,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)

          // No transitions from AM_REBOOT state. Ignore all.
          .addTransition(
              JobStateInternal.REBOOT,
              JobStateInternal.REBOOT,
              EnumSet.of(JobEventType.JOB_INIT,
                  JobEventType.JOB_KILL,
                  JobEventType.JOB_TASK_COMPLETED,
                  JobEventType.JOB_TASK_ATTEMPT_COMPLETED,
                  JobEventType.JOB_MAP_TASK_RESCHEDULED,
                  JobEventType.JOB_DIAGNOSTIC_UPDATE,
                  JobEventType.JOB_UPDATED_NODES,
                  JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE,
                  JobEventType.JOB_SETUP_COMPLETED,
                  JobEventType.JOB_SETUP_FAILED,
                  JobEventType.JOB_COMMIT_COMPLETED,
                  JobEventType.JOB_COMMIT_FAILED,
                  JobEventType.JOB_ABORT_COMPLETED,
                  JobEventType.INTERNAL_ERROR,
                  JobEventType.JOB_AM_REBOOT))
          .addTransition(JobStateInternal.REBOOT, JobStateInternal.REBOOT,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)

          // create the topology tables
          .installTopology();
 
  private final StateMachine<JobStateInternal, JobEventType, JobEvent> stateMachine;

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
  private long startTime;
  private long finishTime;
  private float setupProgress;
  private float mapProgress;
  private float reduceProgress;
  private float cleanupProgress;
  private boolean isUber = false;

  private Credentials jobCredentials;
  private Token<JobTokenIdentifier> jobToken;
  private JobTokenSecretManager jobTokenSecretManager;
  
  private JobStateInternal forcedState = null;

  //Executor used for running future tasks.
  private ScheduledThreadPoolExecutor executor;
  private ScheduledFuture failWaitTriggerScheduledFuture;

  private JobState lastNonFinalState = JobState.NEW;

  private volatile Priority jobPriority = Priority.newInstance(0);

  public JobImpl(JobId jobId, ApplicationAttemptId applicationAttemptId,
      Configuration conf, EventHandler eventHandler,
      TaskAttemptListener taskAttemptListener,
      JobTokenSecretManager jobTokenSecretManager,
      Credentials jobCredentials, Clock clock,
      Map<TaskId, TaskInfo> completedTasksFromPreviousRun, MRAppMetrics metrics,
      OutputCommitter committer, boolean newApiCommitter, String userName,
      long appSubmitTime, List<AMInfo> amInfos, AppContext appContext,
      JobStateInternal forcedState, String forcedDiagnostic) {
    this.applicationAttemptId = applicationAttemptId;
    this.jobId = jobId;
    this.jobName = conf.get(JobContext.JOB_NAME, "<missing job name>");
    this.conf = new JobConf(conf);
    this.metrics = metrics;
    this.clock = clock;
    this.completedTasksFromPreviousRun = completedTasksFromPreviousRun;
    this.amInfos = amInfos;
    this.appContext = appContext;
    this.userName = userName;
    this.queueName = conf.get(MRJobConfig.QUEUE_NAME, "default");
    this.appSubmitTime = appSubmitTime;
    this.oldJobId = TypeConverter.fromYarn(jobId);
    this.committer = committer;
    this.newApiCommitter = newApiCommitter;

    this.taskAttemptListener = taskAttemptListener;
    this.eventHandler = eventHandler;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    this.jobCredentials = jobCredentials;
    this.jobTokenSecretManager = jobTokenSecretManager;

    this.aclsManager = new JobACLsManager(conf);
    this.reporterUserName = System.getProperty("user.name");
    this.jobACLs = aclsManager.constructJobACLs(conf);

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
      .setNameFormat("Job Fail Wait Timeout Monitor #%d")
      .setDaemon(true)
      .build();
    this.executor = new ScheduledThreadPoolExecutor(1, threadFactory);

    // This "this leak" is okay because the retained pointer is in an
    //  instance variable.
    stateMachine = stateMachineFactory.make(this);
    this.forcedState  = forcedState;
    if(forcedDiagnostic != null) {
      this.diagnostics.add(forcedDiagnostic);
    }
    
    this.maxAllowedFetchFailuresFraction = conf.getFloat(
        MRJobConfig.MAX_ALLOWED_FETCH_FAILURES_FRACTION,
        MRJobConfig.DEFAULT_MAX_ALLOWED_FETCH_FAILURES_FRACTION);
    this.maxFetchFailuresNotifications = conf.getInt(
        MRJobConfig.MAX_FETCH_FAILURES_NOTIFICATIONS,
        MRJobConfig.DEFAULT_MAX_FETCH_FAILURES_NOTIFICATIONS);
  }

  protected StateMachine<JobStateInternal, JobEventType, JobEvent> getStateMachine() {
    return stateMachine;
  }

  @Override
  public JobId getID() {
    return jobId;
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
    AccessControlList jobACL = jobACLs.get(jobOperation);
    if (jobACL == null) {
      return true;
    }
    return aclsManager.checkAccess(callerUGI, jobOperation, userName, jobACL);
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
  public Counters getAllCounters() {

    readLock.lock();

    try {
      JobStateInternal state = getInternalState();
      if (state == JobStateInternal.ERROR || state == JobStateInternal.FAILED
          || state == JobStateInternal.KILLED || state == JobStateInternal.SUCCEEDED) {
        this.mayBeConstructFinalFullCounters();
        return fullCounters;
      }

      Counters counters = new Counters();
      counters.incrAllCounters(jobCounters);
      return incrTaskCounters(counters, tasks.values());

    } finally {
      readLock.unlock();
    }
  }

  public static Counters incrTaskCounters(
      Counters counters, Collection<Task> tasks) {
    for (Task task : tasks) {
      counters.incrAllCounters(task.getCounters());
    }
    return counters;
  }

  @Override
  public TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(
      int fromEventId, int maxEvents) {
    TaskAttemptCompletionEvent[] events = EMPTY_TASK_ATTEMPT_COMPLETION_EVENTS;
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
  public TaskCompletionEvent[] getMapAttemptCompletionEvents(
      int startIndex, int maxEvents) {
    TaskCompletionEvent[] events = EMPTY_TASK_COMPLETION_EVENTS;
    readLock.lock();
    try {
      if (mapAttemptCompletionEvents.size() > startIndex) {
        int actualMax = Math.min(maxEvents,
            (mapAttemptCompletionEvents.size() - startIndex));
        events = mapAttemptCompletionEvents.subList(startIndex,
            actualMax + startIndex).toArray(events);
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
      JobState state = getState();

      // jobFile can be null if the job is not yet inited.
      String jobFile =
          remoteJobConfFile == null ? "" : remoteJobConfFile.toString();

      StringBuilder diagsb = new StringBuilder();
      for (String s : getDiagnostics()) {
        diagsb.append(s).append("\n");
      }

      if (getInternalState() == JobStateInternal.NEW) {
        return MRBuilderUtils.newJobReport(jobId, jobName, reporterUserName,
            state, appSubmitTime, startTime, finishTime, setupProgress, 0.0f,
            0.0f, cleanupProgress, jobFile, amInfos, isUber, diagsb.toString());
      }

      computeProgress();
      JobReport report = MRBuilderUtils.newJobReport(jobId, jobName,
          reporterUserName,
          state, appSubmitTime, startTime, finishTime, setupProgress,
          this.mapProgress, this.reduceProgress,
          cleanupProgress, jobFile, amInfos, isUber, diagsb.toString(),
          jobPriority);
      return report;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public float getProgress() {
    this.readLock.lock();
    try {
      computeProgress();
      return (this.setupProgress * this.setupWeight + this.cleanupProgress
          * this.cleanupWeight + this.mapProgress * this.mapWeight + this.reduceProgress
          * this.reduceWeight);
    } finally {
      this.readLock.unlock();
    }
  }

  private void computeProgress() {
    this.readLock.lock();
    try {
      float mapProgress = 0f;
      float reduceProgress = 0f;
      for (Task task : this.tasks.values()) {
        if (task.getType() == TaskType.MAP) {
          mapProgress += (task.isFinished() ? 1f : task.getProgress());
        } else {
          reduceProgress += (task.isFinished() ? 1f : task.getProgress());
        }
      }
      if (this.numMapTasks != 0) {
        mapProgress = mapProgress / this.numMapTasks;
      }
      if (this.numReduceTasks != 0) {
        reduceProgress = reduceProgress / this.numReduceTasks;
      }
      this.mapProgress = mapProgress;
      this.reduceProgress = reduceProgress;
    } finally {
      this.readLock.unlock();
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
      JobState state = getExternalState(getInternalState());
      if (!appContext.hasSuccessfullyUnregistered()
          && (state == JobState.SUCCEEDED || state == JobState.FAILED
          || state == JobState.KILLED || state == JobState.ERROR)) {
        return lastNonFinalState;
      } else {
        return state;
      }
    } finally {
      readLock.unlock();
    }
  }

  protected void scheduleTasks(Set<TaskId> taskIDs,
      boolean recoverTaskOutput) {
    for (TaskId taskID : taskIDs) {
      TaskInfo taskInfo = completedTasksFromPreviousRun.remove(taskID);
      if (taskInfo != null) {
        eventHandler.handle(new TaskRecoverEvent(taskID, taskInfo,
            committer, recoverTaskOutput));
      } else {
        eventHandler.handle(new TaskEvent(taskID, TaskEventType.T_SCHEDULE));
      }
    }
  }

  @Override
  /**
   * The only entry point to change the Job.
   */
  public void handle(JobEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getJobId() + " of type "
          + event.getType());
    }
    try {
      writeLock.lock();
      JobStateInternal oldState = getInternalState();
      try {
         getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.error("Can't handle this event at current state", e);
        addDiagnostic("Invalid event " + event.getType() + 
            " on Job " + this.jobId);
        eventHandler.handle(new JobEvent(this.jobId,
            JobEventType.INTERNAL_ERROR));
      }
      //notify the eventhandler of state change
      if (oldState != getInternalState()) {
        LOG.info(jobId + "Job Transitioned from " + oldState + " to "
                 + getInternalState());
        rememberLastNonFinalState(oldState);
      }
    }
    
    finally {
      writeLock.unlock();
    }
  }

  private void rememberLastNonFinalState(JobStateInternal stateInternal) {
    JobState state = getExternalState(stateInternal);
    // if state is not the final state, set lastNonFinalState
    if (state != JobState.SUCCEEDED && state != JobState.FAILED
        && state != JobState.KILLED && state != JobState.ERROR) {
      lastNonFinalState = state;
    }
  }

  @Private
  public JobStateInternal getInternalState() {
    readLock.lock();
    try {
      if(forcedState != null) {
        return forcedState;
      }
     return getStateMachine().getCurrentState();
    } finally {
      readLock.unlock();
    }
  }
  
  private JobState getExternalState(JobStateInternal smState) {
    switch (smState) {
    case KILL_WAIT:
    case KILL_ABORT:
      return JobState.KILLED;
    case SETUP:
    case COMMITTING:
      return JobState.RUNNING;
    case FAIL_WAIT:
    case FAIL_ABORT:
      return JobState.FAILED;
    case REBOOT:
      if (appContext.isLastAMRetry()) {
        return JobState.ERROR;
      } else {
        // In case of not last retry, return the external state as RUNNING since
        // otherwise JobClient will exit when it polls the AM for job state
        return JobState.RUNNING;
      }
    default:
      return JobState.valueOf(smState.name());
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
  
  /**
   * Create the default file System for this job.
   * @param conf the conf object
   * @return the default filesystem for this job
   * @throws IOException
   */
  protected FileSystem getFileSystem(Configuration conf) throws IOException {
    return FileSystem.get(conf);
  }
  
  protected JobStateInternal checkReadyForCommit() {
    JobStateInternal currentState = getInternalState();
    if (completedTaskCount == tasks.size()
        && currentState == JobStateInternal.RUNNING) {
      eventHandler.handle(new CommitterJobCommitEvent(jobId, getJobContext()));
      return JobStateInternal.COMMITTING;
    }
    // return the current state as job not ready to commit yet
    return getInternalState();
  }

  JobStateInternal finished(JobStateInternal finalState) {
    if (getInternalState() == JobStateInternal.RUNNING) {
      metrics.endRunningJob(this);
    }
    if (finishTime == 0) setFinishTime();
    eventHandler.handle(new JobFinishEvent(jobId));

    switch (finalState) {
      case KILLED:
        metrics.killedJob(this);
        break;
      case REBOOT:
      case ERROR:
      case FAILED:
        metrics.failedJob(this);
        break;
      case SUCCEEDED:
        metrics.completedJob(this);
        break;
      default:
        throw new IllegalArgumentException("Illegal job state: " + finalState);
    }
    return finalState;
  }

  @Override
  public String getUserName() {
    return userName;
  }
  
  @Override
  public String getQueueName() {
    return queueName;
  }
  
  @Override
  public void setQueueName(String queueName) {
    this.queueName = queueName;
    JobQueueChangeEvent jqce = new JobQueueChangeEvent(oldJobId, queueName);
    eventHandler.handle(new JobHistoryEvent(jobId, jqce));
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.job.Job#getConfFile()
   */
  @Override
  public Path getConfFile() {
    return remoteJobConfFile;
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
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.job.Job#getJobACLs()
   */
  @Override
  public Map<JobACL, AccessControlList> getJobACLs() {
    return Collections.unmodifiableMap(jobACLs);
  }
  
  @Override
  public List<AMInfo> getAMInfos() {
    return amInfos;
  }

  /**
   * Decide whether job can be run in uber mode based on various criteria.
   * @param dataInputLength Total length for all splits
   */
  private void makeUberDecision(long dataInputLength) {
    //FIXME:  need new memory criterion for uber-decision (oops, too late here;
    // until AM-resizing supported,
    // must depend on job client to pass fat-slot needs)
    // these are no longer "system" settings, necessarily; user may override
    int sysMaxMaps = conf.getInt(MRJobConfig.JOB_UBERTASK_MAXMAPS, 9);

    int sysMaxReduces = conf.getInt(MRJobConfig.JOB_UBERTASK_MAXREDUCES, 1);

    long sysMaxBytes = conf.getLong(MRJobConfig.JOB_UBERTASK_MAXBYTES,
        fs.getDefaultBlockSize(this.remoteJobSubmitDir)); // FIXME: this is wrong; get FS from
                                   // [File?]InputFormat and default block size
                                   // from that

    long sysMemSizeForUberSlot =
        conf.getInt(MRJobConfig.MR_AM_VMEM_MB,
            MRJobConfig.DEFAULT_MR_AM_VMEM_MB);

    long sysCPUSizeForUberSlot =
        conf.getInt(MRJobConfig.MR_AM_CPU_VCORES,
            MRJobConfig.DEFAULT_MR_AM_CPU_VCORES);

    boolean uberEnabled =
        conf.getBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    boolean smallNumMapTasks = (numMapTasks <= sysMaxMaps);
    boolean smallNumReduceTasks = (numReduceTasks <= sysMaxReduces);
    boolean smallInput = (dataInputLength <= sysMaxBytes);
    // ignoring overhead due to UberAM and statics as negligible here:
    long requiredMapMB = conf.getLong(MRJobConfig.MAP_MEMORY_MB, 0);
    long requiredReduceMB = conf.getLong(MRJobConfig.REDUCE_MEMORY_MB, 0);
    long requiredMB = Math.max(requiredMapMB, requiredReduceMB);
    int requiredMapCores = conf.getInt(
            MRJobConfig.MAP_CPU_VCORES, 
            MRJobConfig.DEFAULT_MAP_CPU_VCORES);
    int requiredReduceCores = conf.getInt(
            MRJobConfig.REDUCE_CPU_VCORES, 
            MRJobConfig.DEFAULT_REDUCE_CPU_VCORES);
    int requiredCores = Math.max(requiredMapCores, requiredReduceCores);    
    if (numReduceTasks == 0) {
      requiredMB = requiredMapMB;
      requiredCores = requiredMapCores;
    }
    boolean smallMemory =
        (requiredMB <= sysMemSizeForUberSlot)
        || (sysMemSizeForUberSlot == JobConf.DISABLED_MEMORY_LIMIT);
    
    boolean smallCpu = requiredCores <= sysCPUSizeForUberSlot;
    boolean notChainJob = !isChainJob(conf);

    // User has overall veto power over uberization, or user can modify
    // limits (overriding system settings and potentially shooting
    // themselves in the head).  Note that ChainMapper/Reducer are
    // fundamentally incompatible with MR-1220; they employ a blocking
    // queue between the maps/reduces and thus require parallel execution,
    // while "uber-AM" (MR AM + LocalContainerLauncher) loops over tasks
    // and thus requires sequential execution.
    isUber = uberEnabled && smallNumMapTasks && smallNumReduceTasks
        && smallInput && smallMemory && smallCpu 
        && notChainJob;

    if (isUber) {
      LOG.info("Uberizing job " + jobId + ": " + numMapTasks + "m+"
          + numReduceTasks + "r tasks (" + dataInputLength
          + " input bytes) will run sequentially on single node.");

      // make sure reduces are scheduled only after all map are completed
      conf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART,
                        1.0f);
      // uber-subtask attempts all get launched on same node; if one fails,
      // probably should retry elsewhere, i.e., move entire uber-AM:  ergo,
      // limit attempts to 1 (or at most 2?  probably not...)
      conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 1);
      conf.setInt(MRJobConfig.REDUCE_MAX_ATTEMPTS, 1);

      // disable speculation
      conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
      conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);
    } else {
      StringBuilder msg = new StringBuilder();
      msg.append("Not uberizing ").append(jobId).append(" because:");
      if (!uberEnabled)
        msg.append(" not enabled;");
      if (!smallNumMapTasks)
        msg.append(" too many maps;");
      if (!smallNumReduceTasks)
        msg.append(" too many reduces;");
      if (!smallInput)
        msg.append(" too much input;");
      if (!smallCpu)
        msg.append(" too much CPU;");
      if (!smallMemory)
        msg.append(" too much RAM;");
      if (!smallCpu)
          msg.append(" too much CPU;");
      if (!notChainJob)
        msg.append(" chainjob;");
      LOG.info(msg.toString());
    }
  }
  
  /**
   * ChainMapper and ChainReducer must execute in parallel, so they're not
   * compatible with uberization/LocalContainerLauncher (100% sequential).
   */
  private boolean isChainJob(Configuration conf) {
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
    } catch (NoClassDefFoundError ignored) {
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
    } catch (NoClassDefFoundError ignored) {
    }
    return isChainJob;
  }
  
  private void actOnUnusableNode(NodeId nodeId, NodeState nodeState) {
    // rerun previously successful map tasks
    // do this only if the job is still in the running state and there are
    // running reducers
    if (getInternalState() == JobStateInternal.RUNNING &&
        !allReducersComplete()) {
      List<TaskAttemptId> taskAttemptIdList =
          nodesToSucceededTaskAttempts.get(nodeId);
      if (taskAttemptIdList != null) {
        String mesg = "TaskAttempt killed because it ran on unusable node "
            + nodeId;
        for (TaskAttemptId id : taskAttemptIdList) {
          if (TaskType.MAP == id.getTaskId().getTaskType()) {
            // reschedule only map tasks because their outputs maybe unusable
            LOG.info(mesg + ". AttemptId:" + id);
            eventHandler.handle(new TaskAttemptKillEvent(id, mesg));
          }
        }
      }
    }
    // currently running task attempts on unusable nodes are handled in
    // RMContainerAllocator
  }

  private boolean allReducersComplete() {
    return numReduceTasks == 0 || numReduceTasks == getCompletedReduces();
  }

  /*
  private int getBlockSize() {
    String inputClassName = conf.get(MRJobConfig.INPUT_FORMAT_CLASS_ATTR);
    if (inputClassName != null) {
      Class<?> inputClass - Class.forName(inputClassName);
      if (FileInputFormat<K, V>)
    }
  }
  */
  /**
    * Get the workflow adjacencies from the job conf
    * The string returned is of the form "key"="value" "key"="value" ...
    */
  private static String getWorkflowAdjacencies(Configuration conf) {
    int prefixLen = MRJobConfig.WORKFLOW_ADJACENCY_PREFIX_STRING.length();
    Map<String,String> adjacencies = 
        conf.getValByRegex(MRJobConfig.WORKFLOW_ADJACENCY_PREFIX_PATTERN);
    if (adjacencies.isEmpty()) {
      return "";
    }
    int size = 0;
    for (Entry<String,String> entry : adjacencies.entrySet()) {
      int keyLen = entry.getKey().length();
      size += keyLen - prefixLen;
      size += entry.getValue().length() + 6;
    }
    StringBuilder sb = new StringBuilder(size);
    for (Entry<String,String> entry : adjacencies.entrySet()) {
      int keyLen = entry.getKey().length();
      sb.append("\"");
      sb.append(escapeString(entry.getKey().substring(prefixLen, keyLen)));
      sb.append("\"=\"");
      sb.append(escapeString(entry.getValue()));
      sb.append("\" ");
    }
    return sb.toString();
  }
  
  public static String escapeString(String data) {
    return StringUtils.escapeString(data, StringUtils.ESCAPE_CHAR,
        new char[] {'"', '=', '.'});
  }

  public static class InitTransition 
      implements MultipleArcTransition<JobImpl, JobEvent, JobStateInternal> {

    /**
     * Note that this transition method is called directly (and synchronously)
     * by MRAppMaster's init() method (i.e., no RPC, no thread-switching;
     * just plain sequential call within AM context), so we can trigger
     * modifications in AM state from here (at least, if AM is written that
     * way; MR version is).
     */
    @Override
    public JobStateInternal transition(JobImpl job, JobEvent event) {
      job.metrics.submittedJob(job);
      job.metrics.preparingJob(job);

      if (job.newApiCommitter) {
        job.jobContext = new JobContextImpl(job.conf,
            job.oldJobId);
      } else {
        job.jobContext = new org.apache.hadoop.mapred.JobContextImpl(
            job.conf, job.oldJobId);
      }
      
      try {
        setup(job);
        job.fs = job.getFileSystem(job.conf);

        //log to job history
        JobSubmittedEvent jse = new JobSubmittedEvent(job.oldJobId,
              job.conf.get(MRJobConfig.JOB_NAME, "test"), 
            job.conf.get(MRJobConfig.USER_NAME, "mapred"),
            job.appSubmitTime,
            job.remoteJobConfFile.toString(),
            job.jobACLs, job.queueName,
            job.conf.get(MRJobConfig.WORKFLOW_ID, ""),
            job.conf.get(MRJobConfig.WORKFLOW_NAME, ""),
            job.conf.get(MRJobConfig.WORKFLOW_NODE_NAME, ""),
            getWorkflowAdjacencies(job.conf),
            job.conf.get(MRJobConfig.WORKFLOW_TAGS, ""));
        job.eventHandler.handle(new JobHistoryEvent(job.jobId, jse));
        //TODO JH Verify jobACLs, UserName via UGI?

        TaskSplitMetaInfo[] taskSplitMetaInfo = createSplits(job, job.jobId);
        job.numMapTasks = taskSplitMetaInfo.length;
        job.numReduceTasks = job.conf.getInt(MRJobConfig.NUM_REDUCES, 0);

        if (job.numMapTasks == 0 && job.numReduceTasks == 0) {
          job.addDiagnostic("No of maps and reduces are 0 " + job.jobId);
        } else if (job.numMapTasks == 0) {
          job.reduceWeight = 0.9f;
        } else if (job.numReduceTasks == 0) {
          job.mapWeight = 0.9f;
        } else {
          job.mapWeight = job.reduceWeight = 0.45f;
        }

        checkTaskLimits();

        long inputLength = 0;
        for (int i = 0; i < job.numMapTasks; ++i) {
          inputLength += taskSplitMetaInfo[i].getInputDataLength();
        }

        job.makeUberDecision(inputLength);
        
        job.taskAttemptCompletionEvents =
            new ArrayList<TaskAttemptCompletionEvent>(
                job.numMapTasks + job.numReduceTasks + 10);
        job.mapAttemptCompletionEvents =
            new ArrayList<TaskCompletionEvent>(job.numMapTasks + 10);
        job.taskCompletionIdxToMapCompletionIdx = new ArrayList<Integer>(
            job.numMapTasks + job.numReduceTasks + 10);

        job.allowedMapFailuresPercent =
            job.conf.getInt(MRJobConfig.MAP_FAILURES_MAX_PERCENT, 0);
        job.allowedReduceFailuresPercent =
            job.conf.getInt(MRJobConfig.REDUCE_FAILURES_MAXPERCENT, 0);

        // create the Tasks but don't start them yet
        createMapTasks(job, inputLength, taskSplitMetaInfo);
        createReduceTasks(job);

        job.metrics.endPreparingJob(job);
        return JobStateInternal.INITED;
      } catch (Exception e) {
        LOG.warn("Job init failed", e);
        job.metrics.endPreparingJob(job);
        job.addDiagnostic("Job init failed : "
            + StringUtils.stringifyException(e));
        // Leave job in the NEW state. The MR AM will detect that the state is
        // not INITED and send a JOB_INIT_FAILED event.
        return JobStateInternal.NEW;
      }
    }

    protected void setup(JobImpl job) throws IOException {

      String oldJobIDString = job.oldJobId.toString();
      String user = 
        UserGroupInformation.getCurrentUser().getShortUserName();
      Path path = MRApps.getStagingAreaDir(job.conf, user);
      if(LOG.isDebugEnabled()) {
        LOG.debug("startJobs: parent=" + path + " child=" + oldJobIDString);
      }

      job.remoteJobSubmitDir =
          FileSystem.get(job.conf).makeQualified(
              new Path(path, oldJobIDString));
      job.remoteJobConfFile =
          new Path(job.remoteJobSubmitDir, MRJobConfig.JOB_CONF_FILE);

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

      // If the job client did not setup the shuffle secret then reuse
      // the job token secret for the shuffle.
      if (TokenCache.getShuffleSecretKey(job.jobCredentials) == null) {
        LOG.warn("Shuffle secret key missing from job credentials."
            + " Using job token secret as shuffle secret.");
        TokenCache.setShuffleSecretKey(job.jobToken.getPassword(),
            job.jobCredentials);
      }
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
                job.jobToken, job.jobCredentials,
                job.clock,
                job.applicationAttemptId.getAttemptId(),
                job.metrics, job.appContext);
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
                job.taskAttemptListener, job.jobToken,
                job.jobCredentials, job.clock,
                job.applicationAttemptId.getAttemptId(),
                job.metrics, job.appContext);
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
        throw new YarnRuntimeException(e);
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

  private static class InitFailedTransition
      implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
        job.eventHandler.handle(new CommitterJobAbortEvent(job.jobId,
                job.jobContext,
                org.apache.hadoop.mapreduce.JobStatus.State.FAILED));
    }
  }

  private static class SetupCompletedTransition
      implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.setupProgress = 1.0f;
      job.scheduleTasks(job.mapTasks, job.numReduceTasks == 0);
      job.scheduleTasks(job.reduceTasks, true);

      // If we have no tasks, just transition to job completed
      if (job.numReduceTasks == 0 && job.numMapTasks == 0) {
        job.eventHandler.handle(new JobEvent(job.jobId,
            JobEventType.JOB_COMPLETED));
      }
    }
  }

  private static class SetupFailedTransition
      implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.metrics.endRunningJob(job);
      job.addDiagnostic("Job setup failed : "
          + ((JobSetupFailedEvent) event).getMessage());
      job.eventHandler.handle(new CommitterJobAbortEvent(job.jobId,
          job.jobContext,
          org.apache.hadoop.mapreduce.JobStatus.State.FAILED));
    }
  }

  public static class StartTransition
  implements SingleArcTransition<JobImpl, JobEvent> {
    /**
     * This transition executes in the event-dispatcher thread, though it's
     * triggered in MRAppMaster's startJobs() method.
     */
    @Override
    public void transition(JobImpl job, JobEvent event) {
      JobStartEvent jse = (JobStartEvent) event;
      if (jse.getRecoveredJobStartTime() != -1L) {
        job.startTime = jse.getRecoveredJobStartTime();
      } else {
        job.startTime = job.clock.getTime();
      }
      JobInitedEvent jie =
        new JobInitedEvent(job.oldJobId,
             job.startTime,
             job.numMapTasks, job.numReduceTasks,
             job.getState().toString(),
             job.isUber());
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, jie));
      JobInfoChangeEvent jice = new JobInfoChangeEvent(job.oldJobId,
          job.appSubmitTime, job.startTime);
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, jice));
      job.metrics.runningJob(job);

      job.eventHandler.handle(new CommitterJobSetupEvent(
              job.jobId, job.jobContext));
    }
  }

  private void unsuccessfulFinish(JobStateInternal finalState) {
      if (finishTime == 0) setFinishTime();
      cleanupProgress = 1.0f;
      JobUnsuccessfulCompletionEvent unsuccessfulJobEvent =
          new JobUnsuccessfulCompletionEvent(oldJobId,
              finishTime,
              succeededMapTaskCount,
              succeededReduceTaskCount,
              finalState.toString(),
              diagnostics);
      eventHandler.handle(new JobHistoryEvent(jobId,
          unsuccessfulJobEvent));
      finished(finalState);
  }

  private static class JobAbortCompletedTransition
  implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      JobStateInternal finalState = JobStateInternal.valueOf(
          ((JobAbortCompletedEvent) event).getFinalState().name());
      job.unsuccessfulFinish(finalState);
    }
  }

  //This transition happens when a job is to be failed. It waits for all the
  //tasks to finish / be killed.
  private static class JobFailWaitTransition
  implements MultipleArcTransition<JobImpl, JobEvent, JobStateInternal> {
    @Override
    public JobStateInternal transition(JobImpl job, JobEvent event) {
      if(!job.failWaitTriggerScheduledFuture.isCancelled()) {
        for(Task task: job.tasks.values()) {
          if(!task.isFinished()) {
            return JobStateInternal.FAIL_WAIT;
          }
        }
      }
      //Finished waiting. All tasks finished / were killed
      job.failWaitTriggerScheduledFuture.cancel(false);
      job.eventHandler.handle(new CommitterJobAbortEvent(job.jobId,
        job.jobContext, org.apache.hadoop.mapreduce.JobStatus.State.FAILED));
      return JobStateInternal.FAIL_ABORT;
    }
  }

  //This transition happens when a job to be failed times out while waiting on
  //tasks that had been sent the KILL signal. It is triggered by a
  //ScheduledFuture task queued in the executor.
  private static class JobFailWaitTimedOutTransition
  implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      LOG.info("Timeout expired in FAIL_WAIT waiting for tasks to get killed."
        + " Going to fail job anyway");
      job.failWaitTriggerScheduledFuture.cancel(false);
      job.eventHandler.handle(new CommitterJobAbortEvent(job.jobId,
        job.jobContext, org.apache.hadoop.mapreduce.JobStatus.State.FAILED));
    }
  }

  // JobFinishedEvent triggers the move of the history file out of the staging
  // area. May need to create a new event type for this if JobFinished should 
  // not be generated for KilledJobs, etc.
  private static JobFinishedEvent createJobFinishedEvent(JobImpl job) {

    job.mayBeConstructFinalFullCounters();

    JobFinishedEvent jfe = new JobFinishedEvent(
        job.oldJobId, job.finishTime,
        job.succeededMapTaskCount, job.succeededReduceTaskCount,
        job.failedMapTaskCount, job.failedReduceTaskCount,
        job.finalMapCounters,
        job.finalReduceCounters,
        job.fullCounters);
    return jfe;
  }

  private void mayBeConstructFinalFullCounters() {
    // Calculating full-counters. This should happen only once for the job.
    synchronized (this.fullCountersLock) {
      if (this.fullCounters != null) {
        // Already constructed. Just return.
        return;
      }
      this.constructFinalFullcounters();
    }
  }

  @Private
  public void constructFinalFullcounters() {
    this.fullCounters = new Counters();
    this.finalMapCounters = new Counters();
    this.finalReduceCounters = new Counters();
    this.fullCounters.incrAllCounters(jobCounters);
    for (Task t : this.tasks.values()) {
      Counters counters = t.getCounters();
      switch (t.getType()) {
      case MAP:
        this.finalMapCounters.incrAllCounters(counters);
        break;
      case REDUCE:
        this.finalReduceCounters.incrAllCounters(counters);
        break;
      default:
        throw new IllegalStateException("Task type neither map nor reduce: " + 
            t.getType());
      }
      this.fullCounters.incrAllCounters(counters);
    }
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
              JobStateInternal.KILLED.toString(), job.diagnostics);
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, failedEvent));
      job.finished(JobStateInternal.KILLED);
    }
  }

  private static class KillInitedJobTransition
  implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.addDiagnostic("Job received Kill in INITED state.");
      job.eventHandler.handle(new CommitterJobAbortEvent(job.jobId,
          job.jobContext,
          org.apache.hadoop.mapreduce.JobStatus.State.KILLED));
    }
  }

  private static class KilledDuringSetupTransition
  implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.metrics.endRunningJob(job);
      job.addDiagnostic("Job received kill in SETUP state.");
      job.eventHandler.handle(new CommitterJobAbortEvent(job.jobId,
          job.jobContext,
          org.apache.hadoop.mapreduce.JobStatus.State.KILLED));
    }
  }

  private static class KillTasksTransition
      implements SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.addDiagnostic(JOB_KILLED_DIAG);
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
      int mapEventIdx = -1;
      if (TaskType.MAP.equals(tce.getAttemptId().getTaskId().getTaskType())) {
        // we track map completions separately from task completions because
        // - getMapAttemptCompletionEvents uses index ranges specific to maps
        // - type converting the same events over and over is expensive
        mapEventIdx = job.mapAttemptCompletionEvents.size();
        job.mapAttemptCompletionEvents.add(TypeConverter.fromYarn(tce));
      }
      job.taskCompletionIdxToMapCompletionIdx.add(mapEventIdx);
      
      TaskAttemptId attemptId = tce.getAttemptId();
      TaskId taskId = attemptId.getTaskId();
      //make the previous completion event as obsolete if it exists
      Integer successEventNo =
          job.successAttemptCompletionEventNoMap.remove(taskId);
      if (successEventNo != null) {
        TaskAttemptCompletionEvent successEvent = 
          job.taskAttemptCompletionEvents.get(successEventNo);
        successEvent.setStatus(TaskAttemptCompletionEventStatus.OBSOLETE);
        int mapCompletionIdx =
            job.taskCompletionIdxToMapCompletionIdx.get(successEventNo);
        if (mapCompletionIdx >= 0) {
          // update the corresponding TaskCompletionEvent for the map
          TaskCompletionEvent mapEvent =
              job.mapAttemptCompletionEvents.get(mapCompletionIdx);
          job.mapAttemptCompletionEvents.set(mapCompletionIdx,
              new TaskCompletionEvent(mapEvent.getEventId(),
                  mapEvent.getTaskAttemptId(), mapEvent.idWithinJob(),
                  mapEvent.isMapTask(), TaskCompletionEvent.Status.OBSOLETE,
                  mapEvent.getTaskTrackerHttp()));
        }
      }
      
      // if this attempt is not successful then why is the previous successful 
      // attempt being removed above - MAPREDUCE-4330
      if (TaskAttemptCompletionEventStatus.SUCCEEDED.equals(tce.getStatus())) {
        job.successAttemptCompletionEventNoMap.put(taskId, tce.getEventId());
        
        // here we could have simply called Task.getSuccessfulAttempt() but
        // the event that triggers this code is sent before
        // Task.successfulAttempt is set and so there is no guarantee that it
        // will be available now
        Task task = job.tasks.get(taskId);
        TaskAttempt attempt = task.getAttempt(attemptId);
        NodeId nodeId = attempt.getNodeId();
        assert (nodeId != null); // node must exist for a successful event
        List<TaskAttemptId> taskAttemptIdList = job.nodesToSucceededTaskAttempts
            .get(nodeId);
        if (taskAttemptIdList == null) {
          taskAttemptIdList = new ArrayList<TaskAttemptId>();
          job.nodesToSucceededTaskAttempts.put(nodeId, taskAttemptIdList);
        }
        taskAttemptIdList.add(attempt.getID());
      }
    }
  }

  private static class TaskAttemptFetchFailureTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      //get number of shuffling reduces
      int shufflingReduceTasks = 0;
      for (TaskId taskId : job.reduceTasks) {
        Task task = job.tasks.get(taskId);
        if (TaskState.RUNNING.equals(task.getState())) {
          for(TaskAttempt attempt : task.getAttempts().values()) {
            if(attempt.getPhase() == Phase.SHUFFLE) {
              shufflingReduceTasks++;
              break;
            }
          }
        }
      }

      JobTaskAttemptFetchFailureEvent fetchfailureEvent = 
        (JobTaskAttemptFetchFailureEvent) event;
      for (org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId mapId : 
            fetchfailureEvent.getMaps()) {
        Integer fetchFailures = job.fetchFailuresMapping.get(mapId);
        fetchFailures = (fetchFailures == null) ? 1 : (fetchFailures+1);
        job.fetchFailuresMapping.put(mapId, fetchFailures);
        
        float failureRate = shufflingReduceTasks == 0 ? 1.0f : 
          (float) fetchFailures / shufflingReduceTasks;
        // declare faulty if fetch-failures >= max-allowed-failures
        if (fetchFailures >= job.getMaxFetchFailuresNotifications()
            && failureRate >= job.getMaxAllowedFetchFailuresFraction()) {
          LOG.info("Too many fetch-failures for output of task attempt: " + 
              mapId + " ... raising fetch failure to map");
          job.eventHandler.handle(new TaskAttemptTooManyFetchFailureEvent(mapId,
              fetchfailureEvent.getReduce(), fetchfailureEvent.getHost()));
          job.fetchFailuresMapping.remove(mapId);
        }
      }
    }
  }

  private static class TaskCompletedTransition implements
      MultipleArcTransition<JobImpl, JobEvent, JobStateInternal> {

    @Override
    public JobStateInternal transition(JobImpl job, JobEvent event) {
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

      return checkJobAfterTaskCompletion(job);
    }

    //This class is used to queue a ScheduledFuture to send an event to a job
    //after some delay. This can be used to wait for maximum amount of time
    //before proceeding anyway. e.g. When a job is waiting in FAIL_WAIT for
    //all tasks to be killed.
    static class TriggerScheduledFuture implements Runnable {
      JobEvent toSend;
      JobImpl job;
      TriggerScheduledFuture(JobImpl job, JobEvent toSend) {
        this.toSend = toSend;
        this.job = job;
      }
      public void run() {
        LOG.info("Sending event " + toSend + " to " + job.getID());
        job.getEventHandler().handle(toSend);
      }
    }

    protected JobStateInternal checkJobAfterTaskCompletion(JobImpl job) {
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

        //Send kill signal to all unfinished tasks here.
        boolean allDone = true;
        for (Task task : job.tasks.values()) {
          if(!task.isFinished()) {
            allDone = false;
            job.eventHandler.handle(
              new TaskEvent(task.getID(), TaskEventType.T_KILL));
          }
        }

        //If all tasks are already done, we should go directly to FAIL_ABORT
        if(allDone) {
          job.eventHandler.handle(new CommitterJobAbortEvent(job.jobId,
            job.jobContext, org.apache.hadoop.mapreduce.JobStatus.State.FAILED)
          );
          return JobStateInternal.FAIL_ABORT;
        }

        //Set max timeout to wait for the tasks to get killed
        job.failWaitTriggerScheduledFuture = job.executor.schedule(
          new TriggerScheduledFuture(job, new JobEvent(job.getID(),
            JobEventType.JOB_FAIL_WAIT_TIMEDOUT)), job.conf.getInt(
                MRJobConfig.MR_AM_COMMITTER_CANCEL_TIMEOUT_MS,
                MRJobConfig.DEFAULT_MR_AM_COMMITTER_CANCEL_TIMEOUT_MS),
                TimeUnit.MILLISECONDS);
        return JobStateInternal.FAIL_WAIT;
      }
      
      return job.checkReadyForCommit();
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
  private static class JobNoTasksCompletedTransition implements
  MultipleArcTransition<JobImpl, JobEvent, JobStateInternal> {

    @Override
    public JobStateInternal transition(JobImpl job, JobEvent event) {
      return job.checkReadyForCommit();
    }
  }

  private static class CommitSucceededTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.logJobHistoryFinishedEvent();
      job.finished(JobStateInternal.SUCCEEDED);
    }
  }

  private static class CommitFailedTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      JobCommitFailedEvent jcfe = (JobCommitFailedEvent)event;
      job.addDiagnostic("Job commit failed: " + jcfe.getMessage());
      job.eventHandler.handle(new CommitterJobAbortEvent(job.jobId,
          job.jobContext,
          org.apache.hadoop.mapreduce.JobStatus.State.FAILED));
    }
  }

  private static class KilledDuringCommitTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.setFinishTime();
      job.eventHandler.handle(new CommitterJobAbortEvent(job.jobId,
          job.jobContext,
          org.apache.hadoop.mapreduce.JobStatus.State.KILLED));
    }
  }

  private static class KilledDuringAbortTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      job.unsuccessfulFinish(JobStateInternal.KILLED);
    }
  }

  @VisibleForTesting
  void decrementSucceededMapperCount() {
    completedTaskCount--;
    succeededMapTaskCount--;
  }

  private static class MapTaskRescheduledTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      //succeeded map task is restarted back
      job.decrementSucceededMapperCount();
    }
  }

  private static class KillWaitTaskCompletedTransition extends  
      TaskCompletedTransition {
    @Override
    protected JobStateInternal checkJobAfterTaskCompletion(JobImpl job) {
      if (job.completedTaskCount == job.tasks.size()) {
        job.setFinishTime();
        job.eventHandler.handle(new CommitterJobAbortEvent(job.jobId,
            job.jobContext,
            org.apache.hadoop.mapreduce.JobStatus.State.KILLED));
        return JobStateInternal.KILL_ABORT;
      }
      //return the current state, Job not finished yet
      return job.getInternalState();
    }
  }

  protected void addDiagnostic(String diag) {
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
        job.jobCounters.findCounter(ci.getCounterKey()).increment(
          ci.getIncrementValue());
      }
    }
  }
  
  private static class UpdatedNodesTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    @Override
    public void transition(JobImpl job, JobEvent event) {
      JobUpdatedNodesEvent updateEvent = (JobUpdatedNodesEvent) event;
      for(NodeReport nr: updateEvent.getUpdatedNodes()) {
        NodeState nodeState = nr.getNodeState();
        if(nodeState.isUnusable()) {
          // act on the updates
          job.actOnUnusableNode(nr.getNodeId(), nodeState);
        }
      }
    }
  }

  private static class InternalTerminationTransition implements
      SingleArcTransition<JobImpl, JobEvent> {
    JobStateInternal terminationState = null;
    String jobHistoryString = null;
    public InternalTerminationTransition(JobStateInternal stateInternal,
        String jobHistoryString) {
      this.terminationState = stateInternal;
      //mostly a hack for jbhistoryserver
      this.jobHistoryString = jobHistoryString;
    }

    @Override
    public void transition(JobImpl job, JobEvent event) {
      //TODO Is this JH event required.
      job.setFinishTime();
      JobUnsuccessfulCompletionEvent failedEvent =
          new JobUnsuccessfulCompletionEvent(job.oldJobId,
              job.finishTime, 0, 0,
              jobHistoryString, job.diagnostics);
      job.eventHandler.handle(new JobHistoryEvent(job.jobId, failedEvent));
      job.finished(terminationState);
    }
  }

  private static class InternalErrorTransition extends InternalTerminationTransition {
    public InternalErrorTransition(){
      super(JobStateInternal.ERROR, JobStateInternal.ERROR.toString());
    }
  }

  private static class InternalRebootTransition extends InternalTerminationTransition  {
    public InternalRebootTransition(){
      super(JobStateInternal.REBOOT, JobStateInternal.ERROR.toString());
    }
  }

  @Override
  public Configuration loadConfFile() throws IOException {
    Path confPath = getConfFile();
    FileContext fc = FileContext.getFileContext(confPath.toUri(), conf);
    Configuration jobConf = new Configuration(false);
    jobConf.addResource(fc.open(confPath), confPath.toString());
    return jobConf;
  }

  public float getMaxAllowedFetchFailuresFraction() {
    return maxAllowedFetchFailuresFraction;
  }

  public int getMaxFetchFailuresNotifications() {
    return maxFetchFailuresNotifications;
  }

  @Override
  public void setJobPriority(Priority priority) {
    this.jobPriority = priority;
  }
}
