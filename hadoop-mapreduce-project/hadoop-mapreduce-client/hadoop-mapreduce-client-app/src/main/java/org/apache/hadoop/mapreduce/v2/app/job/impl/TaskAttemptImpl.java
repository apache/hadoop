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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceChildJVM;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.WrappedJvmID;
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
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.MapAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.ReduceAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStartedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.Avataar;
import org.apache.hadoop.mapreduce.v2.api.records.Locality;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterTaskAbortEvent;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttemptStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptRecoverEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptTooManyFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptKilledEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerRemoteLaunchEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerRequestEvent;
import org.apache.hadoop.mapreduce.v2.app.speculate.SpeculatorEvent;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.RackResolver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of TaskAttempt interface.
 */
@SuppressWarnings({ "rawtypes" })
public abstract class TaskAttemptImpl implements
    org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt,
      EventHandler<TaskAttemptEvent> {

  static final Counters EMPTY_COUNTERS = new Counters();
  private static final Logger LOG =
      LoggerFactory.getLogger(TaskAttemptImpl.class);
  private static final long MEMORY_SPLITS_RESOLUTION = 1024; //TODO Make configurable?
  private final static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  protected final JobConf conf;
  protected final Path jobFile;
  protected final int partition;
  protected EventHandler eventHandler;
  private final TaskAttemptId attemptId;
  private final Clock clock;
  private final org.apache.hadoop.mapred.JobID oldJobId;
  private final TaskAttemptListener taskAttemptListener;
  private final Resource resourceCapability;
  protected Set<String> dataLocalHosts;
  protected Set<String> dataLocalRacks;
  private final List<String> diagnostics = new ArrayList<String>();
  private final Lock readLock;
  private final Lock writeLock;
  private final AppContext appContext;
  private Credentials credentials;
  private Token<JobTokenIdentifier> jobToken;
  private static AtomicBoolean initialClasspathFlag = new AtomicBoolean();
  private static String initialClasspath = null;
  private static String initialAppClasspath = null;
  private static Object commonContainerSpecLock = new Object();
  private static ContainerLaunchContext commonContainerSpec = null;
  private static final Object classpathLock = new Object();
  private long launchTime;
  private long finishTime;
  private WrappedProgressSplitsBlock progressSplitBlock;
  private int shufflePort = -1;
  private String trackerName;
  private int httpPort;
  private Locality locality;
  private Avataar avataar;
  private boolean rescheduleNextAttempt = false;

  private static final CleanupContainerTransition
      CLEANUP_CONTAINER_TRANSITION = new CleanupContainerTransition();
  private static final MoveContainerToSucceededFinishingTransition
      SUCCEEDED_FINISHING_TRANSITION =
          new MoveContainerToSucceededFinishingTransition();
  private static final MoveContainerToFailedFinishingTransition
      FAILED_FINISHING_TRANSITION =
          new MoveContainerToFailedFinishingTransition();
  private static final ExitFinishingOnTimeoutTransition
      FINISHING_ON_TIMEOUT_TRANSITION =
          new ExitFinishingOnTimeoutTransition();

  private static final FinalizeFailedTransition FINALIZE_FAILED_TRANSITION =
      new FinalizeFailedTransition();

  private static final DiagnosticInformationUpdater 
    DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION 
      = new DiagnosticInformationUpdater();

  private static final EnumSet<TaskAttemptEventType>
    FAILED_KILLED_STATE_IGNORED_EVENTS = EnumSet.of(
      TaskAttemptEventType.TA_KILL,
      TaskAttemptEventType.TA_ASSIGNED,
      TaskAttemptEventType.TA_CONTAINER_COMPLETED,
      TaskAttemptEventType.TA_UPDATE,
      // Container launch events can arrive late
      TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
      TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED,
      TaskAttemptEventType.TA_CONTAINER_CLEANED,
      TaskAttemptEventType.TA_COMMIT_PENDING,
      TaskAttemptEventType.TA_DONE,
      TaskAttemptEventType.TA_FAILMSG,
      TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
      TaskAttemptEventType.TA_TIMED_OUT,
      TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE);

  private static final StateMachineFactory
        <TaskAttemptImpl, TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent>
        stateMachineFactory
    = new StateMachineFactory
             <TaskAttemptImpl, TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent>
           (TaskAttemptStateInternal.NEW)

     // Transitions from the NEW state.
     .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.UNASSIGNED,
         TaskAttemptEventType.TA_SCHEDULE, new RequestContainerTransition(false))
     .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.UNASSIGNED,
         TaskAttemptEventType.TA_RESCHEDULE, new RequestContainerTransition(true))
     .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.KILLED,
         TaskAttemptEventType.TA_KILL, new KilledTransition())
     .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT, new FailedTransition())
     .addTransition(TaskAttemptStateInternal.NEW,
         EnumSet.of(TaskAttemptStateInternal.FAILED,
             TaskAttemptStateInternal.KILLED,
             TaskAttemptStateInternal.SUCCEEDED),
         TaskAttemptEventType.TA_RECOVER, new RecoverTransition())
     .addTransition(TaskAttemptStateInternal.NEW,
         TaskAttemptStateInternal.NEW,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)

     // Transitions from the UNASSIGNED state.
     .addTransition(TaskAttemptStateInternal.UNASSIGNED,
         TaskAttemptStateInternal.ASSIGNED, TaskAttemptEventType.TA_ASSIGNED,
         new ContainerAssignedTransition())
     .addTransition(TaskAttemptStateInternal.UNASSIGNED, TaskAttemptStateInternal.KILLED,
         TaskAttemptEventType.TA_KILL, new DeallocateContainerTransition(
         TaskAttemptStateInternal.KILLED, true))
     .addTransition(TaskAttemptStateInternal.UNASSIGNED, TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT, new DeallocateContainerTransition(
             TaskAttemptStateInternal.FAILED, true))
     .addTransition(TaskAttemptStateInternal.UNASSIGNED,
         TaskAttemptStateInternal.UNASSIGNED,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)

     // Transitions from the ASSIGNED state.
     .addTransition(TaskAttemptStateInternal.ASSIGNED, TaskAttemptStateInternal.RUNNING,
         TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
         new LaunchedContainerTransition())
     .addTransition(TaskAttemptStateInternal.ASSIGNED, TaskAttemptStateInternal.ASSIGNED,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     .addTransition(TaskAttemptStateInternal.ASSIGNED, TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED,
         new DeallocateContainerTransition(TaskAttemptStateInternal.FAILED, false))
     .addTransition(TaskAttemptStateInternal.ASSIGNED,
         TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_CONTAINER_COMPLETED,
         FINALIZE_FAILED_TRANSITION)
     .addTransition(TaskAttemptStateInternal.ASSIGNED, 
         TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_KILL, CLEANUP_CONTAINER_TRANSITION)
     .addTransition(TaskAttemptStateInternal.ASSIGNED,
         TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_FAILMSG, FAILED_FINISHING_TRANSITION)
     .addTransition(TaskAttemptStateInternal.ASSIGNED,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             CLEANUP_CONTAINER_TRANSITION)

     // Transitions from RUNNING state.
     .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.RUNNING,
         TaskAttemptEventType.TA_UPDATE, new StatusUpdater())
     .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.RUNNING,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     // If no commit is required, task goes to finishing state
     // This will give a chance for the container to exit by itself
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_DONE, SUCCEEDED_FINISHING_TRANSITION)
     // If commit is required, task goes through commit pending state.
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptEventType.TA_COMMIT_PENDING, new CommitPendingTransition())
     // Failure handling while RUNNING
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_FAILMSG, FAILED_FINISHING_TRANSITION)
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT, CLEANUP_CONTAINER_TRANSITION)
      //for handling container exit without sending the done or fail msg
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_CONTAINER_COMPLETED,
         FINALIZE_FAILED_TRANSITION)
     // Timeout handling while RUNNING
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_TIMED_OUT, CLEANUP_CONTAINER_TRANSITION)
     // if container killed by AM shutting down
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.KILLED,
         TaskAttemptEventType.TA_CONTAINER_CLEANED, new KilledTransition())
     // Kill handling
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_KILL,
         CLEANUP_CONTAINER_TRANSITION)
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.KILLED,
         TaskAttemptEventType.TA_PREEMPTED, new PreemptedTransition())

     // Transitions from SUCCESS_FINISHING_CONTAINER state
     // When the container exits by itself, the notification of container
     // completed event will be routed via NM -> RM -> AM.
     // After MRAppMaster gets notification from RM, it will generate
     // TA_CONTAINER_COMPLETED event.
     .addTransition(TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptStateInternal.SUCCEEDED,
         TaskAttemptEventType.TA_CONTAINER_COMPLETED,
         new ExitFinishingOnContainerCompletedTransition())
     // Given TA notifies task T_ATTEMPT_SUCCEEDED when it transitions to
     // SUCCESS_FINISHING_CONTAINER, it is possible to receive the event
     // TA_CONTAINER_CLEANED in the following scenario.
     // 1. It is the last task for the job.
     // 2. After the task receives T_ATTEMPT_SUCCEEDED, it will notify job.
     // 3. Job will be marked completed.
     // 4. As part of MRAppMaster's shutdown, all containers will be killed.
     .addTransition(TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptStateInternal.SUCCEEDED,
         TaskAttemptEventType.TA_CONTAINER_CLEANED,
         new ExitFinishingOnContainerCleanedupTransition())
     // The client wants to kill the task. Given the task is in finishing
     // state, it could go to succeeded state or killed state. If it is a
     // reducer, it will go to succeeded state;
     // otherwise, it goes to killed state.
     .addTransition(TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         EnumSet.of(TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
             TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP),
         TaskAttemptEventType.TA_KILL,
         new KilledAfterSucceededFinishingTransition())
     // The attempt stays in finishing state for too long
     // Let us clean up the container
     .addTransition(TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_TIMED_OUT, FINISHING_ON_TIMEOUT_TRANSITION)
     .addTransition(TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     // ignore-able events
     .addTransition(TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         EnumSet.of(TaskAttemptEventType.TA_UPDATE,
             TaskAttemptEventType.TA_DONE,
             TaskAttemptEventType.TA_COMMIT_PENDING,
             TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT))

     // Transitions from FAIL_FINISHING_CONTAINER state
     // When the container exits by itself, the notification of container
     // completed event will be routed via NM -> RM -> AM.
     // After MRAppMaster gets notification from RM, it will generate
     // TA_CONTAINER_COMPLETED event.
    .addTransition(TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        TaskAttemptStateInternal.FAILED,
        TaskAttemptEventType.TA_CONTAINER_COMPLETED,
        new ExitFinishingOnContainerCompletedTransition())
     // Given TA notifies task T_ATTEMPT_FAILED when it transitions to
     // FAIL_FINISHING_CONTAINER, it is possible to receive the event
     // TA_CONTAINER_CLEANED in the following scenario.
     // 1. It is the last task attempt for the task.
     // 2. After the task receives T_ATTEMPT_FAILED, it will notify job.
     // 3. Job will be marked failed.
     // 4. As part of MRAppMaster's shutdown, all containers will be killed.
    .addTransition(TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        TaskAttemptStateInternal.FAILED,
        TaskAttemptEventType.TA_CONTAINER_CLEANED,
        new ExitFinishingOnContainerCleanedupTransition())
    .addTransition(TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
        TaskAttemptEventType.TA_TIMED_OUT, FINISHING_ON_TIMEOUT_TRANSITION)
    .addTransition(TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
        DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
        // ignore-able events
    .addTransition(TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
        EnumSet.of(TaskAttemptEventType.TA_KILL,
            TaskAttemptEventType.TA_UPDATE,
            TaskAttemptEventType.TA_DONE,
            TaskAttemptEventType.TA_COMMIT_PENDING,
            TaskAttemptEventType.TA_FAILMSG,
            TaskAttemptEventType.TA_FAILMSG_BY_CLIENT))

     // Transitions from COMMIT_PENDING state
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptEventType.TA_UPDATE,
         new StatusUpdater())
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_DONE, SUCCEEDED_FINISHING_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_KILL,
         CLEANUP_CONTAINER_TRANSITION)
     // if container killed by AM shutting down
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.KILLED,
         TaskAttemptEventType.TA_CONTAINER_CLEANED, new KilledTransition())
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_FAILMSG, FAILED_FINISHING_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             CLEANUP_CONTAINER_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_CONTAINER_COMPLETED,
         FINALIZE_FAILED_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_TIMED_OUT, CLEANUP_CONTAINER_TRANSITION)
     // AM is likely to receive duplicate TA_COMMIT_PENDINGs as the task attempt
     // will re-send the commit message until it doesn't encounter any
     // IOException and succeeds in delivering the commit message.
     // Ignoring the duplicate commit message is a short-term fix. In long term,
     // we need to make use of retry cache to help this and other MR protocol
     // APIs that can be considered as @AtMostOnce.
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptEventType.TA_COMMIT_PENDING)

     // Transitions from SUCCESS_CONTAINER_CLEANUP state
     // kill and cleanup the container
     .addTransition(TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
         TaskAttemptStateInternal.SUCCEEDED,
         TaskAttemptEventType.TA_CONTAINER_CLEANED)
     .addTransition(
          TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
          TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
          TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
          DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
      // Ignore-able events
     .addTransition(TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
         TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
         EnumSet.of(TaskAttemptEventType.TA_KILL,
             TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             TaskAttemptEventType.TA_TIMED_OUT,
             TaskAttemptEventType.TA_CONTAINER_COMPLETED))

     // Transitions from FAIL_CONTAINER_CLEANUP state.
     .addTransition(TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptStateInternal.FAIL_TASK_CLEANUP,
         TaskAttemptEventType.TA_CONTAINER_CLEANED, new TaskCleanupTransition())
     .addTransition(TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
      // Ignore-able events
     .addTransition(TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         EnumSet.of(TaskAttemptEventType.TA_KILL,
             TaskAttemptEventType.TA_CONTAINER_COMPLETED,
             TaskAttemptEventType.TA_UPDATE,
             TaskAttemptEventType.TA_COMMIT_PENDING,
             // Container launch events can arrive late
             TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
             TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED,
             TaskAttemptEventType.TA_DONE,
             TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             TaskAttemptEventType.TA_TIMED_OUT))

      // Transitions from KILL_CONTAINER_CLEANUP
     .addTransition(TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
         TaskAttemptStateInternal.KILL_TASK_CLEANUP,
         TaskAttemptEventType.TA_CONTAINER_CLEANED, new TaskCleanupTransition())
     .addTransition(TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
         TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     // Ignore-able events
     .addTransition(
         TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
         TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
         EnumSet.of(TaskAttemptEventType.TA_KILL,
             TaskAttemptEventType.TA_CONTAINER_COMPLETED,
             TaskAttemptEventType.TA_UPDATE,
             TaskAttemptEventType.TA_COMMIT_PENDING,
             TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
             TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED,
             TaskAttemptEventType.TA_DONE,
             TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             TaskAttemptEventType.TA_TIMED_OUT))

     // Transitions from FAIL_TASK_CLEANUP
     // run the task cleanup
     .addTransition(TaskAttemptStateInternal.FAIL_TASK_CLEANUP,
         TaskAttemptStateInternal.FAILED, TaskAttemptEventType.TA_CLEANUP_DONE,
         new FailedTransition())
     .addTransition(TaskAttemptStateInternal.FAIL_TASK_CLEANUP,
         TaskAttemptStateInternal.FAIL_TASK_CLEANUP,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
      // Ignore-able events
     .addTransition(TaskAttemptStateInternal.FAIL_TASK_CLEANUP,
         TaskAttemptStateInternal.FAIL_TASK_CLEANUP,
         EnumSet.of(TaskAttemptEventType.TA_KILL,
             TaskAttemptEventType.TA_CONTAINER_COMPLETED,
             TaskAttemptEventType.TA_UPDATE,
             TaskAttemptEventType.TA_COMMIT_PENDING,
             TaskAttemptEventType.TA_DONE,
             TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             TaskAttemptEventType.TA_CONTAINER_CLEANED,
             // Container launch events can arrive late
             TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
             TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED))

     // Transitions from KILL_TASK_CLEANUP
     .addTransition(TaskAttemptStateInternal.KILL_TASK_CLEANUP,
         TaskAttemptStateInternal.KILLED, TaskAttemptEventType.TA_CLEANUP_DONE,
         new KilledTransition())
     .addTransition(TaskAttemptStateInternal.KILL_TASK_CLEANUP,
         TaskAttemptStateInternal.KILL_TASK_CLEANUP,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     // Ignore-able events
     .addTransition(TaskAttemptStateInternal.KILL_TASK_CLEANUP,
         TaskAttemptStateInternal.KILL_TASK_CLEANUP,
         EnumSet.of(TaskAttemptEventType.TA_KILL,
             TaskAttemptEventType.TA_CONTAINER_COMPLETED,
             TaskAttemptEventType.TA_UPDATE,
             TaskAttemptEventType.TA_COMMIT_PENDING,
             TaskAttemptEventType.TA_DONE,
             TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             TaskAttemptEventType.TA_CONTAINER_CLEANED,
             TaskAttemptEventType.TA_PREEMPTED,
             // Container launch events can arrive late
             TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
             TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED))

      // Transitions from SUCCEEDED
     .addTransition(TaskAttemptStateInternal.SUCCEEDED, //only possible for map attempts
         TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE,
         new TooManyFetchFailureTransition())
      .addTransition(TaskAttemptStateInternal.SUCCEEDED,
          EnumSet.of(TaskAttemptStateInternal.SUCCEEDED, TaskAttemptStateInternal.KILLED),
          TaskAttemptEventType.TA_KILL,
          new KilledAfterSuccessTransition())
     .addTransition(
         TaskAttemptStateInternal.SUCCEEDED, TaskAttemptStateInternal.SUCCEEDED,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     // Ignore-able events for SUCCEEDED state
     .addTransition(TaskAttemptStateInternal.SUCCEEDED,
         TaskAttemptStateInternal.SUCCEEDED,
         EnumSet.of(TaskAttemptEventType.TA_FAILMSG,
             TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             // TaskAttemptFinishingMonitor might time out the attempt right
             // after the attempt receives TA_CONTAINER_COMPLETED.
             TaskAttemptEventType.TA_TIMED_OUT,
             TaskAttemptEventType.TA_CONTAINER_CLEANED,
             TaskAttemptEventType.TA_CONTAINER_COMPLETED))

     // Transitions from FAILED state
     .addTransition(TaskAttemptStateInternal.FAILED, TaskAttemptStateInternal.FAILED,
       TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
       DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     // Ignore-able events for FAILED state
     .addTransition(TaskAttemptStateInternal.FAILED, TaskAttemptStateInternal.FAILED,
       FAILED_KILLED_STATE_IGNORED_EVENTS)

     // Transitions from KILLED state
     .addTransition(TaskAttemptStateInternal.KILLED, TaskAttemptStateInternal.KILLED,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     // Ignore-able events for KILLED state
     .addTransition(TaskAttemptStateInternal.KILLED, TaskAttemptStateInternal.KILLED,
       FAILED_KILLED_STATE_IGNORED_EVENTS)

     // create the topology tables
     .installTopology();

  private final StateMachine
         <TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent>
    stateMachine;

  @VisibleForTesting
  public Container container;
  private String nodeRackName;
  private WrappedJvmID jvmID;
  
  //this takes good amount of memory ~ 30KB. Instantiate it lazily
  //and make it null once task is launched.
  private org.apache.hadoop.mapred.Task remoteTask;
  
  //this is the last status reported by the REMOTE running attempt
  private TaskAttemptStatus reportedStatus;
  
  private static final String LINE_SEPARATOR = System
      .getProperty("line.separator");

  public TaskAttemptImpl(TaskId taskId, int i, 
      EventHandler eventHandler,
      TaskAttemptListener taskAttemptListener, Path jobFile, int partition,
      JobConf conf, String[] dataLocalHosts,
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      AppContext appContext) {
    oldJobId = TypeConverter.fromYarn(taskId.getJobId());
    this.conf = conf;
    this.clock = clock;
    attemptId = recordFactory.newRecordInstance(TaskAttemptId.class);
    attemptId.setTaskId(taskId);
    attemptId.setId(i);
    this.taskAttemptListener = taskAttemptListener;
    this.appContext = appContext;

    // Initialize reportedStatus
    reportedStatus = new TaskAttemptStatus();
    initTaskAttemptStatus(reportedStatus);

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();

    this.credentials = credentials;
    this.jobToken = jobToken;
    this.eventHandler = eventHandler;
    this.jobFile = jobFile;
    this.partition = partition;

    //TODO:create the resource reqt for this Task attempt
    this.resourceCapability = recordFactory.newRecordInstance(Resource.class);
    this.resourceCapability.setMemorySize(
        getMemoryRequired(conf, taskId.getTaskType()));
    this.resourceCapability.setVirtualCores(
        getCpuRequired(conf, taskId.getTaskType()));

    this.dataLocalHosts = resolveHosts(dataLocalHosts);
    RackResolver.init(conf);
    this.dataLocalRacks = new HashSet<String>(); 
    for (String host : this.dataLocalHosts) {
      this.dataLocalRacks.add(RackResolver.resolve(host).getNetworkLocation());
    }

    locality = Locality.OFF_SWITCH;
    avataar = Avataar.VIRGIN;

    // This "this leak" is okay because the retained pointer is in an
    //  instance variable.
    stateMachine = stateMachineFactory.make(this);
  }

  private int getMemoryRequired(JobConf conf, TaskType taskType) {
    return conf.getMemoryRequired(TypeConverter.fromYarn(taskType));
  }

  private int getCpuRequired(Configuration conf, TaskType taskType) {
    int vcores = 1;
    if (taskType == TaskType.MAP)  {
      vcores =
          conf.getInt(MRJobConfig.MAP_CPU_VCORES,
              MRJobConfig.DEFAULT_MAP_CPU_VCORES);
    } else if (taskType == TaskType.REDUCE) {
      vcores =
          conf.getInt(MRJobConfig.REDUCE_CPU_VCORES,
              MRJobConfig.DEFAULT_REDUCE_CPU_VCORES);
    }
    
    return vcores;
  }

  /**
   * Create a {@link LocalResource} record with all the given parameters.
   */
  private static LocalResource createLocalResource(FileSystem fc, Path file,
      LocalResourceType type, LocalResourceVisibility visibility)
      throws IOException {
    FileStatus fstat = fc.getFileStatus(file);
    URL resourceURL = URL.fromPath(fc.resolvePath(fstat.getPath()));
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();

    return LocalResource.newInstance(resourceURL, type, visibility,
      resourceSize, resourceModificationTime);
  }

  /**
   * Lock this on initialClasspath so that there is only one fork in the AM for
   * getting the initial class-path. TODO: We already construct
   * a parent CLC and use it for all the containers, so this should go away
   * once the mr-generated-classpath stuff is gone.
   */
  private static String getInitialClasspath(Configuration conf) throws IOException {
    synchronized (classpathLock) {
      if (initialClasspathFlag.get()) {
        return initialClasspath;
      }
      Map<String, String> env = new HashMap<String, String>();
      MRApps.setClasspath(env, conf);
      initialClasspath = env.get(Environment.CLASSPATH.name());
      initialAppClasspath = env.get(Environment.APP_CLASSPATH.name());
      initialClasspathFlag.set(true);
      return initialClasspath;
    }
  }


  /**
   * Create the common {@link ContainerLaunchContext} for all attempts.
   * @param applicationACLs 
   */
  private static ContainerLaunchContext createCommonContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs, Configuration conf,
      Token<JobTokenIdentifier> jobToken,
      final org.apache.hadoop.mapred.JobID oldJobId,
      Credentials credentials) {
    // Application resources
    Map<String, LocalResource> localResources = 
        new HashMap<String, LocalResource>();
    
    // Application environment
    Map<String, String> environment;

    // Service data
    Map<String, ByteBuffer> serviceData = new HashMap<String, ByteBuffer>();

    // Tokens
    ByteBuffer taskCredentialsBuffer = ByteBuffer.wrap(new byte[]{});
    try {

      configureJobJar(conf, localResources);

      configureJobConf(conf, localResources, oldJobId);

      // Setup DistributedCache
      MRApps.setupDistributedCache(conf, localResources);

      taskCredentialsBuffer =
          configureTokens(jobToken, credentials, serviceData);

      addExternalShuffleProviders(conf, serviceData);

      environment = configureEnv(conf);

    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }

    // Construct the actual Container
    // The null fields are per-container and will be constructed for each
    // container separately.
    ContainerLaunchContext container =
        ContainerLaunchContext.newInstance(localResources, environment, null,
            serviceData, taskCredentialsBuffer, applicationACLs);

    return container;
  }

  private static Map<String, String> configureEnv(Configuration conf)
      throws IOException {
    Map<String, String> environment = new HashMap<String, String>();
    MRApps.addToEnvironment(environment, Environment.CLASSPATH.name(),
        getInitialClasspath(conf), conf);

    if (initialAppClasspath != null) {
      MRApps.addToEnvironment(environment, Environment.APP_CLASSPATH.name(),
          initialAppClasspath, conf);
    }

    // Shell
    environment.put(Environment.SHELL.name(), conf
        .get(MRJobConfig.MAPRED_ADMIN_USER_SHELL, MRJobConfig.DEFAULT_SHELL));

    // Add pwd to LD_LIBRARY_PATH, add this before adding anything else
    MRApps.addToEnvironment(environment, Environment.LD_LIBRARY_PATH.name(),
        MRApps.crossPlatformifyMREnv(conf, Environment.PWD), conf);

    // Add the env variables passed by the admin
    MRApps.setEnvFromInputString(environment,
        conf.get(MRJobConfig.MAPRED_ADMIN_USER_ENV,
            MRJobConfig.DEFAULT_MAPRED_ADMIN_USER_ENV),
        conf);
    return environment;
  }

  private static void configureJobJar(Configuration conf,
      Map<String, LocalResource> localResources) throws IOException {
    // Set up JobJar to be localized properly on the remote NM.
    String jobJar = conf.get(MRJobConfig.JAR);
    if (jobJar != null) {
      final Path jobJarPath = new Path(jobJar);
      final FileSystem jobJarFs = FileSystem.get(jobJarPath.toUri(), conf);
      Path remoteJobJar = jobJarPath.makeQualified(jobJarFs.getUri(),
          jobJarFs.getWorkingDirectory());
      LocalResource rc = createLocalResource(jobJarFs, remoteJobJar,
          LocalResourceType.PATTERN, LocalResourceVisibility.APPLICATION);
      String pattern = conf.getPattern(JobContext.JAR_UNPACK_PATTERN,
          JobConf.UNPACK_JAR_PATTERN_DEFAULT).pattern();
      rc.setPattern(pattern);
      localResources.put(MRJobConfig.JOB_JAR, rc);
      LOG.info("The job-jar file on the remote FS is "
          + remoteJobJar.toUri().toASCIIString());
    } else {
      // Job jar may be null. For e.g, for pipes, the job jar is the hadoop
      // mapreduce jar itself which is already on the classpath.
      LOG.info("Job jar is not present. "
          + "Not adding any jar to the list of resources.");
    }
  }

  private static void configureJobConf(Configuration conf,
      Map<String, LocalResource> localResources,
      final org.apache.hadoop.mapred.JobID oldJobId) throws IOException {
    // Set up JobConf to be localized properly on the remote NM.
    Path path = MRApps.getStagingAreaDir(conf,
        UserGroupInformation.getCurrentUser().getShortUserName());
    Path remoteJobSubmitDir = new Path(path, oldJobId.toString());
    Path remoteJobConfPath =
        new Path(remoteJobSubmitDir, MRJobConfig.JOB_CONF_FILE);
    FileSystem remoteFS = FileSystem.get(conf);
    localResources.put(MRJobConfig.JOB_CONF_FILE,
        createLocalResource(remoteFS, remoteJobConfPath, LocalResourceType.FILE,
            LocalResourceVisibility.APPLICATION));
    LOG.info("The job-conf file on the remote FS is "
        + remoteJobConfPath.toUri().toASCIIString());
  }

  private static ByteBuffer configureTokens(Token<JobTokenIdentifier> jobToken,
      Credentials credentials,
      Map<String, ByteBuffer> serviceData) throws IOException {
    // Setup up task credentials buffer
    LOG.info("Adding #" + credentials.numberOfTokens() + " tokens and #"
        + credentials.numberOfSecretKeys()
        + " secret keys for NM use for launching container");
    Credentials taskCredentials = new Credentials(credentials);

    // LocalStorageToken is needed irrespective of whether security is enabled
    // or not.
    TokenCache.setJobToken(jobToken, taskCredentials);

    DataOutputBuffer containerTokens_dob = new DataOutputBuffer();
    LOG.info(
        "Size of containertokens_dob is " + taskCredentials.numberOfTokens());
    taskCredentials.writeTokenStorageToStream(containerTokens_dob);
    ByteBuffer taskCredentialsBuffer =
        ByteBuffer.wrap(containerTokens_dob.getData(), 0,
            containerTokens_dob.getLength());

    // Add shuffle secret key
    // The secret key is converted to a JobToken to preserve backwards
    // compatibility with an older ShuffleHandler running on an NM.
    LOG.info("Putting shuffle token in serviceData");
    byte[] shuffleSecret = TokenCache.getShuffleSecretKey(credentials);
    if (shuffleSecret == null) {
      LOG.warn("Cannot locate shuffle secret in credentials."
          + " Using job token as shuffle secret.");
      shuffleSecret = jobToken.getPassword();
    }
    Token<JobTokenIdentifier> shuffleToken =
        new Token<JobTokenIdentifier>(jobToken.getIdentifier(), shuffleSecret,
            jobToken.getKind(), jobToken.getService());
    serviceData.put(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID,
        ShuffleHandler.serializeServiceData(shuffleToken));
    return taskCredentialsBuffer;
  }

  private static void addExternalShuffleProviders(Configuration conf,
      Map<String, ByteBuffer> serviceData) {
    // add external shuffle-providers - if any
    Collection<String> shuffleProviders = conf.getStringCollection(
        MRJobConfig.MAPREDUCE_JOB_SHUFFLE_PROVIDER_SERVICES);
    if (!shuffleProviders.isEmpty()) {
      Collection<String> auxNames =
          conf.getStringCollection(YarnConfiguration.NM_AUX_SERVICES);

      for (final String shuffleProvider : shuffleProviders) {
        if (shuffleProvider
            .equals(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID)) {
          continue; // skip built-in shuffle-provider that was already inserted
                    // with shuffle secret key
        }
        if (auxNames.contains(shuffleProvider)) {
          LOG.info("Adding ShuffleProvider Service: " + shuffleProvider
              + " to serviceData");
          // This only serves for INIT_APP notifications
          // The shuffle service needs to be able to work with the host:port
          // information provided by the AM
          // (i.e. shuffle services which require custom location / other
          // configuration are not supported)
          serviceData.put(shuffleProvider, ByteBuffer.allocate(0));
        } else {
          throw new YarnRuntimeException("ShuffleProvider Service: "
              + shuffleProvider
              + " was NOT found in the list of aux-services that are "
              + "available in this NM. You may need to specify this "
              + "ShuffleProvider as an aux-service in your yarn-site.xml");
        }
      }
    }
  }

  static ContainerLaunchContext createContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs,
      Configuration conf, Token<JobTokenIdentifier> jobToken, Task remoteTask,
      final org.apache.hadoop.mapred.JobID oldJobId,
      WrappedJvmID jvmID,
      TaskAttemptListener taskAttemptListener,
      Credentials credentials) {

    synchronized (commonContainerSpecLock) {
      if (commonContainerSpec == null) {
        commonContainerSpec = createCommonContainerLaunchContext(
            applicationACLs, conf, jobToken, oldJobId, credentials);
      }
    }

    // Fill in the fields needed per-container that are missing in the common
    // spec.

    boolean userClassesTakesPrecedence =
      conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, false);

    // Setup environment by cloning from common env.
    Map<String, String> env = commonContainerSpec.getEnvironment();
    Map<String, String> myEnv = new HashMap<String, String>(env.size());
    myEnv.putAll(env);
    if (userClassesTakesPrecedence) {
      myEnv.put(Environment.CLASSPATH_PREPEND_DISTCACHE.name(), "true");
    }
    MapReduceChildJVM.setVMEnv(myEnv, remoteTask);

    // Set up the launch command
    List<String> commands = MapReduceChildJVM.getVMCommand(
        taskAttemptListener.getAddress(), remoteTask, jvmID);

    // Duplicate the ByteBuffers for access by multiple containers.
    Map<String, ByteBuffer> myServiceData = new HashMap<String, ByteBuffer>();
    for (Entry<String, ByteBuffer> entry : commonContainerSpec
                .getServiceData().entrySet()) {
      myServiceData.put(entry.getKey(), entry.getValue().duplicate());
    }

    // Construct the actual Container
    ContainerLaunchContext container = ContainerLaunchContext.newInstance(
        commonContainerSpec.getLocalResources(), myEnv, commands,
        myServiceData, commonContainerSpec.getTokens().duplicate(),
        applicationACLs);

    return container;
  }

  @Override
  public ContainerId getAssignedContainerID() {
    readLock.lock();
    try {
      return container == null ? null : container.getId();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String getAssignedContainerMgrAddress() {
    readLock.lock();
    try {
      return container == null ? null : StringInterner.weakIntern(container
        .getNodeId().toString());
    } finally {
      readLock.unlock();
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

  @Override 
  public NodeId getNodeId() {
    readLock.lock();
    try {
      return container == null ? null : container.getNodeId();
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
      return container == null ? null : container.getNodeHttpAddress();
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

  protected abstract org.apache.hadoop.mapred.Task createRemoteTask();

  @Override
  public TaskAttemptId getID() {
    return attemptId;
  }

  @Override
  public boolean isFinished() {
    readLock.lock();
    try {
      // TODO: Use stateMachine level method?
      return (getInternalState() == TaskAttemptStateInternal.SUCCEEDED || 
              getInternalState() == TaskAttemptStateInternal.FAILED ||
              getInternalState() == TaskAttemptStateInternal.KILLED);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskAttemptReport getReport() {
    TaskAttemptReport result = recordFactory.newRecordInstance(TaskAttemptReport.class);
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
      if (this.container != null) {
        result.setNodeManagerPort(this.container.getNodeId().getPort());
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
  public Phase getPhase() {
    readLock.lock();
    try {
      return reportedStatus.phase;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskAttemptState getState() {
    readLock.lock();
    try {
      return getExternalState(stateMachine.getCurrentState());
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
    writeLock.lock();
    try {
      final TaskAttemptStateInternal oldState = getInternalState()  ;
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.error("Can't handle this event at current state for "
            + this.attemptId, e);
        eventHandler.handle(new JobDiagnosticsUpdateEvent(
            this.attemptId.getTaskId().getJobId(), "Invalid event " + event.getType() + 
            " on TaskAttempt " + this.attemptId));
        eventHandler.handle(new JobEvent(this.attemptId.getTaskId().getJobId(),
            JobEventType.INTERNAL_ERROR));
      }
      if (oldState != getInternalState()) {
        if (getInternalState() == TaskAttemptStateInternal.FAILED) {
          String nodeId = null == this.container ? "Not-assigned"
              : this.container.getNodeId().toString();
          LOG.info(attemptId + " transitioned from state " + oldState + " to "
              + getInternalState() + ", event type is " + event.getType()
              + " and nodeId=" + nodeId);
        } else {
          LOG.info(attemptId + " TaskAttempt Transitioned from " + oldState
              + " to " + getInternalState());
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  public TaskAttemptStateInternal getInternalState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  public Locality getLocality() {
    return locality;
  }
  
  public void setLocality(Locality locality) {
    this.locality = locality;
  }

  public Avataar getAvataar()
  {
    return avataar;
  }
  
  public void setAvataar(Avataar avataar) {
    this.avataar = avataar;
  }
  
  @SuppressWarnings("unchecked")
  public TaskAttemptStateInternal recover(TaskAttemptInfo taInfo,
      OutputCommitter committer, boolean recoverOutput) {
    ContainerId containerId = taInfo.getContainerId();
    NodeId containerNodeId = NodeId.fromString(
        taInfo.getHostname() + ":" + taInfo.getPort());
    String nodeHttpAddress = StringInterner.weakIntern(taInfo.getHostname() + ":"
        + taInfo.getHttpPort());
    // Resource/Priority/Tokens are only needed while launching the container on
    // an NM, these are already completed tasks, so setting them to null
    container =
        Container.newInstance(containerId, containerNodeId,
          nodeHttpAddress, null, null, null);
    computeRackAndLocality();
    launchTime = taInfo.getStartTime();
    finishTime = (taInfo.getFinishTime() != -1) ?
        taInfo.getFinishTime() : clock.getTime();
    shufflePort = taInfo.getShufflePort();
    trackerName = taInfo.getHostname();
    httpPort = taInfo.getHttpPort();
    sendLaunchedEvents();

    reportedStatus.id = attemptId;
    reportedStatus.progress = 1.0f;
    reportedStatus.counters = taInfo.getCounters();
    reportedStatus.stateString = taInfo.getState();
    reportedStatus.phase = Phase.CLEANUP;
    reportedStatus.mapFinishTime = taInfo.getMapFinishTime();
    reportedStatus.shuffleFinishTime = taInfo.getShuffleFinishTime();
    reportedStatus.sortFinishTime = taInfo.getSortFinishTime();
    addDiagnosticInfo(taInfo.getError());

    boolean needToClean = false;
    String recoveredState = taInfo.getTaskStatus();
    if (recoverOutput
        && TaskAttemptState.SUCCEEDED.toString().equals(recoveredState)) {
      TaskAttemptContext tac = new TaskAttemptContextImpl(conf,
          TypeConverter.fromYarn(attemptId));
      try {
        committer.recoverTask(tac);
        LOG.info("Recovered output from task attempt " + attemptId);
      } catch (Exception e) {
        LOG.error("Unable to recover task attempt " + attemptId, e);
        LOG.info("Task attempt " + attemptId + " will be recovered as KILLED");
        recoveredState = TaskAttemptState.KILLED.toString();
        needToClean = true;
      }
    }

    TaskAttemptStateInternal attemptState;
    if (TaskAttemptState.SUCCEEDED.toString().equals(recoveredState)) {
      attemptState = TaskAttemptStateInternal.SUCCEEDED;
      reportedStatus.taskState = TaskAttemptState.SUCCEEDED;
      eventHandler.handle(createJobCounterUpdateEventTASucceeded(this));
      logAttemptFinishedEvent(attemptState);
    } else if (TaskAttemptState.FAILED.toString().equals(recoveredState)) {
      attemptState = TaskAttemptStateInternal.FAILED;
      reportedStatus.taskState = TaskAttemptState.FAILED;
      eventHandler.handle(createJobCounterUpdateEventTAFailed(this, false));
      TaskAttemptUnsuccessfulCompletionEvent tauce =
          createTaskAttemptUnsuccessfulCompletionEvent(this,
              TaskAttemptStateInternal.FAILED);
      eventHandler.handle(
          new JobHistoryEvent(attemptId.getTaskId().getJobId(), tauce));
    } else {
      if (!TaskAttemptState.KILLED.toString().equals(recoveredState)) {
        if (String.valueOf(recoveredState).isEmpty()) {
          LOG.info("TaskAttempt" + attemptId
              + " had not completed, recovering as KILLED");
        } else {
          LOG.warn("TaskAttempt " + attemptId + " found in unexpected state "
              + recoveredState + ", recovering as KILLED");
        }
        addDiagnosticInfo("Killed during application recovery");
        needToClean = true;
      }
      attemptState = TaskAttemptStateInternal.KILLED;
      reportedStatus.taskState = TaskAttemptState.KILLED;
      eventHandler.handle(createJobCounterUpdateEventTAKilled(this, false));
      TaskAttemptUnsuccessfulCompletionEvent tauce =
          createTaskAttemptUnsuccessfulCompletionEvent(this,
              TaskAttemptStateInternal.KILLED);
      eventHandler.handle(
          new JobHistoryEvent(attemptId.getTaskId().getJobId(), tauce));
    }

    if (needToClean) {
      TaskAttemptContext tac = new TaskAttemptContextImpl(conf,
          TypeConverter.fromYarn(attemptId));
      try {
        committer.abortTask(tac);
      } catch (Exception e) {
        LOG.warn("Task cleanup failed for attempt " + attemptId, e);
      }
    }

    return attemptState;
  }

  protected static TaskAttemptState getExternalState(
      TaskAttemptStateInternal smState) {
    switch (smState) {
    case ASSIGNED:
    case UNASSIGNED:
      return TaskAttemptState.STARTING;
    case COMMIT_PENDING:
      return TaskAttemptState.COMMIT_PENDING;
    case FAIL_CONTAINER_CLEANUP:
    case FAIL_TASK_CLEANUP:
    case FAIL_FINISHING_CONTAINER:
    case FAILED:
      return TaskAttemptState.FAILED;
    case KILL_CONTAINER_CLEANUP:
    case KILL_TASK_CLEANUP:
    case KILLED:
      return TaskAttemptState.KILLED;
    case RUNNING:
      return TaskAttemptState.RUNNING;
    case NEW:
      return TaskAttemptState.NEW;
    case SUCCESS_CONTAINER_CLEANUP:
    case SUCCESS_FINISHING_CONTAINER:
    case SUCCEEDED:
      return TaskAttemptState.SUCCEEDED;
    default:
      throw new YarnRuntimeException("Attempt to convert invalid "
          + "stateMachineTaskAttemptState to externalTaskAttemptState: "
          + smState);
    }
  }

  // check whether the attempt is assigned if container is not null
  boolean isContainerAssigned() {
    return container != null;
  }

  //always called in write lock
  private boolean getRescheduleNextAttempt() {
    return rescheduleNextAttempt;
  }

  //always called in write lock
  private void setRescheduleNextAttempt(boolean reschedule) {
    rescheduleNextAttempt = reschedule;
  }

  //always called in write lock
  private void setFinishTime() {
    //set the finish time only if launch time is set
    if (launchTime != 0) {
      finishTime = clock.getTime();
    }
  }

  private void computeRackAndLocality() {
    NodeId containerNodeId = container.getNodeId();
    nodeRackName = RackResolver.resolve(
        containerNodeId.getHost()).getNetworkLocation();

    locality = Locality.OFF_SWITCH;
    if (dataLocalHosts.size() > 0) {
      String cHost = resolveHost(containerNodeId.getHost());
      if (dataLocalHosts.contains(cHost)) {
        locality = Locality.NODE_LOCAL;
      }
    }
    if (locality == Locality.OFF_SWITCH) {
      if (dataLocalRacks.contains(nodeRackName)) {
        locality = Locality.RACK_LOCAL;
      }
    }
  }
  
  private static void updateMillisCounters(JobCounterUpdateEvent jce,
      TaskAttemptImpl taskAttempt) {
    // if container/resource if not allocated, do not update
    if (null == taskAttempt.container ||
        null == taskAttempt.container.getResource()) {
      return;
    }
    long duration = (taskAttempt.getFinishTime() - taskAttempt.getLaunchTime());
    Resource allocatedResource = taskAttempt.container.getResource();
    int mbAllocated = (int) allocatedResource.getMemorySize();
    int vcoresAllocated = allocatedResource.getVirtualCores();
    int minSlotMemSize = taskAttempt.conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int simSlotsAllocated = minSlotMemSize == 0 ? 0 :
        (int) Math.ceil((float) mbAllocated / minSlotMemSize);

    TaskType taskType = taskAttempt.getID().getTaskId().getTaskType();
    if (taskType == TaskType.MAP) {
      jce.addCounterUpdate(JobCounter.SLOTS_MILLIS_MAPS,
          simSlotsAllocated * duration);
      jce.addCounterUpdate(JobCounter.MB_MILLIS_MAPS, duration * mbAllocated);
      jce.addCounterUpdate(JobCounter.VCORES_MILLIS_MAPS,
          duration * vcoresAllocated);
      jce.addCounterUpdate(JobCounter.MILLIS_MAPS, duration);
    } else {
      jce.addCounterUpdate(JobCounter.SLOTS_MILLIS_REDUCES,
          simSlotsAllocated * duration);
      jce.addCounterUpdate(JobCounter.MB_MILLIS_REDUCES,
          duration * mbAllocated);
      jce.addCounterUpdate(JobCounter.VCORES_MILLIS_REDUCES,
          duration * vcoresAllocated);
      jce.addCounterUpdate(JobCounter.MILLIS_REDUCES, duration);
    }
  }

  private static JobCounterUpdateEvent createJobCounterUpdateEventTASucceeded(
      TaskAttemptImpl taskAttempt) {
    TaskId taskId = taskAttempt.attemptId.getTaskId();
    JobCounterUpdateEvent jce = new JobCounterUpdateEvent(taskId.getJobId());
    updateMillisCounters(jce, taskAttempt);
    return jce;
  }
  
  private static JobCounterUpdateEvent createJobCounterUpdateEventTAFailed(
      TaskAttemptImpl taskAttempt, boolean taskAlreadyCompleted) {
    TaskType taskType = taskAttempt.getID().getTaskId().getTaskType();
    JobCounterUpdateEvent jce = new JobCounterUpdateEvent(taskAttempt.getID().getTaskId().getJobId());
    
    if (taskType == TaskType.MAP) {
      jce.addCounterUpdate(JobCounter.NUM_FAILED_MAPS, 1);
    } else {
      jce.addCounterUpdate(JobCounter.NUM_FAILED_REDUCES, 1);
    }
    if (!taskAlreadyCompleted) {
      updateMillisCounters(jce, taskAttempt);
    }
    return jce;
  }
  
  private static JobCounterUpdateEvent createJobCounterUpdateEventTAKilled(
      TaskAttemptImpl taskAttempt, boolean taskAlreadyCompleted) {
    TaskType taskType = taskAttempt.getID().getTaskId().getTaskType();
    JobCounterUpdateEvent jce = new JobCounterUpdateEvent(taskAttempt.getID().getTaskId().getJobId());
    
    if (taskType == TaskType.MAP) {
      jce.addCounterUpdate(JobCounter.NUM_KILLED_MAPS, 1);
    } else {
      jce.addCounterUpdate(JobCounter.NUM_KILLED_REDUCES, 1);
    }
    if (!taskAlreadyCompleted) {
      updateMillisCounters(jce, taskAttempt);
    }
    return jce;
  }  

  private static
      TaskAttemptUnsuccessfulCompletionEvent
      createTaskAttemptUnsuccessfulCompletionEvent(TaskAttemptImpl taskAttempt,
          TaskAttemptStateInternal attemptState) {
    TaskAttemptUnsuccessfulCompletionEvent tauce =
        new TaskAttemptUnsuccessfulCompletionEvent(
            TypeConverter.fromYarn(taskAttempt.attemptId),
            TypeConverter.fromYarn(taskAttempt.attemptId.getTaskId()
                .getTaskType()), attemptState.toString(),
            taskAttempt.finishTime,
            taskAttempt.container == null ? "UNKNOWN"
                : taskAttempt.container.getNodeId().getHost(),
            taskAttempt.container == null ? -1 
                : taskAttempt.container.getNodeId().getPort(),    
            taskAttempt.nodeRackName == null ? "UNKNOWN" 
                : taskAttempt.nodeRackName,
            StringUtils.join(
                LINE_SEPARATOR, taskAttempt.getDiagnostics()),
                taskAttempt.getCounters(), taskAttempt
                .getProgressSplitBlock().burst(), taskAttempt.launchTime);
    return tauce;
  }

  private static void
      sendJHStartEventForAssignedFailTask(TaskAttemptImpl taskAttempt) {
    if (null == taskAttempt.container) {
      return;
    }
    taskAttempt.launchTime = taskAttempt.clock.getTime();

    InetSocketAddress nodeHttpInetAddr =
        NetUtils.createSocketAddr(taskAttempt.container.getNodeHttpAddress());
    taskAttempt.trackerName = nodeHttpInetAddr.getHostName();
    taskAttempt.httpPort = nodeHttpInetAddr.getPort();
    taskAttempt.sendLaunchedEvents();
  }


  @SuppressWarnings("unchecked")
  private void sendLaunchedEvents() {
    JobCounterUpdateEvent jce = new JobCounterUpdateEvent(attemptId.getTaskId()
        .getJobId());
    jce.addCounterUpdate(attemptId.getTaskId().getTaskType() == TaskType.MAP ?
        JobCounter.TOTAL_LAUNCHED_MAPS : JobCounter.TOTAL_LAUNCHED_REDUCES, 1);
    eventHandler.handle(jce);

    LOG.info("TaskAttempt: [" + attemptId
        + "] using containerId: [" + container.getId() + " on NM: ["
        + StringInterner.weakIntern(container.getNodeId().toString()) + "]");
    TaskAttemptStartedEvent tase =
      new TaskAttemptStartedEvent(TypeConverter.fromYarn(attemptId),
          TypeConverter.fromYarn(attemptId.getTaskId().getTaskType()),
          launchTime, trackerName, httpPort, shufflePort, container.getId(),
          locality.toString(), avataar.toString());
    eventHandler.handle(
        new JobHistoryEvent(attemptId.getTaskId().getJobId(), tase));
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
      long start = getLaunchTime(); // TODO Ensure not 0

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

  private static void finalizeProgress(TaskAttemptImpl taskAttempt) {
    // unregister it to TaskAttemptListener so that it stops listening
    taskAttempt.taskAttemptListener.unregister(
        taskAttempt.attemptId, taskAttempt.jvmID);
    taskAttempt.reportedStatus.progress = 1.0f;
    taskAttempt.updateProgressSplits();
  }


  static class RequestContainerTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    private final boolean rescheduled;
    public RequestContainerTransition(boolean rescheduled) {
      this.rescheduled = rescheduled;
    }
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt, 
        TaskAttemptEvent event) {
      // Tell any speculator that we're requesting a container
      taskAttempt.eventHandler.handle
          (new SpeculatorEvent(taskAttempt.getID().getTaskId(), +1));
      //request for container
      if (rescheduled) {
        taskAttempt.eventHandler.handle(
            ContainerRequestEvent.createContainerRequestEventForFailedContainer(
                taskAttempt.attemptId, 
                taskAttempt.resourceCapability));
      } else {
        taskAttempt.eventHandler.handle(new ContainerRequestEvent(
            taskAttempt.attemptId, taskAttempt.resourceCapability,
            taskAttempt.dataLocalHosts.toArray(
                new String[taskAttempt.dataLocalHosts.size()]),
            taskAttempt.dataLocalRacks.toArray(
                new String[taskAttempt.dataLocalRacks.size()])));
      }
    }
  }

  protected Set<String> resolveHosts(String[] src) {
    Set<String> result = new HashSet<String>();
    if (src != null) {
      for (int i = 0; i < src.length; i++) {
        if (src[i] == null) {
          continue;
        } else if (isIP(src[i])) {
          result.add(resolveHost(src[i]));
        } else {
          result.add(src[i]);
        }
      }
    }
    return result;
  }

  protected String resolveHost(String src) {
    String result = src; // Fallback in case of failure.
    try {
      InetAddress addr = InetAddress.getByName(src);
      result = addr.getHostName();
    } catch (UnknownHostException e) {
      LOG.warn("Failed to resolve address: " + src
          + ". Continuing to use the same.");
    }
    return result;
  }

  private static final Pattern ipPattern = // Pattern for matching ip
    Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
  
  protected boolean isIP(String src) {
    return ipPattern.matcher(src).matches();
  }

  private static class ContainerAssignedTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings({ "unchecked" })
    @Override
    public void transition(final TaskAttemptImpl taskAttempt, 
        TaskAttemptEvent event) {
      final TaskAttemptContainerAssignedEvent cEvent = 
        (TaskAttemptContainerAssignedEvent) event;
      Container container = cEvent.getContainer();
      taskAttempt.container = container;
      // this is a _real_ Task (classic Hadoop mapred flavor):
      taskAttempt.remoteTask = taskAttempt.createRemoteTask();
      taskAttempt.jvmID =
          new WrappedJvmID(taskAttempt.remoteTask.getTaskID().getJobID(),
              taskAttempt.remoteTask.isMapTask(),
              taskAttempt.container.getId().getContainerId());
      taskAttempt.taskAttemptListener.registerPendingTask(
          taskAttempt.remoteTask, taskAttempt.jvmID);

      taskAttempt.computeRackAndLocality();
      
      //launch the container
      //create the container object to be launched for a given Task attempt
      ContainerLaunchContext launchContext = createContainerLaunchContext(
          cEvent.getApplicationACLs(), taskAttempt.conf, taskAttempt.jobToken,
          taskAttempt.remoteTask, taskAttempt.oldJobId, taskAttempt.jvmID,
          taskAttempt.taskAttemptListener, taskAttempt.credentials);
      taskAttempt.eventHandler
        .handle(new ContainerRemoteLaunchEvent(taskAttempt.attemptId,
          launchContext, container, taskAttempt.remoteTask));

      // send event to speculator that our container needs are satisfied
      taskAttempt.eventHandler.handle
          (new SpeculatorEvent(taskAttempt.getID().getTaskId(), -1));
    }
  }

  private static class DeallocateContainerTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    private final TaskAttemptStateInternal finalState;
    private final boolean withdrawsContainerRequest;
    DeallocateContainerTransition
        (TaskAttemptStateInternal finalState, boolean withdrawsContainerRequest) {
      this.finalState = finalState;
      this.withdrawsContainerRequest = withdrawsContainerRequest;
    }
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt, 
        TaskAttemptEvent event) {
      if (taskAttempt.getLaunchTime() == 0) {
        sendJHStartEventForAssignedFailTask(taskAttempt);
      }
      //set the finish time
      taskAttempt.setFinishTime();

      if (event instanceof TaskAttemptKillEvent) {
        taskAttempt.addDiagnosticInfo(
            ((TaskAttemptKillEvent) event).getMessage());
      }

      //send the deallocate event to ContainerAllocator
      taskAttempt.eventHandler.handle(
          new ContainerAllocatorEvent(taskAttempt.attemptId,
          ContainerAllocator.EventType.CONTAINER_DEALLOCATE));

      // send event to speculator that we withdraw our container needs, if
      //  we're transitioning out of UNASSIGNED
      if (withdrawsContainerRequest) {
        taskAttempt.eventHandler.handle
            (new SpeculatorEvent(taskAttempt.getID().getTaskId(), -1));
      }

      switch(finalState) {
        case FAILED:
          taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
              taskAttempt.attemptId,
              TaskEventType.T_ATTEMPT_FAILED));
          break;
        case KILLED:
          taskAttempt.eventHandler.handle(new TaskTAttemptKilledEvent(
              taskAttempt.attemptId, false));
          break;
        default:
          LOG.error("Task final state is not FAILED or KILLED: " + finalState);
      }

      TaskAttemptUnsuccessfulCompletionEvent tauce =
          createTaskAttemptUnsuccessfulCompletionEvent(taskAttempt,
              finalState);
      if(finalState == TaskAttemptStateInternal.FAILED) {
        taskAttempt.eventHandler
          .handle(createJobCounterUpdateEventTAFailed(taskAttempt, false));
      } else if(finalState == TaskAttemptStateInternal.KILLED) {
        taskAttempt.eventHandler
        .handle(createJobCounterUpdateEventTAKilled(taskAttempt, false));
      }
      taskAttempt.eventHandler.handle(new JobHistoryEvent(
          taskAttempt.attemptId.getTaskId().getJobId(), tauce));
    }
  }

  private static class LaunchedContainerTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt, 
        TaskAttemptEvent evnt) {

      TaskAttemptContainerLaunchedEvent event =
        (TaskAttemptContainerLaunchedEvent) evnt;

      //set the launch time
      taskAttempt.launchTime = taskAttempt.clock.getTime();
      taskAttempt.shufflePort = event.getShufflePort();

      // register it to TaskAttemptListener so that it can start monitoring it.
      taskAttempt.taskAttemptListener
        .registerLaunchedTask(taskAttempt.attemptId, taskAttempt.jvmID);
      //TODO Resolve to host / IP in case of a local address.
      InetSocketAddress nodeHttpInetAddr = // TODO: Costly to create sock-addr?
          NetUtils.createSocketAddr(taskAttempt.container.getNodeHttpAddress());
      taskAttempt.trackerName = nodeHttpInetAddr.getHostName();
      taskAttempt.httpPort = nodeHttpInetAddr.getPort();
      taskAttempt.sendLaunchedEvents();
      taskAttempt.eventHandler.handle
          (new SpeculatorEvent
              (taskAttempt.attemptId, true, taskAttempt.clock.getTime()));
      //make remoteTask reference as null as it is no more needed
      //and free up the memory
      taskAttempt.remoteTask = null;
      
      //tell the Task that attempt has started
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
          taskAttempt.attemptId, 
         TaskEventType.T_ATTEMPT_LAUNCHED));
    }
  }
   
  private static class CommitPendingTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt, 
        TaskAttemptEvent event) {
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
          taskAttempt.attemptId, 
         TaskEventType.T_ATTEMPT_COMMIT_PENDING));
    }
  }

  private static class TaskCleanupTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt, 
        TaskAttemptEvent event) {
      TaskAttemptContext taskContext =
        new TaskAttemptContextImpl(taskAttempt.conf,
            TypeConverter.fromYarn(taskAttempt.attemptId));
      taskAttempt.eventHandler.handle(new CommitterTaskAbortEvent(
          taskAttempt.attemptId, taskContext));
    }
  }

  /**
   * Transition from SUCCESS_FINISHING_CONTAINER or FAIL_FINISHING_CONTAINER
   * state upon receiving TA_CONTAINER_COMPLETED event
   */
  private static class ExitFinishingOnContainerCompletedTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
       TaskAttemptEvent event) {
      taskAttempt.appContext.getTaskAttemptFinishingMonitor().unregister(
          taskAttempt.attemptId);
      sendContainerCompleted(taskAttempt);
    }
  }

  private static class ExitFinishingOnContainerCleanedupTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      taskAttempt.appContext.getTaskAttemptFinishingMonitor().unregister(
          taskAttempt.attemptId);
    }
  }

  private static class FailedTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      // set the finish time
      taskAttempt.setFinishTime();
      notifyTaskAttemptFailed(taskAttempt);
    }
  }

  private static class FinalizeFailedTransition extends FailedTransition {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      finalizeProgress(taskAttempt);
      sendContainerCompleted(taskAttempt);
      super.transition(taskAttempt, event);
    }
  }

  @SuppressWarnings("unchecked")
  private static void sendContainerCompleted(TaskAttemptImpl taskAttempt) {
    taskAttempt.eventHandler.handle(new ContainerLauncherEvent(
        taskAttempt.attemptId,
        taskAttempt.container.getId(), StringInterner
        .weakIntern(taskAttempt.container.getNodeId().toString()),
        taskAttempt.container.getContainerToken(),
        ContainerLauncher.EventType.CONTAINER_COMPLETED));
  }

  private static class RecoverTransition implements
      MultipleArcTransition<TaskAttemptImpl, TaskAttemptEvent, TaskAttemptStateInternal> {

    @Override
    public TaskAttemptStateInternal transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      TaskAttemptRecoverEvent tare = (TaskAttemptRecoverEvent) event;
      return taskAttempt.recover(tare.getTaskAttemptInfo(),
          tare.getCommitter(), tare.getRecoverOutput());
    }
  }

  @SuppressWarnings({ "unchecked" })
  private void logAttemptFinishedEvent(TaskAttemptStateInternal state) {
    //Log finished events only if an attempt started.
    if (getLaunchTime() == 0) return; 
    String containerHostName = this.container == null ? "UNKNOWN"
         : this.container.getNodeId().getHost();
    int containerNodePort =
        this.container == null ? -1 : this.container.getNodeId().getPort();
    if (attemptId.getTaskId().getTaskType() == TaskType.MAP) {
      MapAttemptFinishedEvent mfe =
          new MapAttemptFinishedEvent(TypeConverter.fromYarn(attemptId),
          TypeConverter.fromYarn(attemptId.getTaskId().getTaskType()),
          state.toString(),
          this.reportedStatus.mapFinishTime,
          finishTime,
          containerHostName,
          containerNodePort,
          this.nodeRackName == null ? "UNKNOWN" : this.nodeRackName,
          this.reportedStatus.stateString,
          getCounters(),
          getProgressSplitBlock().burst(), launchTime);
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
          containerHostName,
          containerNodePort,
          this.nodeRackName == null ? "UNKNOWN" : this.nodeRackName,
          this.reportedStatus.stateString,
          getCounters(),
          getProgressSplitBlock().burst(), launchTime);
      eventHandler.handle(
          new JobHistoryEvent(attemptId.getTaskId().getJobId(), rfe));
    }
  }

  private static class TooManyFetchFailureTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent event) {
      TaskAttemptTooManyFetchFailureEvent fetchFailureEvent =
          (TaskAttemptTooManyFetchFailureEvent) event;
      // too many fetch failure can only happen for map tasks
      Preconditions
          .checkArgument(taskAttempt.getID().getTaskId().getTaskType() == TaskType.MAP);
      //add to diagnostic
      taskAttempt.addDiagnosticInfo("Too many fetch failures."
          + " Failing the attempt. Last failure reported by " +
          fetchFailureEvent.getReduceId() +
          " from host " + fetchFailureEvent.getReduceHost());

      if (taskAttempt.getLaunchTime() != 0) {
        taskAttempt.eventHandler
            .handle(createJobCounterUpdateEventTAFailed(taskAttempt, true));
        TaskAttemptUnsuccessfulCompletionEvent tauce =
            createTaskAttemptUnsuccessfulCompletionEvent(taskAttempt,
                TaskAttemptStateInternal.FAILED);
        taskAttempt.eventHandler.handle(new JobHistoryEvent(
            taskAttempt.attemptId.getTaskId().getJobId(), tauce));
      }else {
        LOG.debug("Not generating HistoryFinish event since start event not " +
            "generated for taskAttempt: " + taskAttempt.getID());
      }
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
          taskAttempt.attemptId, TaskEventType.T_ATTEMPT_FAILED));
    }
  }
  
  private static class KilledAfterSuccessTransition implements
      MultipleArcTransition<TaskAttemptImpl, TaskAttemptEvent, TaskAttemptStateInternal> {

    @SuppressWarnings("unchecked")
    @Override
    public TaskAttemptStateInternal transition(TaskAttemptImpl taskAttempt, 
        TaskAttemptEvent event) {
      if(taskAttempt.getID().getTaskId().getTaskType() == TaskType.REDUCE) {
        // after a reduce task has succeeded, its outputs are in safe in HDFS.
        // logically such a task should not be killed. we only come here when
        // there is a race condition in the event queue. E.g. some logic sends
        // a kill request to this attempt when the successful completion event
        // for this task is already in the event queue. so the kill event will
        // get executed immediately after the attempt is marked successful and 
        // result in this transition being exercised.
        // ignore this for reduce tasks
        LOG.info("Ignoring killed event for successful reduce task attempt" +
                  taskAttempt.getID().toString());
        return TaskAttemptStateInternal.SUCCEEDED;
      }
      if(event instanceof TaskAttemptKillEvent) {
        TaskAttemptKillEvent msgEvent = (TaskAttemptKillEvent) event;
        //add to diagnostic
        taskAttempt.addDiagnosticInfo(msgEvent.getMessage());
      }

      // not setting a finish time since it was set on success
      assert (taskAttempt.getFinishTime() != 0);

      assert (taskAttempt.getLaunchTime() != 0);
      taskAttempt.eventHandler
          .handle(createJobCounterUpdateEventTAKilled(taskAttempt, true));
      TaskAttemptUnsuccessfulCompletionEvent tauce = createTaskAttemptUnsuccessfulCompletionEvent(
          taskAttempt, TaskAttemptStateInternal.KILLED);
      taskAttempt.eventHandler.handle(new JobHistoryEvent(taskAttempt.attemptId
          .getTaskId().getJobId(), tauce));
      boolean rescheduleNextTaskAttempt = false;
      if (event instanceof TaskAttemptKillEvent) {
        rescheduleNextTaskAttempt =
            ((TaskAttemptKillEvent)event).getRescheduleAttempt();
      }
      taskAttempt.eventHandler.handle(new TaskTAttemptKilledEvent(
          taskAttempt.attemptId, rescheduleNextTaskAttempt));
      return TaskAttemptStateInternal.KILLED;
    }
  }

  private static class KilledAfterSucceededFinishingTransition
      implements MultipleArcTransition<TaskAttemptImpl, TaskAttemptEvent,
      TaskAttemptStateInternal> {

    @SuppressWarnings("unchecked")
    @Override
    public TaskAttemptStateInternal transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      taskAttempt.appContext.getTaskAttemptFinishingMonitor().unregister(
          taskAttempt.attemptId);
      sendContainerCleanup(taskAttempt, event);
      if(taskAttempt.getID().getTaskId().getTaskType() == TaskType.REDUCE) {
        // after a reduce task has succeeded, its outputs are in safe in HDFS.
        // logically such a task should not be killed. we only come here when
        // there is a race condition in the event queue. E.g. some logic sends
        // a kill request to this attempt when the successful completion event
        // for this task is already in the event queue. so the kill event will
        // get executed immediately after the attempt is marked successful and
        // result in this transition being exercised.
        // ignore this for reduce tasks
        LOG.info("Ignoring killed event for successful reduce task attempt" +
            taskAttempt.getID().toString());
        return TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP;
      } else {
        // Store reschedule flag so that after clean up is completed, new
        // attempt is scheduled/rescheduled based on it.
        if (event instanceof TaskAttemptKillEvent) {
          taskAttempt.setRescheduleNextAttempt(
              ((TaskAttemptKillEvent)event).getRescheduleAttempt());
        }
        return TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP;
      }
    }
  }

  private static class KilledTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      if (taskAttempt.getLaunchTime() == 0) {
        sendJHStartEventForAssignedFailTask(taskAttempt);
      }
      //set the finish time
      taskAttempt.setFinishTime();

      taskAttempt.eventHandler
          .handle(createJobCounterUpdateEventTAKilled(taskAttempt, false));
      TaskAttemptUnsuccessfulCompletionEvent tauce =
          createTaskAttemptUnsuccessfulCompletionEvent(taskAttempt,
              TaskAttemptStateInternal.KILLED);
      taskAttempt.eventHandler.handle(new JobHistoryEvent(
          taskAttempt.attemptId.getTaskId().getJobId(), tauce));

      if (event instanceof TaskAttemptKillEvent) {
        taskAttempt.addDiagnosticInfo(
            ((TaskAttemptKillEvent) event).getMessage());
      }

      taskAttempt.eventHandler.handle(new TaskTAttemptKilledEvent(
          taskAttempt.attemptId, taskAttempt.getRescheduleNextAttempt()));
    }
  }

  private static class PreemptedTransition implements
      SingleArcTransition<TaskAttemptImpl,TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      taskAttempt.setFinishTime();
      taskAttempt.taskAttemptListener.unregister(
          taskAttempt.attemptId, taskAttempt.jvmID);
      taskAttempt.eventHandler.handle(new ContainerLauncherEvent(
          taskAttempt.attemptId,
          taskAttempt.getAssignedContainerID(), taskAttempt.getAssignedContainerMgrAddress(),
          taskAttempt.container.getContainerToken(),
          ContainerLauncher.EventType.CONTAINER_REMOTE_CLEANUP, false));
      taskAttempt.eventHandler.handle(new TaskTAttemptKilledEvent(
          taskAttempt.attemptId, false));

    }
  }

  /**
   * Transition from SUCCESS_FINISHING_CONTAINER or FAIL_FINISHING_CONTAINER
   * state upon receiving TA_TIMED_OUT event
   */
  private static class ExitFinishingOnTimeoutTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      taskAttempt.appContext.getTaskAttemptFinishingMonitor().unregister(
          taskAttempt.attemptId);
      // The attempt stays in finishing state for too long
      String msg = "Task attempt " + taskAttempt.getID() + " is done from " +
          "TaskUmbilicalProtocol's point of view. However, it stays in " +
          "finishing state for too long";
      LOG.warn(msg);
      taskAttempt.addDiagnosticInfo(msg);
      sendContainerCleanup(taskAttempt, event);
    }
  }

  /**
   * Finish and clean up the container
   */
  private static class CleanupContainerTransition implements
       SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt, 
        TaskAttemptEvent event) {
      // unregister it to TaskAttemptListener so that it stops listening
      // for it.
      finalizeProgress(taskAttempt);
      sendContainerCleanup(taskAttempt, event);
      // Store reschedule flag so that after clean up is completed, new
      // attempt is scheduled/rescheduled based on it.
      if (event instanceof TaskAttemptKillEvent) {
        taskAttempt.setRescheduleNextAttempt(
            ((TaskAttemptKillEvent)event).getRescheduleAttempt());
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void sendContainerCleanup(TaskAttemptImpl taskAttempt,
      TaskAttemptEvent event) {
    if (event instanceof TaskAttemptKillEvent) {
      taskAttempt.addDiagnosticInfo(
          ((TaskAttemptKillEvent) event).getMessage());
    }
    //send the cleanup event to containerLauncher
    taskAttempt.eventHandler.handle(new ContainerLauncherEvent(
        taskAttempt.attemptId,
        taskAttempt.container.getId(), StringInterner
        .weakIntern(taskAttempt.container.getNodeId().toString()),
        taskAttempt.container.getContainerToken(),
        ContainerLauncher.EventType.CONTAINER_REMOTE_CLEANUP,
        event.getType() == TaskAttemptEventType.TA_TIMED_OUT));
  }

  /**
   * Transition to SUCCESS_FINISHING_CONTAINER upon receiving TA_DONE event
   */
  private static class MoveContainerToSucceededFinishingTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      finalizeProgress(taskAttempt);

      // register it to finishing state
      taskAttempt.appContext.getTaskAttemptFinishingMonitor().register(
          taskAttempt.attemptId);

      // set the finish time
      taskAttempt.setFinishTime();

      // notify job history
      taskAttempt.eventHandler.handle(
          createJobCounterUpdateEventTASucceeded(taskAttempt));
      taskAttempt.logAttemptFinishedEvent(TaskAttemptStateInternal.SUCCEEDED);

      //notify the task even though the container might not have exited yet.
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
          taskAttempt.attemptId,
          TaskEventType.T_ATTEMPT_SUCCEEDED));
      taskAttempt.eventHandler.handle
          (new SpeculatorEvent
              (taskAttempt.reportedStatus, taskAttempt.clock.getTime()));

    }
  }

  /**
   * Transition to FAIL_FINISHING_CONTAINER upon receiving TA_FAILMSG event
   */
  private static class MoveContainerToFailedFinishingTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      finalizeProgress(taskAttempt);
      // register it to finishing state
      taskAttempt.appContext.getTaskAttemptFinishingMonitor().register(
          taskAttempt.attemptId);
      notifyTaskAttemptFailed(taskAttempt);
    }
  }

  @SuppressWarnings("unchecked")
  private static void notifyTaskAttemptFailed(TaskAttemptImpl taskAttempt) {
    if (taskAttempt.getLaunchTime() == 0) {
      sendJHStartEventForAssignedFailTask(taskAttempt);
    }
    // set the finish time
    taskAttempt.setFinishTime();
    taskAttempt.eventHandler
        .handle(createJobCounterUpdateEventTAFailed(taskAttempt, false));
    TaskAttemptUnsuccessfulCompletionEvent tauce =
        createTaskAttemptUnsuccessfulCompletionEvent(taskAttempt,
            TaskAttemptStateInternal.FAILED);
    taskAttempt.eventHandler.handle(new JobHistoryEvent(
        taskAttempt.attemptId.getTaskId().getJobId(), tauce));

    taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
        taskAttempt.attemptId, TaskEventType.T_ATTEMPT_FAILED));

  }

  private void addDiagnosticInfo(String diag) {
    if (diag != null && !diag.equals("")) {
      diagnostics.add(diag);
    }
  }

  private static class StatusUpdater 
       implements SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt, 
        TaskAttemptEvent event) {
      // Status update calls don't really change the state of the attempt.
      TaskAttemptStatus newReportedStatus =
          ((TaskAttemptStatusUpdateEvent) event)
              .getReportedTaskAttemptStatus();
      // Now switch the information in the reportedStatus
      taskAttempt.reportedStatus = newReportedStatus;
      taskAttempt.reportedStatus.taskState = taskAttempt.getState();

      // send event to speculator about the reported status
      taskAttempt.eventHandler.handle
          (new SpeculatorEvent
              (taskAttempt.reportedStatus, taskAttempt.clock.getTime()));
      
      taskAttempt.updateProgressSplits();
      
      //if fetch failures are present, send the fetch failure event to job
      //this only will happen in reduce attempt type
      if (taskAttempt.reportedStatus.fetchFailedMaps != null && 
          taskAttempt.reportedStatus.fetchFailedMaps.size() > 0) {
        String hostname = taskAttempt.container == null ? "UNKNOWN"
            : taskAttempt.container.getNodeId().getHost();
        taskAttempt.eventHandler.handle(new JobTaskAttemptFetchFailureEvent(
            taskAttempt.attemptId, taskAttempt.reportedStatus.fetchFailedMaps,
                hostname));
      }
    }
  }

  private static class DiagnosticInformationUpdater 
        implements SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl taskAttempt, 
        TaskAttemptEvent event) {
      TaskAttemptDiagnosticsUpdateEvent diagEvent =
          (TaskAttemptDiagnosticsUpdateEvent) event;
      LOG.info("Diagnostics report from " + taskAttempt.attemptId + ": "
          + diagEvent.getDiagnosticInfo());
      taskAttempt.addDiagnosticInfo(diagEvent.getDiagnosticInfo());
    }
  }

  private void initTaskAttemptStatus(TaskAttemptStatus result) {
    result.progress = 0.0f;
    result.phase = Phase.STARTING;
    result.stateString = "NEW";
    result.taskState = TaskAttemptState.NEW;
    Counters counters = EMPTY_COUNTERS;
    result.counters = counters;
  }

}
