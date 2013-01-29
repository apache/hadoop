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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
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
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptEvent;
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
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.RackResolver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Implementation of TaskAttempt interface.
 */
@SuppressWarnings({ "rawtypes" })
public abstract class TaskAttemptImpl implements
    org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt,
      EventHandler<TaskAttemptEvent> {

  static final Counters EMPTY_COUNTERS = new Counters();
  private static final Log LOG = LogFactory.getLog(TaskAttemptImpl.class);
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

  private static final CleanupContainerTransition CLEANUP_CONTAINER_TRANSITION =
    new CleanupContainerTransition();

  private static final DiagnosticInformationUpdater 
    DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION 
      = new DiagnosticInformationUpdater();

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
         TaskAttemptEventType.TA_FAILMSG, new FailedTransition())
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
         TaskAttemptEventType.TA_FAILMSG, new DeallocateContainerTransition(
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
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_CONTAINER_COMPLETED,
         CLEANUP_CONTAINER_TRANSITION)
      // ^ If RM kills the container due to expiry, preemption etc. 
     .addTransition(TaskAttemptStateInternal.ASSIGNED, 
         TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_KILL, CLEANUP_CONTAINER_TRANSITION)
     .addTransition(TaskAttemptStateInternal.ASSIGNED, 
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_FAILMSG, CLEANUP_CONTAINER_TRANSITION)

     // Transitions from RUNNING state.
     .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.RUNNING,
         TaskAttemptEventType.TA_UPDATE, new StatusUpdater())
     .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.RUNNING,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     // If no commit is required, task directly goes to success
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_DONE, CLEANUP_CONTAINER_TRANSITION)
     // If commit is required, task goes through commit pending state.
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptEventType.TA_COMMIT_PENDING, new CommitPendingTransition())
     // Failure handling while RUNNING
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_FAILMSG, CLEANUP_CONTAINER_TRANSITION)
      //for handling container exit without sending the done or fail msg
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_CONTAINER_COMPLETED,
         CLEANUP_CONTAINER_TRANSITION)
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
         TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP, TaskAttemptEventType.TA_KILL,
         CLEANUP_CONTAINER_TRANSITION)

     // Transitions from COMMIT_PENDING state
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.COMMIT_PENDING, TaskAttemptEventType.TA_UPDATE,
         new StatusUpdater())
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_DONE, CLEANUP_CONTAINER_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP, TaskAttemptEventType.TA_KILL,
         CLEANUP_CONTAINER_TRANSITION)
     // if container killed by AM shutting down
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.KILLED,
         TaskAttemptEventType.TA_CONTAINER_CLEANED, new KilledTransition())
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_FAILMSG, CLEANUP_CONTAINER_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_CONTAINER_COMPLETED,
         CLEANUP_CONTAINER_TRANSITION)
     .addTransition(TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_TIMED_OUT, CLEANUP_CONTAINER_TRANSITION)

     // Transitions from SUCCESS_CONTAINER_CLEANUP state
     // kill and cleanup the container
     .addTransition(TaskAttemptStateInternal.SUCCESS_CONTAINER_CLEANUP,
         TaskAttemptStateInternal.SUCCEEDED, TaskAttemptEventType.TA_CONTAINER_CLEANED,
         new SucceededTransition())
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
             TaskAttemptEventType.TA_CONTAINER_CLEANED,
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
             TaskAttemptEventType.TA_CONTAINER_CLEANED,
             TaskAttemptEventType.TA_CONTAINER_COMPLETED))

     // Transitions from FAILED state
     .addTransition(TaskAttemptStateInternal.FAILED, TaskAttemptStateInternal.FAILED,
       TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
       DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     // Ignore-able events for FAILED state
     .addTransition(TaskAttemptStateInternal.FAILED, TaskAttemptStateInternal.FAILED,
         EnumSet.of(TaskAttemptEventType.TA_KILL,
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
             TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE))

     // Transitions from KILLED state
     .addTransition(TaskAttemptStateInternal.KILLED, TaskAttemptStateInternal.KILLED,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     // Ignore-able events for KILLED state
     .addTransition(TaskAttemptStateInternal.KILLED, TaskAttemptStateInternal.KILLED,
         EnumSet.of(TaskAttemptEventType.TA_KILL,
             TaskAttemptEventType.TA_ASSIGNED,
             TaskAttemptEventType.TA_CONTAINER_COMPLETED,
             TaskAttemptEventType.TA_UPDATE,
             // Container launch events can arrive late
             TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
             TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED,
             TaskAttemptEventType.TA_CONTAINER_CLEANED,
             TaskAttemptEventType.TA_COMMIT_PENDING,
             TaskAttemptEventType.TA_DONE,
             TaskAttemptEventType.TA_FAILMSG))

     // create the topology tables
     .installTopology();

  private final StateMachine
         <TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent>
    stateMachine;

  private ContainerId containerID;
  private NodeId containerNodeId;
  private String containerMgrAddress;
  private String nodeHttpAddress;
  private String nodeRackName;
  private WrappedJvmID jvmID;
  private ContainerToken containerToken;
  private Resource assignedCapability;
  
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
    this.resourceCapability.setMemory(
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
    URL resourceURL = ConverterUtils.getYarnUrlFromPath(fc.resolvePath(fstat
        .getPath()));
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();

    return BuilderUtils.newLocalResource(resourceURL, type, visibility,
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
    Map<String, String> environment = new HashMap<String, String>();

    // Service data
    Map<String, ByteBuffer> serviceData = new HashMap<String, ByteBuffer>();

    // Tokens
    ByteBuffer taskCredentialsBuffer = ByteBuffer.wrap(new byte[]{});
    try {
      FileSystem remoteFS = FileSystem.get(conf);

      // //////////// Set up JobJar to be localized properly on the remote NM.
      String jobJar = conf.get(MRJobConfig.JAR);
      if (jobJar != null) {
        Path remoteJobJar = (new Path(jobJar)).makeQualified(remoteFS
            .getUri(), remoteFS.getWorkingDirectory());
        LocalResource rc = createLocalResource(remoteFS, remoteJobJar,
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
      // //////////// End of JobJar setup

      // //////////// Set up JobConf to be localized properly on the remote NM.
      Path path =
          MRApps.getStagingAreaDir(conf, UserGroupInformation
              .getCurrentUser().getShortUserName());
      Path remoteJobSubmitDir =
          new Path(path, oldJobId.toString());
      Path remoteJobConfPath = 
          new Path(remoteJobSubmitDir, MRJobConfig.JOB_CONF_FILE);
      localResources.put(
          MRJobConfig.JOB_CONF_FILE,
          createLocalResource(remoteFS, remoteJobConfPath,
              LocalResourceType.FILE, LocalResourceVisibility.APPLICATION));
      LOG.info("The job-conf file on the remote FS is "
          + remoteJobConfPath.toUri().toASCIIString());
      // //////////// End of JobConf setup

      // Setup DistributedCache
      MRApps.setupDistributedCache(conf, localResources);

      // Setup up task credentials buffer
      LOG.info("Adding #" + credentials.numberOfTokens()
          + " tokens and #" + credentials.numberOfSecretKeys()
          + " secret keys for NM use for launching container");
      Credentials taskCredentials = new Credentials(credentials);

      // LocalStorageToken is needed irrespective of whether security is enabled
      // or not.
      TokenCache.setJobToken(jobToken, taskCredentials);

      DataOutputBuffer containerTokens_dob = new DataOutputBuffer();
      LOG.info("Size of containertokens_dob is "
          + taskCredentials.numberOfTokens());
      taskCredentials.writeTokenStorageToStream(containerTokens_dob);
      taskCredentialsBuffer =
          ByteBuffer.wrap(containerTokens_dob.getData(), 0,
              containerTokens_dob.getLength());

      // Add shuffle token
      LOG.info("Putting shuffle token in serviceData");
      serviceData.put(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID,
          ShuffleHandler.serializeServiceData(jobToken));

      Apps.addToEnvironment(
          environment,  
          Environment.CLASSPATH.name(), 
          getInitialClasspath(conf));

      if (initialAppClasspath != null) {
        Apps.addToEnvironment(
            environment,  
            Environment.APP_CLASSPATH.name(), 
            initialAppClasspath);
      }
    } catch (IOException e) {
      throw new YarnException(e);
    }

    // Shell
    environment.put(
        Environment.SHELL.name(), 
        conf.get(
            MRJobConfig.MAPRED_ADMIN_USER_SHELL, 
            MRJobConfig.DEFAULT_SHELL)
            );

    // Add pwd to LD_LIBRARY_PATH, add this before adding anything else
    Apps.addToEnvironment(
        environment, 
        Environment.LD_LIBRARY_PATH.name(), 
        Environment.PWD.$());

    // Add the env variables passed by the admin
    Apps.setEnvFromInputString(
        environment, 
        conf.get(
            MRJobConfig.MAPRED_ADMIN_USER_ENV, 
            MRJobConfig.DEFAULT_MAPRED_ADMIN_USER_ENV)
        );

    // Construct the actual Container
    // The null fields are per-container and will be constructed for each
    // container separately.
    ContainerLaunchContext container = BuilderUtils
        .newContainerLaunchContext(null, conf
            .get(MRJobConfig.USER_NAME), null, localResources,
            environment, null, serviceData, taskCredentialsBuffer,
            applicationACLs);

    return container;
  }

  static ContainerLaunchContext createContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs,
      ContainerId containerID, Configuration conf,
      Token<JobTokenIdentifier> jobToken, Task remoteTask,
      final org.apache.hadoop.mapred.JobID oldJobId,
      Resource assignedCapability, WrappedJvmID jvmID,
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

    // Setup environment by cloning from common env.
    Map<String, String> env = commonContainerSpec.getEnvironment();
    Map<String, String> myEnv = new HashMap<String, String>(env.size());
    myEnv.putAll(env);
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
    ContainerLaunchContext container = BuilderUtils.newContainerLaunchContext(
        containerID, commonContainerSpec.getUser(), assignedCapability,
        commonContainerSpec.getLocalResources(), myEnv, commands,
        myServiceData, commonContainerSpec.getContainerTokens().duplicate(),
        applicationACLs);

    return container;
  }

  @Override
  public ContainerId getAssignedContainerID() {
    readLock.lock();
    try {
      return containerID;
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
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state for "
            + this.attemptId, e);
        eventHandler.handle(new JobDiagnosticsUpdateEvent(
            this.attemptId.getTaskId().getJobId(), "Invalid event " + event.getType() + 
            " on TaskAttempt " + this.attemptId));
        eventHandler.handle(new JobEvent(this.attemptId.getTaskId().getJobId(),
            JobEventType.INTERNAL_ERROR));
      }
      if (oldState != getInternalState()) {
          LOG.info(attemptId + " TaskAttempt Transitioned from " 
           + oldState + " to "
           + getInternalState());
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
  
  private static TaskAttemptState getExternalState(
      TaskAttemptStateInternal smState) {
    switch (smState) {
    case ASSIGNED:
    case UNASSIGNED:
      return TaskAttemptState.STARTING;
    case COMMIT_PENDING:
      return TaskAttemptState.COMMIT_PENDING;
    case FAILED:
      return TaskAttemptState.FAILED;
    case KILLED:
      return TaskAttemptState.KILLED;
      // All CLEANUP states considered as RUNNING since events have not gone out
      // to the Task yet. May be possible to consider them as a Finished state.
    case FAIL_CONTAINER_CLEANUP:
    case FAIL_TASK_CLEANUP:
    case KILL_CONTAINER_CLEANUP:
    case KILL_TASK_CLEANUP:
    case SUCCESS_CONTAINER_CLEANUP:
    case RUNNING:
      return TaskAttemptState.RUNNING;
    case NEW:
      return TaskAttemptState.NEW;
    case SUCCEEDED:
      return TaskAttemptState.SUCCEEDED;
    default:
      throw new YarnException("Attempt to convert invalid "
          + "stateMachineTaskAttemptState to externalTaskAttemptState: "
          + smState);
    }
  }

  //always called in write lock
  private void setFinishTime() {
    //set the finish time only if launch time is set
    if (launchTime != 0) {
      finishTime = clock.getTime();
    }
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

  private static JobCounterUpdateEvent createJobCounterUpdateEventTAFailed(
      TaskAttemptImpl taskAttempt, boolean taskAlreadyCompleted) {
    TaskType taskType = taskAttempt.getID().getTaskId().getTaskType();
    JobCounterUpdateEvent jce = new JobCounterUpdateEvent(taskAttempt.getID().getTaskId().getJobId());
    
    long slotMillisIncrement = computeSlotMillis(taskAttempt);
    
    if (taskType == TaskType.MAP) {
      jce.addCounterUpdate(JobCounter.NUM_FAILED_MAPS, 1);
      if(!taskAlreadyCompleted) {
        // dont double count the elapsed time
        jce.addCounterUpdate(JobCounter.SLOTS_MILLIS_MAPS, slotMillisIncrement);
      }
    } else {
      jce.addCounterUpdate(JobCounter.NUM_FAILED_REDUCES, 1);
      if(!taskAlreadyCompleted) {
        // dont double count the elapsed time
        jce.addCounterUpdate(JobCounter.SLOTS_MILLIS_REDUCES, slotMillisIncrement);
      }
    }
    return jce;
  }
  
  private static JobCounterUpdateEvent createJobCounterUpdateEventTAKilled(
      TaskAttemptImpl taskAttempt, boolean taskAlreadyCompleted) {
    TaskType taskType = taskAttempt.getID().getTaskId().getTaskType();
    JobCounterUpdateEvent jce = new JobCounterUpdateEvent(taskAttempt.getID().getTaskId().getJobId());
    
    long slotMillisIncrement = computeSlotMillis(taskAttempt);
    
    if (taskType == TaskType.MAP) {
      jce.addCounterUpdate(JobCounter.NUM_KILLED_MAPS, 1);
      if(!taskAlreadyCompleted) {
        // dont double count the elapsed time
        jce.addCounterUpdate(JobCounter.SLOTS_MILLIS_MAPS, slotMillisIncrement);
      }
    } else {
      jce.addCounterUpdate(JobCounter.NUM_KILLED_REDUCES, 1);
      if(!taskAlreadyCompleted) {
        // dont double count the elapsed time
        jce.addCounterUpdate(JobCounter.SLOTS_MILLIS_REDUCES, slotMillisIncrement);
      }
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
      taskAttempt.containerID = cEvent.getContainer().getId();
      taskAttempt.containerNodeId = cEvent.getContainer().getNodeId();
      taskAttempt.containerMgrAddress = StringInterner.weakIntern(
          taskAttempt.containerNodeId.toString());
      taskAttempt.nodeHttpAddress = StringInterner.weakIntern(
          cEvent.getContainer().getNodeHttpAddress());
      taskAttempt.nodeRackName = RackResolver.resolve(
          taskAttempt.containerNodeId.getHost()).getNetworkLocation();
      taskAttempt.containerToken = cEvent.getContainer().getContainerToken();
      taskAttempt.assignedCapability = cEvent.getContainer().getResource();
      // this is a _real_ Task (classic Hadoop mapred flavor):
      taskAttempt.remoteTask = taskAttempt.createRemoteTask();
      taskAttempt.jvmID = new WrappedJvmID(
          taskAttempt.remoteTask.getTaskID().getJobID(), 
          taskAttempt.remoteTask.isMapTask(), taskAttempt.containerID.getId());
      taskAttempt.taskAttemptListener.registerPendingTask(
          taskAttempt.remoteTask, taskAttempt.jvmID);

      taskAttempt.locality = Locality.OFF_SWITCH;
      if (taskAttempt.dataLocalHosts.size() > 0) {
        String cHost = taskAttempt.resolveHost(
            taskAttempt.containerNodeId.getHost());
        if (taskAttempt.dataLocalHosts.contains(cHost)) {
          taskAttempt.locality = Locality.NODE_LOCAL;
        }
      }
      if (taskAttempt.locality == Locality.OFF_SWITCH) {
        if (taskAttempt.dataLocalRacks.contains(taskAttempt.nodeRackName)) {
          taskAttempt.locality = Locality.RACK_LOCAL;
        }
      }
      
      //launch the container
      //create the container object to be launched for a given Task attempt
      ContainerLaunchContext launchContext = createContainerLaunchContext(
          cEvent.getApplicationACLs(), taskAttempt.containerID,
          taskAttempt.conf, taskAttempt.jobToken, taskAttempt.remoteTask,
          taskAttempt.oldJobId, taskAttempt.assignedCapability,
          taskAttempt.jvmID, taskAttempt.taskAttemptListener,
          taskAttempt.credentials);
      taskAttempt.eventHandler.handle(new ContainerRemoteLaunchEvent(
          taskAttempt.attemptId, taskAttempt.containerID,
          taskAttempt.containerMgrAddress, taskAttempt.containerToken,
          launchContext, taskAttempt.remoteTask));

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
      //set the finish time
      taskAttempt.setFinishTime();
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
          taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
              taskAttempt.attemptId,
              TaskEventType.T_ATTEMPT_KILLED));
          break;
        default:
          LOG.error("Task final state is not FAILED or KILLED: " + finalState);
      }
      if (taskAttempt.getLaunchTime() != 0) {
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
      } else {
        LOG.debug("Not generating HistoryFinish event since start event not " +
            "generated for taskAttempt: " + taskAttempt.getID());
      }
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
      InetSocketAddress nodeHttpInetAddr =
          NetUtils.createSocketAddr(taskAttempt.nodeHttpAddress); // TODO:
                                                                  // Costly?
      taskAttempt.trackerName = nodeHttpInetAddr.getHostName();
      taskAttempt.httpPort = nodeHttpInetAddr.getPort();
      JobCounterUpdateEvent jce =
          new JobCounterUpdateEvent(taskAttempt.attemptId.getTaskId()
              .getJobId());
      jce.addCounterUpdate(
          taskAttempt.attemptId.getTaskId().getTaskType() == TaskType.MAP ? 
              JobCounter.TOTAL_LAUNCHED_MAPS: JobCounter.TOTAL_LAUNCHED_REDUCES
              , 1);
      taskAttempt.eventHandler.handle(jce);
      
      LOG.info("TaskAttempt: [" + taskAttempt.attemptId
          + "] using containerId: [" + taskAttempt.containerID + " on NM: ["
          + taskAttempt.containerMgrAddress + "]");
      TaskAttemptStartedEvent tase =
        new TaskAttemptStartedEvent(TypeConverter.fromYarn(taskAttempt.attemptId),
            TypeConverter.fromYarn(taskAttempt.attemptId.getTaskId().getTaskType()),
            taskAttempt.launchTime,
            nodeHttpInetAddr.getHostName(), nodeHttpInetAddr.getPort(),
            taskAttempt.shufflePort, taskAttempt.containerID,
            taskAttempt.locality.toString(), taskAttempt.avataar.toString());
      taskAttempt.eventHandler.handle
          (new JobHistoryEvent(taskAttempt.attemptId.getTaskId().getJobId(), tase));
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

  private static class SucceededTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt, 
        TaskAttemptEvent event) {
      //set the finish time
      taskAttempt.setFinishTime();
      long slotMillis = computeSlotMillis(taskAttempt);
      TaskId taskId = taskAttempt.attemptId.getTaskId();
      JobCounterUpdateEvent jce = new JobCounterUpdateEvent(taskId.getJobId());
      jce.addCounterUpdate(
        taskId.getTaskType() == TaskType.MAP ? 
          JobCounter.SLOTS_MILLIS_MAPS : JobCounter.SLOTS_MILLIS_REDUCES,
          slotMillis);
      taskAttempt.eventHandler.handle(jce);
      taskAttempt.logAttemptFinishedEvent(TaskAttemptStateInternal.SUCCEEDED);
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
          taskAttempt.attemptId,
          TaskEventType.T_ATTEMPT_SUCCEEDED));
      taskAttempt.eventHandler.handle
      (new SpeculatorEvent
          (taskAttempt.reportedStatus, taskAttempt.clock.getTime()));
   }
  }

  private static class FailedTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent event) {
      // set the finish time
      taskAttempt.setFinishTime();
      
      if (taskAttempt.getLaunchTime() != 0) {
        taskAttempt.eventHandler
            .handle(createJobCounterUpdateEventTAFailed(taskAttempt, false));
        TaskAttemptUnsuccessfulCompletionEvent tauce =
            createTaskAttemptUnsuccessfulCompletionEvent(taskAttempt,
                TaskAttemptStateInternal.FAILED);
        taskAttempt.eventHandler.handle(new JobHistoryEvent(
            taskAttempt.attemptId.getTaskId().getJobId(), tauce));
        // taskAttempt.logAttemptFinishedEvent(TaskAttemptStateInternal.FAILED); Not
        // handling failed map/reduce events.
      }else {
        LOG.debug("Not generating HistoryFinish event since start event not " +
            "generated for taskAttempt: " + taskAttempt.getID());
      }
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
          taskAttempt.attemptId, TaskEventType.T_ATTEMPT_FAILED));
    }
  }

  @SuppressWarnings({ "unchecked" })
  private void logAttemptFinishedEvent(TaskAttemptStateInternal state) {
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

  private static class TooManyFetchFailureTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent event) {
      // too many fetch failure can only happen for map tasks
      Preconditions
          .checkArgument(taskAttempt.getID().getTaskId().getTaskType() == TaskType.MAP);
      //add to diagnostic
      taskAttempt.addDiagnosticInfo("Too Many fetch failures.Failing the attempt");
      //set the finish time
      taskAttempt.setFinishTime();
      
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
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
          taskAttempt.attemptId, TaskEventType.T_ATTEMPT_KILLED));
      return TaskAttemptStateInternal.KILLED;
    }
  }

  private static class KilledTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {
      //set the finish time
      taskAttempt.setFinishTime();
      if (taskAttempt.getLaunchTime() != 0) {
        taskAttempt.eventHandler
            .handle(createJobCounterUpdateEventTAKilled(taskAttempt, false));
        TaskAttemptUnsuccessfulCompletionEvent tauce =
            createTaskAttemptUnsuccessfulCompletionEvent(taskAttempt,
                TaskAttemptStateInternal.KILLED);
        taskAttempt.eventHandler.handle(new JobHistoryEvent(
            taskAttempt.attemptId.getTaskId().getJobId(), tauce));
      }else {
        LOG.debug("Not generating HistoryFinish event since start event not " +
            "generated for taskAttempt: " + taskAttempt.getID());
      }
//      taskAttempt.logAttemptFinishedEvent(TaskAttemptStateInternal.KILLED); Not logging Map/Reduce attempts in case of failure.
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
          taskAttempt.attemptId,
          TaskEventType.T_ATTEMPT_KILLED));
    }
  }

  private static class CleanupContainerTransition implements
       SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptImpl taskAttempt, 
        TaskAttemptEvent event) {
      // unregister it to TaskAttemptListener so that it stops listening
      // for it
      taskAttempt.taskAttemptListener.unregister(
          taskAttempt.attemptId, taskAttempt.jvmID);
      taskAttempt.reportedStatus.progress = 1.0f;
      taskAttempt.updateProgressSplits();
      //send the cleanup event to containerLauncher
      taskAttempt.eventHandler.handle(new ContainerLauncherEvent(
          taskAttempt.attemptId, 
          taskAttempt.containerID, taskAttempt.containerMgrAddress,
          taskAttempt.containerToken,
          ContainerLauncher.EventType.CONTAINER_REMOTE_CLEANUP));
    }
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
        taskAttempt.eventHandler.handle(new JobTaskAttemptFetchFailureEvent(
            taskAttempt.attemptId, taskAttempt.reportedStatus.fetchFailedMaps));
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
