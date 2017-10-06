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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import static org.apache.hadoop.yarn.util.StringHelper.pjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.BlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.DisabledBlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppFailedAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeFinishedContainersPulledByAMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.AMState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.BoundedAppender;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;

@SuppressWarnings({"unchecked", "rawtypes"})
public class RMAppAttemptImpl implements RMAppAttempt, Recoverable {
  private static final String STATE_CHANGE_MESSAGE =
      "%s State change from %s to %s on event = %s";
  private static final String RECOVERY_MESSAGE =
      "Recovering attempt: %s with final state = %s";
  private static final String DIAGNOSTIC_LIMIT_CONFIG_ERROR_MESSAGE =
      "The value of %s should be a positive integer: %s";

  private static final Log LOG = LogFactory.getLog(RMAppAttemptImpl.class);

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  public final static Priority AM_CONTAINER_PRIORITY = recordFactory
      .newRecordInstance(Priority.class);

  static {
    AM_CONTAINER_PRIORITY.setPriority(0);
  }

  private final StateMachine<RMAppAttemptState,
                             RMAppAttemptEventType,
                             RMAppAttemptEvent> stateMachine;

  private final RMContext rmContext;
  private final EventHandler eventHandler;
  private final YarnScheduler scheduler;
  private final ApplicationMasterService masterService;
  private final RMApp rmApp;

  private final ReadLock readLock;
  private final WriteLock writeLock;

  private final ApplicationAttemptId applicationAttemptId;
  private final ApplicationSubmissionContext submissionContext;
  private Token<AMRMTokenIdentifier> amrmToken = null;
  private volatile Integer amrmTokenKeyId = null;
  private SecretKey clientTokenMasterKey = null;

  private ConcurrentMap<NodeId, List<ContainerStatus>>
      justFinishedContainers = new ConcurrentHashMap<>();
  // Tracks the previous finished containers that are waiting to be
  // verified as received by the AM. If the AM sends the next allocate
  // request it implicitly acks this list.
  private ConcurrentMap<NodeId, List<ContainerStatus>>
      finishedContainersSentToAM = new ConcurrentHashMap<>();
  private volatile Container masterContainer;

  private float progress = 0;
  private String host = "N/A";
  private int rpcPort = -1;
  private String originalTrackingUrl = "N/A";
  private String proxiedTrackingUrl = "N/A";
  private long startTime = 0;
  private long finishTime = 0;
  private long launchAMStartTime = 0;
  private long launchAMEndTime = 0;

  // Set to null initially. Will eventually get set
  // if an RMAppAttemptUnregistrationEvent occurs
  private FinalApplicationStatus finalStatus = null;
  private final BoundedAppender diagnostics;
  private int amContainerExitStatus = ContainerExitStatus.INVALID;

  private Configuration conf;
  private static final ExpiredTransition EXPIRED_TRANSITION =
      new ExpiredTransition();
  private static final AttemptFailedTransition FAILED_TRANSITION =
      new AttemptFailedTransition();
  private static final AMRegisteredTransition REGISTERED_TRANSITION =
      new AMRegisteredTransition();
  private static final AMLaunchedTransition LAUNCHED_TRANSITION =
      new AMLaunchedTransition();
  private RMAppAttemptEvent eventCausingFinalSaving;
  private RMAppAttemptState targetedFinalState;
  private RMAppAttemptState recoveredFinalState;
  private RMAppAttemptState stateBeforeFinalSaving;
  private Object transitionTodo;
  
  private RMAppAttemptMetrics attemptMetrics = null;
  private List<ResourceRequest> amReqs = null;
  private BlacklistManager blacklistedNodesForAM = null;

  private String amLaunchDiagnostics;

  private static final StateMachineFactory<RMAppAttemptImpl,
                                           RMAppAttemptState,
                                           RMAppAttemptEventType,
                                           RMAppAttemptEvent>
       stateMachineFactory  = new StateMachineFactory<RMAppAttemptImpl,
                                            RMAppAttemptState,
                                            RMAppAttemptEventType,
                                     RMAppAttemptEvent>(RMAppAttemptState.NEW)

       // Transitions from NEW State
      .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.SUBMITTED,
          RMAppAttemptEventType.START, new AttemptStartedTransition())
      .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new BaseFinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.FAIL,
          new FinalSavingTransition(FAILED_TRANSITION,
              RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.REGISTERED,
          new FinalSavingTransition(
            new UnexpectedAMRegisteredTransition(), RMAppAttemptState.FAILED))
      .addTransition( RMAppAttemptState.NEW,
          EnumSet.of(RMAppAttemptState.FINISHED, RMAppAttemptState.KILLED,
            RMAppAttemptState.FAILED, RMAppAttemptState.LAUNCHED),
          RMAppAttemptEventType.RECOVER, new AttemptRecoveredTransition())
          
      // Transitions from SUBMITTED state
      .addTransition(RMAppAttemptState.SUBMITTED, 
          EnumSet.of(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING,
                     RMAppAttemptState.SCHEDULED),
          RMAppAttemptEventType.ATTEMPT_ADDED,
          new ScheduleTransition())
      .addTransition(RMAppAttemptState.SUBMITTED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new BaseFinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.SUBMITTED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.FAIL,
          new FinalSavingTransition(FAILED_TRANSITION,
              RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.SUBMITTED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.REGISTERED,
          new FinalSavingTransition(
            new UnexpectedAMRegisteredTransition(), RMAppAttemptState.FAILED))
          
       // Transitions from SCHEDULED State
      .addTransition(RMAppAttemptState.SCHEDULED,
          EnumSet.of(RMAppAttemptState.ALLOCATED_SAVING,
            RMAppAttemptState.SCHEDULED),
          RMAppAttemptEventType.CONTAINER_ALLOCATED,
          new AMContainerAllocatedTransition())
      .addTransition(RMAppAttemptState.SCHEDULED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new BaseFinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.SCHEDULED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.FAIL,
          new FinalSavingTransition(FAILED_TRANSITION,
              RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.SCHEDULED,
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new FinalSavingTransition(
            new AMContainerCrashedBeforeRunningTransition(),
            RMAppAttemptState.FAILED))

       // Transitions from ALLOCATED_SAVING State
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING, 
          RMAppAttemptState.ALLOCATED,
          RMAppAttemptEventType.ATTEMPT_NEW_SAVED, new AttemptStoredTransition())
          
       // App could be killed by the client. So need to handle this. 
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING, 
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new BaseFinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING, 
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new FinalSavingTransition(
            new AMContainerCrashedBeforeRunningTransition(), 
            RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING,
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.FAIL,
          new FinalSavingTransition(FAILED_TRANSITION,
              RMAppAttemptState.FAILED))

       // Transitions from LAUNCHED_UNMANAGED_SAVING State
      .addTransition(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING, 
          RMAppAttemptState.LAUNCHED,
          RMAppAttemptEventType.ATTEMPT_NEW_SAVED, 
          new UnmanagedAMAttemptSavedTransition())
      // attempt should not try to register in this state
      .addTransition(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING, 
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.REGISTERED,
          new FinalSavingTransition(
            new UnexpectedAMRegisteredTransition(), RMAppAttemptState.FAILED))
      // App could be killed by the client. So need to handle this. 
      .addTransition(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING, 
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new BaseFinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING,
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.FAIL,
          new FinalSavingTransition(FAILED_TRANSITION,
              RMAppAttemptState.FAILED))

       // Transitions from ALLOCATED State
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.LAUNCHED,
          RMAppAttemptEventType.LAUNCHED, LAUNCHED_TRANSITION)
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.LAUNCH_FAILED,
          new FinalSavingTransition(new LaunchFailedTransition(),
            RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(
            new KillAllocatedAMTransition(), RMAppAttemptState.KILLED))
          
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.FAIL,
          new FinalSavingTransition(FAILED_TRANSITION,
              RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.RUNNING,
          RMAppAttemptEventType.REGISTERED, REGISTERED_TRANSITION)
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new FinalSavingTransition(
            new AMContainerCrashedBeforeRunningTransition(), RMAppAttemptState.FAILED))

       // Transitions from LAUNCHED State
      .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.RUNNING,
          RMAppAttemptEventType.REGISTERED, REGISTERED_TRANSITION)
      .addTransition(RMAppAttemptState.LAUNCHED,
          EnumSet.of(RMAppAttemptState.LAUNCHED, RMAppAttemptState.FINAL_SAVING),
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new ContainerFinishedTransition(
            new AMContainerCrashedBeforeRunningTransition(),
            RMAppAttemptState.LAUNCHED))
      .addTransition(
          RMAppAttemptState.LAUNCHED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.EXPIRE,
          new FinalSavingTransition(EXPIRED_TRANSITION,
            RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new FinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.FAIL,
          new FinalSavingTransition(FAILED_TRANSITION,
              RMAppAttemptState.FAILED))

       // Transitions from RUNNING State
      .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
          EnumSet.of(
              RMAppAttemptEventType.LAUNCHED,
              // Valid only for UAM restart
              RMAppAttemptEventType.REGISTERED))
      .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.UNREGISTERED, new AMUnregisteredTransition())
      .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
          RMAppAttemptEventType.STATUS_UPDATE, new StatusUpdateTransition())
      .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
          RMAppAttemptEventType.CONTAINER_ALLOCATED)
      .addTransition(
          RMAppAttemptState.RUNNING,
          EnumSet.of(RMAppAttemptState.RUNNING, RMAppAttemptState.FINAL_SAVING),
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new ContainerFinishedTransition(
            new AMContainerCrashedAtRunningTransition(),
            RMAppAttemptState.RUNNING))
      .addTransition(
          RMAppAttemptState.RUNNING, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.EXPIRE,
          new FinalSavingTransition(EXPIRED_TRANSITION,
            RMAppAttemptState.FAILED))
      .addTransition(
          RMAppAttemptState.RUNNING, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new FinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.FAIL,
          new FinalSavingTransition(FAILED_TRANSITION,
            RMAppAttemptState.FAILED))

       // Transitions from FINAL_SAVING State
      .addTransition(RMAppAttemptState.FINAL_SAVING,
          EnumSet.of(RMAppAttemptState.FINISHING, RMAppAttemptState.FAILED,
            RMAppAttemptState.KILLED, RMAppAttemptState.FINISHED),
            RMAppAttemptEventType.ATTEMPT_UPDATE_SAVED,
            new FinalStateSavedTransition())
      .addTransition(RMAppAttemptState.FINAL_SAVING, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new ContainerFinishedAtFinalSavingTransition())
      .addTransition(RMAppAttemptState.FINAL_SAVING, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.EXPIRE,
          new AMExpiredAtFinalSavingTransition())
      .addTransition(RMAppAttemptState.FINAL_SAVING, RMAppAttemptState.FINAL_SAVING,
          EnumSet.of(
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.STATUS_UPDATE,
              RMAppAttemptEventType.LAUNCHED,
              RMAppAttemptEventType.LAUNCH_FAILED,
            // should be fixed to reject container allocate request at Final
            // Saving in scheduler
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
              RMAppAttemptEventType.ATTEMPT_NEW_SAVED,
              RMAppAttemptEventType.KILL,
              RMAppAttemptEventType.FAIL))

      // Transitions from FAILED State
      // For work-preserving AM restart, failed attempt are still capturing
      // CONTAINER_FINISHED event and record the finished containers for the
      // use by the next new attempt.
      .addTransition(RMAppAttemptState.FAILED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new ContainerFinishedAtFinalStateTransition())
      .addTransition(
          RMAppAttemptState.FAILED,
          RMAppAttemptState.FAILED,
          EnumSet.of(
              RMAppAttemptEventType.LAUNCHED,
              RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.KILL,
              RMAppAttemptEventType.FAIL,
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.STATUS_UPDATE,
              RMAppAttemptEventType.CONTAINER_ALLOCATED))

      // Transitions from FINISHING State
      .addTransition(RMAppAttemptState.FINISHING,
          EnumSet.of(RMAppAttemptState.FINISHING, RMAppAttemptState.FINISHED),
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new AMFinishingContainerFinishedTransition())
      .addTransition(RMAppAttemptState.FINISHING, RMAppAttemptState.FINISHED,
          RMAppAttemptEventType.EXPIRE,
          new FinalTransition(RMAppAttemptState.FINISHED))
      .addTransition(RMAppAttemptState.FINISHING, RMAppAttemptState.FINISHING,
          EnumSet.of(
              RMAppAttemptEventType.LAUNCHED,
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.STATUS_UPDATE,
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
            // ignore Kill as we have already saved the final Finished state in
            // state store.
              RMAppAttemptEventType.KILL,
              RMAppAttemptEventType.FAIL))

      // Transitions from FINISHED State
      .addTransition(
          RMAppAttemptState.FINISHED,
          RMAppAttemptState.FINISHED,
          EnumSet.of(
              RMAppAttemptEventType.LAUNCHED,
              RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
              RMAppAttemptEventType.KILL,
              RMAppAttemptEventType.FAIL))
      .addTransition(RMAppAttemptState.FINISHED, 
          RMAppAttemptState.FINISHED, 
          RMAppAttemptEventType.CONTAINER_FINISHED, 
          new ContainerFinishedAtFinalStateTransition())

      // Transitions from KILLED State
      .addTransition(
          RMAppAttemptState.KILLED,
          RMAppAttemptState.KILLED,
          EnumSet.of(RMAppAttemptEventType.ATTEMPT_ADDED,
              RMAppAttemptEventType.LAUNCHED,
              RMAppAttemptEventType.LAUNCH_FAILED,
              RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.REGISTERED,
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.KILL,
              RMAppAttemptEventType.FAIL,
              RMAppAttemptEventType.STATUS_UPDATE))
      .addTransition(RMAppAttemptState.KILLED, 
          RMAppAttemptState.KILLED, 
          RMAppAttemptEventType.CONTAINER_FINISHED, 
          new ContainerFinishedAtFinalStateTransition())
    .installTopology();

  public RMAppAttemptImpl(ApplicationAttemptId appAttemptId,
      RMContext rmContext, YarnScheduler scheduler,
      ApplicationMasterService masterService,
      ApplicationSubmissionContext submissionContext,
      Configuration conf, List<ResourceRequest> amReqs, RMApp rmApp) {
    this(appAttemptId, rmContext, scheduler, masterService, submissionContext,
        conf, amReqs, rmApp, new DisabledBlacklistManager());
  }

  public RMAppAttemptImpl(ApplicationAttemptId appAttemptId,
      RMContext rmContext, YarnScheduler scheduler,
      ApplicationMasterService masterService,
      ApplicationSubmissionContext submissionContext,
      Configuration conf, List<ResourceRequest> amReqs, RMApp rmApp,
      BlacklistManager amBlacklistManager) {
    this.conf = conf;
    this.applicationAttemptId = appAttemptId;
    this.rmContext = rmContext;
    this.eventHandler = rmContext.getDispatcher().getEventHandler();
    this.submissionContext = submissionContext;
    this.scheduler = scheduler;
    this.masterService = masterService;

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.proxiedTrackingUrl = rmContext.getAppProxyUrl(conf,
        appAttemptId.getApplicationId());
    this.stateMachine = stateMachineFactory.make(this);

    this.attemptMetrics =
        new RMAppAttemptMetrics(applicationAttemptId, rmContext);

    this.amReqs = amReqs;
    this.blacklistedNodesForAM = amBlacklistManager;

    final int diagnosticsLimitKC = getDiagnosticsLimitKCOrThrow(conf);

    if (LOG.isDebugEnabled()) {
      LOG.debug(YarnConfiguration.APP_ATTEMPT_DIAGNOSTICS_LIMIT_KC + " : " +
              diagnosticsLimitKC);
    }

    this.diagnostics = new BoundedAppender(diagnosticsLimitKC * 1024);
    this.rmApp = rmApp;
  }

  private int getDiagnosticsLimitKCOrThrow(final Configuration configuration) {
    try {
      final int diagnosticsLimitKC = configuration.getInt(
          YarnConfiguration.APP_ATTEMPT_DIAGNOSTICS_LIMIT_KC,
          YarnConfiguration.DEFAULT_APP_ATTEMPT_DIAGNOSTICS_LIMIT_KC);

      if (diagnosticsLimitKC <= 0) {
        final String message =
            String.format(DIAGNOSTIC_LIMIT_CONFIG_ERROR_MESSAGE,
                YarnConfiguration.APP_ATTEMPT_DIAGNOSTICS_LIMIT_KC,
                diagnosticsLimitKC);
        LOG.error(message);

        throw new YarnRuntimeException(message);
      }

      return diagnosticsLimitKC;
    } catch (final NumberFormatException ignored) {
      final String diagnosticsLimitKCString = configuration
          .get(YarnConfiguration.APP_ATTEMPT_DIAGNOSTICS_LIMIT_KC);
      final String message =
          String.format(DIAGNOSTIC_LIMIT_CONFIG_ERROR_MESSAGE,
              YarnConfiguration.APP_ATTEMPT_DIAGNOSTICS_LIMIT_KC,
              diagnosticsLimitKCString);
      LOG.error(message);

      throw new YarnRuntimeException(message);
    }
  }

  @Override
  public ApplicationAttemptId getAppAttemptId() {
    return this.applicationAttemptId;
  }

  @Override
  public ApplicationSubmissionContext getSubmissionContext() {
    return this.submissionContext;
  }

  @Override
  public FinalApplicationStatus getFinalApplicationStatus() {
    this.readLock.lock();
    try {
      return this.finalStatus;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public RMAppAttemptState getAppAttemptState() {
    this.readLock.lock();
    try {
        return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getHost() {
    this.readLock.lock();

    try {
      return this.host;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public int getRpcPort() {
    this.readLock.lock();

    try {
      return this.rpcPort;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getTrackingUrl() {
    this.readLock.lock();
    try {
      return (getSubmissionContext().getUnmanagedAM()) ? 
              this.originalTrackingUrl : this.proxiedTrackingUrl;
    } finally {
      this.readLock.unlock();
    }
  }
  
  @Override
  public String getOriginalTrackingUrl() {
    this.readLock.lock();
    try {
      return this.originalTrackingUrl;
    } finally {
      this.readLock.unlock();
    }    
  }
  
  @Override
  public String getWebProxyBase() {
    this.readLock.lock();
    try {
      return ProxyUriUtils.getPath(applicationAttemptId.getApplicationId());
    } finally {
      this.readLock.unlock();
    }    
  }
  
  private void setTrackingUrlToRMAppPage(RMAppAttemptState stateToBeStored) {
    originalTrackingUrl = pjoin(
        WebAppUtils.getResolvedRMWebAppURLWithScheme(conf),
        "cluster", "app", getAppAttemptId().getApplicationId());
    switch (stateToBeStored) {
    case KILLED:
    case FAILED:
      proxiedTrackingUrl = originalTrackingUrl;
      break;
    default:
      break;
    }
  }

  private void setTrackingUrlToAHSPage(RMAppAttemptState stateToBeStored) {
    originalTrackingUrl = pjoin(
        WebAppUtils.getHttpSchemePrefix(conf) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(conf),
        "applicationhistory", "app", getAppAttemptId().getApplicationId());
    switch (stateToBeStored) {
    case KILLED:
    case FAILED:
      proxiedTrackingUrl = originalTrackingUrl;
      break;
    default:
      break;
    }
  }

  private void invalidateAMHostAndPort() {
    this.host = "N/A";
    this.rpcPort = -1;
  }

  // This is only used for RMStateStore. Normal operation must invoke the secret
  // manager to get the key and not use the local key directly.
  @Override
  public SecretKey getClientTokenMasterKey() {
    return this.clientTokenMasterKey;
  }

  @Override
  public Token<AMRMTokenIdentifier> getAMRMToken() {
    this.readLock.lock();
    try {
      return this.amrmToken;
    } finally {
      this.readLock.unlock();
    }
  }

  @Private
  public void setAMRMToken(Token<AMRMTokenIdentifier> lastToken) {
    this.writeLock.lock();
    try {
      this.amrmToken = lastToken;
      this.amrmTokenKeyId = null;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Private
  public int getAMRMTokenKeyId() {
    Integer keyId = this.amrmTokenKeyId;
    if (keyId == null) {
      this.readLock.lock();
      try {
        if (this.amrmToken == null) {
          throw new YarnRuntimeException("Missing AMRM token for "
              + this.applicationAttemptId);
        }
        keyId = this.amrmToken.decodeIdentifier().getKeyId();
        this.amrmTokenKeyId = keyId;
      } catch (IOException e) {
        throw new YarnRuntimeException("AMRM token decode error for "
            + this.applicationAttemptId, e);
      } finally {
        this.readLock.unlock();
      }
    }
    return keyId;
  }

  @Override
  public Token<ClientToAMTokenIdentifier> createClientToken(String client) {
    this.readLock.lock();

    try {
      Token<ClientToAMTokenIdentifier> token = null;
      ClientToAMTokenSecretManagerInRM secretMgr =
          this.rmContext.getClientToAMTokenSecretManager();
      if (client != null &&
          secretMgr.getMasterKey(this.applicationAttemptId) != null) {
        token = new Token<ClientToAMTokenIdentifier>(
            new ClientToAMTokenIdentifier(this.applicationAttemptId, client),
            secretMgr);
      }
      return token;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getDiagnostics() {
    this.readLock.lock();
    try {
      if (diagnostics.length() == 0 && amLaunchDiagnostics != null) {
        return amLaunchDiagnostics;
      }
      return this.diagnostics.toString();
    } finally {
      this.readLock.unlock();
    }
  }

  @VisibleForTesting
  void appendDiagnostics(final CharSequence message) {
    this.diagnostics.append(message);
  }

  public int getAMContainerExitStatus() {
    this.readLock.lock();
    try {
      return this.amContainerExitStatus;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public float getProgress() {
    this.readLock.lock();

    try {
      return this.progress;
    } finally {
      this.readLock.unlock();
    }
  }

  @VisibleForTesting
  @Override
  public List<ContainerStatus> getJustFinishedContainers() {
    this.readLock.lock();
    try {
      List<ContainerStatus> returnList = new ArrayList<>();
      for (Collection<ContainerStatus> containerStatusList :
          justFinishedContainers.values()) {
        returnList.addAll(containerStatusList);
      }
      return returnList;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ConcurrentMap<NodeId, List<ContainerStatus>>
  getJustFinishedContainersReference
      () {
    this.readLock.lock();
    try {
      return this.justFinishedContainers;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ConcurrentMap<NodeId, List<ContainerStatus>>
  getFinishedContainersSentToAMReference() {
    this.readLock.lock();
    try {
      return this.finishedContainersSentToAM;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ContainerStatus> pullJustFinishedContainers() {
    this.writeLock.lock();

    try {
      List<ContainerStatus> returnList = new ArrayList<>();

      // A new allocate means the AM received the previously sent
      // finishedContainers. We can ack this to NM now
      sendFinishedContainersToNM();

      // Mark every containerStatus as being sent to AM though we may return
      // only the ones that belong to the current attempt
      boolean keepContainersAcrossAppAttempts = this.submissionContext
          .getKeepContainersAcrossApplicationAttempts();
      for (Map.Entry<NodeId, List<ContainerStatus>> entry :
          justFinishedContainers.entrySet()) {
        NodeId nodeId = entry.getKey();
        List<ContainerStatus> finishedContainers = entry.getValue();
        if (finishedContainers.isEmpty()) {
          continue;
        }

        if (keepContainersAcrossAppAttempts) {
          returnList.addAll(finishedContainers);
        } else {
          // Filter out containers from previous attempt
          for (ContainerStatus containerStatus: finishedContainers) {
            if (containerStatus.getContainerId().getApplicationAttemptId()
                .equals(this.getAppAttemptId())) {
              returnList.add(containerStatus);
            }
          }
        }

        finishedContainersSentToAM.putIfAbsent(nodeId, new ArrayList<>());
        finishedContainersSentToAM.get(nodeId).addAll(finishedContainers);
      }
      justFinishedContainers.clear();

      return returnList;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public Container getMasterContainer() {
    return this.masterContainer;
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public void setMasterContainer(Container container) {
    masterContainer = container;
  }

  @Override
  public void handle(RMAppAttemptEvent event) {

    this.writeLock.lock();

    try {
      ApplicationAttemptId appAttemptID = event.getApplicationAttemptId();
      LOG.debug("Processing event for " + appAttemptID + " of type "
          + event.getType());
      final RMAppAttemptState oldState = getAppAttemptState();
      try {
        /* keep the master in sync with the state machine */
        this.stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.error("App attempt: " + appAttemptID
            + " can't handle this event at current state", e);
        /* TODO fail the application on the failed transition */
      }

      // Log at INFO if we're not recovering or not in a terminal state.
      // Log at DEBUG otherwise.
      if ((oldState != getAppAttemptState()) &&
          ((recoveredFinalState == null) ||
            (event.getType() != RMAppAttemptEventType.RECOVER))) {
        LOG.info(String.format(STATE_CHANGE_MESSAGE, appAttemptID, oldState,
            getAppAttemptState(), event.getType()));
      } else if ((oldState != getAppAttemptState()) && LOG.isDebugEnabled()) {
        LOG.debug(String.format(STATE_CHANGE_MESSAGE, appAttemptID, oldState,
            getAppAttemptState(), event.getType()));
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public ApplicationResourceUsageReport getApplicationResourceUsageReport() {
    this.readLock.lock();
    try {
      ApplicationResourceUsageReport report =
          scheduler.getAppResourceUsageReport(this.getAppAttemptId());
      if (report == null) {
        report = RMServerUtils.DUMMY_APPLICATION_RESOURCE_USAGE_REPORT;
      }
      AggregateAppResourceUsage resUsage =
          this.attemptMetrics.getAggregateAppResourceUsage();
      report.setMemorySeconds(resUsage.getMemorySeconds());
      report.setVcoreSeconds(resUsage.getVcoreSeconds());
      report.setPreemptedMemorySeconds(
          this.attemptMetrics.getPreemptedMemory());
      report.setPreemptedVcoreSeconds(
          this.attemptMetrics.getPreemptedVcore());
      return report;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public void recover(RMState state) {
    ApplicationStateData appState =
        state.getApplicationState().get(getAppAttemptId().getApplicationId());
    ApplicationAttemptStateData attemptState =
        appState.getAttempt(getAppAttemptId());
    assert attemptState != null;

    if (attemptState.getState() == null) {
      LOG.info(String.format(RECOVERY_MESSAGE, getAppAttemptId(), "NONE"));
    } else if (LOG.isDebugEnabled()) {
      LOG.debug(String.format(RECOVERY_MESSAGE, getAppAttemptId(),
          attemptState.getState()));
    }

    this.diagnostics.append("Attempt recovered after RM restart");
    this.diagnostics.append(attemptState.getDiagnostics());
    this.amContainerExitStatus = attemptState.getAMContainerExitStatus();
    if (amContainerExitStatus == ContainerExitStatus.PREEMPTED) {
      this.attemptMetrics.setIsPreempted();
    }

    Credentials credentials = attemptState.getAppAttemptTokens();
    setMasterContainer(attemptState.getMasterContainer());
    recoverAppAttemptCredentials(credentials, attemptState.getState());
    this.recoveredFinalState = attemptState.getState();
    this.originalTrackingUrl = attemptState.getFinalTrackingUrl();
    this.finalStatus = attemptState.getFinalApplicationStatus();
    this.startTime = attemptState.getStartTime();
    this.finishTime = attemptState.getFinishTime();
    this.attemptMetrics.updateAggregateAppResourceUsage(
        attemptState.getMemorySeconds(), attemptState.getVcoreSeconds());
    this.attemptMetrics.updateAggregatePreemptedAppResourceUsage(
        attemptState.getPreemptedMemorySeconds(),
        attemptState.getPreemptedVcoreSeconds());
  }

  public void transferStateFromAttempt(RMAppAttempt attempt) {
    this.justFinishedContainers = attempt.getJustFinishedContainersReference();
    this.finishedContainersSentToAM =
        attempt.getFinishedContainersSentToAMReference();
    // container complete msg was moved from justFinishedContainers to
    // finishedContainersSentToAM in ApplicationMasterService#allocate,
    // if am crashed and not received this response, we should resend
    // this msg again after am restart
    if (!this.finishedContainersSentToAM.isEmpty()) {
      for (Map.Entry<NodeId, List<ContainerStatus>> finishedContainer
          : this.finishedContainersSentToAM.entrySet()) {
        List<ContainerStatus> containerStatuses =
            finishedContainer.getValue();
        NodeId nodeId = finishedContainer.getKey();
        this.justFinishedContainers.putIfAbsent(nodeId, new ArrayList<>());
        this.justFinishedContainers.get(nodeId).addAll(containerStatuses);
      }
      this.finishedContainersSentToAM.clear();
    }
  }

  private void recoverAppAttemptCredentials(Credentials appAttemptTokens,
      RMAppAttemptState state) {
    if (appAttemptTokens == null || state == RMAppAttemptState.FAILED
        || state == RMAppAttemptState.FINISHED
        || state == RMAppAttemptState.KILLED) {
      return;
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      byte[] clientTokenMasterKeyBytes = appAttemptTokens.getSecretKey(
          RMStateStore.AM_CLIENT_TOKEN_MASTER_KEY_NAME);
      if (clientTokenMasterKeyBytes != null) {
        clientTokenMasterKey = rmContext.getClientToAMTokenSecretManager()
            .registerMasterKey(applicationAttemptId, clientTokenMasterKeyBytes);
      }
    }

    setAMRMToken(rmContext.getAMRMTokenSecretManager().createAndGetAMRMToken(
        applicationAttemptId));
  }

  private static class BaseTransition implements
      SingleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent> {

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
    }

  }

  private static final class AttemptStartedTransition extends BaseTransition {
	@Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

	    boolean transferStateFromPreviousAttempt = false;
      if (event instanceof RMAppStartAttemptEvent) {
        transferStateFromPreviousAttempt =
            ((RMAppStartAttemptEvent) event)
              .getTransferStateFromPreviousAttempt();
      }
      appAttempt.startTime = System.currentTimeMillis();

      // Register with the ApplicationMasterService
      appAttempt.masterService
          .registerAppAttempt(appAttempt.applicationAttemptId);

      if (UserGroupInformation.isSecurityEnabled()) {
        appAttempt.clientTokenMasterKey =
            appAttempt.rmContext.getClientToAMTokenSecretManager()
              .createMasterKey(appAttempt.applicationAttemptId);
      }

      // Add the applicationAttempt to the scheduler and inform the scheduler
      // whether to transfer the state from previous attempt.
      appAttempt.eventHandler.handle(new AppAttemptAddedSchedulerEvent(
        appAttempt.applicationAttemptId, transferStateFromPreviousAttempt));
    }
  }

  private static final List<ContainerId> EMPTY_CONTAINER_RELEASE_LIST =
      new ArrayList<ContainerId>();

  private static final List<ResourceRequest> EMPTY_CONTAINER_REQUEST_LIST =
      new ArrayList<ResourceRequest>();

  @VisibleForTesting
  public static final class ScheduleTransition
      implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {
    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      ApplicationSubmissionContext subCtx = appAttempt.submissionContext;
      if (!subCtx.getUnmanagedAM()) {
        // Need reset #containers before create new attempt, because this request
        // will be passed to scheduler, and scheduler will deduct the number after
        // AM container allocated
        
        // Currently, following fields are all hard coded,
        // TODO: change these fields when we want to support
        // priority or multiple containers AM container allocation.
        for (ResourceRequest amReq : appAttempt.amReqs) {
          amReq.setNumContainers(1);
          amReq.setPriority(AM_CONTAINER_PRIORITY);
        }

        int numNodes =
            RMServerUtils.getApplicableNodeCountForAM(appAttempt.rmContext,
                appAttempt.conf, appAttempt.amReqs);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Setting node count for blacklist to " + numNodes);
        }
        appAttempt.getAMBlacklistManager().refreshNodeHostCount(numNodes);

        ResourceBlacklistRequest amBlacklist =
            appAttempt.getAMBlacklistManager().getBlacklistUpdates();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using blacklist for AM: additions(" +
              amBlacklist.getBlacklistAdditions() + ") and removals(" +
              amBlacklist.getBlacklistRemovals() + ")");
        }
        // AM resource has been checked when submission
        Allocation amContainerAllocation =
            appAttempt.scheduler.allocate(
                appAttempt.applicationAttemptId,
                appAttempt.amReqs,
                EMPTY_CONTAINER_RELEASE_LIST,
                amBlacklist.getBlacklistAdditions(),
                amBlacklist.getBlacklistRemovals(),
                new ContainerUpdates());
        if (amContainerAllocation != null
            && amContainerAllocation.getContainers() != null) {
          assert (amContainerAllocation.getContainers().size() == 0);
        }
        return RMAppAttemptState.SCHEDULED;
      } else {
        // save state and then go to LAUNCHED state
        appAttempt.storeAttempt();
        return RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING;
      }
    }
  }

  private static final class AMContainerAllocatedTransition
      implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {
    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      // Acquire the AM container from the scheduler.
      Allocation amContainerAllocation =
          appAttempt.scheduler.allocate(appAttempt.applicationAttemptId,
            EMPTY_CONTAINER_REQUEST_LIST, EMPTY_CONTAINER_RELEASE_LIST, null,
            null, new ContainerUpdates());
      // There must be at least one container allocated, because a
      // CONTAINER_ALLOCATED is emitted after an RMContainer is constructed,
      // and is put in SchedulerApplication#newlyAllocatedContainers.

      // Note that YarnScheduler#allocate is not guaranteed to be able to
      // fetch it since container may not be fetchable for some reason like
      // DNS unavailable causing container token not generated. As such, we
      // return to the previous state and keep retry until am container is
      // fetched.
      if (amContainerAllocation.getContainers().size() == 0) {
        appAttempt.retryFetchingAMContainer(appAttempt);
        return RMAppAttemptState.SCHEDULED;
      }

      // Set the masterContainer
      appAttempt.setMasterContainer(amContainerAllocation.getContainers()
          .get(0));
      RMContainerImpl rmMasterContainer = (RMContainerImpl)appAttempt.scheduler
          .getRMContainer(appAttempt.getMasterContainer().getId());
      rmMasterContainer.setAMContainer(true);
      // The node set in NMTokenSecrentManager is used for marking whether the
      // NMToken has been issued for this node to the AM.
      // When AM container was allocated to RM itself, the node which allocates
      // this AM container was marked as the NMToken already sent. Thus,
      // clear this node set so that the following allocate requests from AM are
      // able to retrieve the corresponding NMToken.
      appAttempt.rmContext.getNMTokenSecretManager()
        .clearNodeSetForAttempt(appAttempt.applicationAttemptId);
      appAttempt.getSubmissionContext().setResource(
        appAttempt.getMasterContainer().getResource());
      appAttempt.storeAttempt();
      return RMAppAttemptState.ALLOCATED_SAVING;
    }
  }

  private void retryFetchingAMContainer(final RMAppAttemptImpl appAttempt) {
    // start a new thread so that we are not blocking main dispatcher thread.
    new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting to resend the"
              + " ContainerAllocated Event.");
        }
        appAttempt.eventHandler.handle(
            new RMAppAttemptEvent(appAttempt.applicationAttemptId,
                RMAppAttemptEventType.CONTAINER_ALLOCATED));
      }
    }.start();
  }

  private static final class AttemptStoredTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
                                                    RMAppAttemptEvent event) {

      appAttempt.registerClientToken();
      appAttempt.launchAttempt();
    }
  }

  private static class AttemptRecoveredTransition
      implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {
    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      RMApp rmApp = appAttempt.rmApp;

      /*
       * If last attempt recovered final state is null .. it means attempt was
       * started but AM container may or may not have started / finished.
       * Therefore we should wait for it to finish.
       */
      if (appAttempt.recoveredFinalState != null) {
        appAttempt.progress = 1.0f;

        if (appAttempt.submissionContext
            .getKeepContainersAcrossApplicationAttempts()
            && rmApp.getCurrentAppAttempt() != appAttempt) {
          appAttempt.transferStateFromAttempt(rmApp.getCurrentAppAttempt());
        }
        // We will replay the final attempt only if last attempt is in final
        // state but application is not in final state.
        if (rmApp.getCurrentAppAttempt() == appAttempt
            && !RMAppImpl.isAppInFinalState(rmApp)) {
          // Add the previous finished attempt to scheduler synchronously so
          // that scheduler knows the previous attempt.
          appAttempt.scheduler.handle(new AppAttemptAddedSchedulerEvent(
            appAttempt.getAppAttemptId(), false, true));
          (new BaseFinalTransition(appAttempt.recoveredFinalState)).transition(
              appAttempt, event);
        }
        return appAttempt.recoveredFinalState;
      } else if (RMAppImpl.isAppInFinalState(rmApp))  {
        // Somehow attempt final state was not saved but app final state was saved.
        // Skip adding the attempt into scheduler
        RMAppState appState = ((RMAppImpl) rmApp).getRecoveredFinalState();
        LOG.warn(rmApp.getApplicationId() + " final state (" + appState
            + ") was recorded, but " + appAttempt.applicationAttemptId
            + " final state (" + appAttempt.recoveredFinalState
            + ") was not recorded.");
        switch (appState) {
        case FINISHED:
          return RMAppAttemptState.FINISHED;
        case FAILED:
          return RMAppAttemptState.FAILED;
        case KILLED:
          return RMAppAttemptState.KILLED;
        }
        return RMAppAttemptState.FAILED;
      } else{
        // Add the current attempt to the scheduler.
        if (appAttempt.rmContext.isWorkPreservingRecoveryEnabled()) {
          // Need to register an app attempt before AM can register
          appAttempt.masterService
              .registerAppAttempt(appAttempt.applicationAttemptId);

          // Add attempt to scheduler synchronously to guarantee scheduler
          // knows attempts before AM or NM re-registers.
          appAttempt.scheduler.handle(new AppAttemptAddedSchedulerEvent(
            appAttempt.getAppAttemptId(), false, true));
        }

        /*
         * Since the application attempt's final state is not saved that means
         * for AM container (previous attempt) state must be one of these.
         * 1) AM container may not have been launched (RM failed right before
         * this).
         * 2) AM container was successfully launched but may or may not have
         * registered / unregistered.
         * In whichever case we will wait (by moving attempt into LAUNCHED
         * state) and mark this attempt failed (assuming non work preserving
         * restart) only after
         * 1) Node manager during re-registration heart beats back saying
         * am container finished.
         * 2) OR AMLivelinessMonitor expires this attempt (when am doesn't
         * heart beat back).  
         */
        LAUNCHED_TRANSITION.transition(appAttempt, event);
        return RMAppAttemptState.LAUNCHED;
      }
    }
  }

  private void rememberTargetTransitions(RMAppAttemptEvent event,
      Object transitionToDo, RMAppAttemptState targetFinalState) {
    transitionTodo = transitionToDo;
    targetedFinalState = targetFinalState;
    eventCausingFinalSaving = event;
  }

  private void rememberTargetTransitionsAndStoreState(RMAppAttemptEvent event,
      Object transitionToDo, RMAppAttemptState targetFinalState,
      RMAppAttemptState stateToBeStored) {

    rememberTargetTransitions(event, transitionToDo, targetFinalState);
    stateBeforeFinalSaving = getState();

    // As of today, finalState, diagnostics, final-tracking-url and
    // finalAppStatus are the only things that we store into the StateStore
    // AFTER the initial saving on app-attempt-start
    // These fields can be visible from outside only after they are saved in
    // StateStore
    BoundedAppender diags = new BoundedAppender(diagnostics.getLimit());

    // don't leave the tracking URL pointing to a non-existent AM
    if (conf.getBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
            YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED)) {
      setTrackingUrlToAHSPage(stateToBeStored);
    } else {
      setTrackingUrlToRMAppPage(stateToBeStored);
    }
    String finalTrackingUrl = getOriginalTrackingUrl();
    FinalApplicationStatus finalStatus = null;
    int exitStatus = ContainerExitStatus.INVALID;
    switch (event.getType()) {
    case LAUNCH_FAILED:
      diags.append(event.getDiagnosticMsg());
      break;
    case REGISTERED:
      diags.append(getUnexpectedAMRegisteredDiagnostics());
      break;
    case UNREGISTERED:
      RMAppAttemptUnregistrationEvent unregisterEvent =
          (RMAppAttemptUnregistrationEvent) event;
      diags.append(unregisterEvent.getDiagnosticMsg());
      // reset finalTrackingUrl to url sent by am
      finalTrackingUrl = sanitizeTrackingUrl(unregisterEvent.getFinalTrackingUrl());
      finalStatus = unregisterEvent.getFinalApplicationStatus();
      break;
    case CONTAINER_FINISHED:
      RMAppAttemptContainerFinishedEvent finishEvent =
          (RMAppAttemptContainerFinishedEvent) event;
      diags.append(getAMContainerCrashedDiagnostics(finishEvent));
      exitStatus = finishEvent.getContainerStatus().getExitStatus();
      break;
    case KILL:
      break;
    case FAIL:
      diags.append(event.getDiagnosticMsg());
      break;
    case EXPIRE:
      diags.append(getAMExpiredDiagnostics(event));
      break;
    default:
      break;
    }
    AggregateAppResourceUsage resUsage =
        this.attemptMetrics.getAggregateAppResourceUsage();
    RMStateStore rmStore = rmContext.getStateStore();
    setFinishTime(System.currentTimeMillis());

    ApplicationAttemptStateData attemptState =
        ApplicationAttemptStateData.newInstance(
            applicationAttemptId,  getMasterContainer(),
            rmStore.getCredentialsFromAppAttempt(this),
            startTime, stateToBeStored, finalTrackingUrl, diags.toString(),
            finalStatus, exitStatus,
          getFinishTime(), resUsage.getMemorySeconds(),
          resUsage.getVcoreSeconds(),
          this.attemptMetrics.getPreemptedMemory(),
          this.attemptMetrics.getPreemptedVcore());
    LOG.info("Updating application attempt " + applicationAttemptId
        + " with final state: " + targetedFinalState + ", and exit status: "
        + exitStatus);
    rmStore.updateApplicationAttemptState(attemptState);
  }

  private static class FinalSavingTransition extends BaseTransition {

    Object transitionToDo;
    RMAppAttemptState targetedFinalState;

    public FinalSavingTransition(Object transitionToDo,
        RMAppAttemptState targetedFinalState) {
      this.transitionToDo = transitionToDo;
      this.targetedFinalState = targetedFinalState;
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent event) {
      // For cases Killed/Failed, targetedFinalState is the same as the state to
      // be stored
      appAttempt.rememberTargetTransitionsAndStoreState(event, transitionToDo,
        targetedFinalState, targetedFinalState);
    }
  }

  private static class FinalStateSavedTransition implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {
    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      RMAppAttemptEvent causeEvent = appAttempt.eventCausingFinalSaving;

      if (appAttempt.transitionTodo instanceof SingleArcTransition) {
        ((SingleArcTransition) appAttempt.transitionTodo).transition(
          appAttempt, causeEvent);
      } else if (appAttempt.transitionTodo instanceof MultipleArcTransition) {
        ((MultipleArcTransition) appAttempt.transitionTodo).transition(
          appAttempt, causeEvent);
      }
      return appAttempt.targetedFinalState;
    }
  }
  
  private static class BaseFinalTransition extends BaseTransition {

    private final RMAppAttemptState finalAttemptState;

    public BaseFinalTransition(RMAppAttemptState finalAttemptState) {
      this.finalAttemptState = finalAttemptState;
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      ApplicationAttemptId appAttemptId = appAttempt.getAppAttemptId();

      // Tell the AMS. Unregister from the ApplicationMasterService
      appAttempt.masterService.unregisterAttempt(appAttemptId);

      // Tell the application and the scheduler
      ApplicationId applicationId = appAttemptId.getApplicationId();
      RMAppEvent appEvent = null;
      boolean keepContainersAcrossAppAttempts = false;
      switch (finalAttemptState) {
        case FINISHED:
        {
          appEvent =
              new RMAppEvent(applicationId, RMAppEventType.ATTEMPT_FINISHED,
              appAttempt.getDiagnostics());
        }
        break;
        case KILLED:
        {
          appAttempt.invalidateAMHostAndPort();
          // Forward diagnostics received in attempt kill event.
          appEvent =
              new RMAppFailedAttemptEvent(applicationId,
                  RMAppEventType.ATTEMPT_KILLED,
                  event.getDiagnosticMsg(), false);
        }
        break;
        case FAILED:
        {
          appAttempt.invalidateAMHostAndPort();

          if (appAttempt.submissionContext
            .getKeepContainersAcrossApplicationAttempts()
              && !appAttempt.submissionContext.getUnmanagedAM()) {
            int numberOfFailure = ((RMAppImpl)appAttempt.rmApp)
                .getNumFailedAppAttempts();
            if (numberOfFailure < appAttempt.rmApp.getMaxAppAttempts()) {
              keepContainersAcrossAppAttempts = true;
            }
          }
          appEvent =
              new RMAppFailedAttemptEvent(applicationId,
                RMAppEventType.ATTEMPT_FAILED, appAttempt.getDiagnostics(),
                keepContainersAcrossAppAttempts);

        }
        break;
        default:
        {
          LOG.error("Cannot get this state!! Error!!");
        }
        break;
      }

      appAttempt.eventHandler.handle(appEvent);
      appAttempt.eventHandler.handle(new AppAttemptRemovedSchedulerEvent(
        appAttemptId, finalAttemptState, keepContainersAcrossAppAttempts));
      appAttempt.removeCredentials(appAttempt);

      appAttempt.rmContext.getRMApplicationHistoryWriter()
          .applicationAttemptFinished(appAttempt, finalAttemptState);
      appAttempt.rmContext.getSystemMetricsPublisher()
          .appAttemptFinished(appAttempt, finalAttemptState,
              appAttempt.rmApp, System.currentTimeMillis());
    }
  }

  private static class AttemptFailedTransition extends BaseFinalTransition {

    public AttemptFailedTransition() {
      super(RMAppAttemptState.FAILED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent event) {
      if (event.getDiagnosticMsg() != null) {
        appAttempt.diagnostics.append(event.getDiagnosticMsg());
      }
      super.transition(appAttempt, event);
    }
  }

  private static class AMLaunchedTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
                            RMAppAttemptEvent event) {
      if (event.getType() == RMAppAttemptEventType.LAUNCHED
          || event.getType() == RMAppAttemptEventType.REGISTERED) {
        appAttempt.launchAMEndTime = System.currentTimeMillis();
        long delay = appAttempt.launchAMEndTime -
            appAttempt.launchAMStartTime;
        ClusterMetrics.getMetrics().addAMLaunchDelay(delay);
      }

      appAttempt
          .updateAMLaunchDiagnostics(AMState.LAUNCHED.getDiagnosticMessage());
      // Register with AMLivelinessMonitor
      appAttempt.attemptLaunched();

    }
  }

  @Override
  public boolean shouldCountTowardsMaxAttemptRetry() {
    long attemptFailuresValidityInterval = this.submissionContext
        .getAttemptFailuresValidityInterval();
    long end = System.currentTimeMillis();
    if (attemptFailuresValidityInterval > 0
        && this.getFinishTime() > 0
        && this.getFinishTime() < (end - attemptFailuresValidityInterval)) {
        return false;
    }
    try {
      this.readLock.lock();
      int exitStatus = getAMContainerExitStatus();
      return !(exitStatus == ContainerExitStatus.PREEMPTED
          || exitStatus == ContainerExitStatus.ABORTED
          || exitStatus == ContainerExitStatus.DISKS_FAILED
          || exitStatus == ContainerExitStatus.KILLED_BY_RESOURCEMANAGER);
    } finally {
      this.readLock.unlock();
    }
  }

  private static boolean shouldCountTowardsNodeBlacklisting(int exitStatus) {
    switch (exitStatus) {
    case ContainerExitStatus.PREEMPTED:
    case ContainerExitStatus.KILLED_BY_RESOURCEMANAGER:
    case ContainerExitStatus.KILLED_BY_APPMASTER:
    case ContainerExitStatus.KILLED_AFTER_APP_COMPLETION:
    case ContainerExitStatus.ABORTED:
      // Neither the app's fault nor the system's fault. This happens by design,
      // so no need for skipping nodes
      return false;
    case ContainerExitStatus.DISKS_FAILED:
      // This container is marked with this exit-status means that the node is
      // already marked as unhealthy given that most of the disks failed. So, no
      // need for any explicit skipping of nodes.
      return false;
    case ContainerExitStatus.KILLED_EXCEEDED_VMEM:
    case ContainerExitStatus.KILLED_EXCEEDED_PMEM:
      // No point in skipping the node as it's not the system's fault
      return false;
    case ContainerExitStatus.SUCCESS:
      return false;
    case ContainerExitStatus.INVALID:
      // Ideally, this shouldn't be considered for skipping a node. But in
      // reality, it seems like there are cases where we are not setting
      // exit-code correctly and so it's better to be conservative. See
      // YARN-4284.
      return true;
    default:
      return true;
    }
  }

  private static final class UnmanagedAMAttemptSavedTransition
                                                extends AMLaunchedTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
                            RMAppAttemptEvent event) {
      // create AMRMToken
      appAttempt.amrmToken =
          appAttempt.rmContext.getAMRMTokenSecretManager().createAndGetAMRMToken(
            appAttempt.applicationAttemptId);
      appAttempt.registerClientToken();
      super.transition(appAttempt, event);
    }    
  }

  private void registerClientToken() {
    // register the ClientTokenMasterKey after it is saved in the store,
    // otherwise client may hold an invalid ClientToken after RM restarts.
    if (UserGroupInformation.isSecurityEnabled()) {
      rmContext.getClientToAMTokenSecretManager()
          .registerApplication(getAppAttemptId(), getClientTokenMasterKey());
    }
  }

  private static final class LaunchFailedTransition extends BaseFinalTransition {

    public LaunchFailedTransition() {
      super(RMAppAttemptState.FAILED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      // Use diagnostic from launcher
      appAttempt.diagnostics.append(event.getDiagnosticMsg());

      // Tell the app, scheduler
      super.transition(appAttempt, event);

    }
  }

  private static final class KillAllocatedAMTransition extends
      BaseFinalTransition {
    public KillAllocatedAMTransition() {
      super(RMAppAttemptState.KILLED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      // Tell the application and scheduler
      super.transition(appAttempt, event);

      // Tell the launcher to cleanup.
      appAttempt.eventHandler.handle(new AMLauncherEvent(
          AMLauncherEventType.CLEANUP, appAttempt));

    }
  }

  private static final class AMRegisteredTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      if (!RMAppAttemptState.LAUNCHED.equals(appAttempt.getState())) {
        // registered received before launch
        LAUNCHED_TRANSITION.transition(appAttempt, event);
      }
      long delay = System.currentTimeMillis() - appAttempt.launchAMEndTime;
      ClusterMetrics.getMetrics().addAMRegisterDelay(delay);
      RMAppAttemptRegistrationEvent registrationEvent
          = (RMAppAttemptRegistrationEvent) event;
      appAttempt.host = registrationEvent.getHost();
      appAttempt.rpcPort = registrationEvent.getRpcport();
      appAttempt.originalTrackingUrl =
          sanitizeTrackingUrl(registrationEvent.getTrackingurl());

      // reset AMLaunchDiagnostics once AM Registers with RM
      appAttempt.updateAMLaunchDiagnostics(null);

      // Let the app know
      appAttempt.eventHandler.handle(new RMAppEvent(appAttempt
          .getAppAttemptId().getApplicationId(),
          RMAppEventType.ATTEMPT_REGISTERED));

      // TODO:FIXME: Note for future. Unfortunately we only do a state-store
      // write at AM launch time, so we don't save the AM's tracking URL anywhere
      // as that would mean an extra state-store write. For now, we hope that in
      // work-preserving restart, AMs are forced to reregister.

      appAttempt.rmContext.getRMApplicationHistoryWriter()
          .applicationAttemptStarted(appAttempt);
      appAttempt.rmContext.getSystemMetricsPublisher()
          .appAttemptRegistered(appAttempt, System.currentTimeMillis());
    }
  }

  private static final class AMContainerCrashedBeforeRunningTransition extends
      BaseFinalTransition {

    public AMContainerCrashedBeforeRunningTransition() {
      super(RMAppAttemptState.FAILED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      RMAppAttemptContainerFinishedEvent finishEvent =
          ((RMAppAttemptContainerFinishedEvent)event);

      // UnRegister from AMLivelinessMonitor
      appAttempt.rmContext.getAMLivelinessMonitor().unregister(
          appAttempt.getAppAttemptId());

      // Setup diagnostic message and exit status
      appAttempt.setAMContainerCrashedDiagnosticsAndExitStatus(finishEvent);

      // Tell the app, scheduler
      super.transition(appAttempt, finishEvent);
    }
  }

  private void setAMContainerCrashedDiagnosticsAndExitStatus(
      RMAppAttemptContainerFinishedEvent finishEvent) {
    ContainerStatus status = finishEvent.getContainerStatus();
    this.diagnostics.append(getAMContainerCrashedDiagnostics(finishEvent));
    this.amContainerExitStatus = status.getExitStatus();
  }

  private String getAMContainerCrashedDiagnostics(
      RMAppAttemptContainerFinishedEvent finishEvent) {
    ContainerStatus status = finishEvent.getContainerStatus();
    StringBuilder diagnosticsBuilder = new StringBuilder();
    diagnosticsBuilder.append("AM Container for ").append(
      finishEvent.getApplicationAttemptId()).append(
      " exited with ").append(" exitCode: ").append(status.getExitStatus()).
      append("\n");
    diagnosticsBuilder.append("Failing this attempt.").append("Diagnostics: ")
        .append(status.getDiagnostics());
    if (this.getTrackingUrl() != null) {
      diagnosticsBuilder.append("For more detailed output,").append(
        " check the application tracking page: ").append(
        this.getTrackingUrl()).append(
        " Then click on links to logs of each attempt.\n");
    }
    return diagnosticsBuilder.toString();
  }

  private static class FinalTransition extends BaseFinalTransition {

    public FinalTransition(RMAppAttemptState finalAttemptState) {
      super(finalAttemptState);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      appAttempt.progress = 1.0f;

      // Tell the app and the scheduler
      super.transition(appAttempt, event);

      // UnRegister from AMLivelinessMonitor. Perhaps for
      // FAILING/KILLED/UnManaged AMs
      appAttempt.rmContext.getAMLivelinessMonitor().unregister(
          appAttempt.getAppAttemptId());
      appAttempt.rmContext.getAMFinishingMonitor().unregister(
          appAttempt.getAppAttemptId());

      if(!appAttempt.submissionContext.getUnmanagedAM()) {
        // Tell the launcher to cleanup.
        appAttempt.eventHandler.handle(new AMLauncherEvent(
            AMLauncherEventType.CLEANUP, appAttempt));
      }
    }
  }

  private static class ExpiredTransition extends FinalTransition {

    public ExpiredTransition() {
      super(RMAppAttemptState.FAILED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      appAttempt.diagnostics.append(getAMExpiredDiagnostics(event));
      super.transition(appAttempt, event);
    }
  }

  private static String getAMExpiredDiagnostics(RMAppAttemptEvent event) {
    String diag =
        "ApplicationMaster for attempt " + event.getApplicationAttemptId()
            + " timed out";
    return diag;
  }

  private static class UnexpectedAMRegisteredTransition extends
      BaseFinalTransition {

    public UnexpectedAMRegisteredTransition() {
      super(RMAppAttemptState.FAILED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent event) {
      assert appAttempt.submissionContext.getUnmanagedAM();
      appAttempt.diagnostics.append(getUnexpectedAMRegisteredDiagnostics());
      super.transition(appAttempt, event);
    }

  }

  private static String getUnexpectedAMRegisteredDiagnostics() {
    return "Unmanaged AM must register after AM attempt reaches LAUNCHED state.";
  }

  private static final class StatusUpdateTransition extends
      BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptStatusupdateEvent statusUpdateEvent
        = (RMAppAttemptStatusupdateEvent) event;

      // Update progress
      appAttempt.progress = statusUpdateEvent.getProgress();

      // Ping to AMLivelinessMonitor
      appAttempt.rmContext.getAMLivelinessMonitor().receivedPing(
          statusUpdateEvent.getApplicationAttemptId());
    }
  }

  private static final class AMUnregisteredTransition extends BaseTransition {

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      // Tell the app
      if (appAttempt.getSubmissionContext().getUnmanagedAM()) {
        // YARN-1815: Saving the attempt final state so that we do not recover
        // the finished Unmanaged AM post RM failover
        // Unmanaged AMs have no container to wait for, so they skip
        // the FINISHING state and go straight to FINISHED.
        appAttempt.rememberTargetTransitionsAndStoreState(event,
            new AMFinishedAfterFinalSavingTransition(event),
            RMAppAttemptState.FINISHED, RMAppAttemptState.FINISHED);
      } else {
        // Saving the attempt final state
        appAttempt.rememberTargetTransitionsAndStoreState(event,
            new FinalStateSavedAfterAMUnregisterTransition(),
            RMAppAttemptState.FINISHING, RMAppAttemptState.FINISHED);
      }
      ApplicationId applicationId =
          appAttempt.getAppAttemptId().getApplicationId();

      // Tell the app immediately that AM is unregistering so that app itself
      // can save its state as soon as possible. Whether we do it like this, or
      // we wait till AppAttempt is saved, it doesn't make any difference on the
      // app side w.r.t failure conditions. The only event going out of
      // AppAttempt to App after this point of time is AM/AppAttempt Finished.
      appAttempt.eventHandler.handle(new RMAppEvent(applicationId,
        RMAppEventType.ATTEMPT_UNREGISTERED));
      return;
    }
  }

  private static class FinalStateSavedAfterAMUnregisterTransition extends
      BaseTransition {
    @Override
    public void
        transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent event) {
      // Unregister from the AMlivenessMonitor and register with AMFinishingMonitor
      appAttempt.rmContext.getAMLivelinessMonitor().unregister(
        appAttempt.applicationAttemptId);
      appAttempt.rmContext.getAMFinishingMonitor().register(
        appAttempt.applicationAttemptId);

      // Do not make any more changes to this transition code. Make all changes
      // to the following method. Unless you are absolutely sure that you have
      // stuff to do that shouldn't be used by the callers of the following
      // method.
      appAttempt.updateInfoOnAMUnregister(event);
    }
  }

  private void updateInfoOnAMUnregister(RMAppAttemptEvent event) {
    progress = 1.0f;
    RMAppAttemptUnregistrationEvent unregisterEvent =
        (RMAppAttemptUnregistrationEvent) event;
    this.diagnostics.append(unregisterEvent.getDiagnosticMsg());
    originalTrackingUrl = sanitizeTrackingUrl(unregisterEvent.getFinalTrackingUrl());
    finalStatus = unregisterEvent.getFinalApplicationStatus();
  }

  private static final class ContainerFinishedTransition
      implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {

    // The transition To Do after attempt final state is saved.
    private BaseTransition transitionToDo;
    private RMAppAttemptState currentState;

    public ContainerFinishedTransition(BaseTransition transitionToDo,
        RMAppAttemptState currentState) {
      this.transitionToDo = transitionToDo;
      this.currentState = currentState;
    }

    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptContainerFinishedEvent containerFinishedEvent =
          (RMAppAttemptContainerFinishedEvent) event;
      ContainerStatus containerStatus =
          containerFinishedEvent.getContainerStatus();

      // Is this container the AmContainer? If the finished container is same as
      // the AMContainer, AppAttempt fails
      if (appAttempt.masterContainer != null
          && appAttempt.masterContainer.getId().equals(
              containerStatus.getContainerId())) {
        appAttempt.amContainerFinished(appAttempt, containerFinishedEvent);

        // Remember the follow up transition and save the final attempt state.
        appAttempt.rememberTargetTransitionsAndStoreState(event,
            transitionToDo, RMAppAttemptState.FAILED, RMAppAttemptState.FAILED);
        return RMAppAttemptState.FINAL_SAVING;
      }

      // Add all finished containers so that they can be acked to NM
      addJustFinishedContainer(appAttempt, containerFinishedEvent);
      return this.currentState;
    }
  }

  // Ack NM to remove finished AM container, not waiting for
  // new appattempt to pull am container complete msg, new  appattempt
  // may launch fail and leaves too many completed container in NM
  private void sendFinishedAMContainerToNM(NodeId nodeId,
      ContainerId containerId) {
    List<ContainerId> containerIdList = new ArrayList<ContainerId>();
    containerIdList.add(containerId);
    eventHandler.handle(new RMNodeFinishedContainersPulledByAMEvent(
        nodeId, containerIdList));
  }

  // Ack NM to remove finished containers from context.
  private void sendFinishedContainersToNM() {
    for (NodeId nodeId : finishedContainersSentToAM.keySet()) {

      // Clear and get current values
      List<ContainerStatus> currentSentContainers =
          finishedContainersSentToAM.put(nodeId, new ArrayList<>());
      List<ContainerId> containerIdList =
          new ArrayList<>(currentSentContainers.size());
      for (ContainerStatus containerStatus : currentSentContainers) {
        containerIdList.add(containerStatus.getContainerId());
      }
      eventHandler.handle(new RMNodeFinishedContainersPulledByAMEvent(nodeId,
        containerIdList));
    }
    this.finishedContainersSentToAM.clear();
  }

  // Add am container to the list so that am container instance will be
  // removed from NMContext.
  private static void amContainerFinished(RMAppAttemptImpl appAttempt,
      RMAppAttemptContainerFinishedEvent containerFinishedEvent) {

    NodeId nodeId = containerFinishedEvent.getNodeId();

    ContainerStatus containerStatus =
        containerFinishedEvent.getContainerStatus();
    if (containerStatus != null) {
      int exitStatus = containerStatus.getExitStatus();
      if (shouldCountTowardsNodeBlacklisting(exitStatus)) {
        appAttempt.addAMNodeToBlackList(nodeId);
      }
    } else {
      LOG.warn("No ContainerStatus in containerFinishedEvent");
    }

    if (!appAttempt.getSubmissionContext()
        .getKeepContainersAcrossApplicationAttempts()) {
      appAttempt.finishedContainersSentToAM.putIfAbsent(nodeId,
          new ArrayList<>());
      appAttempt.finishedContainersSentToAM.get(nodeId).add(containerStatus);
      appAttempt.sendFinishedContainersToNM();
    } else {
      appAttempt.sendFinishedAMContainerToNM(nodeId,
          containerStatus.getContainerId());
    }
  }

  private void addAMNodeToBlackList(NodeId nodeId) {
    SchedulerNode schedulerNode = scheduler.getSchedulerNode(nodeId);
    if (schedulerNode != null) {
      blacklistedNodesForAM.addNode(schedulerNode.getNodeName());
    } else {
      LOG.info(nodeId + " is not added to AM blacklist for "
          + applicationAttemptId + ", because it has been removed");
    }
  }

  @Override
  public BlacklistManager getAMBlacklistManager() {
    return blacklistedNodesForAM;
  }

  private static void addJustFinishedContainer(RMAppAttemptImpl appAttempt,
      RMAppAttemptContainerFinishedEvent containerFinishedEvent) {
    appAttempt.justFinishedContainers.putIfAbsent(containerFinishedEvent
        .getNodeId(), new ArrayList<>());
    appAttempt.justFinishedContainers.get(containerFinishedEvent
            .getNodeId()).add(containerFinishedEvent.getContainerStatus());
  }

  private static final class ContainerFinishedAtFinalStateTransition
      extends BaseTransition {
    @Override
    public void
        transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent event) {
      RMAppAttemptContainerFinishedEvent containerFinishedEvent =
          (RMAppAttemptContainerFinishedEvent) event;
      
      // Normal container. Add it in completed containers list
      addJustFinishedContainer(appAttempt, containerFinishedEvent);
    }
  }

  private static class AMContainerCrashedAtRunningTransition extends
      BaseTransition {
    @Override
    public void
        transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent event) {
      RMAppAttemptContainerFinishedEvent finishEvent =
          (RMAppAttemptContainerFinishedEvent) event;
      // container associated with AM. must not be unmanaged
      assert appAttempt.submissionContext.getUnmanagedAM() == false;
      // Setup diagnostic message and exit status
      appAttempt.setAMContainerCrashedDiagnosticsAndExitStatus(finishEvent);
      new FinalTransition(RMAppAttemptState.FAILED).transition(appAttempt,
        event);
    }
  }

  private static final class AMFinishingContainerFinishedTransition
      implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {

    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptContainerFinishedEvent containerFinishedEvent
        = (RMAppAttemptContainerFinishedEvent) event;
      ContainerStatus containerStatus =
          containerFinishedEvent.getContainerStatus();

      // Is this container the ApplicationMaster container?
      if (appAttempt.masterContainer.getId().equals(
          containerStatus.getContainerId())) {
        new FinalTransition(RMAppAttemptState.FINISHED).transition(
            appAttempt, containerFinishedEvent);
        appAttempt.amContainerFinished(appAttempt, containerFinishedEvent);
        return RMAppAttemptState.FINISHED;
      }
      // Add all finished containers so that they can be acked to NM.
      addJustFinishedContainer(appAttempt, containerFinishedEvent);

      return RMAppAttemptState.FINISHING;
    }
  }

  private static class ContainerFinishedAtFinalSavingTransition extends
      BaseTransition {
    @Override
    public void
        transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent event) {
      RMAppAttemptContainerFinishedEvent containerFinishedEvent =
          (RMAppAttemptContainerFinishedEvent) event;
      ContainerStatus containerStatus =
          containerFinishedEvent.getContainerStatus();

      // If this is the AM container, it means the AM container is finished,
      // but we are not yet acknowledged that the final state has been saved.
      // Thus, we still return FINAL_SAVING state here.
      if (appAttempt.masterContainer.getId().equals(
        containerStatus.getContainerId())) {

        appAttempt.amContainerFinished(appAttempt, containerFinishedEvent);

        if (appAttempt.targetedFinalState.equals(RMAppAttemptState.FAILED)
            || appAttempt.targetedFinalState.equals(RMAppAttemptState.KILLED)) {
          // ignore Container_Finished Event if we were supposed to reach
          // FAILED/KILLED state.
          return;
        }

        // pass in the earlier AMUnregistered Event also, as this is needed for
        // AMFinishedAfterFinalSavingTransition later on
        appAttempt.rememberTargetTransitions(event,
          new AMFinishedAfterFinalSavingTransition(
            appAttempt.eventCausingFinalSaving), RMAppAttemptState.FINISHED);
        return;
      }

      // Add all finished containers so that they can be acked to NM.
      addJustFinishedContainer(appAttempt, containerFinishedEvent);
    }
  }

  private static class AMFinishedAfterFinalSavingTransition extends
      BaseTransition {
    RMAppAttemptEvent amUnregisteredEvent;
    public AMFinishedAfterFinalSavingTransition(
        RMAppAttemptEvent amUnregisteredEvent) {
      this.amUnregisteredEvent = amUnregisteredEvent;
    }

    @Override
    public void
        transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent event) {
      appAttempt.updateInfoOnAMUnregister(amUnregisteredEvent);
      new FinalTransition(RMAppAttemptState.FINISHED).transition(appAttempt,
          event);
    }
  }

  private static class AMExpiredAtFinalSavingTransition extends
      BaseTransition {
    @Override
    public void
        transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent event) {
      if (appAttempt.targetedFinalState.equals(RMAppAttemptState.FAILED)
          || appAttempt.targetedFinalState.equals(RMAppAttemptState.KILLED)) {
        // ignore Container_Finished Event if we were supposed to reach
        // FAILED/KILLED state.
        return;
      }

      // pass in the earlier AMUnregistered Event also, as this is needed for
      // AMFinishedAfterFinalSavingTransition later on
      appAttempt.rememberTargetTransitions(event,
        new AMFinishedAfterFinalSavingTransition(
        appAttempt.eventCausingFinalSaving), RMAppAttemptState.FINISHED);
    }
  }

  @Override
  public long getStartTime() {
    this.readLock.lock();
    try {
      return this.startTime;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public RMAppAttemptState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public YarnApplicationAttemptState createApplicationAttemptState() {
    RMAppAttemptState state = getState();
    // If AppAttempt is in FINAL_SAVING state, return its previous state.
    if (state.equals(RMAppAttemptState.FINAL_SAVING)) {
      state = stateBeforeFinalSaving;
    }
    return RMServerUtils.createApplicationAttemptState(state);
  }

  private void launchAttempt(){
    launchAMStartTime = System.currentTimeMillis();
    // Send event to launch the AM Container
    eventHandler.handle(new AMLauncherEvent(AMLauncherEventType.LAUNCH, this));
  }
  
  private void attemptLaunched() {
    // Register with AMLivelinessMonitor
    rmContext.getAMLivelinessMonitor().register(getAppAttemptId());
  }
  
  private void storeAttempt() {
    // store attempt data in a non-blocking manner to prevent dispatcher
    // thread starvation and wait for state to be saved
    LOG.info("Storing attempt: AppId: " + 
              getAppAttemptId().getApplicationId() 
              + " AttemptId: " + 
              getAppAttemptId()
              + " MasterContainer: " + masterContainer);
    rmContext.getStateStore().storeNewApplicationAttempt(this);
  }

  private void removeCredentials(RMAppAttemptImpl appAttempt) {
    // Unregister from the ClientToAMTokenSecretManager
    if (UserGroupInformation.isSecurityEnabled()) {
      appAttempt.rmContext.getClientToAMTokenSecretManager()
        .unRegisterApplication(appAttempt.getAppAttemptId());
    }

    // Remove the AppAttempt from the AMRMTokenSecretManager
    appAttempt.rmContext.getAMRMTokenSecretManager()
      .applicationMasterFinished(appAttempt.getAppAttemptId());
  }

  private static String sanitizeTrackingUrl(String url) {
    return (url == null || url.trim().isEmpty()) ? "N/A" : url;
  }

  @Override
  public ApplicationAttemptReport createApplicationAttemptReport() {
    this.readLock.lock();
    ApplicationAttemptReport attemptReport = null;
    try {
      // AM container maybe not yet allocated. and also unmangedAM doesn't have
      // am container.
      ContainerId amId =
          masterContainer == null ? null : masterContainer.getId();
      attemptReport = ApplicationAttemptReport.newInstance(
          this.getAppAttemptId(), this.getHost(), this.getRpcPort(),
          this.getTrackingUrl(), this.getOriginalTrackingUrl(),
          this.getDiagnostics(), createApplicationAttemptState(), amId,
          this.startTime, this.finishTime);
    } finally {
      this.readLock.unlock();
    }
    return attemptReport;
  }

  @Override
  public RMAppAttemptMetrics getRMAppAttemptMetrics() {
    // didn't use read/write lock here because RMAppAttemptMetrics has its own
    // lock
    return attemptMetrics;
  }

  @Override
  public long getFinishTime() {
    try {
      this.readLock.lock();
      return this.finishTime;
    } finally {
      this.readLock.unlock();
    }
  }

  private void setFinishTime(long finishTime) {
    try {
      this.writeLock.lock();
      this.finishTime = finishTime;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public void updateAMLaunchDiagnostics(String amLaunchDiagnostics) {
    this.amLaunchDiagnostics = amLaunchDiagnostics;
  }

  public RMAppAttemptState getRecoveredFinalState() {
    return recoveredFinalState;
  }

  public void setRecoveredFinalState(RMAppAttemptState finalState) {
    this.recoveredFinalState = finalState;
  }

  @Override
  public Set<String> getBlacklistedNodes() {
    if (scheduler instanceof AbstractYarnScheduler) {
      AbstractYarnScheduler ayScheduler =
          (AbstractYarnScheduler) scheduler;
      SchedulerApplicationAttempt attempt =
          ayScheduler.getApplicationAttempt(applicationAttemptId);
      if (attempt != null) {
        return attempt.getBlacklistedNodes();
      }
    }
    return Collections.EMPTY_SET;
  }

}
