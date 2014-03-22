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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.crypto.SecretKey;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppFailedAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppFinishedAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAcquiredEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAllocatedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptLaunchFailedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptNewSavedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUpdateSavedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

@SuppressWarnings({"unchecked", "rawtypes"})
public class RMAppAttemptImpl implements RMAppAttempt, Recoverable {

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

  private final ReadLock readLock;
  private final WriteLock writeLock;

  private final ApplicationAttemptId applicationAttemptId;
  private final ApplicationSubmissionContext submissionContext;
  private Token<AMRMTokenIdentifier> amrmToken = null;
  private SecretKey clientTokenMasterKey = null;

  //nodes on while this attempt's containers ran
  private Set<NodeId> ranNodes =
    new HashSet<NodeId>();
  private List<ContainerStatus> justFinishedContainers =
    new ArrayList<ContainerStatus>();
  private Container masterContainer;

  private float progress = 0;
  private String host = "N/A";
  private int rpcPort = -1;
  private String originalTrackingUrl = "N/A";
  private String proxiedTrackingUrl = "N/A";
  private long startTime = 0;

  // Set to null initially. Will eventually get set 
  // if an RMAppAttemptUnregistrationEvent occurs
  private FinalApplicationStatus finalStatus = null;
  private final StringBuilder diagnostics = new StringBuilder();

  private Configuration conf;
  private final boolean isLastAttempt;
  private static final ExpiredTransition EXPIRED_TRANSITION =
      new ExpiredTransition();

  private RMAppAttemptEvent eventCausingFinalSaving;
  private RMAppAttemptState targetedFinalState;
  private RMAppAttemptState recoveredFinalState;
  private RMAppAttemptState stateBeforeFinalSaving;
  private Object transitionTodo;

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
          
       // Transitions from ALLOCATED_SAVING State
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING, 
          RMAppAttemptState.ALLOCATED,
          RMAppAttemptEventType.ATTEMPT_NEW_SAVED, new AttemptStoredTransition())
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING, 
          RMAppAttemptState.ALLOCATED_SAVING,
          RMAppAttemptEventType.CONTAINER_ACQUIRED, 
          new ContainerAcquiredTransition())
       // App could be killed by the client. So need to handle this. 
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING, 
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new BaseFinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))

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

       // Transitions from ALLOCATED State
      .addTransition(RMAppAttemptState.ALLOCATED,
          RMAppAttemptState.ALLOCATED,
          RMAppAttemptEventType.CONTAINER_ACQUIRED,
          new ContainerAcquiredTransition())
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.LAUNCHED,
          RMAppAttemptEventType.LAUNCHED, new AMLaunchedTransition())
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.LAUNCH_FAILED,
          new FinalSavingTransition(new LaunchFailedTransition(),
            RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(
            new KillAllocatedAMTransition(), RMAppAttemptState.KILLED))
          
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new FinalSavingTransition(
            new AMContainerCrashedTransition(), RMAppAttemptState.FAILED))

       // Transitions from LAUNCHED State
      .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.RUNNING,
          RMAppAttemptEventType.REGISTERED, new AMRegisteredTransition())
      .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new FinalSavingTransition(
            new AMContainerCrashedTransition(), RMAppAttemptState.FAILED))
      .addTransition(
          RMAppAttemptState.LAUNCHED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.EXPIRE,
          new FinalSavingTransition(EXPIRED_TRANSITION,
            RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new FinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))

       // Transitions from RUNNING State
      .addTransition(RMAppAttemptState.RUNNING,
          EnumSet.of(RMAppAttemptState.FINAL_SAVING, RMAppAttemptState.FINISHED),
          RMAppAttemptEventType.UNREGISTERED, new AMUnregisteredTransition())
      .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
          RMAppAttemptEventType.STATUS_UPDATE, new StatusUpdateTransition())
      .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
          RMAppAttemptEventType.CONTAINER_ALLOCATED)
      .addTransition(
                RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
                RMAppAttemptEventType.CONTAINER_ACQUIRED,
                new ContainerAcquiredTransition())
      .addTransition(
          RMAppAttemptState.RUNNING,
          EnumSet.of(RMAppAttemptState.RUNNING, RMAppAttemptState.FINAL_SAVING),
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new ContainerFinishedTransition())
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
            // should be fixed to reject container allocate request at Final
            // Saving in scheduler
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
              RMAppAttemptEventType.CONTAINER_ACQUIRED,
              RMAppAttemptEventType.ATTEMPT_NEW_SAVED,
              RMAppAttemptEventType.KILL))

      // Transitions from FAILED State
      // For work-preserving AM restart, failed attempt are still capturing
      // CONTAINER_FINISHED event and record the finished containers for the
      // use by the next new attempt.
      .addTransition(RMAppAttemptState.FAILED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new ContainerFinishedAtFailedTransition())
      .addTransition(
          RMAppAttemptState.FAILED,
          RMAppAttemptState.FAILED,
          EnumSet.of(
              RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.KILL,
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
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.STATUS_UPDATE,
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
            // ignore Kill as we have already saved the final Finished state in
            // state store.
              RMAppAttemptEventType.KILL))

      // Transitions from FINISHED State
      .addTransition(
          RMAppAttemptState.FINISHED,
          RMAppAttemptState.FINISHED,
          EnumSet.of(
              RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
              RMAppAttemptEventType.CONTAINER_FINISHED,
              RMAppAttemptEventType.KILL))

      // Transitions from KILLED State
      .addTransition(
          RMAppAttemptState.KILLED,
          RMAppAttemptState.KILLED,
          EnumSet.of(RMAppAttemptEventType.ATTEMPT_ADDED,
              RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.LAUNCHED,
              RMAppAttemptEventType.LAUNCH_FAILED,
              RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.REGISTERED,
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
              RMAppAttemptEventType.CONTAINER_FINISHED,
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.KILL,
              RMAppAttemptEventType.STATUS_UPDATE))
    .installTopology();

  public RMAppAttemptImpl(ApplicationAttemptId appAttemptId,
      RMContext rmContext, YarnScheduler scheduler,
      ApplicationMasterService masterService,
      ApplicationSubmissionContext submissionContext,
      Configuration conf, boolean isLastAttempt) {
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

    this.proxiedTrackingUrl = generateProxyUriWithScheme(null);
    this.isLastAttempt = isLastAttempt;
    this.stateMachine = stateMachineFactory.make(this);
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
  
  private String generateProxyUriWithScheme(
      final String trackingUriWithoutScheme) {
    this.readLock.lock();
    try {
      final String scheme = WebAppUtils.getHttpSchemePrefix(conf);
      URI trackingUri = StringUtils.isEmpty(trackingUriWithoutScheme) ? null :
        ProxyUriUtils.getUriFromAMUrl(scheme, trackingUriWithoutScheme);
      String proxy = WebAppUtils.getProxyHostAndPort(conf);
      URI proxyUri = ProxyUriUtils.getUriFromAMUrl(scheme, proxy);
      URI result = ProxyUriUtils.getProxyUri(trackingUri, proxyUri,
          applicationAttemptId.getApplicationId());
      return result.toASCIIString();
    } catch (URISyntaxException e) {
      LOG.warn("Could not proxify "+trackingUriWithoutScheme,e);
      return trackingUriWithoutScheme;
    } finally {
      this.readLock.unlock();
    }
  }

  private void setTrackingUrlToRMAppPage() {
    originalTrackingUrl = pjoin(
        WebAppUtils.getResolvedRMWebAppURLWithoutScheme(conf),
        "cluster", "app", getAppAttemptId().getApplicationId());
    proxiedTrackingUrl = originalTrackingUrl;
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
    return this.amrmToken;
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
      return this.diagnostics.toString();
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

  @Override
  public List<ContainerStatus> getJustFinishedContainers() {
    this.readLock.lock();
    try {
      return this.justFinishedContainers;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ContainerStatus> pullJustFinishedContainers() {
    this.writeLock.lock();

    try {
      List<ContainerStatus> returnList = new ArrayList<ContainerStatus>(
          this.justFinishedContainers.size());
      returnList.addAll(this.justFinishedContainers);
      this.justFinishedContainers.clear();
      return returnList;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public Set<NodeId> getRanNodes() {
    return ranNodes;
  }

  @Override
  public Container getMasterContainer() {
    this.readLock.lock();

    try {
      return this.masterContainer;
    } finally {
      this.readLock.unlock();
    }
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
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        /* TODO fail the application on the failed transition */
      }

      if (oldState != getAppAttemptState()) {
        LOG.info(appAttemptID + " State change from " + oldState + " to "
            + getAppAttemptState());
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
        Resource none = Resource.newInstance(0, 0);
        report = ApplicationResourceUsageReport.newInstance(0, 0, none, none,
            none);
      }
      return report;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public void recover(RMState state) throws Exception {
    ApplicationState appState =
        state.getApplicationState().get(getAppAttemptId().getApplicationId());
    ApplicationAttemptState attemptState =
        appState.getAttempt(getAppAttemptId());
    assert attemptState != null;
    LOG.info("Recovering attempt: " + getAppAttemptId() + " with final state: "
        + attemptState.getState());
    diagnostics.append("Attempt recovered after RM restart");
    diagnostics.append(attemptState.getDiagnostics());
    setMasterContainer(attemptState.getMasterContainer());
    recoverAppAttemptCredentials(attemptState.getAppAttemptCredentials());
    this.recoveredFinalState = attemptState.getState();
    this.originalTrackingUrl = attemptState.getFinalTrackingUrl();
    this.proxiedTrackingUrl = generateProxyUriWithScheme(originalTrackingUrl);
    this.finalStatus = attemptState.getFinalApplicationStatus();
    this.startTime = attemptState.getStartTime();
  }

  public void transferStateFromPreviousAttempt(RMAppAttempt attempt) {
    this.justFinishedContainers = attempt.getJustFinishedContainers();
    this.ranNodes = attempt.getRanNodes();
  }

  private void recoverAppAttemptCredentials(Credentials appAttemptTokens)
      throws IOException {
    if (appAttemptTokens == null) {
      return;
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      byte[] clientTokenMasterKeyBytes = appAttemptTokens.getSecretKey(
          RMStateStore.AM_CLIENT_TOKEN_MASTER_KEY_NAME);
      clientTokenMasterKey = rmContext.getClientToAMTokenSecretManager()
          .registerMasterKey(applicationAttemptId, clientTokenMasterKeyBytes);
    }

    // Only one AMRMToken is stored per-attempt, so this should be fine. Can't
    // use TokenSelector as service may change - think fail-over.
    this.amrmToken =
        (Token<AMRMTokenIdentifier>) appAttemptTokens
          .getToken(RMStateStore.AM_RM_TOKEN_SERVICE);
    rmContext.getAMRMTokenSecretManager().addPersistedPassword(this.amrmToken);
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

      // create AMRMToken
      AMRMTokenIdentifier id =
          new AMRMTokenIdentifier(appAttempt.applicationAttemptId);
      appAttempt.amrmToken =
          new Token<AMRMTokenIdentifier>(id,
            appAttempt.rmContext.getAMRMTokenSecretManager());

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

  private static final class ScheduleTransition
      implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {
    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      if (!appAttempt.submissionContext.getUnmanagedAM()) {
        // Request a container for the AM.
        ResourceRequest request =
            BuilderUtils.newResourceRequest(
                AM_CONTAINER_PRIORITY, ResourceRequest.ANY, appAttempt
                    .getSubmissionContext().getResource(), 1);

        // SchedulerUtils.validateResourceRequests is not necessary because
        // AM resource has been checked when submission
        Allocation amContainerAllocation = appAttempt.scheduler.allocate(
            appAttempt.applicationAttemptId,
            Collections.singletonList(request), EMPTY_CONTAINER_RELEASE_LIST, null, null);
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
            null);
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
        appAttempt.eventHandler.handle(new RMAppAttemptContainerAllocatedEvent(
          appAttempt.applicationAttemptId));
      }
    }.start();
  }

  private static final class AttemptStoredTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
                                                    RMAppAttemptEvent event) {
      appAttempt.checkAttemptStoreError(event);
      appAttempt.launchAttempt();
    }
  }

  private static class AttemptRecoveredTransition
      implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {
    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      /*
       * If last attempt recovered final state is null .. it means attempt was
       * started but AM container may or may not have started / finished.
       * Therefore we should wait for it to finish.
       */
      if (appAttempt.recoveredFinalState != null) {
        appAttempt.progress = 1.0f;
        RMApp rmApp =appAttempt.rmContext.getRMApps().get(
            appAttempt.getAppAttemptId().getApplicationId());
        // We will replay the final attempt only if last attempt is in final
        // state but application is not in final state.
        if (rmApp.getCurrentAppAttempt() == appAttempt
            && !RMAppImpl.isAppInFinalState(rmApp)) {
          (new BaseFinalTransition(appAttempt.recoveredFinalState)).transition(
              appAttempt, event);
        }
        return appAttempt.recoveredFinalState;
      } else {
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
        (new AMLaunchedTransition()).transition(appAttempt, event);
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
    String diags = null;
    String finalTrackingUrl = null;
    FinalApplicationStatus finalStatus = null;

    switch (event.getType()) {
    case LAUNCH_FAILED:
      RMAppAttemptLaunchFailedEvent launchFaileEvent =
          (RMAppAttemptLaunchFailedEvent) event;
      diags = launchFaileEvent.getMessage();
      break;
    case REGISTERED:
      diags = getUnexpectedAMRegisteredDiagnostics();
      break;
    case UNREGISTERED:
      RMAppAttemptUnregistrationEvent unregisterEvent =
          (RMAppAttemptUnregistrationEvent) event;
      diags = unregisterEvent.getDiagnostics();
      finalTrackingUrl = sanitizeTrackingUrl(unregisterEvent.getFinalTrackingUrl());
      finalStatus = unregisterEvent.getFinalApplicationStatus();
      break;
    case CONTAINER_FINISHED:
      RMAppAttemptContainerFinishedEvent finishEvent =
          (RMAppAttemptContainerFinishedEvent) event;
      diags = getAMContainerCrashedDiagnostics(finishEvent);
      break;
    case KILL:
      break;
    case EXPIRE:
      diags = getAMExpiredDiagnostics(event);
      break;
    default:
      break;
    }

    RMStateStore rmStore = rmContext.getStateStore();
    ApplicationAttemptState attemptState =
        new ApplicationAttemptState(applicationAttemptId, getMasterContainer(),
          rmStore.getCredentialsFromAppAttempt(this), startTime,
          stateToBeStored, finalTrackingUrl, diags, finalStatus);
    LOG.info("Updating application attempt " + applicationAttemptId
        + " with final state: " + targetedFinalState);
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
      RMAppAttemptUpdateSavedEvent storeEvent = (RMAppAttemptUpdateSavedEvent) event;
      if (storeEvent.getUpdatedException() != null) {
        LOG.error("Failed to update the final state of application attempt: "
            + storeEvent.getApplicationAttemptId(),
          storeEvent.getUpdatedException());
        ExitUtil.terminate(1, storeEvent.getUpdatedException());
      }

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
          appEvent = new RMAppFinishedAttemptEvent(applicationId,
              appAttempt.getDiagnostics());
        }
        break;
        case KILLED:
        {
          // don't leave the tracking URL pointing to a non-existent AM
          appAttempt.setTrackingUrlToRMAppPage();
          appAttempt.invalidateAMHostAndPort();
          appEvent =
              new RMAppFailedAttemptEvent(applicationId,
                  RMAppEventType.ATTEMPT_KILLED,
                  "Application killed by user.", false);
        }
        break;
        case FAILED:
        {
          // don't leave the tracking URL pointing to a non-existent AM
          appAttempt.setTrackingUrlToRMAppPage();
          appAttempt.invalidateAMHostAndPort();
          if (appAttempt.submissionContext
            .getKeepContainersAcrossApplicationAttempts()
              && !appAttempt.isLastAttempt
              && !appAttempt.submissionContext.getUnmanagedAM()) {
            keepContainersAcrossAppAttempts = true;
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
    }
  }

  private static class AMLaunchedTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
                            RMAppAttemptEvent event) {
      // Register with AMLivelinessMonitor
      appAttempt.attemptLaunched();

      // register the ClientTokenMasterKey after it is saved in the store,
      // otherwise client may hold an invalid ClientToken after RM restarts.
      appAttempt.rmContext.getClientToAMTokenSecretManager()
      .registerApplication(appAttempt.getAppAttemptId(),
        appAttempt.getClientTokenMasterKey());
    }
  }
  
  private static final class UnmanagedAMAttemptSavedTransition 
                                                extends AMLaunchedTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
                            RMAppAttemptEvent event) {
      appAttempt.checkAttemptStoreError(event);
      // TODO Today unmanaged AM client is waiting for app state to be Accepted to
      // launch the AM. This is broken since we changed to start the attempt
      // after the application is Accepted. We may need to introduce an attempt
      // report that client can rely on to query the attempt state and choose to
      // launch the unmanaged AM.
      super.transition(appAttempt, event);
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
      RMAppAttemptLaunchFailedEvent launchFaileEvent
        = (RMAppAttemptLaunchFailedEvent) event;
      appAttempt.diagnostics.append(launchFaileEvent.getMessage());

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

      RMAppAttemptRegistrationEvent registrationEvent
          = (RMAppAttemptRegistrationEvent) event;
      appAttempt.host = registrationEvent.getHost();
      appAttempt.rpcPort = registrationEvent.getRpcport();
      appAttempt.originalTrackingUrl =
          sanitizeTrackingUrl(registrationEvent.getTrackingurl());
      appAttempt.proxiedTrackingUrl = 
        appAttempt.generateProxyUriWithScheme(appAttempt.originalTrackingUrl);

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
    }
  }

  private static final class AMContainerCrashedTransition extends
      BaseFinalTransition {

    public AMContainerCrashedTransition() {
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

      // Setup diagnostic message
      appAttempt.diagnostics
        .append(getAMContainerCrashedDiagnostics(finishEvent));
      // Tell the app, scheduler
      super.transition(appAttempt, finishEvent);
    }
  }

  private static String getAMContainerCrashedDiagnostics(
      RMAppAttemptContainerFinishedEvent finishEvent) {
    ContainerStatus status = finishEvent.getContainerStatus();
    String diagnostics =
        "AM Container for " + finishEvent.getApplicationAttemptId()
            + " exited with " + " exitCode: " + status.getExitStatus()
            + " due to: " + status.getDiagnostics() + "."
            + "Failing this attempt.";
    return diagnostics;
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

  private static final class AMUnregisteredTransition implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {

    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      // Tell the app
      if (appAttempt.getSubmissionContext().getUnmanagedAM()) {
        // Unmanaged AMs have no container to wait for, so they skip
        // the FINISHING state and go straight to FINISHED.
        appAttempt.updateInfoOnAMUnregister(event);
        new FinalTransition(RMAppAttemptState.FINISHED).transition(
            appAttempt, event);
        return RMAppAttemptState.FINISHED;
      }
      // Saving the attempt final state
      appAttempt.rememberTargetTransitionsAndStoreState(event,
        new FinalStateSavedAfterAMUnregisterTransition(),
        RMAppAttemptState.FINISHING, RMAppAttemptState.FINISHED);
      ApplicationId applicationId =
          appAttempt.getAppAttemptId().getApplicationId();

      // Tell the app immediately that AM is unregistering so that app itself
      // can save its state as soon as possible. Whether we do it like this, or
      // we wait till AppAttempt is saved, it doesn't make any difference on the
      // app side w.r.t failure conditions. The only event going out of
      // AppAttempt to App after this point of time is AM/AppAttempt Finished.
      appAttempt.eventHandler.handle(new RMAppEvent(applicationId,
        RMAppEventType.ATTEMPT_UNREGISTERED));
      return RMAppAttemptState.FINAL_SAVING;
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
    diagnostics.append(unregisterEvent.getDiagnostics());
    originalTrackingUrl = sanitizeTrackingUrl(unregisterEvent.getFinalTrackingUrl());
    proxiedTrackingUrl = generateProxyUriWithScheme(originalTrackingUrl);
    finalStatus = unregisterEvent.getFinalApplicationStatus();
  }

  private static final class ContainerAcquiredTransition extends
      BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      RMAppAttemptContainerAcquiredEvent acquiredEvent
        = (RMAppAttemptContainerAcquiredEvent) event;
      appAttempt.ranNodes.add(acquiredEvent.getContainer().getNodeId());
    }
  }

  private static final class ContainerFinishedTransition
      implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {

    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptContainerFinishedEvent containerFinishedEvent
        = (RMAppAttemptContainerFinishedEvent) event;
      ContainerStatus containerStatus =
          containerFinishedEvent.getContainerStatus();

      // Is this container the AmContainer? If the finished container is same as
      // the AMContainer, AppAttempt fails
      if (appAttempt.masterContainer != null
          && appAttempt.masterContainer.getId().equals(
            containerStatus.getContainerId())) {
        // Remember the follow up transition and save the final attempt state.
        appAttempt.rememberTargetTransitionsAndStoreState(event,
          new ContainerFinishedFinalStateSavedTransition(),
          RMAppAttemptState.FAILED, RMAppAttemptState.FAILED);
        return RMAppAttemptState.FINAL_SAVING;
      }

      // Normal container.Put it in completedcontainers list
      appAttempt.justFinishedContainers.add(containerStatus);
      return RMAppAttemptState.RUNNING;
    }
  }

  private static final class ContainerFinishedAtFailedTransition
      extends BaseTransition {
    @Override
    public void
        transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent event) {
      RMAppAttemptContainerFinishedEvent containerFinishedEvent =
          (RMAppAttemptContainerFinishedEvent) event;
      ContainerStatus containerStatus =
          containerFinishedEvent.getContainerStatus();
      // Normal container. Add it in completed containers list
      appAttempt.justFinishedContainers.add(containerStatus);
    }
  }

  private static class ContainerFinishedFinalStateSavedTransition extends
      BaseTransition {
    @Override
    public void
        transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent event) {
      RMAppAttemptContainerFinishedEvent containerFinishedEvent =
          (RMAppAttemptContainerFinishedEvent) event;
      // container associated with AM. must not be unmanaged
      assert appAttempt.submissionContext.getUnmanagedAM() == false;
      // Setup diagnostic message
      appAttempt.diagnostics
        .append(getAMContainerCrashedDiagnostics(containerFinishedEvent));
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
        return RMAppAttemptState.FINISHED;
      }
      // Normal container.
      appAttempt.justFinishedContainers.add(containerStatus);
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
      // Normal container.
      appAttempt.justFinishedContainers.add(containerStatus);
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
    // Send event to launch the AM Container
    eventHandler.handle(new AMLauncherEvent(AMLauncherEventType.LAUNCH, this));
  }
  
  private void attemptLaunched() {
    // Register with AMLivelinessMonitor
    rmContext.getAMLivelinessMonitor().register(getAppAttemptId());
  }
  
  private void checkAttemptStoreError(RMAppAttemptEvent event) {
    RMAppAttemptNewSavedEvent storeEvent = (RMAppAttemptNewSavedEvent) event;
    if(storeEvent.getStoredException() != null)
    {
      // This needs to be handled for HA and give up master status if we got
      // fenced
      LOG.error("Failed to store attempt: " + getAppAttemptId(),
                storeEvent.getStoredException());
      ExitUtil.terminate(1, storeEvent.getStoredException());
    }
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
      attemptReport = ApplicationAttemptReport.newInstance(this
          .getAppAttemptId(), this.getHost(), this.getRpcPort(), this
          .getTrackingUrl(), this.getDiagnostics(), YarnApplicationAttemptState
          .valueOf(this.getState().toString()), amId);
    } finally {
      this.readLock.unlock();
    }
    return attemptReport;
  }
}
