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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNodeUpdateEvent.RMAppNodeUpdateType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppStartAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.resource.Resources;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class RMAppImpl implements RMApp, Recoverable {

  private static final Log LOG = LogFactory.getLog(RMAppImpl.class);
  private static final String UNAVAILABLE = "N/A";

  // Immutable fields
  private final ApplicationId applicationId;
  private final RMContext rmContext;
  private final Configuration conf;
  private final String user;
  private final String name;
  private final ApplicationSubmissionContext submissionContext;
  private final Dispatcher dispatcher;
  private final YarnScheduler scheduler;
  private final ApplicationMasterService masterService;
  private final StringBuilder diagnostics = new StringBuilder();
  private final int maxAppAttempts;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final Map<ApplicationAttemptId, RMAppAttempt> attempts
      = new LinkedHashMap<ApplicationAttemptId, RMAppAttempt>();
  private final long submitTime;
  private final Set<RMNode> updatedNodes = new HashSet<RMNode>();
  private final String applicationType;
  private final Set<String> applicationTags;

  // Mutable fields
  private long startTime;
  private long finishTime = 0;
  private long storedFinishTime = 0;
  private RMAppAttempt currentAttempt;
  private String queue;
  private EventHandler handler;
  private static final AppFinishedTransition FINISHED_TRANSITION =
      new AppFinishedTransition();

  // These states stored are only valid when app is at killing or final_saving.
  private RMAppState stateBeforeKilling;
  private RMAppState stateBeforeFinalSaving;
  private RMAppEvent eventCausingFinalSaving;
  private RMAppState targetedFinalState;
  private RMAppState recoveredFinalState;

  Object transitionTodo;

  private static final StateMachineFactory<RMAppImpl,
                                           RMAppState,
                                           RMAppEventType,
                                           RMAppEvent> stateMachineFactory
                               = new StateMachineFactory<RMAppImpl,
                                           RMAppState,
                                           RMAppEventType,
                                           RMAppEvent>(RMAppState.NEW)


     // Transitions from NEW state
    .addTransition(RMAppState.NEW, RMAppState.NEW,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.NEW, RMAppState.NEW_SAVING,
        RMAppEventType.START, new RMAppNewlySavingTransition())
    .addTransition(RMAppState.NEW, EnumSet.of(RMAppState.SUBMITTED,
            RMAppState.ACCEPTED, RMAppState.FINISHED, RMAppState.FAILED,
            RMAppState.KILLED, RMAppState.FINAL_SAVING),
        RMAppEventType.RECOVER, new RMAppRecoveredTransition())
    .addTransition(RMAppState.NEW, RMAppState.KILLED, RMAppEventType.KILL,
        new AppKilledTransition())
    .addTransition(RMAppState.NEW, RMAppState.FAILED,
        RMAppEventType.APP_REJECTED, new AppRejectedTransition())

    // Transitions from NEW_SAVING state
    .addTransition(RMAppState.NEW_SAVING, RMAppState.NEW_SAVING,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.NEW_SAVING, RMAppState.SUBMITTED,
        RMAppEventType.APP_NEW_SAVED, new AddApplicationToSchedulerTransition())
    .addTransition(RMAppState.NEW_SAVING, RMAppState.FINAL_SAVING,
        RMAppEventType.KILL,
        new FinalSavingTransition(
          new AppKilledTransition(), RMAppState.KILLED))
    .addTransition(RMAppState.NEW_SAVING, RMAppState.FINAL_SAVING,
        RMAppEventType.APP_REJECTED,
          new FinalSavingTransition(new AppRejectedTransition(),
            RMAppState.FAILED))

     // Transitions from SUBMITTED state
    .addTransition(RMAppState.SUBMITTED, RMAppState.SUBMITTED,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.SUBMITTED, RMAppState.SUBMITTED,
        RMAppEventType.MOVE, new RMAppMoveTransition())
    .addTransition(RMAppState.SUBMITTED, RMAppState.FINAL_SAVING,
        RMAppEventType.APP_REJECTED,
        new FinalSavingTransition(
          new AppRejectedTransition(), RMAppState.FAILED))
    .addTransition(RMAppState.SUBMITTED, RMAppState.ACCEPTED,
        RMAppEventType.APP_ACCEPTED, new StartAppAttemptTransition())
    .addTransition(RMAppState.SUBMITTED, RMAppState.FINAL_SAVING,
        RMAppEventType.KILL,
        new FinalSavingTransition(
          new AppKilledTransition(), RMAppState.KILLED))


     // Transitions from ACCEPTED state
    .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
        RMAppEventType.MOVE, new RMAppMoveTransition())
    .addTransition(RMAppState.ACCEPTED, RMAppState.RUNNING,
        RMAppEventType.ATTEMPT_REGISTERED)
    .addTransition(RMAppState.ACCEPTED,
        EnumSet.of(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING),
        // ACCEPTED state is possible to receive ATTEMPT_FAILED/ATTEMPT_FINISHED
        // event because RMAppRecoveredTransition is returning ACCEPTED state
        // directly and waiting for the previous AM to exit.
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.ACCEPTED))
    .addTransition(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_FINISHED,
        new FinalSavingTransition(FINISHED_TRANSITION, RMAppState.FINISHED))
    .addTransition(RMAppState.ACCEPTED, RMAppState.KILLING,
        RMAppEventType.KILL, new KillAttemptTransition())
    // ACCECPTED state can once again receive APP_ACCEPTED event, because on
    // recovery the app returns ACCEPTED state and the app once again go
    // through the scheduler and triggers one more APP_ACCEPTED event at
    // ACCEPTED state.
    .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
        RMAppEventType.APP_ACCEPTED)

     // Transitions from RUNNING state
    .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
        RMAppEventType.MOVE, new RMAppMoveTransition())
    .addTransition(RMAppState.RUNNING, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_UNREGISTERED,
        new FinalSavingTransition(
          new AttemptUnregisteredTransition(),
          RMAppState.FINISHING, RMAppState.FINISHED))
    .addTransition(RMAppState.RUNNING, RMAppState.FINISHED,
      // UnManagedAM directly jumps to finished
        RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
    .addTransition(RMAppState.RUNNING,
        EnumSet.of(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING),
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.ACCEPTED))
    .addTransition(RMAppState.RUNNING, RMAppState.KILLING,
        RMAppEventType.KILL, new KillAttemptTransition())

     // Transitions from FINAL_SAVING state
    .addTransition(RMAppState.FINAL_SAVING,
      EnumSet.of(RMAppState.FINISHING, RMAppState.FAILED,
        RMAppState.KILLED, RMAppState.FINISHED), RMAppEventType.APP_UPDATE_SAVED,
        new FinalStateSavedTransition())
    .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_FINISHED,
        new AttemptFinishedAtFinalSavingTransition())
    // ignorable transitions
    .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING,
        EnumSet.of(RMAppEventType.NODE_UPDATE, RMAppEventType.KILL,
          RMAppEventType.APP_NEW_SAVED))

     // Transitions from FINISHING state
    .addTransition(RMAppState.FINISHING, RMAppState.FINISHED,
        RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
    // ignorable transitions
    .addTransition(RMAppState.FINISHING, RMAppState.FINISHING,
      EnumSet.of(RMAppEventType.NODE_UPDATE,
        // ignore Kill as we have already saved the final Finished state in
        // state store.
        RMAppEventType.KILL))

     // Transitions from KILLING state
    .addTransition(RMAppState.KILLING, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_KILLED,
        new FinalSavingTransition(
          new AppKilledTransition(), RMAppState.KILLED))
    .addTransition(RMAppState.KILLING, RMAppState.KILLING,
        EnumSet.of(
            RMAppEventType.NODE_UPDATE,
            RMAppEventType.ATTEMPT_REGISTERED,
            RMAppEventType.ATTEMPT_UNREGISTERED,
            RMAppEventType.ATTEMPT_FINISHED,
            RMAppEventType.ATTEMPT_FAILED,
            RMAppEventType.APP_UPDATE_SAVED,
            RMAppEventType.KILL))

     // Transitions from FINISHED state
     // ignorable transitions
    .addTransition(RMAppState.FINISHED, RMAppState.FINISHED,
        EnumSet.of(
            RMAppEventType.NODE_UPDATE,
            RMAppEventType.ATTEMPT_UNREGISTERED,
            RMAppEventType.ATTEMPT_FINISHED,
            RMAppEventType.KILL))

     // Transitions from FAILED state
     // ignorable transitions
    .addTransition(RMAppState.FAILED, RMAppState.FAILED,
        EnumSet.of(RMAppEventType.KILL, RMAppEventType.NODE_UPDATE))

     // Transitions from KILLED state
     // ignorable transitions
    .addTransition(
        RMAppState.KILLED,
        RMAppState.KILLED,
        EnumSet.of(RMAppEventType.APP_ACCEPTED,
            RMAppEventType.APP_REJECTED, RMAppEventType.KILL,
            RMAppEventType.ATTEMPT_FINISHED, RMAppEventType.ATTEMPT_FAILED,
            RMAppEventType.NODE_UPDATE))

     .installTopology();

  private final StateMachine<RMAppState, RMAppEventType, RMAppEvent>
                                                                 stateMachine;

  private static final ApplicationResourceUsageReport
    DUMMY_APPLICATION_RESOURCE_USAGE_REPORT =
      BuilderUtils.newApplicationResourceUsageReport(-1, -1,
          Resources.createResource(-1, -1), Resources.createResource(-1, -1),
          Resources.createResource(-1, -1));
  private static final int DUMMY_APPLICATION_ATTEMPT_NUMBER = -1;
  
  public RMAppImpl(ApplicationId applicationId, RMContext rmContext,
      Configuration config, String name, String user, String queue,
      ApplicationSubmissionContext submissionContext, YarnScheduler scheduler,
      ApplicationMasterService masterService, long submitTime,
      String applicationType, Set<String> applicationTags) {

    this.applicationId = applicationId;
    this.name = name;
    this.rmContext = rmContext;
    this.dispatcher = rmContext.getDispatcher();
    this.handler = dispatcher.getEventHandler();
    this.conf = config;
    this.user = user;
    this.queue = queue;
    this.submissionContext = submissionContext;
    this.scheduler = scheduler;
    this.masterService = masterService;
    this.submitTime = submitTime;
    this.startTime = System.currentTimeMillis();
    this.applicationType = applicationType;
    this.applicationTags = applicationTags;

    int globalMaxAppAttempts = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    int individualMaxAppAttempts = submissionContext.getMaxAppAttempts();
    if (individualMaxAppAttempts <= 0 ||
        individualMaxAppAttempts > globalMaxAppAttempts) {
      this.maxAppAttempts = globalMaxAppAttempts;
      LOG.warn("The specific max attempts: " + individualMaxAppAttempts
          + " for application: " + applicationId.getId()
          + " is invalid, because it is out of the range [1, "
          + globalMaxAppAttempts + "]. Use the global max attempts instead.");
    } else {
      this.maxAppAttempts = individualMaxAppAttempts;
    }

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.stateMachine = stateMachineFactory.make(this);

    rmContext.getRMApplicationHistoryWriter().applicationStarted(this);
  }

  @Override
  public ApplicationId getApplicationId() {
    return this.applicationId;
  }
  
  @Override
  public ApplicationSubmissionContext getApplicationSubmissionContext() {
    return this.submissionContext;
  }

  @Override
  public FinalApplicationStatus getFinalApplicationStatus() {
    this.readLock.lock();
    try {
      // finish state is obtained based on the state machine's current state 
      // as a fall-back in case the application has not been unregistered 
      // ( or if the app never unregistered itself )
      // when the report is requested
      if (currentAttempt != null 
          && currentAttempt.getFinalApplicationStatus() != null) {
        return currentAttempt.getFinalApplicationStatus();   
      }
      return 
          createFinalApplicationStatus(this.stateMachine.getCurrentState());
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public RMAppState getState() {
    this.readLock.lock();
    try {
        return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getUser() {
    return this.user;
  }

  @Override
  public float getProgress() {
    this.readLock.lock();

    try {
      if (this.currentAttempt != null) {
        return this.currentAttempt.getProgress();
      }
      return 0;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public RMAppAttempt getRMAppAttempt(ApplicationAttemptId appAttemptId) {
    this.readLock.lock();

    try {
      return this.attempts.get(appAttemptId);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getQueue() {
    return this.queue;
  }
  
  @Override
  public void setQueue(String queue) {
    this.queue = queue;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public RMAppAttempt getCurrentAppAttempt() {
    this.readLock.lock();

    try {
      return this.currentAttempt;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<ApplicationAttemptId, RMAppAttempt> getAppAttempts() {
    this.readLock.lock();

    try {
      return Collections.unmodifiableMap(this.attempts);
    } finally {
      this.readLock.unlock();
    }
  }

  private FinalApplicationStatus createFinalApplicationStatus(RMAppState state) {
    switch(state) {
    case NEW:
    case NEW_SAVING:
    case SUBMITTED:
    case ACCEPTED:
    case RUNNING:
    case FINAL_SAVING:
    case KILLING:
      return FinalApplicationStatus.UNDEFINED;    
    // finished without a proper final state is the same as failed  
    case FINISHING:
    case FINISHED:
    case FAILED:
      return FinalApplicationStatus.FAILED;
    case KILLED:
      return FinalApplicationStatus.KILLED;
    }
    throw new YarnRuntimeException("Unknown state passed!");
  }

  @Override
  public int pullRMNodeUpdates(Collection<RMNode> updatedNodes) {
    this.writeLock.lock();
    try {
      int updatedNodeCount = this.updatedNodes.size();
      updatedNodes.addAll(this.updatedNodes);
      this.updatedNodes.clear();
      return updatedNodeCount;
    } finally {
      this.writeLock.unlock();
    }
  }
  
  @Override
  public ApplicationReport createAndGetApplicationReport(String clientUserName,
      boolean allowAccess) {
    this.readLock.lock();

    try {
      ApplicationAttemptId currentApplicationAttemptId = null;
      org.apache.hadoop.yarn.api.records.Token clientToAMToken = null;
      String trackingUrl = UNAVAILABLE;
      String host = UNAVAILABLE;
      String origTrackingUrl = UNAVAILABLE;
      int rpcPort = -1;
      ApplicationResourceUsageReport appUsageReport =
          DUMMY_APPLICATION_RESOURCE_USAGE_REPORT;
      FinalApplicationStatus finishState = getFinalApplicationStatus();
      String diags = UNAVAILABLE;
      float progress = 0.0f;
      org.apache.hadoop.yarn.api.records.Token amrmToken = null;
      if (allowAccess) {
        if (this.currentAttempt != null) {
          currentApplicationAttemptId = this.currentAttempt.getAppAttemptId();
          trackingUrl = this.currentAttempt.getTrackingUrl();
          origTrackingUrl = this.currentAttempt.getOriginalTrackingUrl();
          if (UserGroupInformation.isSecurityEnabled()) {
            // get a token so the client can communicate with the app attempt
            // NOTE: token may be unavailable if the attempt is not running
            Token<ClientToAMTokenIdentifier> attemptClientToAMToken =
                this.currentAttempt.createClientToken(clientUserName);
            if (attemptClientToAMToken != null) {
              clientToAMToken = BuilderUtils.newClientToAMToken(
                  attemptClientToAMToken.getIdentifier(),
                  attemptClientToAMToken.getKind().toString(),
                  attemptClientToAMToken.getPassword(),
                  attemptClientToAMToken.getService().toString());
            }
          }
          host = this.currentAttempt.getHost();
          rpcPort = this.currentAttempt.getRpcPort();
          appUsageReport = currentAttempt.getApplicationResourceUsageReport();
          progress = currentAttempt.getProgress();
        }
        diags = this.diagnostics.toString();

        if (currentAttempt != null && 
            currentAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
          if (getApplicationSubmissionContext().getUnmanagedAM() &&
              clientUserName != null && getUser().equals(clientUserName)) {
            Token<AMRMTokenIdentifier> token = currentAttempt.getAMRMToken();
            if (token != null) {
              amrmToken = BuilderUtils.newAMRMToken(token.getIdentifier(),
                  token.getKind().toString(), token.getPassword(),
                  token.getService().toString());
            }
          }
        }
      }

      if (currentApplicationAttemptId == null) {
        currentApplicationAttemptId = 
            BuilderUtils.newApplicationAttemptId(this.applicationId, 
                DUMMY_APPLICATION_ATTEMPT_NUMBER);
      }

      return BuilderUtils.newApplicationReport(this.applicationId,
          currentApplicationAttemptId, this.user, this.queue,
          this.name, host, rpcPort, clientToAMToken,
          createApplicationState(), diags,
          trackingUrl, this.startTime, this.finishTime, finishState,
          appUsageReport, origTrackingUrl, progress, this.applicationType, 
          amrmToken, applicationTags);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public long getFinishTime() {
    this.readLock.lock();

    try {
      return this.finishTime;
    } finally {
      this.readLock.unlock();
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
  public long getSubmitTime() {
    return this.submitTime;
  }

  @Override
  public String getTrackingUrl() {
    this.readLock.lock();
    
    try {
      if (this.currentAttempt != null) {
        return this.currentAttempt.getTrackingUrl();
      }
      return null;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public StringBuilder getDiagnostics() {
    this.readLock.lock();

    try {
      return this.diagnostics;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public int getMaxAppAttempts() {
    return this.maxAppAttempts;
  }

  @Override
  public void handle(RMAppEvent event) {

    this.writeLock.lock();

    try {
      ApplicationId appID = event.getApplicationId();
      LOG.debug("Processing event for " + appID + " of type "
          + event.getType());
      final RMAppState oldState = getState();
      try {
        /* keep the master in sync with the state machine */
        this.stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        /* TODO fail the application on the failed transition */
      }

      if (oldState != getState()) {
        LOG.info(appID + " State change from " + oldState + " to "
            + getState());
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public void recover(RMState state) throws Exception{
    ApplicationState appState = state.getApplicationState().get(getApplicationId());
    this.recoveredFinalState = appState.getState();
    LOG.info("Recovering app: " + getApplicationId() + " with " + 
        + appState.getAttemptCount() + " attempts and final state = " + this.recoveredFinalState );
    this.diagnostics.append(appState.getDiagnostics());
    this.storedFinishTime = appState.getFinishTime();
    this.startTime = appState.getStartTime();

    for(int i=0; i<appState.getAttemptCount(); ++i) {
      // create attempt
      createNewAttempt();
      ((RMAppAttemptImpl)this.currentAttempt).recover(state);
    }
  }

  private void createNewAttempt() {
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(applicationId, attempts.size() + 1);
    RMAppAttempt attempt =
        new RMAppAttemptImpl(appAttemptId, rmContext, scheduler, masterService,
          submissionContext, conf, maxAppAttempts == attempts.size());
    attempts.put(appAttemptId, attempt);
    currentAttempt = attempt;
  }
  
  private void
      createAndStartNewAttempt(boolean transferStateFromPreviousAttempt) {
    createNewAttempt();
    handler.handle(new RMAppStartAttemptEvent(currentAttempt.getAppAttemptId(),
      transferStateFromPreviousAttempt));
  }

  private void processNodeUpdate(RMAppNodeUpdateType type, RMNode node) {
    NodeState nodeState = node.getState();
    updatedNodes.add(node);
    LOG.debug("Received node update event:" + type + " for node:" + node
        + " with state:" + nodeState);
  }

  private static class RMAppTransition implements
      SingleArcTransition<RMAppImpl, RMAppEvent> {
    public void transition(RMAppImpl app, RMAppEvent event) {
    };

  }

  private static final class RMAppNodeUpdateTransition extends RMAppTransition {
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppNodeUpdateEvent nodeUpdateEvent = (RMAppNodeUpdateEvent) event;
      app.processNodeUpdate(nodeUpdateEvent.getUpdateType(),
          nodeUpdateEvent.getNode());
    };
  }

  /**
   * Move an app to a new queue.
   * This transition must set the result on the Future in the RMAppMoveEvent,
   * either as an exception for failure or null for success, or the client will
   * be left waiting forever.
   */
  private static final class RMAppMoveTransition extends RMAppTransition {
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppMoveEvent moveEvent = (RMAppMoveEvent) event;
      try {
        app.queue = app.scheduler.moveApplication(app.applicationId,
            moveEvent.getTargetQueue());
      } catch (YarnException ex) {
        moveEvent.getResult().setException(ex);
        return;
      }
      
      // TODO: Write out change to state store (YARN-1558)
      // Also take care of RM failover
      moveEvent.getResult().set(null);
    }
  }

  private static final class RMAppRecoveredTransition implements
      MultipleArcTransition<RMAppImpl, RMAppEvent, RMAppState> {

    @Override
    public RMAppState transition(RMAppImpl app, RMAppEvent event) {

      for (RMAppAttempt attempt : app.getAppAttempts().values()) {
        // synchronously recover attempt to ensure any incoming external events
        // to be processed after the attempt processes the recover event.
        attempt.handle(
          new RMAppAttemptEvent(attempt.getAppAttemptId(),
            RMAppAttemptEventType.RECOVER));
      }

      // The app has completed.
      if (app.recoveredFinalState != null) {
        new FinalTransition(app.recoveredFinalState).transition(app, event);
        return app.recoveredFinalState;
      }

      // Last attempt is in final state, do not add to scheduler and just return
      // ACCEPTED waiting for last RMAppAttempt to send finished or failed event
      // back.
      if (app.currentAttempt != null
          && (app.currentAttempt.getState() == RMAppAttemptState.KILLED
              || app.currentAttempt.getState() == RMAppAttemptState.FINISHED
              || (app.currentAttempt.getState() == RMAppAttemptState.FAILED
                  && app.attempts.size() == app.maxAppAttempts))) {
        return RMAppState.ACCEPTED;
      }

      // Notify scheduler about the app on recovery
      new AddApplicationToSchedulerTransition().transition(app, event);

      // No existent attempts means the attempt associated with this app was not
      // started or started but not yet saved.
      if (app.attempts.isEmpty()) {
        return RMAppState.SUBMITTED;
      }

      // YARN-1507 is saving the application state after the application is
      // accepted. So after YARN-1507, an app is saved meaning it is accepted.
      // Thus we return ACCECPTED state on recovery.
      return RMAppState.ACCEPTED;
    }
  }

  private static final class AddApplicationToSchedulerTransition extends
      RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      if (event instanceof RMAppNewSavedEvent) {
        RMAppNewSavedEvent storeEvent = (RMAppNewSavedEvent) event;
        // For HA this exception needs to be handled by giving up
        // master status if we got fenced
        if (((RMAppNewSavedEvent) event).getStoredException() != null) {
          LOG.error(
            "Failed to store application: " + storeEvent.getApplicationId(),
            storeEvent.getStoredException());
          ExitUtil.terminate(1, storeEvent.getStoredException());
        }
      }
      app.handler.handle(new AppAddedSchedulerEvent(app.applicationId,
        app.submissionContext.getQueue(), app.user));
    }
  }

  private static final class StartAppAttemptTransition extends RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.createAndStartNewAttempt(false);
    };
  }

  private static final class FinalStateSavedTransition implements
      MultipleArcTransition<RMAppImpl, RMAppEvent, RMAppState> {

    @Override
    public RMAppState transition(RMAppImpl app, RMAppEvent event) {
      RMAppUpdateSavedEvent storeEvent = (RMAppUpdateSavedEvent) event;
      if (storeEvent.getUpdatedException() != null) {
        LOG.error("Failed to update the final state of application"
              + storeEvent.getApplicationId(), storeEvent.getUpdatedException());
        ExitUtil.terminate(1, storeEvent.getUpdatedException());
      }

      if (app.transitionTodo instanceof SingleArcTransition) {
        ((SingleArcTransition) app.transitionTodo).transition(app,
          app.eventCausingFinalSaving);
      } else if (app.transitionTodo instanceof MultipleArcTransition) {
        ((MultipleArcTransition) app.transitionTodo).transition(app,
          app.eventCausingFinalSaving);
      }
      return app.targetedFinalState;

    }
  }

  private static class AttemptFailedFinalStateSavedTransition extends
      RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      String msg = null;
      if (event instanceof RMAppFailedAttemptEvent) {
        msg = app.getAppAttemptFailedDiagnostics(event);
      }
      LOG.info(msg);
      app.diagnostics.append(msg);
      // Inform the node for app-finish
      new FinalTransition(RMAppState.FAILED).transition(app, event);
    }
  }

  private String getAppAttemptFailedDiagnostics(RMAppEvent event) {
    String msg = null;
    RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent) event;
    if (this.submissionContext.getUnmanagedAM()) {
      // RM does not manage the AM. Do not retry
      msg = "Unmanaged application " + this.getApplicationId()
              + " failed due to " + failedEvent.getDiagnostics()
              + ". Failing the application.";
    } else if (this.attempts.size() >= this.maxAppAttempts) {
      msg = "Application " + this.getApplicationId() + " failed "
              + this.maxAppAttempts + " times due to "
              + failedEvent.getDiagnostics() + ". Failing the application.";
    }
    return msg;
  }

  private static final class RMAppNewlySavingTransition extends RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {

      // If recovery is enabled then store the application information in a
      // non-blocking call so make sure that RM has stored the information
      // needed to restart the AM after RM restart without further client
      // communication
      LOG.info("Storing application with id " + app.applicationId);
      app.rmContext.getStateStore().storeNewApplication(app);
    }
  }

  private void rememberTargetTransitions(RMAppEvent event,
      Object transitionToDo, RMAppState targetFinalState) {
    transitionTodo = transitionToDo;
    targetedFinalState = targetFinalState;
    eventCausingFinalSaving = event;
  }

  private void rememberTargetTransitionsAndStoreState(RMAppEvent event,
      Object transitionToDo, RMAppState targetFinalState,
      RMAppState stateToBeStored) {
    rememberTargetTransitions(event, transitionToDo, targetFinalState);
    this.stateBeforeFinalSaving = getState();
    this.storedFinishTime = System.currentTimeMillis();

    LOG.info("Updating application " + this.applicationId
        + " with final state: " + this.targetedFinalState);
    // we lost attempt_finished diagnostics in app, because attempt_finished
    // diagnostics is sent after app final state is saved. Later on, we will
    // create GetApplicationAttemptReport specifically for getting per attempt
    // info.
    String diags = null;
    switch (event.getType()) {
    case APP_REJECTED:
      RMAppRejectedEvent rejectedEvent = (RMAppRejectedEvent) event;
      diags = rejectedEvent.getMessage();
      break;
    case ATTEMPT_FINISHED:
      RMAppFinishedAttemptEvent finishedEvent =
          (RMAppFinishedAttemptEvent) event;
      diags = finishedEvent.getDiagnostics();
      break;
    case ATTEMPT_FAILED:
      RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent) event;
      diags = getAppAttemptFailedDiagnostics(failedEvent);
      break;
    case ATTEMPT_KILLED:
      diags = getAppKilledDiagnostics();
      break;
    default:
      break;
    }
    ApplicationState appState =
        new ApplicationState(this.submitTime, this.startTime,
          this.submissionContext, this.user, stateToBeStored, diags,
          this.storedFinishTime);
    this.rmContext.getStateStore().updateApplicationState(appState);
  }

  private static final class FinalSavingTransition extends RMAppTransition {
    Object transitionToDo;
    RMAppState targetedFinalState;
    RMAppState stateToBeStored;

    public FinalSavingTransition(Object transitionToDo,
        RMAppState targetedFinalState) {
      this(transitionToDo, targetedFinalState, targetedFinalState);
    }

    public FinalSavingTransition(Object transitionToDo,
        RMAppState targetedFinalState, RMAppState stateToBeStored) {
      this.transitionToDo = transitionToDo;
      this.targetedFinalState = targetedFinalState;
      this.stateToBeStored = stateToBeStored;
    }

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.rememberTargetTransitionsAndStoreState(event, transitionToDo,
        targetedFinalState, stateToBeStored);
    }
  }

  private static class AttemptUnregisteredTransition extends RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.finishTime = app.storedFinishTime;
    }
  }

  private static class AppFinishedTransition extends FinalTransition {
    public AppFinishedTransition() {
      super(RMAppState.FINISHED);
    }

    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppFinishedAttemptEvent finishedEvent =
          (RMAppFinishedAttemptEvent)event;
      app.diagnostics.append(finishedEvent.getDiagnostics());
      super.transition(app, event);
    };
  }

  private static class AttemptFinishedAtFinalSavingTransition extends
      RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      if (app.targetedFinalState.equals(RMAppState.FAILED)
          || app.targetedFinalState.equals(RMAppState.KILLED)) {
        // Ignore Attempt_Finished event if we were supposed to reach FAILED
        // FINISHED state
        return;
      }

      // pass in the earlier attempt_unregistered event, as it is needed in
      // AppFinishedFinalStateSavedTransition later on
      app.rememberTargetTransitions(event,
        new AppFinishedFinalStateSavedTransition(app.eventCausingFinalSaving),
        RMAppState.FINISHED);
    };
  }

  private static class AppFinishedFinalStateSavedTransition extends
      RMAppTransition {
    RMAppEvent attemptUnregistered;

    public AppFinishedFinalStateSavedTransition(RMAppEvent attemptUnregistered) {
      this.attemptUnregistered = attemptUnregistered;
    }
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      new AttemptUnregisteredTransition().transition(app, attemptUnregistered);
      FINISHED_TRANSITION.transition(app, event);
    };
  }


  private static class AppKilledTransition extends FinalTransition {
    public AppKilledTransition() {
      super(RMAppState.KILLED);
    }

    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.diagnostics.append(getAppKilledDiagnostics());
      super.transition(app, event);
    };
  }

  private static String getAppKilledDiagnostics() {
    return "Application killed by user.";
  }

  private static class KillAttemptTransition extends RMAppTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.stateBeforeKilling = app.getState();
      app.handler.handle(new RMAppAttemptEvent(app.currentAttempt
        .getAppAttemptId(), RMAppAttemptEventType.KILL));
    }
  }

  private static final class AppRejectedTransition extends
      FinalTransition{
    public AppRejectedTransition() {
      super(RMAppState.FAILED);
    }

    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppRejectedEvent rejectedEvent = (RMAppRejectedEvent)event;
      app.diagnostics.append(rejectedEvent.getMessage());
      super.transition(app, event);
    };
  }

  private static class FinalTransition extends RMAppTransition {

    private final RMAppState finalState;

    public FinalTransition(RMAppState finalState) {
      this.finalState = finalState;
    }

    private Set<NodeId> getNodesOnWhichAttemptRan(RMAppImpl app) {
      Set<NodeId> nodes = new HashSet<NodeId>();
      for (RMAppAttempt attempt : app.attempts.values()) {
        nodes.addAll(attempt.getRanNodes());
      }
      return nodes;
    }

    public void transition(RMAppImpl app, RMAppEvent event) {
      Set<NodeId> nodes = getNodesOnWhichAttemptRan(app);
      for (NodeId nodeId : nodes) {
        app.handler.handle(
            new RMNodeCleanAppEvent(nodeId, app.applicationId));
      }
      app.finishTime = app.storedFinishTime;
      if (app.finishTime == 0 ) {
        app.finishTime = System.currentTimeMillis();
      }
      app.handler.handle(new AppRemovedSchedulerEvent(app.applicationId,
        finalState));
      app.handler.handle(
          new RMAppManagerEvent(app.applicationId,
          RMAppManagerEventType.APP_COMPLETED));

      app.rmContext.getRMApplicationHistoryWriter()
          .applicationFinished(app, finalState);
    };
  }

  private static final class AttemptFailedTransition implements
      MultipleArcTransition<RMAppImpl, RMAppEvent, RMAppState> {

    private final RMAppState initialState;

    public AttemptFailedTransition(RMAppState initialState) {
      this.initialState = initialState;
    }

    @Override
    public RMAppState transition(RMAppImpl app, RMAppEvent event) {
      if (!app.submissionContext.getUnmanagedAM()
          && app.attempts.size() < app.maxAppAttempts) {
        boolean transferStateFromPreviousAttempt = false;
        RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent) event;
        transferStateFromPreviousAttempt =
            failedEvent.getTransferStateFromPreviousAttempt();

        RMAppAttempt oldAttempt = app.currentAttempt;
        app.createAndStartNewAttempt(transferStateFromPreviousAttempt);
        // Transfer the state from the previous attempt to the current attempt.
        // Note that the previous failed attempt may still be collecting the
        // container events from the scheduler and update its data structures
        // before the new attempt is created.
        if (transferStateFromPreviousAttempt) {
          ((RMAppAttemptImpl) app.currentAttempt)
            .transferStateFromPreviousAttempt(oldAttempt);
        }
        return initialState;
      } else {
        app.rememberTargetTransitionsAndStoreState(event,
          new AttemptFailedFinalStateSavedTransition(), RMAppState.FAILED,
          RMAppState.FAILED);
        return RMAppState.FINAL_SAVING;
      }
    }
  }

  @Override
  public String getApplicationType() {
    return this.applicationType;
  }

  @Override
  public Set<String> getApplicationTags() {
    return this.applicationTags;
  }

  @Override
  public boolean isAppFinalStateStored() {
    RMAppState state = getState();
    return state.equals(RMAppState.FINISHING)
        || state.equals(RMAppState.FINISHED) || state.equals(RMAppState.FAILED)
        || state.equals(RMAppState.KILLED);
  }

  @Override
  public YarnApplicationState createApplicationState() {
    RMAppState rmAppState = getState();
    // If App is in FINAL_SAVING state, return its previous state.
    if (rmAppState.equals(RMAppState.FINAL_SAVING)) {
      rmAppState = stateBeforeFinalSaving;
    }
    if (rmAppState.equals(RMAppState.KILLING)) {
      rmAppState = stateBeforeKilling;
    }
    return RMServerUtils.createApplicationState(rmAppState);
  }
  
  public static boolean isAppInFinalState(RMApp rmApp) {
    RMAppState appState = ((RMAppImpl) rmApp).getRecoveredFinalState();
    return appState == RMAppState.FAILED || appState == RMAppState.FINISHED
        || appState == RMAppState.KILLED;
  }
  
  private RMAppState getRecoveredFinalState() {
    return this.recoveredFinalState;
  }
}
