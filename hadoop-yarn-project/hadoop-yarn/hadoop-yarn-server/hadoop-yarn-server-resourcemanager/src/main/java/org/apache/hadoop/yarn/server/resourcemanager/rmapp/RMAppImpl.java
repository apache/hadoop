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
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

public class RMAppImpl implements RMApp {

  private static final Log LOG = LogFactory.getLog(RMAppImpl.class);
  private static final String UNAVAILABLE = "N/A";

  // Immutable fields
  private final ApplicationId applicationId;
  private final RMContext rmContext;
  private final Configuration conf;
  private final String user;
  private final String queue;
  private final String name;
  private final ApplicationSubmissionContext submissionContext;
  private final String clientTokenStr;
  private final ApplicationStore appStore;
  private final Dispatcher dispatcher;
  private final YarnScheduler scheduler;
  private final ApplicationMasterService masterService;
  private final StringBuilder diagnostics = new StringBuilder();
  private final int maxRetries;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final Map<ApplicationAttemptId, RMAppAttempt> attempts
      = new LinkedHashMap<ApplicationAttemptId, RMAppAttempt>();
  private final long submitTime;

  // Mutable fields
  private long startTime;
  private long finishTime;
  private RMAppAttempt currentAttempt;
  @SuppressWarnings("rawtypes")
  private EventHandler handler;
  private static final FinalTransition FINAL_TRANSITION = new FinalTransition();
  private static final AppFinishedTransition FINISHED_TRANSITION =
      new AppFinishedTransition();

  private static final StateMachineFactory<RMAppImpl,
                                           RMAppState,
                                           RMAppEventType,
                                           RMAppEvent> stateMachineFactory
                               = new StateMachineFactory<RMAppImpl,
                                           RMAppState,
                                           RMAppEventType,
                                           RMAppEvent>(RMAppState.NEW)


     // Transitions from NEW state
    .addTransition(RMAppState.NEW, RMAppState.SUBMITTED,
        RMAppEventType.START, new StartAppAttemptTransition())
    .addTransition(RMAppState.NEW, RMAppState.KILLED, RMAppEventType.KILL,
        new AppKilledTransition())
    .addTransition(RMAppState.NEW, RMAppState.FAILED,
        RMAppEventType.APP_REJECTED, new AppRejectedTransition())

     // Transitions from SUBMITTED state
    .addTransition(RMAppState.SUBMITTED, RMAppState.FAILED,
        RMAppEventType.APP_REJECTED, new AppRejectedTransition())
    .addTransition(RMAppState.SUBMITTED, RMAppState.ACCEPTED,
        RMAppEventType.APP_ACCEPTED)
    .addTransition(RMAppState.SUBMITTED, RMAppState.KILLED,
        RMAppEventType.KILL, new KillAppAndAttemptTransition())

     // Transitions from ACCEPTED state
    .addTransition(RMAppState.ACCEPTED, RMAppState.RUNNING,
        RMAppEventType.ATTEMPT_REGISTERED)
    .addTransition(RMAppState.ACCEPTED,
        EnumSet.of(RMAppState.SUBMITTED, RMAppState.FAILED),
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.SUBMITTED))
    .addTransition(RMAppState.ACCEPTED, RMAppState.KILLED,
        RMAppEventType.KILL, new KillAppAndAttemptTransition())

     // Transitions from RUNNING state
    .addTransition(RMAppState.RUNNING, RMAppState.FINISHED,
        RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
    .addTransition(RMAppState.RUNNING,
        EnumSet.of(RMAppState.SUBMITTED, RMAppState.FAILED),
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.SUBMITTED))
    .addTransition(RMAppState.RUNNING, RMAppState.KILLED,
        RMAppEventType.KILL, new KillAppAndAttemptTransition())

     // Transitions from FINISHED state
    .addTransition(RMAppState.FINISHED, RMAppState.FINISHED,
        RMAppEventType.KILL)

     // Transitions from FAILED state
    .addTransition(RMAppState.FAILED, RMAppState.FAILED,
        RMAppEventType.KILL)

     // Transitions from KILLED state
    .addTransition(
        RMAppState.KILLED,
        RMAppState.KILLED,
        EnumSet.of(RMAppEventType.APP_ACCEPTED,
            RMAppEventType.APP_REJECTED, RMAppEventType.KILL,
            RMAppEventType.ATTEMPT_FINISHED, RMAppEventType.ATTEMPT_FAILED,
            RMAppEventType.ATTEMPT_KILLED))

     .installTopology();

  private final StateMachine<RMAppState, RMAppEventType, RMAppEvent>
                                                                 stateMachine;

  private static final ApplicationResourceUsageReport
    DUMMY_APPLICATION_RESOURCE_USAGE_REPORT =
      BuilderUtils.newApplicationResourceUsageReport(-1, -1,
          Resources.createResource(-1), Resources.createResource(-1),
          Resources.createResource(-1));

  public RMAppImpl(ApplicationId applicationId, RMContext rmContext,
      Configuration config, String name, String user, String queue,
      ApplicationSubmissionContext submissionContext, String clientTokenStr,
      ApplicationStore appStore,
      YarnScheduler scheduler, ApplicationMasterService masterService, 
      long submitTime) {

    this.applicationId = applicationId;
    this.name = name;
    this.rmContext = rmContext;
    this.dispatcher = rmContext.getDispatcher();
    this.handler = dispatcher.getEventHandler();
    this.conf = config;
    this.user = user;
    this.queue = queue;
    this.submissionContext = submissionContext;
    this.clientTokenStr = clientTokenStr;
    this.appStore = appStore;
    this.scheduler = scheduler;
    this.masterService = masterService;
    this.submitTime = submitTime;
    this.startTime = System.currentTimeMillis();

    this.maxRetries = conf.getInt(YarnConfiguration.RM_AM_MAX_RETRIES,
        YarnConfiguration.DEFAULT_RM_AM_MAX_RETRIES);

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.stateMachine = stateMachineFactory.make(this);
  }

  @Override
  public ApplicationId getApplicationId() {
    return this.applicationId;
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

  @Override
  public ApplicationStore getApplicationStore() {
    return this.appStore;
  }

  private YarnApplicationState createApplicationState(RMAppState rmAppState) {
    switch(rmAppState) {
    case NEW:
      return YarnApplicationState.NEW;
    case SUBMITTED:
    case ACCEPTED:
      return YarnApplicationState.SUBMITTED;
    case RUNNING:
      return YarnApplicationState.RUNNING;
    case FINISHED:
      return YarnApplicationState.FINISHED;
    case KILLED:
      return YarnApplicationState.KILLED;
    case FAILED:
      return YarnApplicationState.FAILED;
    }
    throw new YarnException("Unknown state passed!");
  }

  private FinalApplicationStatus createFinalApplicationStatus(RMAppState state) {
    switch(state) {
    case NEW:
    case SUBMITTED:
    case ACCEPTED:
    case RUNNING:
      return FinalApplicationStatus.UNDEFINED;    
    // finished without a proper final state is the same as failed  
    case FINISHED:
    case FAILED:
      return FinalApplicationStatus.FAILED;
    case KILLED:
      return FinalApplicationStatus.KILLED;
    }
    throw new YarnException("Unknown state passed!");
  }

  
  @Override
  public ApplicationReport createAndGetApplicationReport(boolean allowAccess) {
    this.readLock.lock();

    try {
      String clientToken = UNAVAILABLE;
      String trackingUrl = UNAVAILABLE;
      String host = UNAVAILABLE;
      String origTrackingUrl = UNAVAILABLE;
      int rpcPort = -1;
      ApplicationResourceUsageReport appUsageReport =
          DUMMY_APPLICATION_RESOURCE_USAGE_REPORT;
      FinalApplicationStatus finishState = getFinalApplicationStatus();
      String diags = UNAVAILABLE;
      if (allowAccess) {
        if (this.currentAttempt != null) {
          trackingUrl = this.currentAttempt.getTrackingUrl();
          origTrackingUrl = this.currentAttempt.getOriginalTrackingUrl();
          clientToken = this.currentAttempt.getClientToken();
          host = this.currentAttempt.getHost();
          rpcPort = this.currentAttempt.getRpcPort();
          appUsageReport = currentAttempt.getApplicationResourceUsageReport();
        }
        diags = this.diagnostics.toString();
      }
      return BuilderUtils.newApplicationReport(this.applicationId, this.user,
          this.queue, this.name, host, rpcPort, clientToken,
          createApplicationState(this.stateMachine.getCurrentState()),
          diags, trackingUrl,
          this.startTime, this.finishTime, finishState, appUsageReport,
          origTrackingUrl);
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

  @SuppressWarnings("unchecked")
  private void createNewAttempt() {
    ApplicationAttemptId appAttemptId = Records
        .newRecord(ApplicationAttemptId.class);
    appAttemptId.setApplicationId(applicationId);
    appAttemptId.setAttemptId(attempts.size() + 1);

    RMAppAttempt attempt = new RMAppAttemptImpl(appAttemptId,
        clientTokenStr, rmContext, scheduler, masterService,
        submissionContext, conf);
    attempts.put(appAttemptId, attempt);
    currentAttempt = attempt;
    handler.handle(
        new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.START));
  }

  private static class RMAppTransition implements
      SingleArcTransition<RMAppImpl, RMAppEvent> {
    public void transition(RMAppImpl app, RMAppEvent event) {
    };

  }

  private static final class StartAppAttemptTransition extends RMAppTransition {
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.createNewAttempt();
    };
  }

  private static class AppFinishedTransition extends FinalTransition {
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppFinishedAttemptEvent finishedEvent =
          (RMAppFinishedAttemptEvent)event;
      app.diagnostics.append(finishedEvent.getDiagnostics());
      super.transition(app, event);
    };
  }

  private static class AppKilledTransition extends FinalTransition {
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.diagnostics.append("Application killed by user.");
      super.transition(app, event);
    };
  }

  private static class KillAppAndAttemptTransition extends AppKilledTransition {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.handler.handle(new RMAppAttemptEvent(app.currentAttempt.getAppAttemptId(),
          RMAppAttemptEventType.KILL));
      super.transition(app, event);
    }
  }
  private static final class AppRejectedTransition extends
      FinalTransition{
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppRejectedEvent rejectedEvent = (RMAppRejectedEvent)event;
      app.diagnostics.append(rejectedEvent.getMessage());
      super.transition(app, event);
    };
  }

  private static class FinalTransition extends RMAppTransition {

    private Set<NodeId> getNodesOnWhichAttemptRan(RMAppImpl app) {
      Set<NodeId> nodes = new HashSet<NodeId>();
      for (RMAppAttempt attempt : app.attempts.values()) {
        nodes.addAll(attempt.getRanNodes());
      }
      return nodes;
    }

    @SuppressWarnings("unchecked")
    public void transition(RMAppImpl app, RMAppEvent event) {
      Set<NodeId> nodes = getNodesOnWhichAttemptRan(app);
      for (NodeId nodeId : nodes) {
        app.handler.handle(
            new RMNodeCleanAppEvent(nodeId, app.applicationId));
      }
      app.finishTime = System.currentTimeMillis();
      app.handler.handle(
          new RMAppManagerEvent(app.applicationId,
          RMAppManagerEventType.APP_COMPLETED));
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

      RMAppFailedAttemptEvent failedEvent = ((RMAppFailedAttemptEvent)event);
      if (app.attempts.size() == app.maxRetries) {
        String msg = "Application " + app.getApplicationId()
        + " failed " + app.maxRetries
        + " times due to " + failedEvent.getDiagnostics()
        + ". Failing the application.";
        LOG.info(msg);
        app.diagnostics.append(msg);
        // Inform the node for app-finish
        FINAL_TRANSITION.transition(app, event);
        return RMAppState.FAILED;
      }

      app.createNewAttempt();
      return initialState;
    }

  }
}
