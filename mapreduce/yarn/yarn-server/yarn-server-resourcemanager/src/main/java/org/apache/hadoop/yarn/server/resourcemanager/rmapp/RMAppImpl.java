package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

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
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.RMConfig;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
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
  private final StringBuilder diagnostics = new StringBuilder();
  private final int maxRetries;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final Map<ApplicationAttemptId, RMAppAttempt> attempts
      = new LinkedHashMap<ApplicationAttemptId, RMAppAttempt>();

  // Mutable fields
  private long startTime;
  private long finishTime;
  private AMLivelinessMonitor amLivelinessMonitor;
  private RMAppAttempt currentAttempt;

  private static final FinalTransition FINAL_TRANSITION = new FinalTransition();

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
        new NewAppKilledTransition())

     // Transitions from SUBMITTED state
    .addTransition(RMAppState.SUBMITTED, RMAppState.FAILED,
        RMAppEventType.APP_REJECTED, new AppRejectedTransition())
    .addTransition(RMAppState.SUBMITTED, RMAppState.ACCEPTED,
        RMAppEventType.APP_ACCEPTED)

     // Transitions from ACCEPTED state
    .addTransition(RMAppState.ACCEPTED, RMAppState.RUNNING,
        RMAppEventType.ATTEMPT_REGISTERED)
    .addTransition(RMAppState.ACCEPTED,
        EnumSet.of(RMAppState.ACCEPTED, RMAppState.FAILED),
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.ACCEPTED))
    .addTransition(RMAppState.ACCEPTED, RMAppState.KILLED,
        RMAppEventType.KILL)

     // Transitions from RUNNING state
    .addTransition(RMAppState.RUNNING, RMAppState.FINISHED,
        RMAppEventType.ATTEMPT_FINISHED, FINAL_TRANSITION)
    .addTransition(RMAppState.RUNNING,
        EnumSet.of(RMAppState.RESTARTING, RMAppState.FAILED),
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.RUNNING))
    .addTransition(RMAppState.RUNNING, RMAppState.KILLED,
        RMAppEventType.KILL)

     // Transitions from RESTARTING state
    .addTransition(RMAppState.RESTARTING, RMAppState.RUNNING,
        RMAppEventType.ATTEMPT_REGISTERED)
    .addTransition(RMAppState.RESTARTING,
        EnumSet.of(RMAppState.RESTARTING, RMAppState.FAILED),
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.RESTARTING))
    .addTransition(RMAppState.RESTARTING, RMAppState.KILLED,
        RMAppEventType.KILL)

     // Transitions from FINISHED state
    .addTransition(RMAppState.FINISHED, RMAppState.RUNNING,
        RMAppEventType.KILL)

     // Transitions from FAILED state
    .addTransition(RMAppState.FAILED, RMAppState.FAILED,
        RMAppEventType.KILL)

     // Transitions from KILLED state
    .addTransition(
        RMAppState.KILLED,
        RMAppState.KILLED,
        EnumSet.of(RMAppEventType.KILL, RMAppEventType.ATTEMPT_FINISHED,
            RMAppEventType.ATTEMPT_FAILED, RMAppEventType.ATTEMPT_KILLED))

     .installTopology();

  private final StateMachine<RMAppState, RMAppEventType, RMAppEvent>
                                                                 stateMachine;

  public RMAppImpl(ApplicationId applicationId, RMContext rmContext,
      Configuration config, String name, String user, String queue,
      ApplicationSubmissionContext submissionContext, String clientTokenStr,
      ApplicationStore appStore, AMLivelinessMonitor amLivelinessMonitor,
      YarnScheduler scheduler) {

    this.applicationId = applicationId;
    this.name = name;
    this.rmContext = rmContext;
    this.dispatcher = rmContext.getDispatcher();
    this.conf = config;
    this.user = user;
    this.queue = queue;
    this.submissionContext = submissionContext;
    this.clientTokenStr = clientTokenStr;
    this.appStore = appStore;
    this.amLivelinessMonitor = amLivelinessMonitor;
    this.scheduler = scheduler;

    this.maxRetries = conf.getInt(RMConfig.AM_MAX_RETRIES,
        RMConfig.DEFAULT_AM_MAX_RETRIES);

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.stateMachine = stateMachineFactory.make(this);
  }

  @Override
  public ApplicationId getApplicationId() {
    return this.getApplicationId();
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
  public ApplicationStore getApplicationStore() {
    return this.appStore;
  }

  private ApplicationState createApplicationState(RMAppState rmAppState) {
    switch(rmAppState) {
    case NEW:
      return ApplicationState.NEW;
    case SUBMITTED:
    case ACCEPTED:
      return ApplicationState.SUBMITTED;
    case RESTARTING:
      return ApplicationState.RESTARTING;
    case RUNNING:
      return ApplicationState.RUNNING;
    case FINISHED:
      return ApplicationState.SUCCEEDED;
    case KILLED:
      return ApplicationState.KILLED;
    case FAILED:
      return ApplicationState.FAILED;
    }
    throw new YarnException("Unknown state passed!");
  }

  @Override
  public ApplicationReport createAndGetApplicationReport() {
    this.readLock.lock();

    try {
      String clientToken = "N/A";
      String trackingUrl = "N/A";
      String host = "N/A";
      int rpcPort = -1;
      if (this.currentAttempt != null) {
        trackingUrl = this.currentAttempt.getTrackingUrl();
        clientToken = this.currentAttempt.getClientToken();
        host = this.currentAttempt.getHost();
        rpcPort = this.currentAttempt.getRpcPort();
      }
      return BuilderUtils.newApplicationReport(this.applicationId, this.user,
          this.queue, this.name, host, rpcPort, clientToken,
          createApplicationState(this.stateMachine.getCurrentState()),
          this.diagnostics.toString(), trackingUrl);
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
  public String getTrackingUrl() {
    this.readLock.lock();

    try {
      return this.currentAttempt.getTrackingUrl();
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
      LOG.info("Processing event for " + appID + " of type "
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

  private static class RMAppTransition implements
      SingleArcTransition<RMAppImpl, RMAppEvent> {
    public void transition(RMAppImpl app, RMAppEvent event) {
    };

  }

  private static final class StartAppAttemptTransition extends RMAppTransition {
    public void transition(RMAppImpl app, RMAppEvent event) {

      ApplicationAttemptId appAttemptId = Records
          .newRecord(ApplicationAttemptId.class);
      appAttemptId.setApplicationId(app.applicationId);
      appAttemptId.setAttemptId(app.attempts.size() + 1);

      RMAppAttempt attempt = new RMAppAttemptImpl(appAttemptId,
          app.clientTokenStr, app.rmContext, app.scheduler, app.submissionContext);
      app.attempts.put(appAttemptId, attempt);
      app.currentAttempt = attempt;
      app.dispatcher.getEventHandler().handle(
          new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.START));
    };
  }

  private static final class NewAppKilledTransition extends RMAppTransition {
    public void transition(RMAppImpl app, RMAppEvent event) {
      app.diagnostics.append("Application killed by user.");
    };
  }

  private static final class AppRejectedTransition extends
      RMAppTransition {
    public void transition(RMAppImpl app, RMAppEvent event) {
      RMAppRejectedEvent rejectedEvent = (RMAppRejectedEvent)event;
      app.diagnostics.append(rejectedEvent.getMessage());
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

    public void transition(RMAppImpl app, RMAppEvent event) {
      Set<NodeId> nodes = getNodesOnWhichAttemptRan(app);
      for (NodeId nodeId : nodes) {
        app.dispatcher.getEventHandler().handle(
            new RMNodeCleanAppEvent(nodeId, app.applicationId));
      }
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

      if (app.attempts.size() == app.maxRetries) {
        app.diagnostics.append("Application " + app.getApplicationId()
            + " failed " + app.maxRetries
            + " times. Failing the application.");
        // Inform the node for app-finish
        FINAL_TRANSITION.transition(app, event);
        return RMAppState.FAILED;
      }

      ApplicationAttemptId appAttemptId = Records
          .newRecord(ApplicationAttemptId.class);
      appAttemptId.setApplicationId(app.applicationId);
      appAttemptId.setAttemptId(app.attempts.size() + 1);

      // Create a new attempt.
      RMAppAttempt attempt = new RMAppAttemptImpl(appAttemptId,
          app.clientTokenStr, app.rmContext, app.scheduler,
          app.submissionContext);
      app.attempts.put(appAttemptId, attempt);
      app.currentAttempt = attempt;
      app.dispatcher.getEventHandler().handle(
          new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.START));
      return initialState;
    }

  }
}
