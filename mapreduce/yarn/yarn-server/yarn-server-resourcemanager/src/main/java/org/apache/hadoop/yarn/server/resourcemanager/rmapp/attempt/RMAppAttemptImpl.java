package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.ams.ApplicationMasterServiceEvent;
import org.apache.hadoop.yarn.server.resourcemanager.ams.ApplicationMasterServiceEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAllocatedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptLaunchFailedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.BuilderUtils;

public class RMAppAttemptImpl implements RMAppAttempt {

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

  private final ReadLock readLock;
  private final WriteLock writeLock;

  private final ApplicationAttemptId applicationAttemptId;
  private final String clientToken;
  private final ApplicationSubmissionContext submissionContext;

  private Map<ContainerId, ContainerId> liveContainers
    = new HashMap<ContainerId, ContainerId>();

  private List<Container> newlyAllocatedContainers;
  private List<ContainerId> justFinishedContainers;
  private Container masterContainer;

  private float progress = 0;
  private String host;
  private int rpcPort;
  private String trackingUrl;
  private String finalState;
  private final StringBuilder diagnostics = new StringBuilder();

  private static final CancelContainerTransition CANCEL_CONTAINER_TRANSITION
      = new CancelContainerTransition();

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
      .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL)

      // Transitions from SUBMITTED state
      .addTransition(RMAppAttemptState.SUBMITTED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.APP_REJECTED, new AppRejectedTransition())
      .addTransition(RMAppAttemptState.SUBMITTED, RMAppAttemptState.SCHEDULED,
          RMAppAttemptEventType.APP_ACCEPTED, new ScheduleTransition())
      .addTransition(RMAppAttemptState.SUBMITTED, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new BaseFinalTransition(RMAppAttemptState.KILLED))

       // Transitions from SCHEDULED State
      .addTransition(RMAppAttemptState.SCHEDULED,
          RMAppAttemptState.ALLOCATED,
          RMAppAttemptEventType.CONTAINER_ALLOCATED,
          new AMContainerAllocatedTransition())
      .addTransition(RMAppAttemptState.SCHEDULED, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new BaseFinalTransition(RMAppAttemptState.KILLED))

       // Transitions from ALLOCATED State
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.LAUNCHED,
          RMAppAttemptEventType.LAUNCHED, new AMLaunchedTransition())
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.LAUNCH_FAILED, new LaunchFailedTransition())
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL, new KillAllocatedAMTransition())

       // Transitions from LAUNCHED State
      .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.RUNNING,
          RMAppAttemptEventType.REGISTERED, new AMRegisteredTransition())
      .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new AMContainerCrashedTransition())
      .addTransition(
          RMAppAttemptState.LAUNCHED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.EXPIRE,
          new FinalTransition(RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new FinalTransition(RMAppAttemptState.KILLED))

       // Transitions from RUNNING State
      .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.FINISHED,
          RMAppAttemptEventType.UNREGISTERED, new AMUnregisteredTransition())
      .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
          RMAppAttemptEventType.STATUS_UPDATE, new StatusUpdateTransition())
      .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
          RMAppAttemptEventType.CONTAINER_ALLOCATED,
          new NormalContainerAllocatedTransition())
      .addTransition(
          RMAppAttemptState.RUNNING,
          EnumSet.of(RMAppAttemptState.RUNNING, RMAppAttemptState.FAILED),
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new ContainerFinishedTransition())
      .addTransition(
          RMAppAttemptState.RUNNING, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.EXPIRE,
          new FinalTransition(RMAppAttemptState.FAILED))
      .addTransition(
          RMAppAttemptState.RUNNING, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new FinalTransition(RMAppAttemptState.KILLED))

      // Transitions from FAILED State
      .addTransition(RMAppAttemptState.FAILED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.CONTAINER_ALLOCATED,
          CANCEL_CONTAINER_TRANSITION)
      .addTransition(
          RMAppAttemptState.FAILED,
          RMAppAttemptState.FAILED,
          EnumSet.of(RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.KILL,
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.STATUS_UPDATE,
              RMAppAttemptEventType.CONTAINER_FINISHED))

      // Transitions from FINISHED State
      .addTransition(RMAppAttemptState.FINISHED, RMAppAttemptState.FINISHED,
          RMAppAttemptEventType.CONTAINER_ALLOCATED,
          CANCEL_CONTAINER_TRANSITION)
      .addTransition(
          RMAppAttemptState.FINISHED,
          RMAppAttemptState.FINISHED,
          EnumSet.of(RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.CONTAINER_FINISHED,
              RMAppAttemptEventType.KILL))

      // Transitions from KILLED State
      .addTransition(RMAppAttemptState.KILLED, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.CONTAINER_ALLOCATED,
          CANCEL_CONTAINER_TRANSITION)
      .addTransition(
          RMAppAttemptState.KILLED,
          RMAppAttemptState.KILLED,
          EnumSet.of(RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.LAUNCHED,
              RMAppAttemptEventType.LAUNCH_FAILED,
              RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.REGISTERED,
              RMAppAttemptEventType.CONTAINER_FINISHED,
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.KILL,
              RMAppAttemptEventType.STATUS_UPDATE))

    .installTopology();

  public RMAppAttemptImpl(ApplicationAttemptId appAttemptId,
      String clientToken, RMContext rmContext, YarnScheduler scheduler,
      ApplicationSubmissionContext submissionContext) {

    this.applicationAttemptId = appAttemptId;
    this.rmContext = rmContext;
    this.eventHandler = rmContext.getDispatcher().getEventHandler();
    this.submissionContext = submissionContext;
    this.scheduler = scheduler;
    this.clientToken = clientToken;

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

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
      return this.trackingUrl;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getClientToken() {
    return this.clientToken;
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
  public float getProgress() {
    this.readLock.lock();

    try {
      return this.progress;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<Container> pullJustFinishedContainers() {
    this.writeLock.lock();

    try {
      return null;  // TODO: Should just be ContainerId 
//      List<Container> returnList = new ArrayList<Container>(
//          this.justFinishedContainers.size());
//      returnList.addAll(this.justFinishedContainers);
//      this.justFinishedContainers.clear();
//      return returnList;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public List<Container> pullNewlyAllocatedContainers() {
    this.writeLock.lock();

    try {
      List<Container> returnList = new ArrayList<Container>(
          this.newlyAllocatedContainers.size());
      returnList.addAll(this.newlyAllocatedContainers);
      this.newlyAllocatedContainers.clear();
      return returnList;
    } finally {
      this.writeLock.unlock();
    }
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

  @Override
  public void handle(RMAppAttemptEvent event) {

    this.writeLock.lock();

    try {
      ApplicationAttemptId appAttemptID = event.getApplicationAttemptId();
      LOG.info("Processing event for " + appAttemptID + " of type "
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

      // Register with the ApplicationMasterService
      appAttempt.eventHandler.handle(new ApplicationMasterServiceEvent(
          appAttempt.applicationAttemptId,
          ApplicationMasterServiceEventType.REGISTER));

      // Add the application to the scheduler
      appAttempt.eventHandler.handle(
          new AppAddedSchedulerEvent(appAttempt.applicationAttemptId,
              appAttempt.submissionContext.getQueue(),
              appAttempt.submissionContext.getUser()));
    }
  }

  private static final class AppRejectedTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptRejectedEvent rejectedEvent = (RMAppAttemptRejectedEvent) event;
      // Send the rejection event to app
      appAttempt.eventHandler.handle(new RMAppRejectedEvent(rejectedEvent
          .getApplicationAttemptId().getApplicationId(), rejectedEvent
          .getMessage()));
    }
  }

  private static final class ScheduleTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      // Send the acceptance to the app
      appAttempt.eventHandler.handle(new RMAppEvent(event
          .getApplicationAttemptId().getApplicationId(),
          RMAppEventType.APP_ACCEPTED));

      // Request a container for the AM.
      ResourceRequest request = BuilderUtils.newResourceRequest(
          AM_CONTAINER_PRIORITY, "*", appAttempt.submissionContext
              .getMasterCapability(), 1);
      LOG.debug("About to request resources for AM of "
          + appAttempt.applicationAttemptId + " required " + request);

      appAttempt.scheduler.allocate(appAttempt.applicationAttemptId,
          Collections.singletonList(request));
    }
  }

  private static final class AMContainerAllocatedTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      // Set the masterContainer
      RMAppAttemptContainerAllocatedEvent allocatedEvent
        = (RMAppAttemptContainerAllocatedEvent) event;
      appAttempt.masterContainer = allocatedEvent.getContainer();

      // Send event to launch the AM Container
      appAttempt.eventHandler.handle(new AMLauncherEvent(
          AMLauncherEventType.LAUNCH, appAttempt));
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

      // Tell the AMS
      // Register with the ApplicationMasterService
      appAttempt.eventHandler.handle(new ApplicationMasterServiceEvent(
          appAttempt.applicationAttemptId,
          ApplicationMasterServiceEventType.UNREGISTER));

      // Tell the application and the scheduler
      RMAppEventType eventToApp = null;
      switch (finalAttemptState) {
      case FINISHED:
        eventToApp = RMAppEventType.ATTEMPT_FINISHED;
        break;
      case KILLED:
        eventToApp = RMAppEventType.ATTEMPT_KILLED;
        break;
      case FAILED:
        eventToApp = RMAppEventType.ATTEMPT_FAILED;
        break;
      default:
        LOG.info("Cannot get this state!! Error!!");
        break;
      }
      appAttempt.eventHandler.handle(new RMAppEvent(
          appAttempt.applicationAttemptId.getApplicationId(), eventToApp));
      appAttempt.eventHandler.handle(new AppRemovedSchedulerEvent(appAttempt
          .getAppAttemptId(), finalAttemptState));
    }
  }

  private static final class AMLaunchedTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      // Register with AMLivelinessMonitor
      appAttempt.rmContext.getAMLivelinessMonitor().register(
          appAttempt.applicationAttemptId);

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
      appAttempt.trackingUrl = registrationEvent.getTrackingurl();

      // Let the app know
      appAttempt.eventHandler.handle(new RMAppEvent(appAttempt
          .getAppAttemptId().getApplicationId(),
          RMAppEventType.ATTEMPT_REGISTERED));
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

      // UnRegister from AMLivelinessMonitor
      appAttempt.rmContext.getAMLivelinessMonitor().unregister(
          appAttempt.getAppAttemptId());

      // Tell the app, scheduler
      super.transition(appAttempt, event);

      // Use diagnostic saying crashed.
      appAttempt.diagnostics.append("AM Container for "
          + appAttempt.getAppAttemptId() + " exited. Failing this attempt.");
    }
  }

  private static class FinalTransition extends BaseFinalTransition {

    public FinalTransition(RMAppAttemptState finalAttemptState) {
      super(finalAttemptState);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      // Tell the app and the scheduler
      super.transition(appAttempt, event);

      // UnRegister from AMLivelinessMonitor
      appAttempt.rmContext.getAMLivelinessMonitor().unregister(
          appAttempt.getAppAttemptId());

      // Tell the launcher to cleanup.
      appAttempt.eventHandler.handle(new AMLauncherEvent(
          AMLauncherEventType.CLEANUP, appAttempt));

      // Kill all the allocated containers.
      for (ContainerId containerid : appAttempt.liveContainers.keySet()) {
        appAttempt.eventHandler.handle(new RMContainerEvent(containerid,
            RMContainerEventType.KILL));
      }
    }
  }

  private static final class NormalContainerAllocatedTransition extends
      BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      RMAppAttemptContainerAllocatedEvent allocatedEvent
          = (RMAppAttemptContainerAllocatedEvent) event;

      // Store the container for pulling
      Container container = allocatedEvent.getContainer();
      appAttempt.newlyAllocatedContainers.add(container);

      // Add it to allContainers list.
      appAttempt.liveContainers.put(container.getId(), container.getId());
    }
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

  private static final class AMUnregisteredTransition extends FinalTransition {

    public AMUnregisteredTransition() {
      super(RMAppAttemptState.FINISHED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptUnregistrationEvent unregisterEvent
        = (RMAppAttemptUnregistrationEvent) event;
      unregisterEvent.getFinalState();
      appAttempt.diagnostics.append(unregisterEvent.getDiagnostics());
      appAttempt.trackingUrl = unregisterEvent.getTrackingUrl();
      appAttempt.finalState = unregisterEvent.getFinalState();

      // Tell the app and the scheduler
      super.transition(appAttempt, event);
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
      ContainerId containerId = containerFinishedEvent.getContainerId();

      // Is this container the AmContainer? If the finished container is same as
      // the AMContainer, AppAttempt fails
      if (appAttempt.masterContainer.getId().equals(
          containerId)) {
        new BaseFinalTransition(RMAppAttemptState.FAILED).transition(
            appAttempt, containerFinishedEvent);
        return RMAppAttemptState.FAILED;
      }

      // Normal container.

      // Remove it from allContainers list.
      appAttempt.liveContainers.remove(containerId);

      // Put it in completedcontainers list
      appAttempt.justFinishedContainers.add(containerId);
      return RMAppAttemptState.RUNNING;
    }
  }

  private static final class CancelContainerTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      RMAppAttemptContainerAllocatedEvent containerAllocatedEvent
          = (RMAppAttemptContainerAllocatedEvent) event;
      // Kill this container.
      appAttempt.eventHandler.handle(new RMContainerEvent(
          containerAllocatedEvent.getContainer().getId(),
          RMContainerEventType.KILL));
    }
  }

}
