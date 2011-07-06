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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMConfig;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.AMFinishEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.AMAllocatedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.AMRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.AMStatusUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationTrackerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.SNEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

/**
 * This class manages the state of a application master. Also, it
 * provide a read only interface for all the services to get information
 * about this application.
 *
 */
@Private
@Unstable
public class ApplicationImpl implements Application,
    EventHandler<ApplicationEvent> {
  private static final Log LOG = LogFactory.getLog(ApplicationImpl.class);
  private final ApplicationSubmissionContext submissionContext;
  private ApplicationMaster master;
  private final EventHandler handler;
  /** only to be used during recovery **/
  private final EventHandler syncHandler;
  private Container masterContainer;
  final private String user;
  private long startTime = 0;
  private long finishTime = 0;
  private String diagnostic;
  private static String DIAGNOSTIC_KILL_APPLICATION = "Application was killed.";
  private static String DIAGNOSTIC_AM_FAILED = "Application Master failed";
  private static String DIAGNOSTIC_AM_LAUNCH_FAILED = "Application Master failed to launch";

  private final int amMaxRetries;
  private final AMLivelinessMonitor amLivelinessMonitor;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private int numFailed = 0;
  private final ApplicationStore appStore;
  
  /* the list of nodes that this AM was launched on */
  List<String> hostNamesLaunched = new ArrayList<String>();
  /* this transition is too generalized, needs to be broken up as and when we 
   * keeping adding states. This will keep evolving and is not final yet.
   */
  private final  KillTransition killTransition =  new KillTransition();
  private final StatusUpdateTransition statusUpdatetransition = new StatusUpdateTransition();
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private final ExpireTransition expireTransition = new ExpireTransition();
  private final FailedTransition failedTransition = new FailedTransition();
  private final AllocateTransition allocateTransition = new AllocateTransition();
  private final LaunchTransition launchTransition =  new LaunchTransition();
  private final LaunchedTransition launchedTransition = new LaunchedTransition();
  private final FailedLaunchTransition failedLaunchTransition = new FailedLaunchTransition();
  
  private final StateMachine<ApplicationState,
                ApplicationEventType, ApplicationEvent> stateMachine;

  private final StateMachineFactory<ApplicationImpl, ApplicationState,
    ApplicationEventType, ApplicationEvent> stateMachineFactory
          = new StateMachineFactory<ApplicationImpl, ApplicationState,
    ApplicationEventType, ApplicationEvent>(ApplicationState.PENDING)

  // Transitions from PENDING State
  .addTransition(ApplicationState.PENDING, ApplicationState.ALLOCATING,
      ApplicationEventType.ALLOCATE, allocateTransition)
  .addTransition(ApplicationState.PENDING, ApplicationState.FAILED,
      ApplicationEventType.FAILED)
  .addTransition(ApplicationState.PENDING, ApplicationState.KILLED,
      ApplicationEventType.KILL)
  .addTransition(ApplicationState.PENDING, ApplicationState.ALLOCATING,
      ApplicationEventType.RECOVER, allocateTransition)
  .addTransition(ApplicationState.PENDING, ApplicationState.ALLOCATING,
      ApplicationEventType.RELEASED, new ScheduleTransition())

   // Transitions from ALLOCATING State
  .addTransition(ApplicationState.ALLOCATING, ApplicationState.ALLOCATED,
      ApplicationEventType.ALLOCATED, new AllocatedTransition())
  .addTransition(ApplicationState.ALLOCATING,
      ApplicationState.ALLOCATING, ApplicationEventType.RECOVER,
      allocateTransition)
  .addTransition(ApplicationState.ALLOCATING, ApplicationState.KILLED,
      ApplicationEventType.KILL, new AllocatingKillTransition())

  // Transitions from ALLOCATED State
  .addTransition(ApplicationState.ALLOCATED, ApplicationState.KILLED,
      ApplicationEventType.KILL, killTransition)
  .addTransition(ApplicationState.ALLOCATED, ApplicationState.LAUNCHING,
      ApplicationEventType.LAUNCH, launchTransition)
  .addTransition(ApplicationState.ALLOCATED, ApplicationState.LAUNCHING,
      ApplicationEventType.RECOVER, new RecoverLaunchTransition())

  // Transitions from LAUNCHING State
  .addTransition(ApplicationState.LAUNCHING, ApplicationState.LAUNCHED,
      ApplicationEventType.LAUNCHED, launchedTransition)
  .addTransition(ApplicationState.LAUNCHING, ApplicationState.PENDING,
      ApplicationEventType.LAUNCH_FAILED, failedLaunchTransition)
  // We cant say if the application was launched or not on a recovery, so
  // for now we assume it was launched and wait for its restart.
  .addTransition(ApplicationState.LAUNCHING, ApplicationState.LAUNCHED,
      ApplicationEventType.RECOVER, new RecoverLaunchedTransition())
  .addTransition(ApplicationState.LAUNCHING, ApplicationState.KILLED,
      ApplicationEventType.KILL, killTransition)

  // Transitions from LAUNCHED State
  .addTransition(ApplicationState.LAUNCHED, ApplicationState.CLEANUP,
      ApplicationEventType.KILL, killTransition)
   .addTransition(ApplicationState.LAUNCHED, ApplicationState.RUNNING,
      ApplicationEventType.REGISTERED, new RegisterTransition())
  .addTransition(ApplicationState.LAUNCHED, ApplicationState.LAUNCHED,
      ApplicationEventType.RECOVER)
  // for now we assume that acting on expiry is synchronous and we do not
  // have to wait for cleanup acks from scheduler negotiator and launcher.
  .addTransition(ApplicationState.LAUNCHED,
      ApplicationState.EXPIRED_PENDING, ApplicationEventType.EXPIRE,
      expireTransition)

  // Transitions from RUNNING State
  .addTransition(ApplicationState.RUNNING,
      ApplicationState.EXPIRED_PENDING, ApplicationEventType.EXPIRE,
      expireTransition)
  .addTransition(ApplicationState.RUNNING,
      EnumSet.of(ApplicationState.COMPLETED, ApplicationState.FAILED),
      ApplicationEventType.FINISH, new DoneTransition())
      // TODO: For now, no KILLED above. As all kills come to RM directly.
  .addTransition(ApplicationState.RUNNING, ApplicationState.RUNNING,
      ApplicationEventType.STATUSUPDATE, statusUpdatetransition)
  .addTransition(ApplicationState.RUNNING, ApplicationState.KILLED,
      ApplicationEventType.KILL, killTransition)
  .addTransition(ApplicationState.RUNNING, ApplicationState.RUNNING, 
      ApplicationEventType.RECOVER, new RecoverRunningTransition())

  // Transitions from EXPIRED_PENDING State
  .addTransition(ApplicationState.EXPIRED_PENDING,
      ApplicationState.ALLOCATING, ApplicationEventType.ALLOCATE,
      allocateTransition)
  .addTransition(ApplicationState.EXPIRED_PENDING,
      ApplicationState.ALLOCATING, ApplicationEventType.RECOVER,
      allocateTransition)
   .addTransition(ApplicationState.EXPIRED_PENDING,
      ApplicationState.FAILED, ApplicationEventType.FAILED_MAX_RETRIES,
      failedTransition)
   .addTransition(ApplicationState.EXPIRED_PENDING,
      ApplicationState.KILLED, ApplicationEventType.KILL, killTransition)

  // Transitions from COMPLETED State
  .addTransition(ApplicationState.COMPLETED, ApplicationState.COMPLETED,
      EnumSet.of(ApplicationEventType.FINISH, ApplicationEventType.KILL,
          ApplicationEventType.RECOVER))

  // Transitions from FAILED State
  .addTransition(ApplicationState.FAILED, ApplicationState.FAILED,
      EnumSet.of(ApplicationEventType.RECOVER, 
           ApplicationEventType.FINISH,
           ApplicationEventType.KILL))

  // Transitions from KILLED State
  .addTransition(ApplicationState.KILLED, ApplicationState.KILLED, 
      EnumSet.of(ApplicationEventType.RECOVER,
           ApplicationEventType.KILL,
           ApplicationEventType.FINISH))

  .installTopology();



  public ApplicationImpl(RMContext context, Configuration conf,
      String user, ApplicationSubmissionContext submissionContext,
      String clientToken, ApplicationStore appStore,
      AMLivelinessMonitor amLivelinessMonitor) {
    this.user = user;
    this.handler = context.getDispatcher().getEventHandler();
    this.syncHandler = context.getDispatcher().getSyncHandler();
    this.submissionContext = submissionContext;
    master = recordFactory.newRecordInstance(ApplicationMaster.class);
    master.setApplicationId(submissionContext.getApplicationId());
    master.setStatus(recordFactory.newRecordInstance(ApplicationStatus.class));
    master.getStatus().setApplicationId(submissionContext.getApplicationId());
    master.getStatus().setProgress(-1.0f);
    master.setAMFailCount(0);
    master.setContainerCount(0);
    stateMachine = stateMachineFactory.make(this);
    master.setState(ApplicationState.PENDING);
    master.setClientToken(clientToken);
    master.setDiagnostics("");
    this.appStore = appStore;
    this.startTime = System.currentTimeMillis();
    this.amMaxRetries =  conf.getInt(RMConfig.AM_MAX_RETRIES, 
        RMConfig.DEFAULT_AM_MAX_RETRIES);
    LOG.info("AM max retries: " + this.amMaxRetries);
    this.amLivelinessMonitor = amLivelinessMonitor;

    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
  }

  @Override
  public ApplicationSubmissionContext getSubmissionContext() {
    return submissionContext;
  }

  @Override
  public Resource getResource() {
    return submissionContext.getMasterCapability();
  }

  @Override
  public synchronized ApplicationId getApplicationID() {
    return this.master.getApplicationId();
  }

  @Override
  public synchronized ApplicationStatus getStatus() {
    return master.getStatus();
  }

  @Override
  public synchronized ApplicationMaster getMaster() {
    return master;
  }

  @Override
  /* make sure the master state is in sync with statemachine state */
  public synchronized ApplicationState getState() {
    return master.getState();
  }
  
  @Override
  public synchronized long getStartTime() {
    return this.startTime;
  }
  
  @Override
  public synchronized long getFinishTime() {
    return this.finishTime;
  }
  
  @Override
  public synchronized Container getMasterContainer() {
    return masterContainer;
  }


  @Override
  public String getUser() {
    return this.user;
  }


  @Override
  public synchronized int getFailedCount() {
    return numFailed;
  }
  
  @Override
  public String getName() {
    return submissionContext.getApplicationName();
  }

  @Override
  public String getQueue() {
    return submissionContext.getQueue();
  }
  
  @Override
  public ApplicationStore getStore() {
    return this.appStore;
  }
  
  /* the applicaiton master completed successfully */
  private static class DoneTransition
      implements
      MultipleArcTransition<ApplicationImpl, ApplicationEvent, ApplicationState> {

    @Override
    public ApplicationState transition(ApplicationImpl application,
        ApplicationEvent event) {
      application.handler.handle(new ASMEvent<SNEventType>(
        SNEventType.RELEASE, application));
      application.handler.handle(new ASMEvent<AMLauncherEventType>(
        AMLauncherEventType.CLEANUP, application));
      application.handler.handle(new ASMEvent<ApplicationTrackerEventType>(
      ApplicationTrackerEventType.REMOVE, application));
      application.finishTime = System.currentTimeMillis();

      application.amLivelinessMonitor.unRegister(event.getApplicationId());

      AMFinishEvent finishEvent = (AMFinishEvent) event;
      application.master.setTrackingUrl(finishEvent.getTrackingUrl());
      application.master.setDiagnostics(finishEvent.getDiagnostics());
      return finishEvent.getFinalApplicationState();
    }
  }
  
  private static class AllocatingKillTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      application.handler.handle(new ASMEvent<ApplicationTrackerEventType>(ApplicationTrackerEventType.REMOVE,
          application));
    }
  }
  
  private static class KillTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      application.finishTime = System.currentTimeMillis();
      application.getMaster().setDiagnostics(DIAGNOSTIC_KILL_APPLICATION);
      application.handler.handle(new ASMEvent<SNEventType>(SNEventType.RELEASE, application));
      application.handler.handle(new ASMEvent<AMLauncherEventType>(AMLauncherEventType.CLEANUP, application));
      application.handler.handle(new ASMEvent<ApplicationTrackerEventType>(ApplicationTrackerEventType.REMOVE,
          application));
    }
  }

  private static class RecoverLaunchTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {

    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      application.syncHandler.handle(new ASMEvent<ApplicationTrackerEventType>(
          ApplicationTrackerEventType.ADD, application));
        
      application.handler.handle(new ASMEvent<AMLauncherEventType>(
          AMLauncherEventType.LAUNCH, application));
    }
  }
  
  private static class FailedLaunchTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      application.finishTime = System.currentTimeMillis();
      application.getMaster().setDiagnostics(DIAGNOSTIC_AM_LAUNCH_FAILED);
      application.handler.handle(new ASMEvent<SNEventType>(
      SNEventType.RELEASE, application));
    }
  }
  
  private static class LaunchTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      application.handler.handle(new ASMEvent<AMLauncherEventType>(
      AMLauncherEventType.LAUNCH, application));
    }
  }
  
  private static class RecoverRunningTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      application.syncHandler.handle(new ASMEvent<ApplicationTrackerEventType>(
          ApplicationTrackerEventType.ADD, application));
      /* make sure the time stamp is update else expiry thread will expire this */
      application.amLivelinessMonitor.receivedPing(event.getApplicationId());
    }
  }
  
  private static class RecoverLaunchedTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      application.syncHandler.handle(new ASMEvent<ApplicationTrackerEventType>(
          ApplicationTrackerEventType.ADD, application));
        
      application.amLivelinessMonitor.register(event.getApplicationId());
    }
  }


  private static class LaunchedTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      application.amLivelinessMonitor.register(event.getApplicationId());
    }
  }

  private static class ExpireTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      /* for now this is the same as killed transition but will change later */
      application.handler.handle(new ASMEvent<SNEventType>(SNEventType.RELEASE,
        application));
      application.handler.handle(new ASMEvent<AMLauncherEventType>(
        AMLauncherEventType.CLEANUP, application));
      application.handler.handle(new ASMEvent<ApplicationTrackerEventType>(
          ApplicationTrackerEventType.EXPIRE, application));
      application.numFailed++;

      /* check to see if the number of retries are reached or not */
      if (application.getFailedCount() < application.amMaxRetries) {
        application.handler.handle(new ApplicationEvent(
            ApplicationEventType.ALLOCATE, event.getApplicationId()));
      } else {
        application.handler.handle(new ApplicationEvent(
            ApplicationEventType.FAILED_MAX_RETRIES, application
                .getApplicationID()));
      }
    }
  }


  /* Transition to schedule again on a container launch failure for AM */
  private static class ScheduleTransition implements 
  SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      application.masterContainer = null;
      /* schedule for a slot */
      application.handler.handle(new ASMEvent<SNEventType>(SNEventType.SCHEDULE,
      application));
    }
  }
  
  /* Transition to start the process of allocating for the AM container */
  private static class AllocateTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      /* notify tracking applications that an applicaiton has been added */
      // TODO: For now, changing to synchHandler. Instead we should use register/deregister.
      application.syncHandler.handle(new ASMEvent<ApplicationTrackerEventType>(
        ApplicationTrackerEventType.ADD, application));
      
      /* schedule for a slot */
      application.handler.handle(new ASMEvent<SNEventType>(
          SNEventType.SCHEDULE, application));
    }
  }
  
  /* Transition on a container allocated for a container */
  private static class AllocatedTransition
      implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {

    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      /* set the container that was generated by the scheduler negotiator */
      AMAllocatedEvent allocatedEvent = 
         (AMAllocatedEvent) event;
      application.masterContainer = allocatedEvent.getMasterContainer();
      try {
        application.appStore.storeMasterContainer(application.masterContainer);
      } catch(IOException ie) {
        //TODO ignore for now fix later.
      }

      /* we need to launch the applicaiton master on allocated transition */
      application.handler.handle(new ApplicationEvent(
          ApplicationEventType.LAUNCH, application.getApplicationID()));
    }    
  }

  private static class RegisterTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      AMRegistrationEvent registrationEvent =
        (AMRegistrationEvent) event;
      ApplicationMaster registeredMaster = registrationEvent
          .getApplicationMaster();
      application.master.setHost(registeredMaster.getHost());
      application.master.setTrackingUrl(registeredMaster.getTrackingUrl());
      application.master.setRpcPort(registeredMaster.getRpcPort());
      application.master.setStatus(registeredMaster.getStatus());
      application.master.getStatus().setProgress(0.0f);
      application.amLivelinessMonitor.receivedPing(event.getApplicationId());
      try {
        application.appStore.updateApplicationState(application.master);
      } catch(IOException ie) {
        //TODO fix this later. on error we should exit
      }
    }
  }

  /* transition to finishing state on a cleanup, for now its not used, but will need it 
   * later */
  private static class FailedTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {

    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      LOG.info("Failed application: " + application.getApplicationID());
    } 
  }


  /* Just a status update transition */
  private static class StatusUpdateTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {

    @Override
    public void transition(ApplicationImpl application,
        ApplicationEvent event) {
      AMStatusUpdateEvent statusUpdateEvent = 
        (AMStatusUpdateEvent) event;
      application.master.setStatus(statusUpdateEvent.getApplicationStatus());
      application.amLivelinessMonitor.receivedPing(event.getApplicationId());
    }
  }

  @Override
  public synchronized void handle(ApplicationEvent event) {

    this.writeLock.lock();

    try {
      ApplicationId appID = event.getApplicationId();
      LOG.info("Processing event for " + appID + " of type "
          + event.getType());
      final ApplicationState oldState = getState();
      try {
        /* keep the master in sync with the state machine */
        stateMachine.doTransition(event.getType(), event);
        master.setState(stateMachine.getCurrentState());
        LOG.info("State is " + stateMachine.getCurrentState());
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        /* TODO fail the application on the failed transition */
      }
      try {
        appStore.updateApplicationState(master);
      } catch (IOException ie) {
        // TODO ignore for now
      }
      if (oldState != getState()) {
        LOG.info(appID + " State change from " + oldState + " to "
            + getState());
      }
    } finally {
      this.writeLock.unlock();
    }
  }
}
