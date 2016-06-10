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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.LogAggregationContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.LogAggregationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import com.google.common.annotations.VisibleForTesting;

/**
 * The state machine for the representation of an Application
 * within the NodeManager.
 */
public class ApplicationImpl implements Application {

  final Dispatcher dispatcher;
  final String user;
  final ApplicationId appId;
  final Credentials credentials;
  Map<ApplicationAccessType, String> applicationACLs;
  final ApplicationACLsManager aclsManager;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final Context context;

  private static final Log LOG = LogFactory.getLog(ApplicationImpl.class);

  private LogAggregationContext logAggregationContext;

  Map<ContainerId, Container> containers =
      new HashMap<ContainerId, Container>();

  /**
   * The timestamp when the log aggregation has started for this application.
   * Used to determine the age of application log files during log aggregation.
   * When logAggregationRentention policy is enabled, log files older than
   * the retention policy will not be uploaded but scheduled for deletion.
   */
  private long applicationLogInitedTimestamp = -1;
  private final NMStateStoreService appStateStore;

  public ApplicationImpl(Dispatcher dispatcher, String user,
      ApplicationId appId, Credentials credentials,
      Context context, long recoveredLogInitedTime) {
    this.dispatcher = dispatcher;
    this.user = user;
    this.appId = appId;
    this.credentials = credentials;
    this.aclsManager = context.getApplicationACLsManager();
    this.context = context;
    this.appStateStore = context.getNMStateStore();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
    stateMachine = stateMachineFactory.make(this);
    setAppLogInitedTimestamp(recoveredLogInitedTime);
  }

  public ApplicationImpl(Dispatcher dispatcher, String user,
      ApplicationId appId, Credentials credentials, Context context) {
    this(dispatcher, user, appId, credentials, context, -1);
  }

  @Override
  public String getUser() {
    return user.toString();
  }

  @Override
  public ApplicationId getAppId() {
    return appId;
  }

  @Override
  public ApplicationState getApplicationState() {
    this.readLock.lock();
    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<ContainerId, Container> getContainers() {
    this.readLock.lock();
    try {
      return this.containers;
    } finally {
      this.readLock.unlock();
    }
  }

  private static final ContainerDoneTransition CONTAINER_DONE_TRANSITION =
      new ContainerDoneTransition();

  private static StateMachineFactory<ApplicationImpl, ApplicationState,
          ApplicationEventType, ApplicationEvent> stateMachineFactory =
      new StateMachineFactory<ApplicationImpl, ApplicationState,
          ApplicationEventType, ApplicationEvent>(ApplicationState.NEW)

           // Transitions from NEW state
           .addTransition(ApplicationState.NEW, ApplicationState.INITING,
               ApplicationEventType.INIT_APPLICATION, new AppInitTransition())
           .addTransition(ApplicationState.NEW, ApplicationState.NEW,
               ApplicationEventType.INIT_CONTAINER,
               new InitContainerTransition())

           // Transitions from INITING state
           .addTransition(ApplicationState.INITING, ApplicationState.INITING,
               ApplicationEventType.INIT_CONTAINER,
               new InitContainerTransition())
           .addTransition(ApplicationState.INITING,
               EnumSet.of(ApplicationState.FINISHING_CONTAINERS_WAIT,
                   ApplicationState.APPLICATION_RESOURCES_CLEANINGUP),
               ApplicationEventType.FINISH_APPLICATION,
               new AppFinishTriggeredTransition())
           .addTransition(ApplicationState.INITING, ApplicationState.INITING,
               ApplicationEventType.APPLICATION_CONTAINER_FINISHED,
               CONTAINER_DONE_TRANSITION)
           .addTransition(ApplicationState.INITING, ApplicationState.INITING,
               ApplicationEventType.APPLICATION_LOG_HANDLING_INITED,
               new AppLogInitDoneTransition())
           .addTransition(ApplicationState.INITING, ApplicationState.INITING,
               ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED,
               new AppLogInitFailTransition())
           .addTransition(ApplicationState.INITING, ApplicationState.RUNNING,
               ApplicationEventType.APPLICATION_INITED,
               new AppInitDoneTransition())

           // Transitions from RUNNING state
           .addTransition(ApplicationState.RUNNING,
               ApplicationState.RUNNING,
               ApplicationEventType.INIT_CONTAINER,
               new InitContainerTransition())
           .addTransition(ApplicationState.RUNNING,
               ApplicationState.RUNNING,
               ApplicationEventType.APPLICATION_CONTAINER_FINISHED,
               CONTAINER_DONE_TRANSITION)
           .addTransition(
               ApplicationState.RUNNING,
               EnumSet.of(ApplicationState.FINISHING_CONTAINERS_WAIT,
                   ApplicationState.APPLICATION_RESOURCES_CLEANINGUP),
               ApplicationEventType.FINISH_APPLICATION,
               new AppFinishTriggeredTransition())

           // Transitions from FINISHING_CONTAINERS_WAIT state.
           .addTransition(
               ApplicationState.FINISHING_CONTAINERS_WAIT,
               EnumSet.of(ApplicationState.FINISHING_CONTAINERS_WAIT,
                   ApplicationState.APPLICATION_RESOURCES_CLEANINGUP),
               ApplicationEventType.APPLICATION_CONTAINER_FINISHED,
               new AppFinishTransition())
          .addTransition(ApplicationState.FINISHING_CONTAINERS_WAIT,
              ApplicationState.FINISHING_CONTAINERS_WAIT,
              EnumSet.of(
                  ApplicationEventType.APPLICATION_LOG_HANDLING_INITED,
                  ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED,
                  ApplicationEventType.APPLICATION_INITED,
                  ApplicationEventType.FINISH_APPLICATION))

           // Transitions from APPLICATION_RESOURCES_CLEANINGUP state
           .addTransition(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
               ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
               ApplicationEventType.APPLICATION_CONTAINER_FINISHED)
           .addTransition(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
               ApplicationState.FINISHED,
               ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP,
               new AppCompletelyDoneTransition())
          .addTransition(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
              ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
              EnumSet.of(
                  ApplicationEventType.APPLICATION_LOG_HANDLING_INITED,
                  ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED,
                  ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED,
                  ApplicationEventType.APPLICATION_INITED,
                  ApplicationEventType.FINISH_APPLICATION))

           // Transitions from FINISHED state
           .addTransition(ApplicationState.FINISHED,
               ApplicationState.FINISHED,
               EnumSet.of(
                   ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED,
                   ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED),
               new AppLogsAggregatedTransition())
           .addTransition(ApplicationState.FINISHED, ApplicationState.FINISHED,
               EnumSet.of(
                  ApplicationEventType.APPLICATION_LOG_HANDLING_INITED,
                  ApplicationEventType.FINISH_APPLICATION))
           // create the topology tables
           .installTopology();

  private final StateMachine<ApplicationState, ApplicationEventType, ApplicationEvent> stateMachine;

  /**
   * Notify services of new application.
   * 
   * In particular, this initializes the {@link LogAggregationService}
   */
  @SuppressWarnings("unchecked")
  static class AppInitTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationInitEvent initEvent = (ApplicationInitEvent)event;
      app.applicationACLs = initEvent.getApplicationACLs();
      app.aclsManager.addApplication(app.getAppId(), app.applicationACLs);
      // Inform the logAggregator
      app.logAggregationContext = initEvent.getLogAggregationContext();
      app.dispatcher.getEventHandler().handle(
          new LogHandlerAppStartedEvent(app.appId, app.user,
              app.credentials, app.applicationACLs,
              app.logAggregationContext, app.applicationLogInitedTimestamp));
    }
  }

  /**
   * Handles the APPLICATION_LOG_HANDLING_INITED event that occurs after
   * {@link LogAggregationService} has created the directories for the app
   * and started the aggregation thread for the app.
   * 
   * In particular, this requests that the {@link ResourceLocalizationService}
   * localize the application-scoped resources.
   */
  @SuppressWarnings("unchecked")
  static class AppLogInitDoneTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      app.dispatcher.getEventHandler().handle(
          new ApplicationLocalizationEvent(
              LocalizationEventType.INIT_APPLICATION_RESOURCES, app));
      app.setAppLogInitedTimestamp(event.getTimestamp());
      try {
        app.appStateStore.storeApplication(app.appId, buildAppProto(app));
      } catch (Exception ex) {
        LOG.warn("failed to update application state in state store", ex);
      }
    }
  }

  @VisibleForTesting
  void setAppLogInitedTimestamp(long appLogInitedTimestamp) {
    this.applicationLogInitedTimestamp = appLogInitedTimestamp;
  }

  static ContainerManagerApplicationProto buildAppProto(ApplicationImpl app)
      throws IOException {
    ContainerManagerApplicationProto.Builder builder =
        ContainerManagerApplicationProto.newBuilder();
    builder.setId(((ApplicationIdPBImpl) app.appId).getProto());
    builder.setUser(app.getUser());

    if (app.logAggregationContext != null) {
      builder.setLogAggregationContext((
          (LogAggregationContextPBImpl)app.logAggregationContext).getProto());
    }

    builder.clearCredentials();
    if (app.credentials != null) {
      DataOutputBuffer dob = new DataOutputBuffer();
      app.credentials.writeTokenStorageToStream(dob);
      builder.setCredentials(ByteString.copyFrom(dob.getData()));
    }

    builder.clearAcls();
    if (app.applicationACLs != null) {
      for (Map.Entry<ApplicationAccessType, String> acl :  app
          .applicationACLs.entrySet()) {
        YarnProtos.ApplicationACLMapProto p = YarnProtos
            .ApplicationACLMapProto.newBuilder()
            .setAccessType(ProtoUtils.convertToProtoFormat(acl.getKey()))
            .setAcl(acl.getValue())
            .build();
        builder.addAcls(p);
      }
    }

    builder.setAppLogAggregationInitedTime(app.applicationLogInitedTimestamp);

    return builder.build();
  }

  /**
   * Handles the APPLICATION_LOG_HANDLING_FAILED event that occurs after
   * {@link LogAggregationService} has failed to initialize the log 
   * aggregation service
   * 
   * In particular, this requests that the {@link ResourceLocalizationService}
   * localize the application-scoped resources.
   */
  @SuppressWarnings("unchecked")
  static class AppLogInitFailTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      LOG.warn("Log Aggregation service failed to initialize, there will " + 
               "be no logs for this application");
      app.dispatcher.getEventHandler().handle(
          new ApplicationLocalizationEvent(
              LocalizationEventType.INIT_APPLICATION_RESOURCES, app));
    }
  }
  /**
   * Handles INIT_CONTAINER events which request that we launch a new
   * container. When we're still in the INITTING state, we simply
   * queue these up. When we're in the RUNNING state, we pass along
   * an ContainerInitEvent to the appropriate ContainerImpl.
   */
  @SuppressWarnings("unchecked")
  static class InitContainerTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationContainerInitEvent initEvent =
        (ApplicationContainerInitEvent) event;
      Container container = initEvent.getContainer();
      app.containers.put(container.getContainerId(), container);
      LOG.info("Adding " + container.getContainerId()
          + " to application " + app.toString());
      
      switch (app.getApplicationState()) {
      case RUNNING:
        app.dispatcher.getEventHandler().handle(new ContainerInitEvent(
            container.getContainerId()));
        break;
      case INITING:
      case NEW:
        // these get queued up and sent out in AppInitDoneTransition
        break;
      default:
        assert false : "Invalid state for InitContainerTransition: " +
            app.getApplicationState();
      }
    }
  }

  @SuppressWarnings("unchecked")
  static class AppInitDoneTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      // Start all the containers waiting for ApplicationInit
      for (Container container : app.containers.values()) {
        app.dispatcher.getEventHandler().handle(new ContainerInitEvent(
              container.getContainerId()));
      }
    }
  }

  
  static final class ContainerDoneTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationContainerFinishedEvent containerEvent =
          (ApplicationContainerFinishedEvent) event;
      if (null == app.containers.remove(containerEvent.getContainerID())) {
        LOG.warn("Removing unknown " + containerEvent.getContainerID() +
            " from application " + app.toString());
      } else {
        LOG.info("Removing " + containerEvent.getContainerID() +
            " from application " + app.toString());
      }
    }
  }

  @SuppressWarnings("unchecked")
  void handleAppFinishWithContainersCleanedup() {
    // Delete Application level resources
    this.dispatcher.getEventHandler().handle(
        new ApplicationLocalizationEvent(
            LocalizationEventType.DESTROY_APPLICATION_RESOURCES, this));

    // tell any auxiliary services that the app is done 
    this.dispatcher.getEventHandler().handle(
        new AuxServicesEvent(AuxServicesEventType.APPLICATION_STOP, appId));

    // TODO: Trigger the LogsManager
  }

  @SuppressWarnings("unchecked")
  static class AppFinishTriggeredTransition
      implements
      MultipleArcTransition<ApplicationImpl, ApplicationEvent, ApplicationState> {
    @Override
    public ApplicationState transition(ApplicationImpl app,
        ApplicationEvent event) {
      ApplicationFinishEvent appEvent = (ApplicationFinishEvent)event;
      if (app.containers.isEmpty()) {
        // No container to cleanup. Cleanup app level resources.
        app.handleAppFinishWithContainersCleanedup();
        return ApplicationState.APPLICATION_RESOURCES_CLEANINGUP;
      }

      // Send event to ContainersLauncher to finish all the containers of this
      // application.
      for (ContainerId containerID : app.containers.keySet()) {
        app.dispatcher.getEventHandler().handle(
            new ContainerKillEvent(containerID,
                ContainerExitStatus.KILLED_AFTER_APP_COMPLETION,
                "Container killed on application-finish event: " + appEvent.getDiagnostic()));
      }
      return ApplicationState.FINISHING_CONTAINERS_WAIT;
    }
  }

  static class AppFinishTransition implements
    MultipleArcTransition<ApplicationImpl, ApplicationEvent, ApplicationState> {

    @Override
    public ApplicationState transition(ApplicationImpl app,
        ApplicationEvent event) {

      ApplicationContainerFinishedEvent containerFinishEvent =
          (ApplicationContainerFinishedEvent) event;
      LOG.info("Removing " + containerFinishEvent.getContainerID()
          + " from application " + app.toString());
      app.containers.remove(containerFinishEvent.getContainerID());

      if (app.containers.isEmpty()) {
        // All containers are cleanedup.
        app.handleAppFinishWithContainersCleanedup();
        return ApplicationState.APPLICATION_RESOURCES_CLEANINGUP;
      }

      return ApplicationState.FINISHING_CONTAINERS_WAIT;
    }

  }

  @SuppressWarnings("unchecked")
  static class AppCompletelyDoneTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {

      // Inform the logService
      app.dispatcher.getEventHandler().handle(
          new LogHandlerAppFinishedEvent(app.appId));

      app.context.getNMTokenSecretManager().appFinished(app.getAppId());
    }
  }

  static class AppLogsAggregatedTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationId appId = event.getApplicationID();
      app.context.getApplications().remove(appId);
      app.aclsManager.removeApplication(appId);
      try {
        app.context.getNMStateStore().removeApplication(appId);
      } catch (IOException e) {
        LOG.error("Unable to remove application from state store", e);
      }
    }
  }

  @Override
  public void handle(ApplicationEvent event) {

    this.writeLock.lock();

    try {
      ApplicationId applicationID = event.getApplicationID();
      LOG.debug("Processing " + applicationID + " of type " + event.getType());

      ApplicationState oldState = stateMachine.getCurrentState();
      ApplicationState newState = null;
      try {
        // queue event requesting init of the same app
        newState = stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.warn("Can't handle this event at current state", e);
      }
      if (oldState != newState) {
        LOG.info("Application " + applicationID + " transitioned from "
            + oldState + " to " + newState);
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public String toString() {
    return appId.toString();
  }

  @VisibleForTesting
  public LogAggregationContext getLogAggregationContext() {
    try {
      this.readLock.lock();
      return this.logAggregationContext;
    } finally {
      this.readLock.unlock();
    }
  }
}
