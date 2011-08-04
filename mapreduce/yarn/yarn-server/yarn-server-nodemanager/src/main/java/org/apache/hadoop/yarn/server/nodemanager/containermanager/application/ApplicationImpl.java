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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.ContainerLogsRetentionPolicy;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.event.LogAggregatorAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.event.LogAggregatorAppStartedEvent;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class ApplicationImpl implements Application {

  final Dispatcher dispatcher;
  final String user;
  final ApplicationId appId;
  final Credentials credentials;

  private static final Log LOG = LogFactory.getLog(Application.class);

  Map<ContainerId, Container> containers =
      new HashMap<ContainerId, Container>();

  public ApplicationImpl(Dispatcher dispatcher, String user,
      ApplicationId appId, Credentials credentials) {
    this.dispatcher = dispatcher;
    this.user = user.toString();
    this.appId = appId;
    this.credentials = credentials;
    stateMachine = stateMachineFactory.make(this);
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
  public synchronized ApplicationState getApplicationState() {
    // TODO: Synchro should be at statemachine level.
    // This is only for tests?
    return this.stateMachine.getCurrentState();
  }

  @Override
  public Map<ContainerId, Container> getContainers() {
    return this.containers;
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

           // Transitions from INITING state
           .addTransition(ApplicationState.INITING, ApplicationState.INITING,
               ApplicationEventType.INIT_APPLICATION,
               new AppIsInitingTransition())
           .addTransition(ApplicationState.INITING,
               EnumSet.of(ApplicationState.FINISHING_CONTAINERS_WAIT,
                   ApplicationState.APPLICATION_RESOURCES_CLEANINGUP),
               ApplicationEventType.FINISH_APPLICATION,
               new AppFinishTriggeredTransition())
           .addTransition(ApplicationState.INITING, ApplicationState.RUNNING,
               ApplicationEventType.APPLICATION_INITED,
               new AppInitDoneTransition())

           // Transitions from RUNNING state
           .addTransition(ApplicationState.RUNNING,
               ApplicationState.RUNNING,
               ApplicationEventType.INIT_APPLICATION,
               new DuplicateAppInitTransition())
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

           // Transitions from APPLICATION_RESOURCES_CLEANINGUP state
           .addTransition(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
               ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
               ApplicationEventType.APPLICATION_CONTAINER_FINISHED)
           .addTransition(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
               ApplicationState.FINISHED,
               ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP,
               new AppCompletelyDoneTransition())

           // create the topology tables
           .installTopology();

  private final StateMachine<ApplicationState, ApplicationEventType, ApplicationEvent> stateMachine;

  /**
   * Notify services of new application.
   */
  static class AppInitTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationInitEvent initEvent = (ApplicationInitEvent) event;
      Container container = initEvent.getContainer();
      app.containers.put(container.getContainerID(), container);
      app.dispatcher.getEventHandler().handle(
          new ApplicationLocalizationEvent(
              LocalizationEventType.INIT_APPLICATION_RESOURCES, app));
    }
  }

  /**
   * Absorb initialization events while the application initializes.
   */
  static class AppIsInitingTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationInitEvent initEvent = (ApplicationInitEvent) event;
      Container container = initEvent.getContainer();
      app.containers.put(container.getContainerID(), container);
      LOG.info("Adding " + container.getContainerID()
          + " to application " + app.toString());
    }
  }

  static class AppInitDoneTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {

      // Inform the logAggregator
      app.dispatcher.getEventHandler().handle(
            new LogAggregatorAppStartedEvent(app.appId, app.user,
                app.credentials,
                ContainerLogsRetentionPolicy.ALL_CONTAINERS)); // TODO: Fix

      // Start all the containers waiting for ApplicationInit
      for (Container container : app.containers.values()) {
        app.dispatcher.getEventHandler().handle(new ContainerInitEvent(
              container.getContainerID()));
      }
    }
  }

  static class DuplicateAppInitTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationInitEvent initEvent = (ApplicationInitEvent) event;
      Container container = initEvent.getContainer();
      app.containers.put(container.getContainerID(), container);
      LOG.info("Adding " + container.getContainerID()
          + " to application " + app.toString());
      app.dispatcher.getEventHandler().handle(new ContainerInitEvent(
            container.getContainerID()));
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

  void handleAppFinishWithContainersCleanedup() {
    // Delete Application level resources
    this.dispatcher.getEventHandler().handle(
        new ApplicationLocalizationEvent(
            LocalizationEventType.DESTROY_APPLICATION_RESOURCES, this));

    // TODO: Trigger the LogsManager
  }

  static class AppFinishTriggeredTransition
      implements
      MultipleArcTransition<ApplicationImpl, ApplicationEvent, ApplicationState> {
    @Override
    public ApplicationState transition(ApplicationImpl app,
        ApplicationEvent event) {

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
                "Container killed on application-finish event from RM."));
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

  static class AppCompletelyDoneTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      // Inform the logService
      app.dispatcher.getEventHandler().handle(
          new LogAggregatorAppFinishedEvent(app.appId));
    }
  }

  @Override
  public synchronized void handle(ApplicationEvent event) {

    ApplicationId applicationID = event.getApplicationID();
    LOG.info("Processing " + applicationID + " of type " + event.getType());

    ApplicationState oldState = stateMachine.getCurrentState();
    ApplicationState newState = null;
    try {
      // queue event requesting init of the same app
      newState = stateMachine.doTransition(event.getType(), event);
    } catch (InvalidStateTransitonException e) {
      LOG.warn("Can't handle this event at current state", e);
    }
    if (oldState != newState) {
      LOG.info("Application " + applicationID + " transitioned from "
          + oldState + " to " + newState);
    }
  }

  @Override
  public String toString() {
    return ConverterUtils.toString(appId);
  }
}
