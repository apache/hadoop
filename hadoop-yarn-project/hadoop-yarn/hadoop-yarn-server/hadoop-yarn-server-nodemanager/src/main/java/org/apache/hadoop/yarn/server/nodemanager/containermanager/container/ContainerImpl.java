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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerStartMonitoringEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerStopMonitoringEvent;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class ContainerImpl implements Container {

  private final Lock readLock;
  private final Lock writeLock;
  private final Dispatcher dispatcher;
  private final Credentials credentials;
  private final NodeManagerMetrics metrics;
  private final ContainerLaunchContext launchContext;
  private final ContainerTokenIdentifier containerTokenIdentifier;
  private final ContainerId containerId;
  private final Resource resource;
  private final String user;
  private int exitCode = ContainerExitStatus.INVALID;
  private final StringBuilder diagnostics;
  private boolean wasLaunched;

  /** The NM-wide configuration - not specific to this container */
  private final Configuration daemonConf;

  private static final Log LOG = LogFactory.getLog(Container.class);
  private final Map<LocalResourceRequest,List<String>> pendingResources =
    new HashMap<LocalResourceRequest,List<String>>();
  private final Map<Path,List<String>> localizedResources =
    new HashMap<Path,List<String>>();
  private final List<LocalResourceRequest> publicRsrcs =
    new ArrayList<LocalResourceRequest>();
  private final List<LocalResourceRequest> privateRsrcs =
    new ArrayList<LocalResourceRequest>();
  private final List<LocalResourceRequest> appRsrcs =
    new ArrayList<LocalResourceRequest>();

  public ContainerImpl(Configuration conf, Dispatcher dispatcher,
      ContainerLaunchContext launchContext, Credentials creds,
      NodeManagerMetrics metrics,
      ContainerTokenIdentifier containerTokenIdentifier) {
    this.daemonConf = conf;
    this.dispatcher = dispatcher;
    this.launchContext = launchContext;
    this.containerTokenIdentifier = containerTokenIdentifier;
    this.containerId = containerTokenIdentifier.getContainerID();
    this.resource = containerTokenIdentifier.getResource();
    this.diagnostics = new StringBuilder();
    this.credentials = creds;
    this.metrics = metrics;
    user = containerTokenIdentifier.getApplicationSubmitter();
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    stateMachine = stateMachineFactory.make(this);
  }

  private static final ContainerDoneTransition CONTAINER_DONE_TRANSITION =
    new ContainerDoneTransition();

  private static final ContainerDiagnosticsUpdateTransition UPDATE_DIAGNOSTICS_TRANSITION =
      new ContainerDiagnosticsUpdateTransition();

  // State Machine for each container.
  private static StateMachineFactory
           <ContainerImpl, ContainerState, ContainerEventType, ContainerEvent>
        stateMachineFactory =
      new StateMachineFactory<ContainerImpl, ContainerState, ContainerEventType, ContainerEvent>(ContainerState.NEW)
    // From NEW State
    .addTransition(ContainerState.NEW,
        EnumSet.of(ContainerState.LOCALIZING, ContainerState.LOCALIZED,
            ContainerState.LOCALIZATION_FAILED),
        ContainerEventType.INIT_CONTAINER, new RequestResourcesTransition())
    .addTransition(ContainerState.NEW, ContainerState.NEW,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.NEW, ContainerState.DONE,
        ContainerEventType.KILL_CONTAINER, new KillOnNewTransition())

    // From LOCALIZING State
    .addTransition(ContainerState.LOCALIZING,
        EnumSet.of(ContainerState.LOCALIZING, ContainerState.LOCALIZED),
        ContainerEventType.RESOURCE_LOCALIZED, new LocalizedTransition())
    .addTransition(ContainerState.LOCALIZING,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.RESOURCE_FAILED,
        new ResourceFailedTransition())
    .addTransition(ContainerState.LOCALIZING, ContainerState.LOCALIZING,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.LOCALIZING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER,
        new KillDuringLocalizationTransition())

    // From LOCALIZATION_FAILED State
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.DONE,
        ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
        CONTAINER_DONE_TRANSITION)
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    // container not launched so kill is a no-op
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.KILL_CONTAINER)
    // container cleanup triggers a release of all resources
    // regardless of whether they were localized or not
    // LocalizedResource handles release event in all states
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.RESOURCE_LOCALIZED)
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.RESOURCE_FAILED)

    // From LOCALIZED State
    .addTransition(ContainerState.LOCALIZED, ContainerState.RUNNING,
        ContainerEventType.CONTAINER_LAUNCHED, new LaunchTransition())
    .addTransition(ContainerState.LOCALIZED, ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition(true))
    .addTransition(ContainerState.LOCALIZED, ContainerState.LOCALIZED,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.LOCALIZED, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER, new KillTransition())

    // From RUNNING State
    .addTransition(ContainerState.RUNNING,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
        new ExitedWithSuccessTransition(true))
    .addTransition(ContainerState.RUNNING,
        ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition(true))
    .addTransition(ContainerState.RUNNING, ContainerState.RUNNING,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.RUNNING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER, new KillTransition())
    .addTransition(ContainerState.RUNNING, ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
        new KilledExternallyTransition()) 

    // From CONTAINER_EXITED_WITH_SUCCESS State
    .addTransition(ContainerState.EXITED_WITH_SUCCESS, ContainerState.DONE,
        ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
        CONTAINER_DONE_TRANSITION)
    .addTransition(ContainerState.EXITED_WITH_SUCCESS,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.EXITED_WITH_SUCCESS,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.KILL_CONTAINER)

    // From EXITED_WITH_FAILURE State
    .addTransition(ContainerState.EXITED_WITH_FAILURE, ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
            CONTAINER_DONE_TRANSITION)
    .addTransition(ContainerState.EXITED_WITH_FAILURE,
        ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.EXITED_WITH_FAILURE,
                   ContainerState.EXITED_WITH_FAILURE,
                   ContainerEventType.KILL_CONTAINER)

    // From KILLING State.
    .addTransition(ContainerState.KILLING,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
        new ContainerKilledTransition())
    .addTransition(ContainerState.KILLING,
        ContainerState.KILLING,
        ContainerEventType.RESOURCE_LOCALIZED,
        new LocalizedResourceDuringKillTransition())
    .addTransition(ContainerState.KILLING, 
        ContainerState.KILLING, 
        ContainerEventType.RESOURCE_FAILED)
    .addTransition(ContainerState.KILLING, ContainerState.KILLING,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.KILLING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER)
    .addTransition(ContainerState.KILLING, ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
        new ExitedWithSuccessTransition(false))
    .addTransition(ContainerState.KILLING, ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition(false))
    .addTransition(ContainerState.KILLING,
            ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
            CONTAINER_DONE_TRANSITION)
    // Handle a launched container during killing stage is a no-op
    // as cleanup container is always handled after launch container event
    // in the container launcher
    .addTransition(ContainerState.KILLING,
        ContainerState.KILLING,
        ContainerEventType.CONTAINER_LAUNCHED)

    // From CONTAINER_CLEANEDUP_AFTER_KILL State.
    .addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
            ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
            CONTAINER_DONE_TRANSITION)
    .addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerEventType.KILL_CONTAINER)

    // From DONE
    .addTransition(ContainerState.DONE, ContainerState.DONE,
        ContainerEventType.KILL_CONTAINER)
    .addTransition(ContainerState.DONE, ContainerState.DONE,
        ContainerEventType.INIT_CONTAINER)
    .addTransition(ContainerState.DONE, ContainerState.DONE,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    // This transition may result when
    // we notify container of failed localization if localizer thread (for
    // that container) fails for some reason
    .addTransition(ContainerState.DONE, ContainerState.DONE,
        ContainerEventType.RESOURCE_FAILED)

    // create the topology tables
    .installTopology();

  private final StateMachine<ContainerState, ContainerEventType, ContainerEvent>
    stateMachine;

  public org.apache.hadoop.yarn.api.records.ContainerState getCurrentState() {
    switch (stateMachine.getCurrentState()) {
    case NEW:
    case LOCALIZING:
    case LOCALIZATION_FAILED:
    case LOCALIZED:
    case RUNNING:
    case EXITED_WITH_SUCCESS:
    case EXITED_WITH_FAILURE:
    case KILLING:
    case CONTAINER_CLEANEDUP_AFTER_KILL:
    case CONTAINER_RESOURCES_CLEANINGUP:
      return org.apache.hadoop.yarn.api.records.ContainerState.RUNNING;
    case DONE:
    default:
      return org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE;
    }
  }

  @Override
  public String getUser() {
    this.readLock.lock();
    try {
      return this.user;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<Path,List<String>> getLocalizedResources() {
    this.readLock.lock();
    try {
      if (ContainerState.LOCALIZED == getContainerState()) {
        return localizedResources;
      } else {
        return null;
      }
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Credentials getCredentials() {
    this.readLock.lock();
    try {
      return credentials;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerState getContainerState() {
    this.readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerLaunchContext getLaunchContext() {
    this.readLock.lock();
    try {
      return launchContext;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerStatus cloneAndGetContainerStatus() {
    this.readLock.lock();
    try {
      return BuilderUtils.newContainerStatus(this.containerId,
        getCurrentState(), diagnostics.toString(), exitCode);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public NMContainerStatus getNMContainerStatus() {
    this.readLock.lock();
    try {
      return NMContainerStatus.newInstance(this.containerId, getCurrentState(),
        getResource(), diagnostics.toString(), exitCode,
        containerTokenIdentifier.getPriority(),
        containerTokenIdentifier.getCreationTime());
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerId getContainerId() {
    return this.containerId;
  }

  @Override
  public Resource getResource() {
    return this.resource;
  }

  @Override
  public ContainerTokenIdentifier getContainerTokenIdentifier() {
    this.readLock.lock();
    try {
      return this.containerTokenIdentifier;
    } finally {
      this.readLock.unlock();
    }
  }

  @SuppressWarnings({"fallthrough", "unchecked"})
  private void finished() {
    ApplicationId applicationId =
        containerId.getApplicationAttemptId().getApplicationId();
    switch (getContainerState()) {
      case EXITED_WITH_SUCCESS:
        metrics.endRunningContainer();
        metrics.completedContainer();
        NMAuditLogger.logSuccess(user,
            AuditConstants.FINISH_SUCCESS_CONTAINER, "ContainerImpl",
            applicationId, containerId);
        break;
      case EXITED_WITH_FAILURE:
        if (wasLaunched) {
          metrics.endRunningContainer();
        }
        // fall through
      case LOCALIZATION_FAILED:
        metrics.failedContainer();
        NMAuditLogger.logFailure(user,
            AuditConstants.FINISH_FAILED_CONTAINER, "ContainerImpl",
            "Container failed with state: " + getContainerState(),
            applicationId, containerId);
        break;
      case CONTAINER_CLEANEDUP_AFTER_KILL:
        if (wasLaunched) {
          metrics.endRunningContainer();
        }
        // fall through
      case NEW:
        metrics.killedContainer();
        NMAuditLogger.logSuccess(user,
            AuditConstants.FINISH_KILLED_CONTAINER, "ContainerImpl",
            applicationId,
            containerId);
    }

    metrics.releaseContainer(this.resource);

    // Inform the application
    @SuppressWarnings("rawtypes")
    EventHandler eventHandler = dispatcher.getEventHandler();
    eventHandler.handle(new ApplicationContainerFinishedEvent(containerId));
    // Remove the container from the resource-monitor
    eventHandler.handle(new ContainerStopMonitoringEvent(containerId));
    // Tell the logService too
    eventHandler.handle(new LogHandlerContainerFinishedEvent(
      containerId, exitCode));
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  public void cleanup() {
    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrc =
      new HashMap<LocalResourceVisibility, 
                  Collection<LocalResourceRequest>>();
    if (!publicRsrcs.isEmpty()) {
      rsrc.put(LocalResourceVisibility.PUBLIC, publicRsrcs);
    }
    if (!privateRsrcs.isEmpty()) {
      rsrc.put(LocalResourceVisibility.PRIVATE, privateRsrcs);
    }
    if (!appRsrcs.isEmpty()) {
      rsrc.put(LocalResourceVisibility.APPLICATION, appRsrcs);
    }
    dispatcher.getEventHandler().handle(
        new ContainerLocalizationCleanupEvent(this, rsrc));
  }

  static class ContainerTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Just drain the event and change the state.
    }

  }

  /**
   * State transition when a NEW container receives the INIT_CONTAINER
   * message.
   * 
   * If there are resources to localize, sends a
   * ContainerLocalizationRequest (INIT_CONTAINER_RESOURCES) 
   * to the ResourceLocalizationManager and enters LOCALIZING state.
   * 
   * If there are no resources to localize, sends LAUNCH_CONTAINER event
   * and enters LOCALIZED state directly.
   * 
   * If there are any invalid resources specified, enters LOCALIZATION_FAILED
   * directly.
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  static class RequestResourcesTransition implements
      MultipleArcTransition<ContainerImpl,ContainerEvent,ContainerState> {
    @Override
    public ContainerState transition(ContainerImpl container,
        ContainerEvent event) {
      final ContainerLaunchContext ctxt = container.launchContext;
      container.metrics.initingContainer();

      container.dispatcher.getEventHandler().handle(new AuxServicesEvent
          (AuxServicesEventType.CONTAINER_INIT, container));

      // Inform the AuxServices about the opaque serviceData
      Map<String,ByteBuffer> csd = ctxt.getServiceData();
      if (csd != null) {
        // This can happen more than once per Application as each container may
        // have distinct service data
        for (Map.Entry<String,ByteBuffer> service : csd.entrySet()) {
          container.dispatcher.getEventHandler().handle(
              new AuxServicesEvent(AuxServicesEventType.APPLICATION_INIT,
                  container.user, container.containerId
                      .getApplicationAttemptId().getApplicationId(),
                  service.getKey().toString(), service.getValue()));
        }
      }

      // Send requests for public, private resources
      Map<String,LocalResource> cntrRsrc = ctxt.getLocalResources();
      if (!cntrRsrc.isEmpty()) {
        try {
          for (Map.Entry<String,LocalResource> rsrc : cntrRsrc.entrySet()) {
            try {
              LocalResourceRequest req =
                  new LocalResourceRequest(rsrc.getValue());
              List<String> links = container.pendingResources.get(req);
              if (links == null) {
                links = new ArrayList<String>();
                container.pendingResources.put(req, links);
              }
              links.add(rsrc.getKey());
              switch (rsrc.getValue().getVisibility()) {
              case PUBLIC:
                container.publicRsrcs.add(req);
                break;
              case PRIVATE:
                container.privateRsrcs.add(req);
                break;
              case APPLICATION:
                container.appRsrcs.add(req);
                break;
              }
            } catch (URISyntaxException e) {
              LOG.info("Got exception parsing " + rsrc.getKey()
                  + " and value " + rsrc.getValue());
              throw e;
            }
          }
        } catch (URISyntaxException e) {
          // malformed resource; abort container launch
          LOG.warn("Failed to parse resource-request", e);
          container.cleanup();
          container.metrics.endInitingContainer();
          return ContainerState.LOCALIZATION_FAILED;
        }
        Map<LocalResourceVisibility, Collection<LocalResourceRequest>> req =
            new HashMap<LocalResourceVisibility, 
                        Collection<LocalResourceRequest>>();
        if (!container.publicRsrcs.isEmpty()) {
          req.put(LocalResourceVisibility.PUBLIC, container.publicRsrcs);
        }
        if (!container.privateRsrcs.isEmpty()) {
          req.put(LocalResourceVisibility.PRIVATE, container.privateRsrcs);
        }
        if (!container.appRsrcs.isEmpty()) {
          req.put(LocalResourceVisibility.APPLICATION, container.appRsrcs);
        }
        
        container.dispatcher.getEventHandler().handle(
              new ContainerLocalizationRequestEvent(container, req));
        return ContainerState.LOCALIZING;
      } else {
        container.dispatcher.getEventHandler().handle(
            new ContainersLauncherEvent(container,
                ContainersLauncherEventType.LAUNCH_CONTAINER));
        container.metrics.endInitingContainer();
        return ContainerState.LOCALIZED;
      }
    }
  }

  /**
   * Transition when one of the requested resources for this container
   * has been successfully localized.
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  static class LocalizedTransition implements
      MultipleArcTransition<ContainerImpl,ContainerEvent,ContainerState> {
    @Override
    public ContainerState transition(ContainerImpl container,
        ContainerEvent event) {
      ContainerResourceLocalizedEvent rsrcEvent = (ContainerResourceLocalizedEvent) event;
      List<String> syms =
          container.pendingResources.remove(rsrcEvent.getResource());
      if (null == syms) {
        LOG.warn("Localized unknown resource " + rsrcEvent.getResource() +
                 " for container " + container.containerId);
        assert false;
        // fail container?
        return ContainerState.LOCALIZING;
      }
      container.localizedResources.put(rsrcEvent.getLocation(), syms);
      if (!container.pendingResources.isEmpty()) {
        return ContainerState.LOCALIZING;
      }
      container.dispatcher.getEventHandler().handle(
          new ContainersLauncherEvent(container,
              ContainersLauncherEventType.LAUNCH_CONTAINER));
      container.metrics.endInitingContainer();
      return ContainerState.LOCALIZED;
    }
  }

  /**
   * Transition from LOCALIZED state to RUNNING state upon receiving
   * a CONTAINER_LAUNCHED event
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  static class LaunchTransition extends ContainerTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Inform the ContainersMonitor to start monitoring the container's
      // resource usage.
      long pmemBytes =
          container.getResource().getMemory() * 1024 * 1024L;
      float pmemRatio = container.daemonConf.getFloat(
          YarnConfiguration.NM_VMEM_PMEM_RATIO,
          YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO);
      long vmemBytes = (long) (pmemRatio * pmemBytes);
      
      container.dispatcher.getEventHandler().handle(
          new ContainerStartMonitoringEvent(container.containerId,
              vmemBytes, pmemBytes));
      container.metrics.runningContainer();
      container.wasLaunched  = true;
    }
  }

  /**
   * Transition from RUNNING or KILLING state to EXITED_WITH_SUCCESS state
   * upon EXITED_WITH_SUCCESS message.
   */
  @SuppressWarnings("unchecked")  // dispatcher not typed
  static class ExitedWithSuccessTransition extends ContainerTransition {

    boolean clCleanupRequired;

    public ExitedWithSuccessTransition(boolean clCleanupRequired) {
      this.clCleanupRequired = clCleanupRequired;
    }

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Set exit code to 0 on success    	
      container.exitCode = 0;
    	
      // TODO: Add containerWorkDir to the deletion service.

      if (clCleanupRequired) {
        container.dispatcher.getEventHandler().handle(
            new ContainersLauncherEvent(container,
                ContainersLauncherEventType.CLEANUP_CONTAINER));
      }

      container.cleanup();
    }
  }

  /**
   * Transition to EXITED_WITH_FAILURE state upon
   * CONTAINER_EXITED_WITH_FAILURE state.
   **/
  @SuppressWarnings("unchecked")  // dispatcher not typed
  static class ExitedWithFailureTransition extends ContainerTransition {

    boolean clCleanupRequired;

    public ExitedWithFailureTransition(boolean clCleanupRequired) {
      this.clCleanupRequired = clCleanupRequired;
    }

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerExitEvent exitEvent = (ContainerExitEvent) event;
      container.exitCode = exitEvent.getExitCode();
      if (exitEvent.getDiagnosticInfo() != null) {
        container.diagnostics.append(exitEvent.getDiagnosticInfo())
          .append('\n');
      }

      // TODO: Add containerWorkDir to the deletion service.
      // TODO: Add containerOuputDir to the deletion service.

      if (clCleanupRequired) {
        container.dispatcher.getEventHandler().handle(
            new ContainersLauncherEvent(container,
                ContainersLauncherEventType.CLEANUP_CONTAINER));
      }

      container.cleanup();
    }
  }

  /**
   * Transition to EXITED_WITH_FAILURE upon receiving KILLED_ON_REQUEST
   */
  static class KilledExternallyTransition extends ExitedWithFailureTransition {
    KilledExternallyTransition() {
      super(true);
    }

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      super.transition(container, event);
      container.diagnostics.append("Killed by external signal\n");
    }
  }

  /**
   * Transition from LOCALIZING to LOCALIZATION_FAILED upon receiving
   * RESOURCE_FAILED event.
   */
  static class ResourceFailedTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {

      ContainerResourceFailedEvent rsrcFailedEvent =
          (ContainerResourceFailedEvent) event;
      container.diagnostics.append(rsrcFailedEvent.getDiagnosticMessage()
          + "\n");
          

      // Inform the localizer to decrement reference counts and cleanup
      // resources.
      container.cleanup();
      container.metrics.endInitingContainer();
    }
  }

  /**
   * Transition from LOCALIZING to KILLING upon receiving
   * KILL_CONTAINER event.
   */
  static class KillDuringLocalizationTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Inform the localizer to decrement reference counts and cleanup
      // resources.
      container.cleanup();
      container.metrics.endInitingContainer();
      ContainerKillEvent killEvent = (ContainerKillEvent) event;
      container.exitCode = killEvent.getContainerExitStatus();
      container.diagnostics.append(killEvent.getDiagnostic()).append("\n");
      container.diagnostics.append("Container is killed before being launched.\n");
    }
  }

  /**
   * Remain in KILLING state when receiving a RESOURCE_LOCALIZED request
   * while in the process of killing.
   */
  static class LocalizedResourceDuringKillTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerResourceLocalizedEvent rsrcEvent = (ContainerResourceLocalizedEvent) event;
      List<String> syms =
          container.pendingResources.remove(rsrcEvent.getResource());
      if (null == syms) {
        LOG.warn("Localized unknown resource " + rsrcEvent.getResource() +
                 " for container " + container.containerId);
        assert false;
        // fail container?
        return;
      }
      container.localizedResources.put(rsrcEvent.getLocation(), syms);
    }
  }

  /**
   * Transitions upon receiving KILL_CONTAINER:
   * - LOCALIZED -> KILLING
   * - RUNNING -> KILLING
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  static class KillTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Kill the process/process-grp
      container.dispatcher.getEventHandler().handle(
          new ContainersLauncherEvent(container,
              ContainersLauncherEventType.CLEANUP_CONTAINER));
      ContainerKillEvent killEvent = (ContainerKillEvent) event;
      container.diagnostics.append(killEvent.getDiagnostic()).append("\n");
      container.exitCode = killEvent.getContainerExitStatus();
    }
  }

  /**
   * Transition from KILLING to CONTAINER_CLEANEDUP_AFTER_KILL
   * upon receiving CONTAINER_KILLED_ON_REQUEST.
   */
  static class ContainerKilledTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerExitEvent exitEvent = (ContainerExitEvent) event;
      if (container.hasDefaultExitCode()) {
        container.exitCode = exitEvent.getExitCode();
      }

      if (exitEvent.getDiagnosticInfo() != null) {
        container.diagnostics.append(exitEvent.getDiagnosticInfo())
          .append('\n');
      }

      // The process/process-grp is killed. Decrement reference counts and
      // cleanup resources
      container.cleanup();
    }
  }

  /**
   * Handle the following transitions:
   * - {LOCALIZATION_FAILED, EXITED_WITH_SUCCESS, EXITED_WITH_FAILURE,
   *    KILLING, CONTAINER_CLEANEDUP_AFTER_KILL}
   *   -> DONE upon CONTAINER_RESOURCES_CLEANEDUP
   */
  static class ContainerDoneTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    @SuppressWarnings("unchecked")
    public void transition(ContainerImpl container, ContainerEvent event) {
      container.finished();
      //if the current state is NEW it means the CONTAINER_INIT was never 
      // sent for the event, thus no need to send the CONTAINER_STOP
      if (container.getCurrentState() 
          != org.apache.hadoop.yarn.api.records.ContainerState.NEW) {
        container.dispatcher.getEventHandler().handle(new AuxServicesEvent
            (AuxServicesEventType.CONTAINER_STOP, container));
      }
    }
  }

  /**
   * Handle the following transition:
   * - NEW -> DONE upon KILL_CONTAINER
   */
  static class KillOnNewTransition extends ContainerDoneTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerKillEvent killEvent = (ContainerKillEvent) event;
      container.exitCode = killEvent.getContainerExitStatus();
      container.diagnostics.append(killEvent.getDiagnostic()).append("\n");
      container.diagnostics.append("Container is killed before being launched.\n");
      super.transition(container, event);
    }
  }

  /**
   * Update diagnostics, staying in the same state.
   */
  static class ContainerDiagnosticsUpdateTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerDiagnosticsUpdateEvent updateEvent =
          (ContainerDiagnosticsUpdateEvent) event;
      container.diagnostics.append(updateEvent.getDiagnosticsUpdate())
          .append("\n");
    }
  }

  @Override
  public void handle(ContainerEvent event) {
    try {
      this.writeLock.lock();

      ContainerId containerID = event.getContainerID();
      LOG.debug("Processing " + containerID + " of type " + event.getType());

      ContainerState oldState = stateMachine.getCurrentState();
      ContainerState newState = null;
      try {
        newState =
            stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.warn("Can't handle this event at current state: Current: ["
            + oldState + "], eventType: [" + event.getType() + "]", e);
      }
      if (oldState != newState) {
        LOG.info("Container " + containerID + " transitioned from "
            + oldState
            + " to " + newState);
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public String toString() {
    this.readLock.lock();
    try {
      return ConverterUtils.toString(this.containerId);
    } finally {
      this.readLock.unlock();
    }
  }

  private boolean hasDefaultExitCode() {
    return (this.exitCode == ContainerExitStatus.INVALID);
  }
}
