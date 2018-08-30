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

package org.apache.hadoop.yarn.service.component.instance;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.service.ServiceScheduler;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.ComponentState;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.component.ComponentEvent;
import org.apache.hadoop.yarn.service.component.ComponentEventType;
import org.apache.hadoop.yarn.service.component.ComponentRestartPolicy;
import org.apache.hadoop.yarn.service.monitor.probe.ProbeStatus;
import org.apache.hadoop.yarn.service.registry.YarnRegistryViewForProviders;
import org.apache.hadoop.yarn.service.timelineservice.ServiceTimelinePublisher;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.BoundedAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import static org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes.*;

import static org.apache.hadoop.yarn.api.records.ContainerExitStatus
    .KILLED_AFTER_APP_COMPLETION;
import static org.apache.hadoop.yarn.api.records.ContainerExitStatus.KILLED_BY_APPMASTER;
import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType.*;
import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceState.*;

public class ComponentInstance implements EventHandler<ComponentInstanceEvent>,
    Comparable<ComponentInstance> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ComponentInstance.class);
  private static final String FAILED_BEFORE_LAUNCH_DIAG =
      "failed before launch";

  private  StateMachine<ComponentInstanceState, ComponentInstanceEventType,
      ComponentInstanceEvent> stateMachine;
  private Component component;
  private final ReadLock readLock;
  private final WriteLock writeLock;

  private ComponentInstanceId compInstanceId = null;
  private Path compInstanceDir;
  private Container container;
  private YarnRegistryViewForProviders yarnRegistryOperations;
  private FileSystem fs;
  private boolean timelineServiceEnabled = false;
  private ServiceTimelinePublisher serviceTimelinePublisher;
  private ServiceScheduler scheduler;
  private BoundedAppender diagnostics = new BoundedAppender(64 * 1024);
  private volatile ScheduledFuture containerStatusFuture;
  private volatile ContainerStatus status;
  private long containerStartedTime = 0;
  // This container object is used for rest API query
  private org.apache.hadoop.yarn.service.api.records.Container containerSpec;
  private String serviceVersion;


  private static final StateMachineFactory<ComponentInstance,
      ComponentInstanceState, ComponentInstanceEventType,
      ComponentInstanceEvent>
      stateMachineFactory =
      new StateMachineFactory<ComponentInstance, ComponentInstanceState,
          ComponentInstanceEventType, ComponentInstanceEvent>(INIT)
      .addTransition(INIT, STARTED, START,
          new ContainerStartedTransition())
      .addTransition(INIT, INIT, STOP,
          // container failed before launching, nothing to cleanup from registry
          // This could happen if NMClient#startContainerAsync failed, container
          // will be completed, but COMP_INSTANCE is still at INIT.
          new ContainerStoppedTransition(true))

      //From Running
      .addTransition(STARTED, INIT, STOP,
          new ContainerStoppedTransition())
      .addTransition(STARTED, READY, BECOME_READY,
          new ContainerBecomeReadyTransition())

      // FROM READY
      .addTransition(READY, STARTED, BECOME_NOT_READY,
          new ContainerBecomeNotReadyTransition())
      .addTransition(READY, INIT, STOP, new ContainerStoppedTransition())
      .addTransition(READY, UPGRADING, UPGRADE,
          new ContainerUpgradeTransition())
      .addTransition(UPGRADING, UPGRADING, UPGRADE,
          new ContainerUpgradeTransition())
      .addTransition(UPGRADING, READY, BECOME_READY,
          new ContainerBecomeReadyTransition())
      .addTransition(UPGRADING, INIT, STOP, new ContainerStoppedTransition())
      .installTopology();

  public ComponentInstance(Component component,
      ComponentInstanceId compInstanceId) {
    this.stateMachine = stateMachineFactory.make(this);
    this.component = component;
    this.compInstanceId = compInstanceId;
    this.scheduler = component.getScheduler();
    this.yarnRegistryOperations =
        component.getScheduler().getYarnRegistryOperations();
    this.serviceTimelinePublisher =
        component.getScheduler().getServiceTimelinePublisher();
    if (YarnConfiguration
        .timelineServiceV2Enabled(component.getScheduler().getConfig())) {
      this.timelineServiceEnabled = true;
    }
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
    this.fs = scheduler.getContext().fs.getFileSystem();
  }

  private static class ContainerStartedTransition extends  BaseTransition {
    @Override public void transition(ComponentInstance compInstance,
        ComponentInstanceEvent event) {
      // Query container status for ip and host
      boolean cancelOnSuccess = true;
      if (compInstance.getCompSpec().getArtifact() != null && compInstance
          .getCompSpec().getArtifact().getType() == Artifact.TypeEnum.DOCKER) {
        // A docker container might get a different IP if the container is
        // relaunched by the NM, so we need to keep checking the status.
        // This is a temporary fix until the NM provides a callback for
        // container relaunch (see YARN-8265).
        cancelOnSuccess = false;
      }
      compInstance.containerStatusFuture =
          compInstance.scheduler.executorService.scheduleAtFixedRate(
              new ContainerStatusRetriever(compInstance.scheduler,
                  event.getContainerId(), compInstance, cancelOnSuccess), 0, 1,
              TimeUnit.SECONDS);
      long containerStartTime = System.currentTimeMillis();
      try {
        ContainerTokenIdentifier containerTokenIdentifier = BuilderUtils
            .newContainerTokenIdentifier(compInstance.getContainer()
                .getContainerToken());
        containerStartTime = containerTokenIdentifier.getCreationTime();
      } catch (Exception e) {
        LOG.info("Could not get container creation time, using current time");
      }
      org.apache.hadoop.yarn.service.api.records.Container container =
          new org.apache.hadoop.yarn.service.api.records.Container();
      container.setId(event.getContainerId().toString());
      container.setLaunchTime(new Date(containerStartTime));
      container.setState(ContainerState.RUNNING_BUT_UNREADY);
      container.setBareHost(compInstance.getNodeId().getHost());
      container.setComponentInstanceName(compInstance.getCompInstanceName());
      if (compInstance.containerSpec != null) {
        // remove the previous container.
        compInstance.getCompSpec().removeContainer(compInstance.containerSpec);
      }
      compInstance.containerSpec = container;
      compInstance.getCompSpec().addContainer(container);
      compInstance.containerStartedTime = containerStartTime;
      compInstance.component.incRunningContainers();
      compInstance.serviceVersion = compInstance.scheduler.getApp()
          .getVersion();

      if (compInstance.timelineServiceEnabled) {
        compInstance.serviceTimelinePublisher
            .componentInstanceStarted(container, compInstance);
      }
    }
  }

  private static class ContainerBecomeReadyTransition extends BaseTransition {
    @Override
    public void transition(ComponentInstance compInstance,
        ComponentInstanceEvent event) {
      compInstance.containerSpec.setState(ContainerState.READY);
      if (compInstance.getState().equals(ComponentInstanceState.UPGRADING)) {
        compInstance.component.incContainersReady(false);
        compInstance.component.decContainersThatNeedUpgrade();
        compInstance.serviceVersion = compInstance.component.getUpgradeEvent()
            .getUpgradeVersion();
        ComponentEvent checkState = new ComponentEvent(
            compInstance.component.getName(), ComponentEventType.CHECK_STABLE);
        compInstance.scheduler.getDispatcher().getEventHandler().handle(
            checkState);

      } else {
        compInstance.component.incContainersReady(true);
      }
      if (compInstance.timelineServiceEnabled) {
        compInstance.serviceTimelinePublisher
            .componentInstanceBecomeReady(compInstance.containerSpec);
      }
    }
  }

  private static class ContainerBecomeNotReadyTransition extends BaseTransition {
    @Override
    public void transition(ComponentInstance compInstance,
        ComponentInstanceEvent event) {
      compInstance.containerSpec.setState(ContainerState.RUNNING_BUT_UNREADY);
      compInstance.component.decContainersReady(true);
    }
  }

  @VisibleForTesting
  static void handleComponentInstanceRelaunch(ComponentInstance compInstance,
      ComponentInstanceEvent event, boolean failureBeforeLaunch,
      String containerDiag) {
    Component comp = compInstance.getComponent();

    // Do we need to relaunch the service?
    boolean hasContainerFailed = failureBeforeLaunch || hasContainerFailed(
        event.getStatus());

    ComponentRestartPolicy restartPolicy = comp.getRestartPolicyHandler();
    ContainerState containerState =
        hasContainerFailed ? ContainerState.FAILED : ContainerState.SUCCEEDED;

    if (compInstance.getContainerSpec() != null) {
      compInstance.getContainerSpec().setState(containerState);
    }

    if (restartPolicy.shouldRelaunchInstance(compInstance, event.getStatus())) {
      // re-ask the failed container.
      comp.requestContainers(1);
      comp.reInsertPendingInstance(compInstance);

      StringBuilder builder = new StringBuilder();
      builder.append(compInstance.getCompInstanceId()).append(": ");
      builder.append(event.getContainerId()).append(
          " completed. Reinsert back to pending list and requested ");
      builder.append("a new container.").append(System.lineSeparator());
      builder.append(" exitStatus=").append(
          failureBeforeLaunch ? null : event.getStatus().getExitStatus());
      builder.append(", diagnostics=");
      builder.append(failureBeforeLaunch ?
          FAILED_BEFORE_LAUNCH_DIAG :
          event.getStatus().getDiagnostics());

      if (event.getStatus() != null && event.getStatus().getExitStatus() != 0) {
        LOG.error(builder.toString());
      } else{
        LOG.info(builder.toString());
      }

      if (compInstance.timelineServiceEnabled) {
        // record in ATS
        LOG.info("Publishing component instance status {} {} ",
            event.getContainerId(), containerState);
        compInstance.serviceTimelinePublisher.componentInstanceFinished(
            event.getContainerId(), event.getStatus().getExitStatus(),
            containerState, containerDiag);
      }

    } else{
      // When no relaunch, update component's #succeeded/#failed
      // instances.
      if (hasContainerFailed) {
        comp.markAsFailed(compInstance);
      } else{
        comp.markAsSucceeded(compInstance);
      }

      if (compInstance.timelineServiceEnabled) {
        // record in ATS
        compInstance.serviceTimelinePublisher.componentInstanceFinished(
            event.getContainerId(), event.getStatus().getExitStatus(),
            containerState, containerDiag);
      }

      LOG.info(compInstance.getCompInstanceId() + (!hasContainerFailed ?
          " succeeded" :
          " failed") + " without retry, exitStatus=" + event.getStatus());
      comp.getScheduler().terminateServiceIfAllComponentsFinished();
    }
  }

  public static boolean hasContainerFailed(ContainerStatus containerStatus) {
    //Mark conainer as failed if we cant get its exit status i.e null?
    return containerStatus == null || containerStatus
        .getExitStatus() != ContainerExitStatus.SUCCESS;
  }

  private static class ContainerStoppedTransition extends  BaseTransition {
    // whether the container failed before launched by AM or not.
    boolean failedBeforeLaunching = false;
    public ContainerStoppedTransition(boolean failedBeforeLaunching) {
      this.failedBeforeLaunching = failedBeforeLaunching;
    }

    public ContainerStoppedTransition() {
      this(false);
    }

    @Override
    public void transition(ComponentInstance compInstance,
        ComponentInstanceEvent event) {

      Component comp = compInstance.component;
      String containerDiag = compInstance.getCompInstanceId() + ": " + (
          failedBeforeLaunching ?
              FAILED_BEFORE_LAUNCH_DIAG :
              event.getStatus().getDiagnostics());
      compInstance.diagnostics.append(containerDiag + System.lineSeparator());
      compInstance.cancelContainerStatusRetriever();
      if (compInstance.getState().equals(ComponentInstanceState.UPGRADING)) {
        compInstance.component.decContainersThatNeedUpgrade();
      }
      if (compInstance.getState().equals(READY)) {
        compInstance.component.decContainersReady(true);
      }
      compInstance.component.decRunningContainers();
      // Should we fail (terminate) the service?
      boolean shouldFailService = false;

      final ServiceScheduler scheduler = comp.getScheduler();
      scheduler.getAmRMClient().releaseAssignedContainer(
          event.getContainerId());

      // Check if it exceeds the failure threshold, but only if health threshold
      // monitor is not enabled
      if (!comp.isHealthThresholdMonitorEnabled()
          && comp.currentContainerFailure.get()
          > comp.maxContainerFailurePerComp) {
        String exitDiag = MessageFormat.format(
            "[COMPONENT {0}]: Failed {1} times, exceeded the limit - {2}. "
                + "Shutting down now... "
                + System.lineSeparator(), comp.getName(),
            comp.currentContainerFailure.get(),
            comp.maxContainerFailurePerComp);
        compInstance.diagnostics.append(exitDiag);
        // append to global diagnostics that will be reported to RM.
        scheduler.getDiagnostics().append(containerDiag);
        scheduler.getDiagnostics().append(exitDiag);
        LOG.warn(exitDiag);

        compInstance.getContainerSpec().setState(ContainerState.FAILED);
        comp.getComponentSpec().setState(ComponentState.FAILED);
        comp.getScheduler().getApp().setState(ServiceState.FAILED);

        if (compInstance.timelineServiceEnabled) {
          // record in ATS
          compInstance.scheduler.getServiceTimelinePublisher()
              .componentInstanceFinished(compInstance.getContainer().getId(),
                  failedBeforeLaunching ?
                      -1 :
                      event.getStatus().getExitStatus(), ContainerState.FAILED,
                  containerDiag);

          // mark other component-instances/containers as STOPPED
          for (ContainerId containerId : scheduler.getLiveInstances()
              .keySet()) {
            if (!compInstance.container.getId().equals(containerId)
                && !isFinalState(compInstance.getContainerSpec().getState())) {
              compInstance.getContainerSpec().setState(ContainerState.STOPPED);
              compInstance.scheduler.getServiceTimelinePublisher()
                  .componentInstanceFinished(containerId,
                      KILLED_AFTER_APP_COMPLETION, ContainerState.STOPPED,
                      scheduler.getDiagnostics().toString());
            }
          }

          compInstance.scheduler.getServiceTimelinePublisher()
              .componentFinished(comp.getComponentSpec(), ComponentState.FAILED,
                  scheduler.getSystemClock().getTime());

          compInstance.scheduler.getServiceTimelinePublisher()
              .serviceAttemptUnregistered(comp.getContext(),
                  FinalApplicationStatus.FAILED,
                  scheduler.getDiagnostics().toString());
        }

        shouldFailService = true;
      }

      if (!failedBeforeLaunching) {
        // clean up registry
        // If the container failed before launching, no need to cleanup
        // registry,
        // because it was not registered before.
        // hdfs dir content will be overwritten when a new container gets
        // started,
        // so no need remove.
        compInstance.scheduler.executorService.submit(
            () -> compInstance.cleanupRegistry(event.getContainerId()));
      }

      // remove the failed ContainerId -> CompInstance mapping
      scheduler.removeLiveCompInstance(event.getContainerId());

      // According to component restart policy, handle container restart
      // or finish the service (if all components finished)
      handleComponentInstanceRelaunch(compInstance, event,
          failedBeforeLaunching, containerDiag);

      if (shouldFailService) {
        scheduler.getTerminationHandler().terminate(-1);
      }
    }
  }

  public static boolean isFinalState(ContainerState state) {
    return ContainerState.FAILED.equals(state) || ContainerState.STOPPED
        .equals(state) || ContainerState.SUCCEEDED.equals(state);
  }

  private static class ContainerUpgradeTransition extends BaseTransition {

    @Override
    public void transition(ComponentInstance compInstance,
        ComponentInstanceEvent event) {
      if (!compInstance.containerSpec.getState().equals(
          ContainerState.NEEDS_UPGRADE)) {
        //nothing to upgrade. this may happen with express upgrade.
        return;
      }
      compInstance.containerSpec.setState(ContainerState.UPGRADING);
      compInstance.component.decContainersReady(false);
      ComponentEvent upgradeEvent = compInstance.component.getUpgradeEvent();
      compInstance.scheduler.getContainerLaunchService()
          .reInitCompInstance(compInstance.scheduler.getApp(), compInstance,
              compInstance.container,
              compInstance.component.createLaunchContext(
                  upgradeEvent.getTargetSpec(),
                  upgradeEvent.getUpgradeVersion()));
    }
  }

  public ComponentInstanceState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Returns the version of service at which the instance is at.
   */
  public String getServiceVersion() {
    this.readLock.lock();
    try {
      return this.serviceVersion;
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Returns the state of the container in the container spec.
   */
  public ContainerState getContainerState() {
    this.readLock.lock();
    try {
      return this.containerSpec.getState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public void handle(ComponentInstanceEvent event) {
    try {
      writeLock.lock();
      ComponentInstanceState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.error(getCompInstanceId() + ": Invalid event " + event.getType() +
            " at " + oldState, e);
      }
      if (oldState != getState()) {
        LOG.info(getCompInstanceId() + " Transitioned from " + oldState + " to "
            + getState() + " on " + event.getType() + " event");
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void setContainer(Container container) {
    this.container = container;
    this.compInstanceId.setContainerId(container.getId());
  }

  public String getCompInstanceName() {
    return compInstanceId.getCompInstanceName();
  }

  public ContainerStatus getContainerStatus() {
    return status;
  }

  public void updateContainerStatus(ContainerStatus status) {
    this.status = status;
    org.apache.hadoop.yarn.service.api.records.Container container =
        getCompSpec().getContainer(status.getContainerId().toString());
    boolean doRegistryUpdate = true;
    if (container != null) {
      String existingIP = container.getIp();
      String newIP = StringUtils.join(",", status.getIPs());
      container.setIp(newIP);
      container.setHostname(status.getHost());
      if (existingIP != null && newIP.equals(existingIP)) {
        doRegistryUpdate = false;
      }
      if (timelineServiceEnabled && doRegistryUpdate) {
        serviceTimelinePublisher.componentInstanceIPHostUpdated(container);
      }
    }
    if (doRegistryUpdate) {
      cleanupRegistry(status.getContainerId());
      LOG.info(
          getCompInstanceId() + " new IP = " + status.getIPs() + ", host = "
              + status.getHost() + ", updating registry");
      updateServiceRecord(yarnRegistryOperations, status);
    }
  }

  public String getCompName() {
    return compInstanceId.getCompName();
  }

  public void setCompInstanceDir(Path dir) {
    this.compInstanceDir = dir;
  }

  public Component getComponent() {
    return component;
  }

  public Container getContainer() {
    return container;
  }

  public ComponentInstanceId getCompInstanceId() {
    return compInstanceId;
  }

  public NodeId getNodeId() {
    return this.container.getNodeId();
  }

  private org.apache.hadoop.yarn.service.api.records.Component getCompSpec() {
    return component.getComponentSpec();
  }

  private static class BaseTransition implements
      SingleArcTransition<ComponentInstance, ComponentInstanceEvent> {

    @Override public void transition(ComponentInstance compInstance,
        ComponentInstanceEvent event) {
    }
  }

  public ProbeStatus ping() {
    if (component.getProbe() == null) {
      ProbeStatus status = new ProbeStatus();
      status.setSuccess(true);
      return status;
    }
    return component.getProbe().ping(this);
  }

  // Write service record into registry
  private  void updateServiceRecord(
      YarnRegistryViewForProviders yarnRegistry, ContainerStatus status) {
    ServiceRecord record = new ServiceRecord();
    String containerId = status.getContainerId().toString();
    record.set(YARN_ID, containerId);
    record.description = getCompInstanceName();
    record.set(YARN_PERSISTENCE, PersistencePolicies.CONTAINER);
    record.set(YARN_IP, status.getIPs().get(0));
    record.set(YARN_HOSTNAME, status.getHost());
    record.set(YARN_COMPONENT, component.getName());
    try {
      yarnRegistry
          .putComponent(RegistryPathUtils.encodeYarnID(containerId), record);
    } catch (IOException e) {
      LOG.error(
          "Failed to update service record in registry: " + containerId + "");
    }
  }

  // Called when user flexed down the container and ContainerStoppedTransition
  // is not executed in this case.
  // Release the container, dec running,
  // cleanup registry, hdfs dir, and send record to ATS
  public void destroy() {
    LOG.info(getCompInstanceId() + ": Flexed down by user, destroying.");
    diagnostics.append(getCompInstanceId() + ": Flexed down by user");

    // update metrics
    if (getState() == STARTED) {
      component.decRunningContainers();
    }
    if (getState() == READY) {
      component.decContainersReady(true);
      component.decRunningContainers();
    }
    getCompSpec().removeContainer(containerSpec);

    if (container == null) {
      LOG.info(getCompInstanceId() + " no container is assigned when " +
          "destroying");
      return;
    }

    ContainerId containerId = container.getId();
    scheduler.removeLiveCompInstance(containerId);
    component.getScheduler().getAmRMClient()
        .releaseAssignedContainer(containerId);

    if (timelineServiceEnabled) {
      serviceTimelinePublisher.componentInstanceFinished(containerId,
          KILLED_BY_APPMASTER, ContainerState.STOPPED, diagnostics.toString());
    }
    cancelContainerStatusRetriever();
    scheduler.executorService.submit(() ->
        cleanupRegistryAndCompHdfsDir(containerId));
  }

  private void cleanupRegistry(ContainerId containerId) {
    String cid = RegistryPathUtils.encodeYarnID(containerId.toString());
    try {
       yarnRegistryOperations.deleteComponent(getCompInstanceId(), cid);
    } catch (IOException e) {
      LOG.error(getCompInstanceId() + ": Failed to delete registry", e);
    }
  }

  //TODO Maybe have a dedicated cleanup service.
  public void cleanupRegistryAndCompHdfsDir(ContainerId containerId) {
    cleanupRegistry(containerId);
    try {
      if (compInstanceDir != null && fs.exists(compInstanceDir)) {
        boolean deleted = fs.delete(compInstanceDir, true);
        if (!deleted) {
          LOG.error(getCompInstanceId()
              + ": Failed to delete component instance dir: "
              + compInstanceDir);
        } else {
          LOG.info(getCompInstanceId() + ": Deleted component instance dir: "
              + compInstanceDir);
        }
      }
    } catch (IOException e) {
      LOG.warn(getCompInstanceId() + ": Failed to delete directory", e);
    }
  }

  // Query container status until ip and hostname are available and update
  // the service record into registry service
  private static class ContainerStatusRetriever implements Runnable {
    private ContainerId containerId;
    private NodeId nodeId;
    private NMClient nmClient;
    private ComponentInstance instance;
    private boolean cancelOnSuccess;
    ContainerStatusRetriever(ServiceScheduler scheduler,
        ContainerId containerId, ComponentInstance instance, boolean
        cancelOnSuccess) {
      this.containerId = containerId;
      this.nodeId = instance.getNodeId();
      this.nmClient = scheduler.getNmClient().getClient();
      this.instance = instance;
      this.cancelOnSuccess = cancelOnSuccess;
    }
    @Override public void run() {
      ContainerStatus status = null;
      try {
        status = nmClient.getContainerStatus(containerId, nodeId);
      } catch (Exception e) {
        if (e instanceof YarnException) {
          throw new YarnRuntimeException(
              instance.compInstanceId + " Failed to get container status on "
                  + nodeId + " , cancelling.", e);
        }
        LOG.error(instance.compInstanceId + " Failed to get container status on "
            + nodeId + ", will try again", e);
        return;
      }
      if (ServiceUtils.isEmpty(status.getIPs()) || ServiceUtils
          .isUnset(status.getHost())) {
        return;
      }
      instance.updateContainerStatus(status);
      if (cancelOnSuccess) {
        LOG.info(
            instance.compInstanceId + " IP = " + status.getIPs() + ", host = "
                + status.getHost() + ", cancel container status retriever");
        instance.containerStatusFuture.cancel(false);
      }
    }
  }

  private void cancelContainerStatusRetriever() {
    if (containerStatusFuture != null && !containerStatusFuture.isDone()) {
      containerStatusFuture.cancel(true);
    }
  }

  public String getHostname() {
    String domain = getComponent().getScheduler().getConfig()
        .get(RegistryConstants.KEY_DNS_DOMAIN);
    String hostname;
    if (domain == null || domain.isEmpty()) {
      hostname = MessageFormat
          .format("{0}.{1}.{2}", getCompInstanceName(),
              getComponent().getContext().service.getName(),
              RegistryUtils.currentUser());
    } else {
      hostname = MessageFormat
          .format("{0}.{1}.{2}.{3}", getCompInstanceName(),
              getComponent().getContext().service.getName(),
              RegistryUtils.currentUser(), domain);
    }
    return hostname;
  }

  @Override
  public int compareTo(ComponentInstance to) {
    return getCompInstanceId().compareTo(to.getCompInstanceId());
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    ComponentInstance instance = (ComponentInstance) o;

    if (containerStartedTime != instance.containerStartedTime)
      return false;
    return compInstanceId.equals(instance.compInstanceId);
  }

  @Override public int hashCode() {
    int result = compInstanceId.hashCode();
    result = 31 * result + (int) (containerStartedTime ^ (containerStartedTime
        >>> 32));
    return result;
  }

  /**
   * Returns container spec.
   */
  public org.apache.hadoop.yarn.service.api.records
      .Container getContainerSpec() {
    readLock.lock();
    try {
      return containerSpec;
    } finally {
      readLock.unlock();
    }
  }
}
