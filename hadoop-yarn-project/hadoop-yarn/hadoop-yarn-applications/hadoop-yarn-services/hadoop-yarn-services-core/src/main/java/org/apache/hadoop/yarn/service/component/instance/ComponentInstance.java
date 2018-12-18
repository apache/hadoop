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
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
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
import org.apache.hadoop.yarn.service.monitor.probe.DefaultProbe;
import org.apache.hadoop.yarn.service.monitor.probe.ProbeStatus;
import org.apache.hadoop.yarn.service.registry.YarnRegistryViewForProviders;
import org.apache.hadoop.yarn.service.timelineservice.ServiceTimelinePublisher;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.BoundedAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private static final String UPGRADE_FAILED = "upgrade failed";

  private StateMachine<ComponentInstanceState, ComponentInstanceEventType,
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
  private AtomicBoolean upgradeInProgress = new AtomicBoolean(false);
  private boolean pendingCancelUpgrade = false;

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
          new ContainerBecomeReadyTransition(false))

      // FROM READY
      .addTransition(READY, STARTED, BECOME_NOT_READY,
          new ContainerBecomeNotReadyTransition())
      .addTransition(READY, INIT, STOP, new ContainerStoppedTransition())
      .addTransition(READY, UPGRADING, UPGRADE, new UpgradeTransition())
      .addTransition(READY, EnumSet.of(READY, CANCEL_UPGRADING), CANCEL_UPGRADE,
          new CancelUpgradeTransition())

      // FROM UPGRADING
      .addTransition(UPGRADING, EnumSet.of(READY, CANCEL_UPGRADING),
          CANCEL_UPGRADE, new CancelUpgradeTransition())
      .addTransition(UPGRADING, EnumSet.of(REINITIALIZED), START,
          new StartedAfterUpgradeTransition())
      .addTransition(UPGRADING, UPGRADING, STOP,
          new StoppedAfterUpgradeTransition())

      // FROM CANCEL_UPGRADING
      .addTransition(CANCEL_UPGRADING, EnumSet.of(CANCEL_UPGRADING,
          REINITIALIZED), START, new StartedAfterUpgradeTransition())
      .addTransition(CANCEL_UPGRADING, EnumSet.of(CANCEL_UPGRADING, INIT),
          STOP, new StoppedAfterCancelUpgradeTransition())

      // FROM REINITIALIZED
      .addTransition(REINITIALIZED, CANCEL_UPGRADING, CANCEL_UPGRADE,
          new CancelledAfterReinitTransition())
      .addTransition(REINITIALIZED, READY, BECOME_READY,
           new ContainerBecomeReadyTransition(true))
      .addTransition(REINITIALIZED, REINITIALIZED, STOP,
          new StoppedAfterUpgradeTransition())
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
      compInstance.initializeStatusRetriever(event, 0);
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
    private final boolean isReinitialized;

    ContainerBecomeReadyTransition(boolean isReinitialized) {
      this.isReinitialized = isReinitialized;
    }

    @Override
    public void transition(ComponentInstance compInstance,
        ComponentInstanceEvent event) {
      compInstance.setContainerState(ContainerState.READY);
      if (!isReinitialized) {
        compInstance.component.incContainersReady(true);
      } else {
        compInstance.component.incContainersReady(false);
        ComponentEvent checkState = new ComponentEvent(
            compInstance.component.getName(), ComponentEventType.CHECK_STABLE);
        compInstance.scheduler.getDispatcher().getEventHandler().handle(
            checkState);
      }
      compInstance.postContainerReady();
    }
  }

  private static class StartedAfterUpgradeTransition implements
      MultipleArcTransition<ComponentInstance, ComponentInstanceEvent,
          ComponentInstanceState> {

    @Override
    public ComponentInstanceState transition(ComponentInstance instance,
        ComponentInstanceEvent event) {

      if (instance.pendingCancelUpgrade) {
        // cancellation of upgrade was triggered before the upgrade was
        // finished.
        LOG.info("{} received started but cancellation pending",
            event.getContainerId());
        instance.upgradeInProgress.set(true);
        instance.cancelUpgrade();
        instance.pendingCancelUpgrade = false;
        return instance.getState();
      }

      instance.upgradeInProgress.set(false);
      instance.setContainerState(ContainerState.RUNNING_BUT_UNREADY);
      if (instance.component.getProbe() != null &&
          instance.component.getProbe() instanceof DefaultProbe) {
        instance.initializeStatusRetriever(event, 30);
      } else {
        instance.initializeStatusRetriever(event, 0);
      }

      Component.UpgradeStatus status = instance.getState().equals(UPGRADING) ?
          instance.component.getUpgradeStatus() :
          instance.component.getCancelUpgradeStatus();
      status.decContainersThatNeedUpgrade();

      instance.serviceVersion = status.getTargetVersion();
      return ComponentInstanceState.REINITIALIZED;
    }
  }

  private void postContainerReady() {
    if (timelineServiceEnabled) {
      serviceTimelinePublisher.componentInstanceBecomeReady(containerSpec);
    }
  }

  private static class ContainerBecomeNotReadyTransition extends BaseTransition {
    @Override
    public void transition(ComponentInstance compInstance,
        ComponentInstanceEvent event) {
      compInstance.setContainerState(ContainerState.RUNNING_BUT_UNREADY);
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
          failureBeforeLaunch || event.getStatus() == null ? null :
              event.getStatus().getExitStatus());
      builder.append(", diagnostics=");
      builder.append(failureBeforeLaunch ?
          FAILED_BEFORE_LAUNCH_DIAG :
          (event.getStatus() != null ? event.getStatus().getDiagnostics() :
              UPGRADE_FAILED));

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
      ContainerStatus status = event.getStatus();
      // status is not available when upgrade fails
      String containerDiag = compInstance.getCompInstanceId() + ": " + (
          failedBeforeLaunching ? FAILED_BEFORE_LAUNCH_DIAG :
              (status != null ? status.getDiagnostics() : UPGRADE_FAILED));
      compInstance.diagnostics.append(containerDiag + System.lineSeparator());
      compInstance.cancelContainerStatusRetriever();

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
                  failedBeforeLaunching || status == null ? -1 :
                      status.getExitStatus(),
                  ContainerState.FAILED, containerDiag);

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

  private static class StoppedAfterUpgradeTransition extends
      BaseTransition {

    @Override
    public void transition(ComponentInstance instance,
        ComponentInstanceEvent event) {
      instance.component.getUpgradeStatus().decContainersThatNeedUpgrade();
      instance.component.decRunningContainers();

      final ServiceScheduler scheduler = instance.component.getScheduler();
      scheduler.getAmRMClient().releaseAssignedContainer(
          event.getContainerId());
      instance.scheduler.executorService.submit(
          () -> instance.cleanupRegistry(event.getContainerId()));
      scheduler.removeLiveCompInstance(event.getContainerId());
      instance.component.getUpgradeStatus().containerFailedUpgrade();
      instance.setContainerState(ContainerState.FAILED_UPGRADE);
      instance.upgradeInProgress.set(false);
    }
  }

  private static class StoppedAfterCancelUpgradeTransition implements
      MultipleArcTransition<ComponentInstance, ComponentInstanceEvent,
          ComponentInstanceState> {

    private ContainerStoppedTransition stoppedTransition =
        new ContainerStoppedTransition();

    @Override
    public ComponentInstanceState transition(ComponentInstance instance,
        ComponentInstanceEvent event) {
      if (instance.pendingCancelUpgrade) {
        // cancellation of upgrade was triggered before the upgrade was
        // finished.
        LOG.info("{} received stopped but cancellation pending",
            event.getContainerId());
        instance.upgradeInProgress.set(true);
        instance.cancelUpgrade();
        instance.pendingCancelUpgrade = false;
        return instance.getState();
      }

      // When upgrade is cancelled, and container re-init fails
      instance.component.getCancelUpgradeStatus()
          .decContainersThatNeedUpgrade();
      instance.upgradeInProgress.set(false);
      stoppedTransition.transition(instance, event);
      return ComponentInstanceState.INIT;
    }
  }

  private static class UpgradeTransition extends BaseTransition {

    @Override
    public void transition(ComponentInstance instance,
        ComponentInstanceEvent event) {
      if (!instance.component.getCancelUpgradeStatus().isCompleted()) {
        // last check to see if cancellation was triggered. The component may
        // have processed the cancel upgrade event but the instance doesn't know
        // it yet. If cancellation has been triggered then no point in
        // upgrading.
        return;
      }
      instance.upgradeInProgress.set(true);
      instance.setContainerState(ContainerState.UPGRADING);
      instance.component.decContainersReady(false);

      Component.UpgradeStatus upgradeStatus = instance.component.
          getUpgradeStatus();
      instance.reInitHelper(upgradeStatus);
    }
  }

  private static class CancelledAfterReinitTransition extends BaseTransition {
    @Override
    public void transition(ComponentInstance instance,
        ComponentInstanceEvent event) {
      if (instance.upgradeInProgress.compareAndSet(false, true)) {
        instance.cancelUpgrade();
      } else {
        LOG.info("{} pending cancellation", event.getContainerId());
        instance.pendingCancelUpgrade = true;
      }
    }
  }

  private static class CancelUpgradeTransition implements
      MultipleArcTransition<ComponentInstance, ComponentInstanceEvent,
          ComponentInstanceState> {

    @Override
    public ComponentInstanceState transition(ComponentInstance instance,
        ComponentInstanceEvent event) {
      if (instance.upgradeInProgress.compareAndSet(false, true)) {

        Component.UpgradeStatus cancelStatus = instance.component
            .getCancelUpgradeStatus();

        if (instance.getServiceVersion().equals(
            cancelStatus.getTargetVersion())) {
          // previous upgrade didn't happen so just go back to READY
          LOG.info("{} nothing to cancel", event.getContainerId());
          cancelStatus.decContainersThatNeedUpgrade();
          instance.setContainerState(ContainerState.READY);
          ComponentEvent checkState = new ComponentEvent(
              instance.component.getName(), ComponentEventType.CHECK_STABLE);
          instance.scheduler.getDispatcher().getEventHandler()
              .handle(checkState);
          return ComponentInstanceState.READY;
        } else {
          instance.component.decContainersReady(false);
          instance.cancelUpgrade();
        }
      } else {
        LOG.info("{} pending cancellation", event.getContainerId());
        instance.pendingCancelUpgrade = true;
      }
      return ComponentInstanceState.CANCEL_UPGRADING;
    }
  }

  private void cancelUpgrade() {
    LOG.info("{} cancelling upgrade", container.getId());
    setContainerState(ContainerState.UPGRADING);
    Component.UpgradeStatus cancelStatus = component.getCancelUpgradeStatus();
    reInitHelper(cancelStatus);
  }

  private void reInitHelper(Component.UpgradeStatus upgradeStatus) {
    cancelContainerStatusRetriever();
    setContainerStatus(container.getId(), null);
    scheduler.executorService.submit(() -> cleanupRegistry(container.getId()));
    scheduler.getContainerLaunchService()
        .reInitCompInstance(scheduler.getApp(), this,
            this.container, this.component.createLaunchContext(
                upgradeStatus.getTargetSpec(),
                upgradeStatus.getTargetVersion()));
  }

  private void initializeStatusRetriever(ComponentInstanceEvent event,
      long initialDelay) {
    boolean cancelOnSuccess = true;
    if (getCompSpec().getArtifact() != null &&
        getCompSpec().getArtifact().getType() == Artifact.TypeEnum.DOCKER) {
      // A docker container might get a different IP if the container is
      // relaunched by the NM, so we need to keep checking the status.
      // This is a temporary fix until the NM provides a callback for
      // container relaunch (see YARN-8265).
      cancelOnSuccess = false;
    }
    LOG.info("{} retrieve status after {}", compInstanceId, initialDelay);
    containerStatusFuture =
        scheduler.executorService.scheduleAtFixedRate(
            new ContainerStatusRetriever(scheduler, event.getContainerId(),
                this, cancelOnSuccess), initialDelay, 1,
            TimeUnit.SECONDS);
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

  /**
   * Sets the state of the container in the container spec. It is write
   * protected.
   *
   * @param state container state
   */
  public void setContainerState(ContainerState state) {
    this.writeLock.lock();
    try {
      ContainerState curState = containerSpec.getState();
      if (!curState.equals(state)) {
        containerSpec.setState(state);
        LOG.info("{} spec state state changed from {} -> {}",
            getCompInstanceId(), curState, state);
      }
    } finally {
      this.writeLock.unlock();
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
    try {
      readLock.lock();
      return status;
    } finally {
      readLock.unlock();
    }
  }

  private void setContainerStatus(ContainerId containerId,
      ContainerStatus latestStatus) {
    try {
      writeLock.lock();
      this.status = latestStatus;
      org.apache.hadoop.yarn.service.api.records.Container containerRec =
          getCompSpec().getContainer(containerId.toString());

      if (containerRec != null) {
        if (latestStatus != null) {
          containerRec.setIp(StringUtils.join(",", latestStatus.getIPs()));
          containerRec.setHostname(latestStatus.getHost());
        } else {
          containerRec.setIp(null);
          containerRec.setHostname(null);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void updateContainerStatus(ContainerStatus status) {
    org.apache.hadoop.yarn.service.api.records.Container containerRec =
        getCompSpec().getContainer(status.getContainerId().toString());
    boolean doRegistryUpdate = true;
    if (containerRec != null) {
      String existingIP = containerRec.getIp();
      String newIP = StringUtils.join(",", status.getIPs());
      if (existingIP != null && newIP.equals(existingIP)) {
        doRegistryUpdate = false;
      }
    }
    setContainerStatus(status.getContainerId(), status);
    if (containerRec != null && timelineServiceEnabled && doRegistryUpdate) {
      serviceTimelinePublisher.componentInstanceIPHostUpdated(containerRec);
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
    return getCompInstanceName() + getComponent().getHostnameSuffix();
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
