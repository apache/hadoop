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

package org.apache.hadoop.yarn.service.component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import static org.apache.hadoop.yarn.service.api.records.Component
    .RestartPolicyEnum;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.ServiceEvent;
import org.apache.hadoop.yarn.service.ServiceEventType;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.ResourceInformation;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceId;
import org.apache.hadoop.yarn.service.ContainerFailureTracker;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.ServiceMetrics;
import org.apache.hadoop.yarn.service.ServiceScheduler;
import org.apache.hadoop.yarn.service.api.records.PlacementPolicy;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEvent;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.apache.hadoop.yarn.service.monitor.ComponentHealthThresholdMonitor;
import org.apache.hadoop.yarn.service.monitor.probe.MonitorUtils;
import org.apache.hadoop.yarn.service.monitor.probe.Probe;
import org.apache.hadoop.yarn.service.containerlaunch.ContainerLaunchService;
import org.apache.hadoop.yarn.service.provider.ProviderService;
import org.apache.hadoop.yarn.service.provider.ProviderUtils;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Apps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.api.records.ContainerExitStatus.*;
import static org.apache.hadoop.yarn.service.api.ServiceApiConstants.*;
import static org.apache.hadoop.yarn.service.component.ComponentEventType.*;
import static org.apache.hadoop.yarn.service.component.ComponentEventType.CANCEL_UPGRADE;
import static org.apache.hadoop.yarn.service.component.ComponentEventType.UPGRADE;
import static org.apache.hadoop.yarn.service.component.ComponentState.*;
import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType.*;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.*;

public class Component implements EventHandler<ComponentEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(Component.class);

  private org.apache.hadoop.yarn.service.api.records.Component componentSpec;
  private long allocateId;
  private Priority priority;
  private ServiceMetrics componentMetrics;
  private ServiceScheduler scheduler;
  private ServiceContext context;
  private AMRMClientAsync<ContainerRequest> amrmClient;
  private AtomicLong instanceIdCounter = new AtomicLong();
  private Map<String, ComponentInstance> compInstances =
      new ConcurrentHashMap<>();
  // component instances to be assigned with a container
  private List<ComponentInstance> pendingInstances =
      Collections.synchronizedList(new LinkedList<>());
  private ContainerFailureTracker failureTracker;
  private Probe probe;
  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;
  public int maxContainerFailurePerComp;
  // The number of containers failed since last reset. This excludes preempted,
  // disk_failed containers etc. This will be reset to 0 periodically.
  public AtomicInteger currentContainerFailure = new AtomicInteger(0);

  //succeeded and Failed instances are Populated only for RestartPolicyEnum
  //.ON_FAILURE/NEVER
  private Map<String, ComponentInstance> succeededInstances =
      new ConcurrentHashMap<>();
  private Map<String, ComponentInstance> failedInstances =
      new ConcurrentHashMap<>();
  private boolean healthThresholdMonitorEnabled = false;

  private UpgradeStatus upgradeStatus = new UpgradeStatus();
  private UpgradeStatus cancelUpgradeStatus = new UpgradeStatus();

  private StateMachine<ComponentState, ComponentEventType, ComponentEvent>
      stateMachine;
  private AsyncDispatcher dispatcher;
  private static final StateMachineFactory<Component, ComponentState, ComponentEventType, ComponentEvent>
      stateMachineFactory =
      new StateMachineFactory<Component, ComponentState, ComponentEventType, ComponentEvent>(
          INIT)
           // INIT will only got to FLEXING
          .addTransition(INIT, EnumSet.of(STABLE, FLEXING, INIT),
              FLEX, new FlexComponentTransition())
          // container recovered on AM restart
          .addTransition(INIT, INIT, CONTAINER_RECOVERED,
              new ContainerRecoveredTransition())
          // instance decommissioned
          .addTransition(INIT, INIT, DECOMMISSION_INSTANCE,
              new DecommissionInstanceTransition())

          // container recovered in AM heartbeat
          .addTransition(FLEXING, FLEXING, CONTAINER_RECOVERED,
              new ContainerRecoveredTransition())

          // container allocated by RM
          .addTransition(FLEXING, FLEXING, CONTAINER_ALLOCATED,
              new ContainerAllocatedTransition())
          // container launched on NM
          .addTransition(FLEXING, EnumSet.of(STABLE, FLEXING, UPGRADING),
              CONTAINER_STARTED, new ContainerStartedTransition())
          // container failed while flexing
          .addTransition(FLEXING, FLEXING, CONTAINER_COMPLETED,
              new ContainerCompletedTransition())
          // Flex while previous flex is still in progress
          .addTransition(FLEXING, EnumSet.of(FLEXING, STABLE), FLEX,
              new FlexComponentTransition())
          .addTransition(FLEXING, EnumSet.of(UPGRADING, FLEXING, STABLE),
              CHECK_STABLE, new CheckStableTransition())
          // instance decommissioned
          .addTransition(FLEXING, FLEXING, DECOMMISSION_INSTANCE,
              new DecommissionInstanceTransition())

          // container failed while stable
          .addTransition(STABLE, FLEXING, CONTAINER_COMPLETED,
              new ContainerCompletedTransition())
          // Ignore surplus container
          .addTransition(STABLE, STABLE, CONTAINER_ALLOCATED,
              new ContainerAllocatedTransition())
          // Flex by user
          // For flex up, go to FLEXING state
          // For flex down, go to STABLE state
          .addTransition(STABLE, EnumSet.of(STABLE, FLEXING),
              FLEX, new FlexComponentTransition())
          // instance decommissioned
          .addTransition(STABLE, STABLE, DECOMMISSION_INSTANCE,
              new DecommissionInstanceTransition())
          // upgrade component
          .addTransition(STABLE, UPGRADING, UPGRADE,
              new NeedsUpgradeTransition())
          .addTransition(STABLE, CANCEL_UPGRADING, CANCEL_UPGRADE,
              new NeedsUpgradeTransition())
          .addTransition(STABLE, EnumSet.of(STABLE, FLEXING), CHECK_STABLE,
              new CheckStableTransition())

          // Cancel upgrade while previous upgrade is still in progress
          .addTransition(UPGRADING, CANCEL_UPGRADING,
              CANCEL_UPGRADE, new NeedsUpgradeTransition())
          .addTransition(UPGRADING, EnumSet.of(UPGRADING, FLEXING, STABLE),
              CHECK_STABLE, new CheckStableTransition())
          .addTransition(UPGRADING, UPGRADING, CONTAINER_COMPLETED,
              new CompletedAfterUpgradeTransition())
          // instance decommissioned
          .addTransition(UPGRADING, UPGRADING, DECOMMISSION_INSTANCE,
              new DecommissionInstanceTransition())

          .addTransition(CANCEL_UPGRADING, EnumSet.of(CANCEL_UPGRADING, FLEXING,
              STABLE), CHECK_STABLE, new CheckStableTransition())
          .addTransition(CANCEL_UPGRADING, CANCEL_UPGRADING,
              CONTAINER_COMPLETED, new CompletedAfterUpgradeTransition())
          .addTransition(CANCEL_UPGRADING, FLEXING, CONTAINER_ALLOCATED,
              new ContainerAllocatedTransition())
          // instance decommissioned
          .addTransition(CANCEL_UPGRADING, CANCEL_UPGRADING,
              DECOMMISSION_INSTANCE, new DecommissionInstanceTransition())
          .installTopology();

  public Component(
      org.apache.hadoop.yarn.service.api.records.Component component,
      long allocateId, ServiceContext context) {
    this.allocateId = allocateId;
    this.priority = Priority.newInstance((int) allocateId);
    this.componentSpec = component;
    componentMetrics = ServiceMetrics.register(component.getName(),
        "Metrics for component " + component.getName());
    componentMetrics
        .tag("type", "Metrics type [component or service]", "component");
    this.scheduler = context.scheduler;
    this.context = context;
    amrmClient = scheduler.getAmRMClient();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
    this.stateMachine = stateMachineFactory.make(this);
    dispatcher = scheduler.getDispatcher();
    failureTracker =
        new ContainerFailureTracker(context, this);
    if (componentSpec.getReadinessCheck() != null ||
        YarnServiceConf.getBoolean(DEFAULT_READINESS_CHECK_ENABLED,
            DEFAULT_READINESS_CHECK_ENABLED_DEFAULT,
            componentSpec.getConfiguration(), scheduler.getConfig())) {
      probe = MonitorUtils.getProbe(componentSpec.getReadinessCheck());
    }
    maxContainerFailurePerComp = YarnServiceConf.getInt(
        CONTAINER_FAILURE_THRESHOLD, DEFAULT_CONTAINER_FAILURE_THRESHOLD,
        componentSpec.getConfiguration(), scheduler.getConfig());
    createNumCompInstances(component.getNumberOfContainers());
    setDesiredContainers(component.getNumberOfContainers().intValue());
    checkAndScheduleHealthThresholdMonitor();
  }

  private void createNumCompInstances(long count) {
    for (int i = 0; i < count; i++) {
      createOneCompInstance();
    }
  }

  private void createOneCompInstance() {
    ComponentInstanceId id =
        new ComponentInstanceId(instanceIdCounter.getAndIncrement(),
            componentSpec.getName());
    while (componentSpec.getDecommissionedInstances().contains(id
        .getCompInstanceName())) {
      id = new ComponentInstanceId(instanceIdCounter.getAndIncrement(),
          componentSpec.getName());
    }
    ComponentInstance instance = new ComponentInstance(this, id);
    compInstances.put(instance.getCompInstanceName(), instance);
    pendingInstances.add(instance);
  }

  private void checkAndScheduleHealthThresholdMonitor() {
    // Determine health threshold percent
    int healthThresholdPercent = YarnServiceConf.getInt(
        CONTAINER_HEALTH_THRESHOLD_PERCENT,
        DEFAULT_CONTAINER_HEALTH_THRESHOLD_PERCENT,
        componentSpec.getConfiguration(), scheduler.getConfig());
    // Validations
    if (healthThresholdPercent == CONTAINER_HEALTH_THRESHOLD_PERCENT_DISABLED) {
      LOG.info("No health threshold monitor enabled for component {}",
          componentSpec.getName());
      return;
    }
    // If threshold is set to outside acceptable range then don't enable monitor
    if (healthThresholdPercent <= 0 || healthThresholdPercent > 100) {
      LOG.error(
          "Invalid health threshold percent {}% for component {}. Monitor not "
              + "enabled.",
          healthThresholdPercent, componentSpec.getName());
      return;
    }
    // Determine the threshold properties
    long window = YarnServiceConf.getLong(CONTAINER_HEALTH_THRESHOLD_WINDOW_SEC,
        DEFAULT_CONTAINER_HEALTH_THRESHOLD_WINDOW_SEC,
        componentSpec.getConfiguration(), scheduler.getConfig());
    long initDelay = YarnServiceConf.getLong(
        CONTAINER_HEALTH_THRESHOLD_INIT_DELAY_SEC,
        DEFAULT_CONTAINER_HEALTH_THRESHOLD_INIT_DELAY_SEC,
        componentSpec.getConfiguration(), scheduler.getConfig());
    long pollFrequency = YarnServiceConf.getLong(
        CONTAINER_HEALTH_THRESHOLD_POLL_FREQUENCY_SEC,
        DEFAULT_CONTAINER_HEALTH_THRESHOLD_POLL_FREQUENCY_SEC,
        componentSpec.getConfiguration(), scheduler.getConfig());
    // Validations
    if (window <= 0) {
      LOG.error(
          "Invalid health monitor window {} secs for component {}. Monitor not "
              + "enabled.",
          window, componentSpec.getName());
      return;
    }
    if (initDelay < 0) {
      LOG.error("Invalid health monitor init delay {} secs for component {}. "
          + "Monitor not enabled.", initDelay, componentSpec.getName());
      return;
    }
    if (pollFrequency <= 0) {
      LOG.error(
          "Invalid health monitor poll frequency {} secs for component {}. "
              + "Monitor not enabled.",
          pollFrequency, componentSpec.getName());
      return;
    }
    LOG.info(
        "Scheduling the health threshold monitor for component {} with percent "
            + "= {}%, window = {} secs, poll freq = {} secs, init-delay = {} "
            + "secs",
        componentSpec.getName(), healthThresholdPercent, window, pollFrequency,
        initDelay);
    // Add 3 extra seconds to initial delay to account for the time taken to
    // request containers before the monitor starts calculating health.
    this.scheduler.executorService.scheduleAtFixedRate(
        new ComponentHealthThresholdMonitor(this, healthThresholdPercent,
            window),
        initDelay + 3, pollFrequency, TimeUnit.SECONDS);
    setHealthThresholdMonitorEnabled(true);
  }

  private static class FlexComponentTransition implements
      MultipleArcTransition<Component, ComponentEvent, ComponentState> {
    // For flex up, go to FLEXING state
    // For flex down, go to STABLE state
    @Override
    public ComponentState transition(Component component,
        ComponentEvent event) {
      component.setDesiredContainers((int) event.getDesired());
      if (!component.areDependenciesReady()) {
        LOG.info("[FLEX COMPONENT {}]: Flex deferred because dependencies not"
            + " satisfied.", component.getName());
        return component.getState();
      }
      if (component.getState() == INIT) {
        // This happens on init
        LOG.info("[INIT COMPONENT " + component.getName() + "]: " + event
            .getDesired() + " instances.");
        component.requestContainers(component.pendingInstances.size());
        return checkIfStable(component);
      }
      long before = component.getComponentSpec().getNumberOfContainers();
      long delta = event.getDesired() - before;
      component.getComponentSpec().setNumberOfContainers(event.getDesired());
      if (delta > 0) {
        // Scale up
        LOG.info("[FLEX UP COMPONENT " + component.getName() + "]: scaling up from "
                + before + " to " + event.getDesired());
        component.requestContainers(delta);
        component.createNumCompInstances(delta);
        component.setComponentState(
            org.apache.hadoop.yarn.service.api.records.ComponentState.FLEXING);
        component.getScheduler().getApp().setState(ServiceState.STARTED);
        return FLEXING;
      } else if (delta < 0) {
        delta = 0 - delta;
        // scale down
        LOG.info("[FLEX DOWN COMPONENT " + component.getName()
            + "]: scaling down from " + before + " to " + event.getDesired());
        List<ComponentInstance> list =
            new ArrayList<>(component.getAllComponentInstances());

        // sort in Most recent -> oldest order, destroy most recent ones.
        list.sort(Collections.reverseOrder());
        for (int i = 0; i < delta; i++) {
          ComponentInstance instance = list.get(i);
          // remove the instance
          component.compInstances.remove(instance.getCompInstanceName());
          component.pendingInstances.remove(instance);
          // decrement id counter
          component.instanceIdCounter.decrementAndGet();
          instance.destroy();
        }
        checkAndUpdateComponentState(component, false);
        return component.componentSpec.getState()
            == org.apache.hadoop.yarn.service.api.records.ComponentState.STABLE
                ? STABLE : FLEXING;
      } else {
        LOG.info("[FLEX COMPONENT " + component.getName() + "]: already has " +
            event.getDesired() + " instances, ignoring");
        return STABLE;
      }
    }
  }

  private static class DecommissionInstanceTransition extends BaseTransition {
    @Override
    public void transition(Component component, ComponentEvent event) {
      String instanceName = event.getInstanceName();
      String hostnameSuffix = component.getHostnameSuffix();
      if (instanceName.endsWith(hostnameSuffix)) {
        instanceName = instanceName.substring(0,
            instanceName.length() - hostnameSuffix.length());
      }
      if (component.getComponentSpec().getDecommissionedInstances()
          .contains(instanceName)) {
        LOG.info("Instance {} already decommissioned", instanceName);
        return;
      }
      component.getComponentSpec().addDecommissionedInstance(instanceName);
      ComponentInstance instance = component.getComponentInstance(instanceName);
      if (instance == null) {
        LOG.info("Instance was null for decommissioned instance {}",
            instanceName);
        return;
      }
      // remove the instance
      component.compInstances.remove(instance.getCompInstanceName());
      component.pendingInstances.remove(instance);
      component.scheduler.getServiceMetrics().containersDesired.decr();
      component.componentMetrics.containersDesired.decr();
      component.getComponentSpec().setNumberOfContainers(component
          .getComponentSpec().getNumberOfContainers() - 1);
      instance.destroy();
    }
  }

  private static class ContainerAllocatedTransition extends BaseTransition {
    @Override
    public void transition(Component component, ComponentEvent event) {
      component.assignContainerToCompInstance(event.getContainer());
    }
  }

  private static class ContainerRecoveredTransition extends BaseTransition {
    @Override
    public void transition(Component component, ComponentEvent event) {
      ComponentInstance instance = event.getInstance();
      Container container = event.getContainer();
      if (instance == null) {
        LOG.info("[COMPONENT {}]: Trying to recover {} but event did not " +
                "specify component instance",
            component.getName(), container.getId());
        component.releaseContainer(container);
        return;
      }

      component.pendingInstances.remove(instance);
      instance.setContainer(container);

      ProviderUtils.initCompInstanceDir(component.getContext().fs,
          component.createLaunchContext(component.componentSpec,
              component.scheduler.getApp().getVersion()), instance);
      component.getScheduler().addLiveCompInstance(container.getId(), instance);
      LOG.info("[COMPONENT {}]: Recovered {} for component instance {} on " +
              "host {}, num pending component instances reduced to {} ",
          component.getName(), container.getId(),
          instance.getCompInstanceName(), container.getNodeId(),
          component.pendingInstances.size());
      component.dispatcher.getEventHandler().handle(
          new ComponentInstanceEvent(container.getId(), START));
    }
  }

  private static class ContainerStartedTransition implements
      MultipleArcTransition<Component,ComponentEvent,ComponentState> {

    @Override public ComponentState transition(Component component,
        ComponentEvent event) {
      component.dispatcher.getEventHandler().handle(
          new ComponentInstanceEvent(event.getContainerId(), START));
      return checkIfStable(component);
    }
  }

  @VisibleForTesting
  static ComponentState checkIfStable(Component component) {
    if (component.getRestartPolicyHandler().isLongLived()) {
      return updateStateForLongRunningComponents(component);
    } else{
      //NEVER/ON_FAILURE
      return updateStateForTerminatingComponents(component);
    }
  }

  private static ComponentState updateStateForTerminatingComponents(
      Component component) {
    if (component.getNumRunningInstances() + component
        .getNumSucceededInstances() + component.getNumFailedInstances()
        < component.getComponentSpec().getNumberOfContainers()) {
      component.setComponentState(
          org.apache.hadoop.yarn.service.api.records.ComponentState.FLEXING);
      return FLEXING;
    } else{
      component.setComponentState(
          org.apache.hadoop.yarn.service.api.records.ComponentState.STABLE);
      return STABLE;
    }
  }

  private static ComponentState updateStateForLongRunningComponents(
      Component component) {
    // if desired == running
    if (component.componentMetrics.containersReady.value() == component
        .getComponentSpec().getNumberOfContainers() &&
        !component.doesNeedUpgrade()) {
      component.setComponentState(
          org.apache.hadoop.yarn.service.api.records.ComponentState.STABLE);
      return STABLE;
    } else if (component.doesNeedUpgrade()) {
      component.setComponentState(org.apache.hadoop.yarn.service.api.records.
          ComponentState.NEEDS_UPGRADE);
      return component.getState();
    } else if (component.componentMetrics.containersReady.value() != component
        .getComponentSpec().getNumberOfContainers()) {
      component.setComponentState(
          org.apache.hadoop.yarn.service.api.records.ComponentState.FLEXING);
      return FLEXING;
    }
    return component.getState();
  }

  // This method should be called whenever there is an increment or decrement
  // of a READY state container of a component
  //This should not matter for terminating components
  private static synchronized void checkAndUpdateComponentState(
      Component component, boolean isIncrement) {

    if (component.getRestartPolicyHandler().isLongLived()) {
      if (isIncrement) {
        // check if all containers are in READY state
        if (!component.upgradeStatus.areContainersUpgrading() &&
            !component.cancelUpgradeStatus.areContainersUpgrading() &&
            component.componentMetrics.containersReady.value() ==
                component.componentMetrics.containersDesired.value()) {
          component.setComponentState(
              org.apache.hadoop.yarn.service.api.records.ComponentState.STABLE);
          // component state change will trigger re-check of service state
          component.context.getServiceManager().checkAndUpdateServiceState();
        }
      } else{
        // container moving out of READY state could be because of FLEX down so
        // still need to verify the count before changing the component state
        if (component.componentMetrics.containersReady.value()
            < component.componentMetrics.containersDesired.value()) {
          component.setComponentState(
              org.apache.hadoop.yarn.service.api.records.ComponentState
                  .FLEXING);
        } else if (component.componentMetrics.containersReady.value()
            == component.componentMetrics.containersDesired.value()) {
          component.setComponentState(
              org.apache.hadoop.yarn.service.api.records.ComponentState.STABLE);
        }
        // component state change will trigger re-check of service state
        component.context.getServiceManager().checkAndUpdateServiceState();
      }
    } else {
      // component state change will trigger re-check of service state
      component.context.getServiceManager().checkAndUpdateServiceState();
    }
    // triggers the state machine in component to reach appropriate state
    // once the state in spec is changed.
    component.dispatcher.getEventHandler().handle(
        new ComponentEvent(component.getName(),
            ComponentEventType.CHECK_STABLE));
  }

  private static class ContainerCompletedTransition extends BaseTransition {
    @Override
    public void transition(Component component, ComponentEvent event) {
      Preconditions.checkNotNull(event.getContainerId());
      component.updateMetrics(event.getStatus());
      component.dispatcher.getEventHandler().handle(
          new ComponentInstanceEvent(event.getContainerId(), STOP)
              .setStatus(event.getStatus()));

      ComponentRestartPolicy restartPolicy =
          component.getRestartPolicyHandler();

      if (restartPolicy.shouldRelaunchInstance(event.getInstance(),
          event.getStatus())) {
        component.componentSpec.setState(
            org.apache.hadoop.yarn.service.api.records.ComponentState.FLEXING);

        if (component.context.service.getState().equals(ServiceState.STABLE)) {
          component.getScheduler().getApp().setState(ServiceState.STARTED);
          LOG.info("Service def state changed from {} -> {}",
              ServiceState.STABLE, ServiceState.STARTED);
        }
      }
    }
  }

  private static class CompletedAfterUpgradeTransition extends BaseTransition {
    @Override
    public void transition(Component component, ComponentEvent event) {
      Preconditions.checkNotNull(event.getContainerId());
      component.updateMetrics(event.getStatus());
      component.dispatcher.getEventHandler().handle(
          new ComponentInstanceEvent(event.getContainerId(), STOP)
              .setStatus(event.getStatus()));
    }
  }

  private static class NeedsUpgradeTransition extends BaseTransition {
    @Override
    public void transition(Component component, ComponentEvent event) {
      boolean isCancel = event.getType().equals(CANCEL_UPGRADE);
      UpgradeStatus status = !isCancel ? component.upgradeStatus :
          component.cancelUpgradeStatus;

      status.inProgress.set(true);
      status.targetSpec = event.getTargetSpec();
      status.targetVersion = event.getUpgradeVersion();
      LOG.info("[COMPONENT {}]: need upgrade to {}",
          component.getName(), status.targetVersion);

      status.containersNeedUpgrade.set(
          component.componentSpec.getNumberOfContainers());

      component.setComponentState(org.apache.hadoop.yarn.service.api.
          records.ComponentState.NEEDS_UPGRADE);

      component.getAllComponentInstances().forEach(instance -> {
        instance.setContainerState(ContainerState.NEEDS_UPGRADE);
      });

      if (event.getType().equals(CANCEL_UPGRADE)) {
        component.upgradeStatus.reset();
      }
    }
  }

  private static class CheckStableTransition implements MultipleArcTransition
      <Component, ComponentEvent, ComponentState> {

    @Override
    public ComponentState transition(Component component,
        ComponentEvent componentEvent) {
      // checkIfStable also updates the state in definition when STABLE
      ComponentState targetState = checkIfStable(component);

      if (targetState.equals(STABLE) &&
          !(component.upgradeStatus.isCompleted() &&
              component.cancelUpgradeStatus.isCompleted())) {
        // Component stable after upgrade or cancel upgrade
        UpgradeStatus status = !component.cancelUpgradeStatus.isCompleted() ?
            component.cancelUpgradeStatus : component.upgradeStatus;

        component.componentSpec.overwrite(status.getTargetSpec());
        status.reset();

        ServiceEvent checkStable = new ServiceEvent(ServiceEventType.
            CHECK_STABLE);
        component.dispatcher.getEventHandler().handle(checkStable);
      }
      return targetState;
    }
  }

  public void removePendingInstance(ComponentInstance instance) {
    pendingInstances.remove(instance);
  }

  public void reInsertPendingInstance(ComponentInstance instance) {
    pendingInstances.add(instance);
  }

  private void releaseContainer(Container container) {
    scheduler.getAmRMClient().releaseAssignedContainer(container.getId());
    componentMetrics.surplusContainers.incr();
    scheduler.getServiceMetrics().surplusContainers.incr();
  }

  private void assignContainerToCompInstance(Container container) {
    if (pendingInstances.size() == 0) {
      LOG.info(
          "[COMPONENT {}]: No pending component instance left, release surplus container {}",
          getName(), container.getId());
      releaseContainer(container);
      return;
    }
    ComponentInstance instance = pendingInstances.remove(0);
    LOG.info(
        "[COMPONENT {}]: {} allocated, num pending component instances reduced to {}",
        getName(), container.getId(), pendingInstances.size());
    instance.setContainer(container);
    scheduler.addLiveCompInstance(container.getId(), instance);
    LOG.info(
        "[COMPONENT {}]: Assigned {} to component instance {} and launch on host {} ",
        getName(), container.getId(), instance.getCompInstanceName(),
        container.getNodeId());
    Future<ProviderService.ResolvedLaunchParams> resolvedParamFuture;
    if (!(upgradeStatus.isCompleted() && cancelUpgradeStatus.isCompleted())) {
      UpgradeStatus status = !cancelUpgradeStatus.isCompleted() ?
          cancelUpgradeStatus : upgradeStatus;

      resolvedParamFuture = scheduler.getContainerLaunchService()
          .launchCompInstance(scheduler.getApp(), instance, container,
              createLaunchContext(status.getTargetSpec(),
                  status.getTargetVersion()));
    } else {
      resolvedParamFuture = scheduler.getContainerLaunchService()
          .launchCompInstance(
          scheduler.getApp(), instance, container,
          createLaunchContext(componentSpec, scheduler.getApp().getVersion()));
    }
    instance.updateResolvedLaunchParams(resolvedParamFuture);
  }

  public ContainerLaunchService.ComponentLaunchContext createLaunchContext(
      org.apache.hadoop.yarn.service.api.records.Component compSpec,
      String version) {
    ContainerLaunchService.ComponentLaunchContext launchContext =
        new ContainerLaunchService.ComponentLaunchContext(compSpec.getName(),
            version);
    launchContext.setArtifact(compSpec.getArtifact())
        .setConfiguration(compSpec.getConfiguration())
        .setLaunchCommand(compSpec.getLaunchCommand())
        .setRunPrivilegedContainer(compSpec.getRunPrivilegedContainer());
    return launchContext;
  }

  @SuppressWarnings({ "unchecked" })
  public void requestContainers(long count) {
    LOG.info("[COMPONENT {}] Requesting for {} container(s)",
        componentSpec.getName(), count);
    org.apache.hadoop.yarn.service.api.records.Resource componentResource =
        componentSpec.getResource();

    Resource resource = Resource.newInstance(componentResource.calcMemoryMB(),
        componentResource.getCpus());

    if (componentResource.getAdditional() != null) {
      for (Map.Entry<String, ResourceInformation> entry : componentResource
          .getAdditional().entrySet()) {

        String resourceName = entry.getKey();

        // Avoid setting memory/cpu under "additional"
        if (resourceName.equals(
            org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI)
            || resourceName.equals(
            org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI)) {
          LOG.warn("Please set memory/vcore in the main section of resource, "
              + "ignoring this entry=" + resourceName);
          continue;
        }

        ResourceInformation specInfo = entry.getValue();
        org.apache.hadoop.yarn.api.records.ResourceInformation ri =
            org.apache.hadoop.yarn.api.records.ResourceInformation.newInstance(
                entry.getKey(),
                specInfo.getUnit(),
                specInfo.getValue(),
                specInfo.getTags(),
                specInfo.getAttributes());
        resource.setResourceInformation(resourceName, ri);
      }
    }

    if (!scheduler.hasAtLeastOnePlacementConstraint()) {
      for (int i = 0; i < count; i++) {
        ContainerRequest request = ContainerRequest.newBuilder()
            .capability(resource).priority(priority)
            .allocationRequestId(allocateId).relaxLocality(true).build();
        LOG.info("[COMPONENT {}] Submitting container request : {}",
            componentSpec.getName(), request);
        amrmClient.addContainerRequest(request);
      }
    } else {
      // Schedule placement requests. Validation of non-null target tags and
      // that they refer to existing component names are already done. So, no
      // need to validate here.
      PlacementPolicy placementPolicy = componentSpec.getPlacementPolicy();
      Collection<SchedulingRequest> schedulingRequests = new HashSet<>();
      // We prepare an AND-ed composite constraint to be the final composite
      // constraint. If placement expressions are specified to create advanced
      // composite constraints then this AND-ed composite constraint is not
      // used.
      PlacementConstraint finalConstraint = null;
      if (placementPolicy != null) {
        for (org.apache.hadoop.yarn.service.api.records.PlacementConstraint
            yarnServiceConstraint : placementPolicy.getConstraints()) {
          List<TargetExpression> targetExpressions = new ArrayList<>();
          // Currently only intra-application allocation tags are supported.
          if (!yarnServiceConstraint.getTargetTags().isEmpty()) {
            targetExpressions.add(PlacementTargets.allocationTag(
                yarnServiceConstraint.getTargetTags().toArray(new String[0])));
          }
          // Add all node attributes
          for (Map.Entry<String, List<String>> attribute : yarnServiceConstraint
              .getNodeAttributes().entrySet()) {
            targetExpressions
                .add(PlacementTargets.nodeAttribute(attribute.getKey(),
                    attribute.getValue().toArray(new String[0])));
          }
          // Add all node partitions
          if (!yarnServiceConstraint.getNodePartitions().isEmpty()) {
            targetExpressions
                .add(PlacementTargets.nodePartition(yarnServiceConstraint
                    .getNodePartitions().toArray(new String[0])));
          }
          PlacementConstraint constraint = null;
          switch (yarnServiceConstraint.getType()) {
          case AFFINITY:
            constraint = PlacementConstraints
                .targetIn(yarnServiceConstraint.getScope().getValue(),
                    targetExpressions.toArray(new TargetExpression[0]))
                .build();
            break;
          case ANTI_AFFINITY:
            constraint = PlacementConstraints
                .targetNotIn(yarnServiceConstraint.getScope().getValue(),
                    targetExpressions.toArray(new TargetExpression[0]))
                .build();
            break;
          case AFFINITY_WITH_CARDINALITY:
            constraint = PlacementConstraints.targetCardinality(
                yarnServiceConstraint.getScope().name().toLowerCase(),
                yarnServiceConstraint.getMinCardinality() == null ? 0
                    : yarnServiceConstraint.getMinCardinality().intValue(),
                yarnServiceConstraint.getMaxCardinality() == null
                    ? Integer.MAX_VALUE
                    : yarnServiceConstraint.getMaxCardinality().intValue(),
                targetExpressions.toArray(new TargetExpression[0])).build();
            break;
          }
          // The default AND-ed final composite constraint
          if (finalConstraint != null) {
            finalConstraint = PlacementConstraints
                .and(constraint.getConstraintExpr(),
                    finalConstraint.getConstraintExpr())
                .build();
          } else {
            finalConstraint = constraint;
          }
          LOG.debug("[COMPONENT {}] Placement constraint: {}",
              componentSpec.getName(),
              constraint.getConstraintExpr().toString());
        }
      }
      ResourceSizing resourceSizing = ResourceSizing.newInstance((int) count,
          resource);
      LOG.debug("[COMPONENT {}] Resource sizing: {}", componentSpec.getName(),
          resourceSizing);
      SchedulingRequest request = SchedulingRequest.newBuilder()
          .priority(priority).allocationRequestId(allocateId)
          .allocationTags(Collections.singleton(componentSpec.getName()))
          .executionType(
              ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED, true))
          .placementConstraintExpression(finalConstraint)
          .resourceSizing(resourceSizing).build();
      LOG.info("[COMPONENT {}] Submitting scheduling request: {}",
          componentSpec.getName(), request);
      schedulingRequests.add(request);
      amrmClient.addSchedulingRequests(schedulingRequests);
    }
  }

  private void setDesiredContainers(int n) {
    int delta = n - scheduler.getServiceMetrics().containersDesired.value();
    if (delta != 0) {
      scheduler.getServiceMetrics().containersDesired.incr(delta);
    }
    componentMetrics.containersDesired.set(n);
  }

  private void updateMetrics(ContainerStatus status) {
    //when a container preparation fails while building launch context, then
    //the container status may not exist.
    if (status != null) {
      switch (status.getExitStatus()) {
        case SUCCESS:
          componentMetrics.containersSucceeded.incr();
          scheduler.getServiceMetrics().containersSucceeded.incr();
          return;
        case PREEMPTED:
          componentMetrics.containersPreempted.incr();
          scheduler.getServiceMetrics().containersPreempted.incr();
          break;
        case DISKS_FAILED:
          componentMetrics.containersDiskFailure.incr();
          scheduler.getServiceMetrics().containersDiskFailure.incr();
          break;
        default:
          break;
      }
    }

    // containersFailed include preempted, disks_failed etc.
    componentMetrics.containersFailed.incr();
    scheduler.getServiceMetrics().containersFailed.incr();

    if (status != null && Apps.shouldCountTowardsNodeBlacklisting(
        status.getExitStatus())) {
      String host = scheduler.getLiveInstances().get(status.getContainerId())
          .getNodeId().getHost();
      failureTracker.incNodeFailure(host);
      currentContainerFailure.getAndIncrement();
    }
  }

  private boolean doesNeedUpgrade() {
    return cancelUpgradeStatus.areContainersUpgrading() ||
        upgradeStatus.areContainersUpgrading() ||
        upgradeStatus.failed.get();
  }

  public boolean areDependenciesReady() {
    List<String> dependencies = componentSpec.getDependencies();
    if (ServiceUtils.isEmpty(dependencies)) {
      return true;
    }
    for (String dependency : dependencies) {
      Component dependentComponent = scheduler.getAllComponents().get(
          dependency);
      if (dependentComponent == null) {
        LOG.error("Couldn't find dependency {} for {} (should never happen)",
            dependency, getName());
        continue;
      }

      if (!dependentComponent.isReadyForDownstream()) {
        LOG.info("[COMPONENT {}]: Dependency {} not satisfied, only {} of {}"
                + " instances are ready or the dependent component has not "
                + "completed ", getName(), dependency,
            dependentComponent.getNumReadyInstances(),
            dependentComponent.getNumDesiredInstances());
        return false;
      }
    }
    return true;
  }


  public Map<String, String> getDependencyHostIpTokens() {
    Map<String, String> tokens = new HashMap<>();
    List<String> dependencies = componentSpec.getDependencies();
    if (ServiceUtils.isEmpty(dependencies)) {
      return tokens;
    }
    for (String dependency : dependencies) {
      Collection<ComponentInstance> instances = scheduler.getAllComponents()
          .get(dependency).getAllComponentInstances();
      for (ComponentInstance instance : instances) {
        if (instance.getContainerStatus() == null) {
          continue;
        }
        if (ServiceUtils.isEmpty(instance.getContainerStatus().getIPs()) ||
            ServiceUtils.isUnset(instance.getContainerStatus().getHost())) {
          continue;
        }
        String ip = instance.getContainerStatus().getIPs().get(0);
        String host = instance.getContainerStatus().getHost();
        tokens.put(String.format(COMPONENT_INSTANCE_IP,
            instance.getCompInstanceName().toUpperCase()), ip);
        tokens.put(String.format(COMPONENT_INSTANCE_HOST,
            instance.getCompInstanceName().toUpperCase()), host);
      }
    }
    return tokens;
  }

  public void incRunningContainers() {
    componentMetrics.containersRunning.incr();
    scheduler.getServiceMetrics().containersRunning.incr();
  }

  public void decRunningContainers() {
    componentMetrics.containersRunning.decr();
    scheduler.getServiceMetrics().containersRunning.decr();
  }

  public void incContainersReady(boolean updateDefinition) {
    componentMetrics.containersReady.incr();
    scheduler.getServiceMetrics().containersReady.incr();
    if (updateDefinition) {
      checkAndUpdateComponentState(this, true);
    }
  }

  public void decContainersReady(boolean updateDefinition) {
    componentMetrics.containersReady.decr();
    scheduler.getServiceMetrics().containersReady.decr();
    if (updateDefinition) {
      checkAndUpdateComponentState(this, false);
    }
  }

  public int getNumReadyInstances() {
    return componentMetrics.containersReady.value();
  }

  public int getNumRunningInstances() {
    return componentMetrics.containersRunning.value();
  }

  public int getNumDesiredInstances() {
    return componentMetrics.containersDesired.value();
  }

  public ComponentInstance getComponentInstance(String componentInstanceName) {
    return compInstances.get(componentInstanceName);
  }

  public Collection<ComponentInstance> getAllComponentInstances() {
    return compInstances.values();
  }

  public org.apache.hadoop.yarn.service.api.records.Component getComponentSpec() {
    return this.componentSpec;
  }

  public void resetCompFailureCount() {
    LOG.info("[COMPONENT {}]: Reset container failure count from {} to 0.",
        getName(), currentContainerFailure.get());
    currentContainerFailure.set(0);
    failureTracker.resetContainerFailures();
  }

  public Probe getProbe() {
    return probe;
  }

  public Priority getPriority() {
    return priority;
  }

  public long getAllocateId() {
    return allocateId;
  }

  public String getName () {
    return componentSpec.getName();
  }

  public ComponentState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Returns whether a component is upgrading or not.
   */
  public boolean isUpgrading() {
    this.readLock.lock();

    try {
      return !(upgradeStatus.isCompleted() &&
          cancelUpgradeStatus.isCompleted());
    } finally {
      this.readLock.unlock();
    }
  }

  public UpgradeStatus getUpgradeStatus() {
    this.readLock.lock();
    try {
      return upgradeStatus;
    } finally {
      this.readLock.unlock();
    }
  }

  public UpgradeStatus getCancelUpgradeStatus() {
    this.readLock.lock();
    try {
      return cancelUpgradeStatus;
    } finally {
      this.readLock.unlock();
    }
  }

  public ServiceScheduler getScheduler() {
    return scheduler;
  }

  @Override
  public void handle(ComponentEvent event) {
    writeLock.lock();
    try {
      ComponentState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.error(MessageFormat.format("[COMPONENT {0}]: Invalid event {1} at {2}",
            componentSpec.getName(), event.getType(), oldState), e);
      }
      if (oldState != getState()) {
        LOG.info("[COMPONENT {}] Transitioned from {} to {} on {} event.",
            componentSpec.getName(), oldState, getState(), event.getType());
      }
    } finally {
      writeLock.unlock();
    }
  }

  private static class BaseTransition implements
      SingleArcTransition<Component, ComponentEvent> {

    @Override public void transition(Component component,
        ComponentEvent event) {
    }
  }

  /**
   * Sets the state of the component in the component spec.
   * @param state component state
   */
  private void setComponentState(
      org.apache.hadoop.yarn.service.api.records.ComponentState state) {
    org.apache.hadoop.yarn.service.api.records.ComponentState curState =
        componentSpec.getState();
    if (!curState.equals(state)) {
      componentSpec.setState(state);
      LOG.info("[COMPONENT {}] spec state changed from {} -> {}",
          componentSpec.getName(), curState, state);
    }
  }

  /**
   * Status of upgrade.
   */
  public static class UpgradeStatus {
    private org.apache.hadoop.yarn.service.api.records.Component targetSpec;
    private String targetVersion;
    private AtomicBoolean inProgress = new AtomicBoolean(false);
    private AtomicLong containersNeedUpgrade = new AtomicLong(0);
    private AtomicBoolean failed = new AtomicBoolean(false);

    public org.apache.hadoop.yarn.service.api.records.
        Component getTargetSpec() {
      return targetSpec;
    }

    public String getTargetVersion() {
      return targetVersion;
    }

    /*
     * @return whether the upgrade is completed or not
     */
    public boolean isCompleted() {
      return !inProgress.get();
    }

    public void decContainersThatNeedUpgrade() {
      if (inProgress.get()) {
        containersNeedUpgrade.decrementAndGet();
      }
    }

    public void containerFailedUpgrade() {
      failed.set(true);
    }

    void reset() {
      containersNeedUpgrade.set(0);
      targetSpec = null;
      targetVersion = null;
      inProgress.set(false);
      failed.set(false);
    }

    boolean areContainersUpgrading() {
      return containersNeedUpgrade.get() != 0;
    }
  }

  public ServiceContext getContext() {
    return context;
  }

  // Only for testing
  public List<ComponentInstance> getPendingInstances() {
    return pendingInstances;
  }

  public boolean isHealthThresholdMonitorEnabled() {
    return healthThresholdMonitorEnabled;
  }

  public void setHealthThresholdMonitorEnabled(
      boolean healthThresholdMonitorEnabled) {
    this.healthThresholdMonitorEnabled = healthThresholdMonitorEnabled;
  }

  public Collection<ComponentInstance> getSucceededInstances() {
    return succeededInstances.values();
  }

  public long getNumSucceededInstances() {
    return succeededInstances.size();
  }

  public long getNumFailedInstances() {
    return failedInstances.size();
  }

  public Collection<ComponentInstance> getFailedInstances() {
    return failedInstances.values();
  }

  public synchronized void markAsSucceeded(ComponentInstance instance) {
    removeFailedInstanceIfExists(instance);
    succeededInstances.put(instance.getCompInstanceName(), instance);
  }

  public synchronized void markAsFailed(ComponentInstance instance) {
    removeSuccessfulInstanceIfExists(instance);
    failedInstances.put(instance.getCompInstanceName(), instance);
  }

  public boolean removeFailedInstanceIfExists(ComponentInstance instance) {
    if (failedInstances.containsKey(instance.getCompInstanceName())) {
      failedInstances.remove(instance.getCompInstanceName());
      return true;
    }
    return false;
  }

  public boolean removeSuccessfulInstanceIfExists(ComponentInstance instance) {
    if (succeededInstances.containsKey(instance.getCompInstanceName())) {
      succeededInstances.remove(instance.getCompInstanceName());
      return true;
    }
    return false;
  }

  public boolean isReadyForDownstream() {
    return getRestartPolicyHandler().isReadyForDownStream(this);
  }

  public static ComponentRestartPolicy getRestartPolicyHandler(
      RestartPolicyEnum restartPolicyEnum) {

    if (RestartPolicyEnum.NEVER == restartPolicyEnum) {
      return NeverRestartPolicy.getInstance();
    } else if (RestartPolicyEnum.ON_FAILURE == restartPolicyEnum) {
      return OnFailureRestartPolicy.getInstance();
    } else{
      return AlwaysRestartPolicy.getInstance();
    }
  }

  public ComponentRestartPolicy getRestartPolicyHandler() {
    RestartPolicyEnum restartPolicyEnum = getComponentSpec().getRestartPolicy();
    return getRestartPolicyHandler(restartPolicyEnum);
  }

  public String getHostnameSuffix() {
    return ServiceApiUtil.getHostnameSuffix(context.service.getName(),
        scheduler.getConfig());
  }
}
