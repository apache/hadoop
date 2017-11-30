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

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceId;
import org.apache.hadoop.yarn.service.ContainerFailureTracker;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.ServiceScheduler;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEvent;
import org.apache.hadoop.yarn.service.ServiceMetrics;
import org.apache.hadoop.yarn.service.provider.ProviderUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.apache.hadoop.yarn.service.monitor.probe.MonitorUtils;
import org.apache.hadoop.yarn.service.monitor.probe.Probe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.api.records.ContainerExitStatus.*;
import static org.apache.hadoop.yarn.service.api.ServiceApiConstants.*;
import static org.apache.hadoop.yarn.service.component.ComponentEventType.*;
import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType.START;
import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType.STOP;
import static org.apache.hadoop.yarn.service.component.ComponentState.*;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.CONTAINER_FAILURE_THRESHOLD;

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

  private StateMachine<ComponentState, ComponentEventType, ComponentEvent>
      stateMachine;
  private AsyncDispatcher dispatcher;
  private static final StateMachineFactory<Component, ComponentState, ComponentEventType, ComponentEvent>
      stateMachineFactory =
      new StateMachineFactory<Component, ComponentState, ComponentEventType, ComponentEvent>(
          INIT)
           // INIT will only got to FLEXING
          .addTransition(INIT, EnumSet.of(STABLE, FLEXING),
              FLEX, new FlexComponentTransition())
          // container recovered on AM restart
          .addTransition(INIT, INIT, CONTAINER_RECOVERED,
              new ContainerRecoveredTransition())

          // container allocated by RM
          .addTransition(FLEXING, FLEXING, CONTAINER_ALLOCATED,
              new ContainerAllocatedTransition())
          // container launched on NM
          .addTransition(FLEXING, EnumSet.of(STABLE, FLEXING),
              CONTAINER_STARTED, new ContainerStartedTransition())
          // container failed while flexing
          .addTransition(FLEXING, FLEXING, CONTAINER_COMPLETED,
              new ContainerCompletedTransition())
          // Flex while previous flex is still in progress
          .addTransition(FLEXING, EnumSet.of(FLEXING, STABLE), FLEX,
              new FlexComponentTransition())

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
    probe = MonitorUtils.getProbe(componentSpec.getReadinessCheck());
    maxContainerFailurePerComp = componentSpec.getConfiguration()
        .getPropertyInt(CONTAINER_FAILURE_THRESHOLD, 10);
    createNumCompInstances(component.getNumberOfContainers());
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
    ComponentInstance instance = new ComponentInstance(this, id);
    compInstances.put(instance.getCompInstanceName(), instance);
    pendingInstances.add(instance);
  }

  private static class FlexComponentTransition implements
      MultipleArcTransition<Component, ComponentEvent, ComponentState> {
    // For flex up, go to FLEXING state
    // For flex down, go to STABLE state
    @Override
    public ComponentState transition(Component component,
        ComponentEvent event) {
      component.setDesiredContainers((int)event.getDesired());
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
        component.componentSpec.setState(
            org.apache.hadoop.yarn.service.api.records.ComponentState.FLEXING);
        return FLEXING;
      } else if (delta < 0){
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
        component.componentSpec.setState(
            org.apache.hadoop.yarn.service.api.records.ComponentState.STABLE);
        return STABLE;
      } else {
        LOG.info("[FLEX COMPONENT " + component.getName() + "]: already has " +
            event.getDesired() + " instances, ignoring");
        component.componentSpec.setState(
            org.apache.hadoop.yarn.service.api.records.ComponentState.STABLE);
        return STABLE;
      }
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
      ProviderUtils.initCompInstanceDir(component.getContext().fs, instance);
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

  private static ComponentState checkIfStable(Component component) {
    // if desired == running
    if (component.componentMetrics.containersRunning.value() == component
        .getComponentSpec().getNumberOfContainers()) {
      component.componentSpec.setState(
          org.apache.hadoop.yarn.service.api.records.ComponentState.STABLE);
      return STABLE;
    } else {
      component.componentSpec.setState(
          org.apache.hadoop.yarn.service.api.records.ComponentState.FLEXING);
      return FLEXING;
    }
  }

  private static class ContainerCompletedTransition extends BaseTransition {
    @Override
    public void transition(Component component, ComponentEvent event) {
      component.updateMetrics(event.getStatus());
      component.dispatcher.getEventHandler().handle(
          new ComponentInstanceEvent(event.getStatus().getContainerId(),
              STOP).setStatus(event.getStatus()));
      component.componentSpec.setState(
          org.apache.hadoop.yarn.service.api.records.ComponentState.FLEXING);
    }
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
    scheduler.getContainerLaunchService()
        .launchCompInstance(scheduler.getApp(), instance, container);
  }

  @SuppressWarnings({ "unchecked" })
  public void requestContainers(long count) {
    Resource resource = Resource
        .newInstance(componentSpec.getResource().getMemoryMB(),
            componentSpec.getResource().getCpus());

    for (int i = 0; i < count; i++) {
      //TODO Once YARN-5468 is done, use that for anti-affinity
      ContainerRequest request =
          ContainerRequest.newBuilder().capability(resource).priority(priority)
              .allocationRequestId(allocateId).relaxLocality(true).build();
      amrmClient.addContainerRequest(request);
    }
  }

  private void setDesiredContainers(int n) {
    int delta = n - scheduler.getServiceMetrics().containersDesired.value();
    if (delta > 0) {
      scheduler.getServiceMetrics().containersDesired.incr(delta);
    } else {
      scheduler.getServiceMetrics().containersDesired.decr(delta);
    }
    componentMetrics.containersDesired.set(n);
  }



  private void updateMetrics(ContainerStatus status) {
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

    // containersFailed include preempted, disks_failed etc.
    componentMetrics.containersFailed.incr();
    scheduler.getServiceMetrics().containersFailed.incr();

    if (Apps.shouldCountTowardsNodeBlacklisting(status.getExitStatus())) {
      String host = scheduler.getLiveInstances().get(status.getContainerId())
          .getNodeId().getHost();
      failureTracker.incNodeFailure(host);
      currentContainerFailure.getAndIncrement() ;
    }
  }

  public boolean areDependenciesReady() {
    List<String> dependencies = componentSpec.getDependencies();
    if (ServiceUtils.isEmpty(dependencies)) {
      return true;
    }
    for (String dependency : dependencies) {
      Component dependentComponent =
          scheduler.getAllComponents().get(dependency);
      if (dependentComponent == null) {
        LOG.error("Couldn't find dependency {} for {} (should never happen)",
            dependency, getName());
        continue;
      }
      if (dependentComponent.getNumReadyInstances() < dependentComponent
          .getNumDesiredInstances()) {
        LOG.info("[COMPONENT {}]: Dependency {} not satisfied, only {} of {}"
                + " instances are ready.", getName(), dependency,
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

  public void incContainersReady() {
    componentMetrics.containersReady.incr();
    scheduler.getServiceMetrics().containersReady.incr();
  }

  public void decContainersReady() {
    componentMetrics.containersReady.decr();
    scheduler.getServiceMetrics().containersReady.decr();
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
  public ServiceScheduler getScheduler() {
    return scheduler;
  }

  @Override
  public void handle(ComponentEvent event) {
    try {
      writeLock.lock();
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

  public ServiceContext getContext() {
    return context;
  }

  // Only for testing
  public List<ComponentInstance> getPendingInstances() {
    return pendingInstances;
  }
}
