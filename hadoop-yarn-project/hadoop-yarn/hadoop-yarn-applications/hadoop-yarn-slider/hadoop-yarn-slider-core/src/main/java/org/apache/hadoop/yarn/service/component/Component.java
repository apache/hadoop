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
import org.apache.hadoop.yarn.service.compinstance.ComponentInstance;
import org.apache.hadoop.yarn.service.compinstance.ComponentInstanceId;
import org.apache.hadoop.yarn.service.ContainerFailureTracker;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.ServiceScheduler;
import org.apache.hadoop.yarn.service.compinstance.ComponentInstanceEvent;
import org.apache.hadoop.yarn.service.metrics.ServiceMetrics;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.server.servicemonitor.MonitorUtils;
import org.apache.slider.server.servicemonitor.Probe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.api.records.ContainerExitStatus.*;
import static org.apache.hadoop.yarn.service.component.ComponentEventType.*;
import static org.apache.hadoop.yarn.service.compinstance.ComponentInstanceEventType.STARTED;
import static org.apache.hadoop.yarn.service.compinstance.ComponentInstanceEventType.STOP;
import static org.apache.hadoop.yarn.service.component.ComponentState.*;
import static org.apache.slider.api.ResourceKeys.CONTAINER_FAILURE_THRESHOLD;

public class Component implements EventHandler<ComponentEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(Component.class);

  private org.apache.slider.api.resource.Component componentSpec;
  private long allocateId;
  private Priority priority;
  private ServiceMetrics componentMetrics;
  private ServiceScheduler scheduler;
  private ServiceContext context;
  private AMRMClientAsync<ContainerRequest> amrmClient;
  private AtomicLong instanceIdCounter = new AtomicLong();
  private Map<ComponentInstanceId, ComponentInstance> compInstances =
      new ConcurrentHashMap<>();
  // component instances to be assigned with a container
  private List<ComponentInstance> pendingInstances = new LinkedList<>();
  private ContainerFailureTracker failureTracker;
  private Probe probe;
  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;
  public int maxContainerFailurePerComp;
  // The number of containers failed since last reset. This excludes preempted,
  // disk_failed containers etc. This will be reset to 0 periodically.
  public volatile int currentContainerFailure;

  private StateMachine<ComponentState, ComponentEventType, ComponentEvent>
      stateMachine;
  private AsyncDispatcher compInstanceDispatcher;
  private static final StateMachineFactory<Component, ComponentState, ComponentEventType, ComponentEvent>
      stateMachineFactory =
      new StateMachineFactory<Component, ComponentState, ComponentEventType, ComponentEvent>(
          INIT)
           // INIT will only got to FLEXING
          .addTransition(INIT, EnumSet.of(STABLE, FLEXING),
              FLEX, new FlexComponentTransition())

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
          .addTransition(FLEXING, EnumSet.of(FLEXING), FLEX,
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

  public Component(org.apache.slider.api.resource.Component component,
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
    compInstanceDispatcher = scheduler.getCompInstanceDispatcher();
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
    compInstances.put(id, instance);
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
        component.requestContainers(event.getDesired());
        return FLEXING;
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
        return FLEXING;
      } else if (delta < 0){
        delta = 0 - delta;
        // scale down
        LOG.info("[FLEX DOWN COMPONENT " + component.getName()
            + "]: scaling down from " + before + " to " + event.getDesired());
        List<ComponentInstance> list =
            new ArrayList<>(component.compInstances.values());

        // sort in Most recent -> oldest order, destroy most recent ones.
        Collections.sort(list, Collections.reverseOrder());
        for (int i = 0; i < delta; i++) {
          ComponentInstance instance = list.get(i);
          // remove the instance
          component.compInstances.remove(instance.getCompInstanceId());
          component.pendingInstances.remove(instance);
          component.componentMetrics.containersFailed.incr();
          component.componentMetrics.containersRunning.decr();
          // decrement id counter
          component.instanceIdCounter.decrementAndGet();
          instance.destroy();
        }
        return STABLE;
      } else {
        LOG.info("[FLEX COMPONENT " + component.getName() + "]: already has " +
            event.getDesired() + " instances, ignoring");
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

  private static class ContainerStartedTransition implements
      MultipleArcTransition<Component,ComponentEvent,ComponentState> {

    @Override public ComponentState transition(Component component,
        ComponentEvent event) {
      component.compInstanceDispatcher.getEventHandler().handle(
          new ComponentInstanceEvent(event.getInstance().getContainerId(),
              STARTED));
      component.incRunningContainers();
      return checkIfStable(component);
    }
  }

  private static ComponentState checkIfStable(Component component) {
    // if desired == running
    if (component.componentMetrics.containersRunning.value() == component
        .getComponentSpec().getNumberOfContainers()) {
      return STABLE;
    } else {
      return FLEXING;
    }
  }

  private static class ContainerCompletedTransition extends BaseTransition {
    @Override
    public void transition(Component component, ComponentEvent event) {
      component.updateMetrics(event.getStatus());

      // add back to pending list
      component.pendingInstances.add(event.getInstance());
      LOG.info(
          "[COMPONENT {}]: {} completed, num pending comp instances increased to {}.",
          component.getName(), event.getStatus().getContainerId(),
          component.pendingInstances.size());
      component.compInstanceDispatcher.getEventHandler().handle(
          new ComponentInstanceEvent(event.getStatus().getContainerId(),
              STOP).setStatus(event.getStatus()));
    }
  }

  public ServiceMetrics getCompMetrics () {
    return componentMetrics;
  }

  private void assignContainerToCompInstance(Container container) {
    if (pendingInstances.size() == 0) {
      LOG.info(
          "[COMPONENT {}]: No pending component instance left, release surplus container {}",
          getName(), container.getId());
      scheduler.getAmRMClient().releaseAssignedContainer(container.getId());
      componentMetrics.surplusContainers.incr();
      scheduler.getServiceMetrics().surplusContainers.incr();
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

    // dec running container
    decRunningContainers();

    if (Apps.shouldCountTowardsNodeBlacklisting(status.getExitStatus())) {
      String host = scheduler.getLiveInstances().get(status.getContainerId())
          .getNodeId().getHost();
      failureTracker.incNodeFailure(host);
      currentContainerFailure++ ;
    }
  }

  public boolean areDependenciesReady() {
    List<String> dependencies = componentSpec.getDependencies();
    if (SliderUtils.isEmpty(dependencies)) {
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

  private void incRunningContainers() {
    componentMetrics.containersRunning.incr();
    scheduler.getServiceMetrics().containersRunning.incr();
  }

  public void incContainersReady() {
    componentMetrics.containersReady.incr();
  }

  public void decContainersReady() {
    componentMetrics.containersReady.decr();
  }

  private void decRunningContainers() {
    componentMetrics.containersRunning.decr();
    scheduler.getServiceMetrics().containersRunning.decr();
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

  public Map<ComponentInstanceId, ComponentInstance> getAllComponentInstances() {
    return compInstances;
  }

  public org.apache.slider.api.resource.Component getComponentSpec() {
    return this.componentSpec;
  }

  public void resetCompFailureCount() {
    LOG.info("[COMPONENT {}]: Reset container failure count from {} to 0.",
        getName(), currentContainerFailure);
    currentContainerFailure = 0;
    failureTracker.resetContainerFailures();
  }

  public Probe getProbe() {
    return probe;
  }

  public Priority getPriority() {
    return priority;
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
        LOG.error("Invalid event " + event.getType() +
            " at " + oldState + " for component " + componentSpec.getName(), e);
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
}
