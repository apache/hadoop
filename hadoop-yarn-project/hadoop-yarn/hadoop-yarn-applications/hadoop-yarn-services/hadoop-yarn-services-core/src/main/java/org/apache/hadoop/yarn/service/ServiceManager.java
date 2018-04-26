/*
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

package org.apache.hadoop.yarn.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.component.ComponentEvent;
import org.apache.hadoop.yarn.service.component.ComponentEventType;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.service.utils.ServiceApiUtil.jsonSerDeser;

/**
 * Manages the state of Service.
 */
public class ServiceManager implements EventHandler<ServiceEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(
      ServiceManager.class);

  private final Service serviceSpec;
  private final ServiceContext context;
  private final ServiceScheduler scheduler;
  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;

  private final StateMachine<State, ServiceEventType, ServiceEvent>
      stateMachine;
  private final UpgradeComponentsFinder componentsFinder;

  private final AsyncDispatcher dispatcher;
  private final SliderFileSystem fs;

  private String upgradeVersion;

  private static final StateMachineFactory<ServiceManager, State,
      ServiceEventType, ServiceEvent> STATE_MACHINE_FACTORY =
      new StateMachineFactory<ServiceManager, State,
          ServiceEventType, ServiceEvent>(State.STABLE)

          .addTransition(State.STABLE, EnumSet.of(State.STABLE,
              State.UPGRADING), ServiceEventType.UPGRADE,
              new StartUpgradeTransition())

          .addTransition(State.STABLE, EnumSet.of(State.STABLE),
              ServiceEventType.CHECK_STABLE, new CheckStableTransition())

          .addTransition(State.UPGRADING, EnumSet.of(State.STABLE,
              State.UPGRADING), ServiceEventType.START,
              new CheckStableTransition())

          .addTransition(State.UPGRADING, EnumSet.of(State.STABLE,
              State.UPGRADING), ServiceEventType.CHECK_STABLE,
              new CheckStableTransition())
          .installTopology();

  public ServiceManager(ServiceContext context) {
    Preconditions.checkNotNull(context);
    this.context = context;
    serviceSpec = context.service;
    scheduler = context.scheduler;
    stateMachine = STATE_MACHINE_FACTORY.make(this);
    dispatcher = scheduler.getDispatcher();

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
    fs = context.fs;
    componentsFinder = new UpgradeComponentsFinder
        .DefaultUpgradeComponentsFinder();
  }

  @Override
  public void handle(ServiceEvent event) {
    try {
      writeLock.lock();
      State oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.error(MessageFormat.format(
            "[SERVICE]: Invalid event {1} at {2}.", event.getType(),
            oldState), e);
      }
      if (oldState != getState()) {
        LOG.info("[SERVICE] Transitioned from {} to {} on {} event.",
            oldState, getState(), event.getType());
      }
    } finally {
      writeLock.unlock();
    }
  }

  private State getState() {
    this.readLock.lock();
    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  private static class StartUpgradeTransition implements
      MultipleArcTransition<ServiceManager, ServiceEvent, State> {

    @Override
    public State transition(ServiceManager serviceManager,
        ServiceEvent event) {
      try {
        if (!event.isAutoFinalize()) {
          serviceManager.serviceSpec.setState(ServiceState.UPGRADING);
        } else {
          serviceManager.serviceSpec.setState(
              ServiceState.UPGRADING_AUTO_FINALIZE);
        }
        serviceManager.upgradeVersion = event.getVersion();
        return State.UPGRADING;
      } catch (Throwable e) {
        LOG.error("[SERVICE]: Upgrade to version {} failed", event.getVersion(),
            e);
        return State.STABLE;
      }
    }
  }

  private static class CheckStableTransition implements
      MultipleArcTransition<ServiceManager, ServiceEvent, State> {

    @Override
    public State transition(ServiceManager serviceManager,
        ServiceEvent event) {
      //trigger check of service state
      ServiceState currState = serviceManager.serviceSpec.getState();
      if (currState.equals(ServiceState.STABLE)) {
        return State.STABLE;
      }
      if (currState.equals(ServiceState.UPGRADING_AUTO_FINALIZE) ||
          event.getType().equals(ServiceEventType.START)) {
        ServiceState targetState = checkIfStable(serviceManager.serviceSpec);
        if (targetState.equals(ServiceState.STABLE)) {
          if (serviceManager.finalizeUpgrade()) {
            LOG.info("Service def state changed from {} -> {}", currState,
                serviceManager.serviceSpec.getState());
            return State.STABLE;
          }
        }
      }
      return State.UPGRADING;
    }
  }

  /**
   * @return whether finalization of upgrade was successful.
   */
  private boolean finalizeUpgrade() {
    try {
      // save the application id and state to
      Service targetSpec = ServiceApiUtil.loadServiceUpgrade(
          fs, getName(), upgradeVersion);
      targetSpec.setId(serviceSpec.getId());
      targetSpec.setState(ServiceState.STABLE);
      Map<String, Component> allComps = scheduler.getAllComponents();
      targetSpec.getComponents().forEach(compSpec -> {
        Component comp = allComps.get(compSpec.getName());
        compSpec.setState(comp.getComponentSpec().getState());
      });
      jsonSerDeser.save(fs.getFileSystem(),
          ServiceApiUtil.getServiceJsonPath(fs, getName()), targetSpec, true);
      fs.deleteClusterUpgradeDir(getName(), upgradeVersion);
    } catch (IOException e) {
      LOG.error("Upgrade did not complete because unable to re-write the" +
          " service definition", e);
      return false;
    }

    try {
      fs.deleteClusterUpgradeDir(getName(), upgradeVersion);
    } catch (IOException e) {
      LOG.warn("Unable to delete upgrade definition for service {} " +
          "version {}", getName(), upgradeVersion);
    }
    serviceSpec.setState(ServiceState.STABLE);
    serviceSpec.setVersion(upgradeVersion);
    upgradeVersion = null;
    return true;
  }

  private static ServiceState checkIfStable(Service service) {
    // if desired == running
    for (org.apache.hadoop.yarn.service.api.records.Component comp :
        service.getComponents()) {
      if (!comp.getState().equals(
          org.apache.hadoop.yarn.service.api.records.ComponentState.STABLE)) {
        return service.getState();
      }
    }
    return ServiceState.STABLE;
  }

  /**
   * Service state gets directly modified by ServiceMaster and Component.
   * This is a problem for upgrade and flexing. For now, invoking
   * ServiceMaster.checkAndUpdateServiceState here to make it easy to fix
   * this in future.
   */
  public void checkAndUpdateServiceState(boolean isIncrement) {
    writeLock.lock();
    try {
      if (!getState().equals(State.UPGRADING)) {
        ServiceMaster.checkAndUpdateServiceState(this.scheduler,
            isIncrement);
      }
    } finally {
      writeLock.unlock();
    }
  }

  void processUpgradeRequest(String upgradeVersion,
      boolean autoFinalize) throws IOException {
    Service targetSpec = ServiceApiUtil.loadServiceUpgrade(
        context.fs, context.service.getName(), upgradeVersion);

    List<org.apache.hadoop.yarn.service.api.records.Component>
        compsThatNeedUpgrade = componentsFinder.
        findTargetComponentSpecs(context.service, targetSpec);
    ServiceEvent event = new ServiceEvent(ServiceEventType.UPGRADE)
        .setVersion(upgradeVersion)
        .setAutoFinalize(autoFinalize);
    context.scheduler.getDispatcher().getEventHandler().handle(event);

    if (compsThatNeedUpgrade != null && !compsThatNeedUpgrade.isEmpty()) {
      if (autoFinalize) {
        event.setAutoFinalize(true);
      }
      compsThatNeedUpgrade.forEach(component -> {
        ComponentEvent needUpgradeEvent = new ComponentEvent(
            component.getName(), ComponentEventType.UPGRADE)
            .setTargetSpec(component)
            .setUpgradeVersion(event.getVersion());
        context.scheduler.getDispatcher().getEventHandler().handle(
            needUpgradeEvent);
      });
    } else {
      // nothing to upgrade if upgrade auto finalize is requested, trigger a
      // state check.
      if (autoFinalize) {
        context.scheduler.getDispatcher().getEventHandler().handle(
            new ServiceEvent(ServiceEventType.CHECK_STABLE));
      }
    }
  }

  /**
   * Returns the name of the service.
   */
  public String getName() {
    return serviceSpec.getName();
  }

  /**
   * State of {@link ServiceManager}.
   */
  public enum State {
    STABLE, UPGRADING
  }

  @VisibleForTesting
  Service getServiceSpec() {
    return serviceSpec;
  }
}
