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

package org.apache.hadoop.yarn.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages the state of the service.
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

  private final AsyncDispatcher dispatcher;
  private final SliderFileSystem fs;
  private final UpgradeComponentsFinder componentsFinder;

  private String upgradeVersion;

  private static final StateMachineFactory<ServiceManager, State,
      ServiceEventType, ServiceEvent> STATE_MACHINE_FACTORY =
      new StateMachineFactory<ServiceManager, State,
          ServiceEventType, ServiceEvent>(State.STABLE)

          .addTransition(State.STABLE, EnumSet.of(State.STABLE,
              State.UPGRADING), ServiceEventType.UPGRADE,
              new StartUpgradeTransition())

          .addTransition(State.UPGRADING, EnumSet.of(State.STABLE,
              State.UPGRADING), ServiceEventType.START,
              new StopUpgradeTransition())
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
            "[SERVICE]: Invalid event {0} at {1}.", event.getType(),
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
        Service targetSpec = ServiceApiUtil.loadServiceUpgrade(
            serviceManager.fs, serviceManager.getName(), event.getVersion());

        serviceManager.serviceSpec.setState(ServiceState.UPGRADING);
        List<org.apache.hadoop.yarn.service.api.records.Component>
            compsThatNeedUpgrade = serviceManager.componentsFinder.
            findTargetComponentSpecs(serviceManager.serviceSpec, targetSpec);

        if (compsThatNeedUpgrade != null && !compsThatNeedUpgrade.isEmpty()) {
          compsThatNeedUpgrade.forEach(component -> {
            ComponentEvent needUpgradeEvent = new ComponentEvent(
                component.getName(), ComponentEventType.UPGRADE).
                setTargetSpec(component);
            serviceManager.dispatcher.getEventHandler().handle(
                needUpgradeEvent);
          });
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

  private static class StopUpgradeTransition implements
      MultipleArcTransition<ServiceManager, ServiceEvent, State> {

    @Override
    public State transition(ServiceManager serviceManager,
        ServiceEvent event) {
      //abort is not supported currently
      //trigger re-check of service state
      ServiceMaster.checkAndUpdateServiceState(serviceManager.scheduler,
          true);
      if (serviceManager.serviceSpec.getState().equals(ServiceState.STABLE)) {
        return serviceManager.finalizeUpgrade() ? State.STABLE :
            State.UPGRADING;
      } else {
        return State.UPGRADING;
      }
    }
  }

  /**
   * @return whether finalization of upgrade was successful.
   */
  private boolean finalizeUpgrade() {
    try {
      Service upgradeSpec = ServiceApiUtil.loadServiceUpgrade(
          fs, getName(), upgradeVersion);
      ServiceApiUtil.writeAppDefinition(fs,
          ServiceApiUtil.getServiceJsonPath(fs, getName()), upgradeSpec);
    } catch (IOException e) {
      LOG.error("Upgrade did not complete because unable to overwrite the" +
          " service definition", e);
      return false;
    }

    try {
      fs.deleteClusterUpgradeDir(getName(), upgradeVersion);
    } catch (IOException e) {
      LOG.warn("Unable to delete upgrade definition for service {} " +
              "version {}", getName(), upgradeVersion);
    }
    serviceSpec.setVersion(upgradeVersion);
    upgradeVersion = null;
    return true;
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
