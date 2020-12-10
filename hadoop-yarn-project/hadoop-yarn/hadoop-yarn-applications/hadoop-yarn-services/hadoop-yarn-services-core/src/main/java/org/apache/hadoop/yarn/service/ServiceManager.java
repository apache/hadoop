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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.component.ComponentEvent;
import org.apache.hadoop.yarn.service.component.ComponentEventType;
import org.apache.hadoop.yarn.service.component.ComponentRestartPolicy;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEvent;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.apache.hadoop.yarn.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
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
  private List<org.apache.hadoop.yarn.service.api.records
      .Component> componentsToUpgrade;
  private List<String> compsAffectedByUpgrade = new ArrayList<>();
  private String cancelledVersion;

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
              new StartFromUpgradeTransition())

          .addTransition(State.UPGRADING, EnumSet.of(State.STABLE,
              State.UPGRADING), ServiceEventType.CHECK_STABLE,
              new CheckStableTransition())

          .addTransition(State.UPGRADING, State.UPGRADING,
              ServiceEventType.CANCEL_UPGRADE, new CancelUpgradeTransition())
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
    writeLock.lock();
    try {
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
      serviceManager.upgradeVersion = event.getVersion();
      serviceManager.componentsToUpgrade = event.getCompsToUpgrade();
      event.getCompsToUpgrade().forEach(comp ->
          serviceManager.compsAffectedByUpgrade.add(comp.getName()));
      try {
        if (event.isExpressUpgrade()) {
          serviceManager.dispatchNeedUpgradeEvents(false);
          serviceManager.upgradeNextCompIfAny(false);
        } else {
          serviceManager.dispatchNeedUpgradeEvents(false);
        }

        if (event.isExpressUpgrade()) {
          serviceManager.setServiceState(ServiceState.EXPRESS_UPGRADING);
        } else if (event.isAutoFinalize()) {
          serviceManager.setServiceState(ServiceState.UPGRADING_AUTO_FINALIZE);
        } else {
          serviceManager.setServiceState(ServiceState.UPGRADING);
        }
        ServiceApiUtil.checkServiceDependencySatisified(serviceManager
            .getServiceSpec());
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
      if (currState.equals(ServiceState.EXPRESS_UPGRADING) ||
          currState.equals(ServiceState.CANCEL_UPGRADING)) {

        if (!serviceManager.componentsToUpgrade.isEmpty()) {
          org.apache.hadoop.yarn.service.api.records.Component compSpec =
              serviceManager.componentsToUpgrade.get(0);
          Component component = serviceManager.scheduler.getAllComponents()
              .get(compSpec.getName());

          if (!component.isUpgrading()) {
            serviceManager.componentsToUpgrade.remove(0);
            serviceManager.upgradeNextCompIfAny(
                currState.equals(ServiceState.CANCEL_UPGRADING));
          }
        }
      }

      if (currState.equals(ServiceState.UPGRADING_AUTO_FINALIZE) ||
          ((currState.equals(ServiceState.EXPRESS_UPGRADING) ||
              currState.equals(ServiceState.CANCEL_UPGRADING)) &&
              serviceManager.componentsToUpgrade.isEmpty())) {

        ServiceState targetState = checkIfStable(serviceManager.serviceSpec);
        if (targetState.equals(ServiceState.STABLE)) {
          if (serviceManager.finalizeUpgrade(
              currState.equals(ServiceState.CANCEL_UPGRADING))) {
            return State.STABLE;
          }
        }
      }
      return State.UPGRADING;
    }
  }

  private static class StartFromUpgradeTransition implements
      MultipleArcTransition<ServiceManager, ServiceEvent, State> {

    @Override
    public State transition(ServiceManager serviceManager, ServiceEvent event) {
      ServiceState currState = serviceManager.serviceSpec.getState();

      ServiceState targetState = checkIfStable(serviceManager.serviceSpec);
      if (targetState.equals(ServiceState.STABLE)) {
        if (serviceManager.finalizeUpgrade(
            currState.equals(ServiceState.CANCEL_UPGRADING))) {
          return State.STABLE;
        }
      }
      return State.UPGRADING;
    }
  }

  private static class CancelUpgradeTransition implements
      SingleArcTransition<ServiceManager, ServiceEvent> {

    @Override
    public void transition(ServiceManager serviceManager,
        ServiceEvent event) {
      if (!serviceManager.getState().equals(State.UPGRADING)) {
        LOG.info("[SERVICE]: Cannot cancel the upgrade in {} state",
            serviceManager.getState());
        return;
      }
      try {
        Service targetSpec = ServiceApiUtil.loadService(
            serviceManager.context.fs, serviceManager.getName());

        Service sourceSpec = ServiceApiUtil.loadServiceUpgrade(
            serviceManager.context.fs, serviceManager.getName(),
            serviceManager.upgradeVersion);
        serviceManager.cancelledVersion = serviceManager.upgradeVersion;
        LOG.info("[SERVICE] cancel version {}",
            serviceManager.cancelledVersion);
        serviceManager.upgradeVersion = serviceManager.serviceSpec.getVersion();
        serviceManager.componentsToUpgrade = serviceManager
            .resolveCompsToUpgrade(sourceSpec, targetSpec);

        serviceManager.compsAffectedByUpgrade.clear();
        serviceManager.componentsToUpgrade.forEach(comp ->
            serviceManager.compsAffectedByUpgrade.add(comp.getName()));

        serviceManager.dispatchNeedUpgradeEvents(true);
        serviceManager.upgradeNextCompIfAny(true);
        serviceManager.setServiceState(ServiceState.CANCEL_UPGRADING);
      } catch (Throwable e) {
        LOG.error("[SERVICE]: Cancellation of upgrade failed", e);
      }
    }
  }

  private void upgradeNextCompIfAny(boolean cancelUpgrade) {
    if (!componentsToUpgrade.isEmpty()) {
      org.apache.hadoop.yarn.service.api.records.Component component =
          componentsToUpgrade.get(0);

      serviceSpec.getComponent(component.getName()).getContainers().forEach(
          container -> {
            ComponentInstanceEvent upgradeEvent = new ComponentInstanceEvent(
                ContainerId.fromString(container.getId()),
                !cancelUpgrade ? ComponentInstanceEventType.UPGRADE :
                    ComponentInstanceEventType.CANCEL_UPGRADE);
            LOG.info("Upgrade container {} {}", container.getId(),
                cancelUpgrade);
            dispatcher.getEventHandler().handle(upgradeEvent);
          });
    }
  }

  private void dispatchNeedUpgradeEvents(boolean cancelUpgrade) {
    if (componentsToUpgrade != null) {
      componentsToUpgrade.forEach(component -> {
        ComponentEvent needUpgradeEvent = new ComponentEvent(
            component.getName(), !cancelUpgrade ? ComponentEventType.UPGRADE :
            ComponentEventType.CANCEL_UPGRADE)
            .setTargetSpec(component)
            .setUpgradeVersion(upgradeVersion);
        LOG.info("Upgrade component {} {}", component.getName(), cancelUpgrade);
        context.scheduler.getDispatcher().getEventHandler()
            .handle(needUpgradeEvent);
      });
    }
  }

  /**
   * @return whether finalization of upgrade was successful.
   */
  private boolean finalizeUpgrade(boolean cancelUpgrade) {
    if (!cancelUpgrade) {
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
      } catch (IOException e) {
        LOG.error("Upgrade did not complete because unable to re-write the" +
            " service definition", e);
        return false;
      }
    }

    try {
      String upgradeVersionToDel = cancelUpgrade? cancelledVersion :
          upgradeVersion;
      LOG.info("[SERVICE]: delete upgrade dir version {}", upgradeVersionToDel);
      fs.deleteClusterUpgradeDir(getName(), upgradeVersionToDel);

      for (String comp : compsAffectedByUpgrade) {
        String compDirVersionToDel = cancelUpgrade? cancelledVersion :
            serviceSpec.getVersion();
        LOG.info("[SERVICE]: delete {} dir version {}",  comp,
            compDirVersionToDel);
        fs.deleteComponentDir(compDirVersionToDel, comp);
      }

      if (cancelUpgrade) {
        fs.deleteComponentsVersionDirIfEmpty(cancelledVersion);
      } else {
        fs.deleteComponentsVersionDirIfEmpty(serviceSpec.getVersion());
      }

    } catch (IOException e) {
      LOG.warn("Unable to delete upgrade definition for service {} " +
          "version {}", getName(), upgradeVersion);
    }
    setServiceState(ServiceState.STABLE);
    serviceSpec.setVersion(upgradeVersion);
    upgradeVersion = null;
    cancelledVersion = null;
    compsAffectedByUpgrade.clear();
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
  public void checkAndUpdateServiceState() {
    writeLock.lock();
    try {
      if (!getState().equals(State.UPGRADING)) {
        ServiceMaster.checkAndUpdateServiceState(this.scheduler);
      }
    } finally {
      writeLock.unlock();
    }
  }

  void processUpgradeRequest(String upgradeVersion,
      boolean autoFinalize, boolean expressUpgrade) throws IOException {
    Service targetSpec = ServiceApiUtil.loadServiceUpgrade(
        context.fs, context.service.getName(), upgradeVersion);

    List<org.apache.hadoop.yarn.service.api.records.Component>
        compsNeedUpgradeList = resolveCompsToUpgrade(context.service,
        targetSpec);

    ServiceEvent event = new ServiceEvent(ServiceEventType.UPGRADE)
        .setVersion(upgradeVersion)
        .setAutoFinalize(autoFinalize)
        .setExpressUpgrade(expressUpgrade);

    if (expressUpgrade) {
      // In case of express upgrade  components need to be upgraded in order.
      // Once the service manager gets notified that a component finished
      // upgrading, it then issues event to upgrade the next component.
      Map<String, org.apache.hadoop.yarn.service.api.records.Component>
          compsNeedUpgradeByName = new HashMap<>();
      if (compsNeedUpgradeList != null) {
        compsNeedUpgradeList.forEach(component ->
            compsNeedUpgradeByName.put(component.getName(), component));
      }
      List<String> resolvedComps = ServiceApiUtil
          .resolveCompsDependency(targetSpec);

      List<org.apache.hadoop.yarn.service.api.records.Component>
          orderedCompUpgrade = new LinkedList<>();
      resolvedComps.forEach(compName -> {
        org.apache.hadoop.yarn.service.api.records.Component component =
            compsNeedUpgradeByName.get(compName);
        if (component != null ) {
          orderedCompUpgrade.add(component);
        }
      });
      event.setCompsToUpgrade(orderedCompUpgrade);
    } else {
      event.setCompsToUpgrade(compsNeedUpgradeList);
    }
    context.scheduler.getDispatcher().getEventHandler().handle(
        event);

    if (autoFinalize && (compsNeedUpgradeList == null ||
        compsNeedUpgradeList.isEmpty())) {
      // nothing to upgrade and auto finalize is requested, trigger a
      // state check.
      context.scheduler.getDispatcher().getEventHandler().handle(
          new ServiceEvent(ServiceEventType.CHECK_STABLE));
    }
  }

  private List<org.apache.hadoop.yarn.service.api.records.Component>
      resolveCompsToUpgrade(Service sourceSpec, Service targetSpec) {

    List<org.apache.hadoop.yarn.service.api.records.Component>
        compsNeedUpgradeList = componentsFinder.
        findTargetComponentSpecs(sourceSpec, targetSpec);

    // remove all components from need upgrade list if there restart policy
    // doesn't all upgrade.
    if (compsNeedUpgradeList != null) {
      compsNeedUpgradeList.removeIf(component -> {
        org.apache.hadoop.yarn.service.api.records.Component.RestartPolicyEnum
            restartPolicy = component.getRestartPolicy();

        final ComponentRestartPolicy restartPolicyHandler =
            Component.getRestartPolicyHandler(restartPolicy);
        // Do not allow upgrades for components which have NEVER/ON_FAILURE
        // restart policy
        if (!restartPolicyHandler.allowUpgrades()) {
          LOG.info("The component {} has a restart policy that doesnt " +
                  "allow upgrades {} ", component.getName(),
              component.getRestartPolicy().toString());
          return true;
        }

        return false;
      });
    }

    return compsNeedUpgradeList;
  }

  /**
   * Sets the state of the service in the service spec.
   * @param state service state
   */
  private void setServiceState(
      org.apache.hadoop.yarn.service.api.records.ServiceState state) {
    org.apache.hadoop.yarn.service.api.records.ServiceState curState =
        serviceSpec.getState();
    if (!curState.equals(state)) {
      serviceSpec.setState(state);
      LOG.info("[SERVICE] spec state changed from {} -> {}", curState, state);
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
