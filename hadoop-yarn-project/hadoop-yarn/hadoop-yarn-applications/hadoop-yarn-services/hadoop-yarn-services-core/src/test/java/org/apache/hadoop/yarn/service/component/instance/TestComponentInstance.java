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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.ServiceScheduler;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.component.ComponentEvent;
import org.apache.hadoop.yarn.service.component.ComponentEventType;
import org.apache.hadoop.yarn.service.component.TestComponent;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ComponentInstance}.
 */
public class TestComponentInstance {

  @Rule
  public ServiceTestUtils.ServiceFSWatcher rule =
      new ServiceTestUtils.ServiceFSWatcher();

  @Test
  public void testContainerUpgrade() throws Exception {
    ServiceContext context = TestComponent.createTestContext(rule,
        "testContainerUpgrade");
    Component component = context.scheduler.getAllComponents().entrySet()
        .iterator().next().getValue();
    upgradeComponent(component);

    ComponentInstance instance = component.getAllComponentInstances().iterator()
        .next();
    ComponentInstanceEvent instanceEvent = new ComponentInstanceEvent(
        instance.getContainer().getId(), ComponentInstanceEventType.UPGRADE);
    instance.handle(instanceEvent);
    Container containerSpec = component.getComponentSpec().getContainer(
        instance.getContainer().getId().toString());
    Assert.assertEquals("instance not upgrading", ContainerState.UPGRADING,
        containerSpec.getState());
  }

  @Test
  public void testContainerReadyAfterUpgrade() throws Exception {
    ServiceContext context = TestComponent.createTestContext(rule,
        "testContainerReadyAfterUpgrade");
    Component component = context.scheduler.getAllComponents().entrySet()
        .iterator().next().getValue();
    upgradeComponent(component);

    ComponentInstance instance = component.getAllComponentInstances().iterator()
        .next();

    ComponentInstanceEvent instanceEvent = new ComponentInstanceEvent(
        instance.getContainer().getId(), ComponentInstanceEventType.UPGRADE);
    instance.handle(instanceEvent);
    instance.handle(new ComponentInstanceEvent(instance.getContainer().getId(),
        ComponentInstanceEventType.START));
    Assert.assertEquals("instance not running",
        ContainerState.RUNNING_BUT_UNREADY,
        component.getComponentSpec().getContainer(instance.getContainer()
            .getId().toString()).getState());
    instance.handle(new ComponentInstanceEvent(instance.getContainer().getId(),
        ComponentInstanceEventType.BECOME_READY));
    Assert.assertEquals("instance not ready", ContainerState.READY,
        component.getComponentSpec().getContainer(instance.getContainer()
            .getId().toString()).getState());
  }


  @Test
  public void testContainerUpgradeFailed() throws Exception {
    ServiceContext context = TestComponent.createTestContext(rule,
        "testContainerUpgradeFailed");
    Component component = context.scheduler.getAllComponents().entrySet()
        .iterator().next().getValue();
    upgradeComponent(component);

    ComponentInstance instance = component.getAllComponentInstances().iterator()
        .next();

    ComponentInstanceEvent upgradeEvent = new ComponentInstanceEvent(
        instance.getContainer().getId(), ComponentInstanceEventType.UPGRADE);
    instance.handle(upgradeEvent);

    ContainerStatus containerStatus = mock(ContainerStatus.class);
    when(containerStatus.getExitStatus()).thenReturn(
        ContainerExitStatus.ABORTED);
    ComponentInstanceEvent stopEvent = new ComponentInstanceEvent(
        instance.getContainer().getId(), ComponentInstanceEventType.STOP)
        .setStatus(containerStatus);
    // this is the call back from NM for the upgrade
    instance.handle(stopEvent);
    Assert.assertEquals("instance did not fail", ContainerState.FAILED_UPGRADE,
        component.getComponentSpec().getContainer(instance.getContainer()
            .getId().toString()).getState());
  }

  @Test
  public void testFailureAfterReinit() throws Exception {
    ServiceContext context = TestComponent.createTestContext(rule,
        "testContainerUpgradeFailed");
    Component component = context.scheduler.getAllComponents().entrySet()
        .iterator().next().getValue();
    upgradeComponent(component);

    ComponentInstance instance = component.getAllComponentInstances().iterator()
        .next();

    ComponentInstanceEvent upgradeEvent = new ComponentInstanceEvent(
        instance.getContainer().getId(), ComponentInstanceEventType.UPGRADE);
    instance.handle(upgradeEvent);

    // NM finished updgrae
    instance.handle(new ComponentInstanceEvent(instance.getContainer().getId(),
        ComponentInstanceEventType.START));
    Assert.assertEquals("instance not running",
        ContainerState.RUNNING_BUT_UNREADY,
        component.getComponentSpec().getContainer(instance.getContainer()
            .getId().toString()).getState());

    ContainerStatus containerStatus = mock(ContainerStatus.class);
    when(containerStatus.getExitStatus()).thenReturn(
        ContainerExitStatus.ABORTED);
    ComponentInstanceEvent stopEvent = new ComponentInstanceEvent(
        instance.getContainer().getId(), ComponentInstanceEventType.STOP)
        .setStatus(containerStatus);
    // this is the call back from NM for the upgrade
    instance.handle(stopEvent);
    Assert.assertEquals("instance did not fail", ContainerState.FAILED_UPGRADE,
        component.getComponentSpec().getContainer(instance.getContainer()
            .getId().toString()).getState());
  }

  @Test
  public void testCancelNothingToUpgrade() throws Exception {
    ServiceContext context = TestComponent.createTestContext(rule,
        "testCancelUpgradeWhenContainerReady");
    Component component = context.scheduler.getAllComponents().entrySet()
        .iterator().next().getValue();
    cancelCompUpgrade(component);

    ComponentInstance instance = component.getAllComponentInstances().iterator()
        .next();

    ComponentInstanceEvent cancelEvent = new ComponentInstanceEvent(
        instance.getContainer().getId(),
        ComponentInstanceEventType.CANCEL_UPGRADE);
    instance.handle(cancelEvent);

    Assert.assertEquals("instance not ready", ContainerState.READY,
        component.getComponentSpec().getContainer(instance.getContainer()
            .getId().toString()).getState());
  }

  @Test
  public void testCancelUpgradeFailed() throws Exception {
    ServiceContext context = TestComponent.createTestContext(rule,
        "testCancelUpgradeFailed");
    Component component = context.scheduler.getAllComponents().entrySet()
        .iterator().next().getValue();
    cancelCompUpgrade(component);

    ComponentInstance instance = component.getAllComponentInstances().iterator()
        .next();

    ComponentInstanceEvent cancelEvent = new ComponentInstanceEvent(
        instance.getContainer().getId(),
        ComponentInstanceEventType.CANCEL_UPGRADE);
    instance.handle(cancelEvent);

    instance.handle(new ComponentInstanceEvent(instance.getContainer().getId(),
        ComponentInstanceEventType.STOP));
    Assert.assertEquals("instance not init", ComponentInstanceState.INIT,
        instance.getState());
  }

  @Test
  public void testCancelAfterCompProcessedCancel() throws Exception {
    ServiceContext context = TestComponent.createTestContext(rule,
        "testCancelAfterCompProcessedCancel");
    Component component = context.scheduler.getAllComponents().entrySet()
        .iterator().next().getValue();
    upgradeComponent(component);
    cancelCompUpgrade(component);

    ComponentInstance instance = component.getAllComponentInstances().iterator()
        .next();
    ComponentInstanceEvent upgradeEvent = new ComponentInstanceEvent(
        instance.getContainer().getId(), ComponentInstanceEventType.UPGRADE);
    instance.handle(upgradeEvent);

    Assert.assertEquals("instance should start upgrading",
        ContainerState.NEEDS_UPGRADE,
        component.getComponentSpec().getContainer(instance.getContainer()
            .getId().toString()).getState());
  }

  @Test
  public void testCancelWhileUpgradeWithSuccess() throws Exception {
    validateCancelWhileUpgrading(true, true);
  }

  @Test
  public void testCancelWhileUpgradeWithFailure() throws Exception {
    validateCancelWhileUpgrading(false, true);
  }

  @Test
  public void testCancelFailedWhileUpgradeWithSuccess() throws Exception {
    validateCancelWhileUpgrading(true, false);
  }

  @Test
  public void testCancelFailedWhileUpgradeWithFailure() throws Exception {
    validateCancelWhileUpgrading(false, false);
  }

  private void validateCancelWhileUpgrading(boolean upgradeSuccessful,
      boolean cancelUpgradeSuccessful)
      throws Exception {
    ServiceContext context = TestComponent.createTestContext(rule,
        "testCancelWhileUpgrading");
    Component component = context.scheduler.getAllComponents().entrySet()
        .iterator().next().getValue();
    upgradeComponent(component);

    ComponentInstance instance = component.getAllComponentInstances().iterator()
        .next();
    ComponentInstanceEvent upgradeEvent = new ComponentInstanceEvent(
        instance.getContainer().getId(), ComponentInstanceEventType.UPGRADE);
    instance.handle(upgradeEvent);

    Assert.assertEquals("instance should be upgrading",
        ContainerState.UPGRADING,
        component.getComponentSpec().getContainer(instance.getContainer()
            .getId().toString()).getState());

    cancelCompUpgrade(component);
    ComponentInstanceEvent cancelEvent = new ComponentInstanceEvent(
        instance.getContainer().getId(),
        ComponentInstanceEventType.CANCEL_UPGRADE);
    instance.handle(cancelEvent);

    // either upgrade failed or successful
    if (upgradeSuccessful) {
      instance.handle(new ComponentInstanceEvent(
          instance.getContainer().getId(), ComponentInstanceEventType.START));
      instance.handle(new ComponentInstanceEvent(
          instance.getContainer().getId(),
          ComponentInstanceEventType.BECOME_READY));
    } else {
      instance.handle(new ComponentInstanceEvent(
          instance.getContainer().getId(),
          ComponentInstanceEventType.STOP));
    }

    Assert.assertEquals("instance not upgrading", ContainerState.UPGRADING,
        component.getComponentSpec().getContainer(instance.getContainer()
            .getId().toString()).getState());

    // response for cancel received
    if (cancelUpgradeSuccessful) {
      instance.handle(new ComponentInstanceEvent(
          instance.getContainer().getId(), ComponentInstanceEventType.START));
      instance.handle(new ComponentInstanceEvent(
          instance.getContainer().getId(),
          ComponentInstanceEventType.BECOME_READY));
    } else {
      instance.handle(new ComponentInstanceEvent(
          instance.getContainer().getId(), ComponentInstanceEventType.STOP));
    }
    if (cancelUpgradeSuccessful) {
      Assert.assertEquals("instance not ready", ContainerState.READY,
          component.getComponentSpec().getContainer(instance.getContainer()
              .getId().toString()).getState());
    } else {
      Assert.assertEquals("instance not init", ComponentInstanceState.INIT,
          instance.getState());
    }
  }

  private void upgradeComponent(Component component) {
    component.handle(new ComponentEvent(component.getName(),
        ComponentEventType.UPGRADE).setTargetSpec(component.getComponentSpec())
        .setUpgradeVersion("v2"));
  }

  private void cancelCompUpgrade(Component component) {
    component.handle(new ComponentEvent(component.getName(),
        ComponentEventType.CANCEL_UPGRADE)
        .setTargetSpec(component.getComponentSpec())
        .setUpgradeVersion("v1"));
  }

  private Component createComponent(ServiceScheduler scheduler,
      org.apache.hadoop.yarn.service.api.records.Component.RestartPolicyEnum
          restartPolicy, int nSucceededInstances, int nFailedInstances,
      int totalAsk, int componentId) {

    assert (nSucceededInstances + nFailedInstances) <= totalAsk;

    Component comp = mock(Component.class);
    org.apache.hadoop.yarn.service.api.records.Component componentSpec = mock(
        org.apache.hadoop.yarn.service.api.records.Component.class);
    when(componentSpec.getRestartPolicy()).thenReturn(restartPolicy);
    when(comp.getRestartPolicyHandler()).thenReturn(
        Component.getRestartPolicyHandler(restartPolicy));
    when(componentSpec.getNumberOfContainers()).thenReturn(
        Long.valueOf(totalAsk));
    when(comp.getComponentSpec()).thenReturn(componentSpec);
    when(comp.getScheduler()).thenReturn(scheduler);

    Map<String, ComponentInstance> succeeded = new ConcurrentHashMap<>();
    Map<String, ComponentInstance> failed = new ConcurrentHashMap<>();
    scheduler.getAllComponents().put("comp" + componentId, comp);

    Map<String, ComponentInstance> componentInstances = new HashMap<>();

    for (int i = 0; i < nSucceededInstances; i++) {
      ComponentInstance componentInstance = createComponentInstance(comp, i);
      componentInstances.put(componentInstance.getCompInstanceName(),
          componentInstance);
      succeeded.put(componentInstance.getCompInstanceName(), componentInstance);
    }

    for (int i = 0; i < nFailedInstances; i++) {
      ComponentInstance componentInstance = createComponentInstance(comp,
          i + nSucceededInstances);
      componentInstances.put(componentInstance.getCompInstanceName(),
          componentInstance);
      failed.put(componentInstance.getCompInstanceName(), componentInstance);
    }

    int delta = totalAsk - nFailedInstances - nSucceededInstances;

    for (int i = 0; i < delta; i++) {
      ComponentInstance componentInstance = createComponentInstance(comp,
          i + nSucceededInstances + nFailedInstances);
      componentInstances.put(componentInstance.getCompInstanceName(),
          componentInstance);
    }

    when(comp.getAllComponentInstances()).thenReturn(
        componentInstances.values());
    when(comp.getSucceededInstances()).thenReturn(succeeded.values());
    when(comp.getFailedInstances()).thenReturn(failed.values());
    return comp;
  }

  private Component createComponent(ServiceScheduler scheduler,
      org.apache.hadoop.yarn.service.api.records.Component.RestartPolicyEnum
          restartPolicy,
      int totalAsk, int componentId) {

    Component comp = mock(Component.class);
    org.apache.hadoop.yarn.service.api.records.Component componentSpec = mock(
        org.apache.hadoop.yarn.service.api.records.Component.class);
    when(componentSpec.getRestartPolicy()).thenReturn(restartPolicy);
    when(comp.getRestartPolicyHandler()).thenReturn(
        Component.getRestartPolicyHandler(restartPolicy));
    when(componentSpec.getNumberOfContainers()).thenReturn(
        Long.valueOf(totalAsk));
    when(comp.getComponentSpec()).thenReturn(componentSpec);
    when(comp.getScheduler()).thenReturn(scheduler);

    scheduler.getAllComponents().put("comp" + componentId, comp);

    Map<String, ComponentInstance> componentInstances = new HashMap<>();

    for (int i = 0; i < totalAsk; i++) {
      ComponentInstance componentInstance = createComponentInstance(comp, i);
      componentInstances.put(componentInstance.getCompInstanceName(),
          componentInstance);
    }

    when(comp.getAllComponentInstances()).thenReturn(
        componentInstances.values());
    return comp;
  }

  private ComponentInstance createComponentInstance(Component component,
      int instanceId) {

    ComponentInstance componentInstance = mock(ComponentInstance.class);
    when(componentInstance.getComponent()).thenReturn(component);
    when(componentInstance.getCompInstanceName()).thenReturn(
        "compInstance" + instanceId);
    Container container = mock(Container.class);
    when(componentInstance.getContainerSpec()).thenReturn(container);

    ServiceUtils.ProcessTerminationHandler terminationHandler = mock(
        ServiceUtils.ProcessTerminationHandler.class);
    when(component.getScheduler().getTerminationHandler()).thenReturn(
        terminationHandler);

    return componentInstance;
  }

  @Test
  public void testComponentRestartPolicy() {

    Map<String, Component> allComponents = new HashMap<>();
    Service mockService = mock(Service.class);
    ServiceContext serviceContext = mock(ServiceContext.class);
    when(serviceContext.getService()).thenReturn(mockService);
    ServiceScheduler serviceSchedulerInstance = new ServiceScheduler(
        serviceContext);
    ServiceScheduler serviceScheduler = spy(serviceSchedulerInstance);
    when(serviceScheduler.getAllComponents()).thenReturn(allComponents);
    Mockito.doNothing().when(serviceScheduler).setGracefulStop(
        any(FinalApplicationStatus.class));

    final String containerDiag = "Container succeeded";

    ComponentInstanceEvent componentInstanceEvent = mock(
        ComponentInstanceEvent.class);
    ContainerId containerId = ContainerId.newContainerId(ApplicationAttemptId
        .newInstance(ApplicationId.newInstance(1234L, 1), 1), 1);
    ContainerStatus containerStatus = ContainerStatus.newInstance(containerId,
        org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE,
        containerDiag, 0);

    when(componentInstanceEvent.getStatus()).thenReturn(containerStatus);

    // Test case1: one component, one instance, restart policy = ALWAYS, exit=0
    Component comp = createComponent(serviceScheduler,
        org.apache.hadoop.yarn.service.api.records.Component
            .RestartPolicyEnum.ALWAYS,
        1, 0, 1, 0);
    ComponentInstance componentInstance =
        comp.getAllComponentInstances().iterator().next();

    ComponentInstance.handleComponentInstanceRelaunch(componentInstance,
        componentInstanceEvent, false, containerDiag);

    verify(comp, never()).markAsSucceeded(any(ComponentInstance.class));
    verify(comp, never()).markAsFailed(any(ComponentInstance.class));
    verify(comp, times(1)).reInsertPendingInstance(
        any(ComponentInstance.class));
    verify(serviceScheduler.getTerminationHandler(), never()).terminate(
        anyInt());

    // Test case2: one component, one instance, restart policy = ALWAYS, exit=1
    comp = createComponent(serviceScheduler,
        org.apache.hadoop.yarn.service.api.records.Component
            .RestartPolicyEnum.ALWAYS,
        0, 1, 1, 0);
    componentInstance = comp.getAllComponentInstances().iterator().next();
    containerStatus.setExitStatus(1);
    ComponentInstance.handleComponentInstanceRelaunch(componentInstance,
        componentInstanceEvent, false, containerDiag);
    verify(comp, never()).markAsSucceeded(any(ComponentInstance.class));
    verify(comp, never()).markAsFailed(any(ComponentInstance.class));
    verify(comp, times(1)).reInsertPendingInstance(
        any(ComponentInstance.class));
    verify(serviceScheduler.getTerminationHandler(), never()).terminate(
        anyInt());

    // Test case3: one component, one instance, restart policy = NEVER, exit=0
    // Should exit with code=0
    comp = createComponent(serviceScheduler,
        org.apache.hadoop.yarn.service.api.records.Component
            .RestartPolicyEnum.NEVER,
        1, 0, 1, 0);
    componentInstance = comp.getAllComponentInstances().iterator().next();
    containerStatus.setExitStatus(0);

    Map<String, ComponentInstance> succeededInstances = new HashMap<>();
    succeededInstances.put(componentInstance.getCompInstanceName(),
        componentInstance);
    when(comp.getSucceededInstances()).thenReturn(succeededInstances.values());
    when(comp.getNumSucceededInstances()).thenReturn(new Long(1));

    ComponentInstance.handleComponentInstanceRelaunch(componentInstance,
        componentInstanceEvent, false, containerDiag);
    verify(comp, times(1)).markAsSucceeded(any(ComponentInstance.class));
    verify(comp, never()).markAsFailed(any(ComponentInstance.class));
    verify(comp, times(0)).reInsertPendingInstance(
        any(ComponentInstance.class));
    verify(serviceScheduler.getTerminationHandler(), times(1)).terminate(eq(0));

    // Test case4: one component, one instance, restart policy = NEVER, exit=1
    // Should exit with code=-1
    comp = createComponent(serviceScheduler,
        org.apache.hadoop.yarn.service.api.records.Component
            .RestartPolicyEnum.NEVER,
        0, 1, 1, 0);
    componentInstance = comp.getAllComponentInstances().iterator().next();
    containerStatus.setExitStatus(-1);

    when(comp.getNumFailedInstances()).thenReturn(new Long(1));
    ComponentInstance.handleComponentInstanceRelaunch(componentInstance,
        componentInstanceEvent, false, containerDiag);
    verify(comp, never()).markAsSucceeded(any(ComponentInstance.class));
    verify(comp, times(1)).markAsFailed(any(ComponentInstance.class));
    verify(comp, times(0)).reInsertPendingInstance(
        any(ComponentInstance.class));
    verify(serviceScheduler.getTerminationHandler(), times(1)).terminate(
        eq(-1));

    // Test case5: one component, one instance, restart policy = ON_FAILURE,
    // exit=1
    // Should continue run.
    comp = createComponent(serviceScheduler,
        org.apache.hadoop.yarn.service.api.records.Component
            .RestartPolicyEnum.ON_FAILURE,
        0, 1, 1, 0);
    componentInstance = comp.getAllComponentInstances().iterator().next();
    containerStatus.setExitStatus(1);
    ComponentInstance.handleComponentInstanceRelaunch(componentInstance,
        componentInstanceEvent, false, containerDiag);
    verify(comp, never()).markAsSucceeded(any(ComponentInstance.class));
    verify(comp, never()).markAsFailed(any(ComponentInstance.class));
    verify(comp, times(1)).reInsertPendingInstance(
        any(ComponentInstance.class));
    verify(serviceScheduler.getTerminationHandler(), times(0)).terminate(
        anyInt());

    // Test case6: one component, 3 instances, restart policy = NEVER, exit=1
    // 2 of the instances not completed, it should continue run.
    comp = createComponent(serviceScheduler,
        org.apache.hadoop.yarn.service.api.records.Component
            .RestartPolicyEnum.NEVER,
        0, 1, 3, 0);
    componentInstance = comp.getAllComponentInstances().iterator().next();
    containerStatus.setExitStatus(1);
    ComponentInstance.handleComponentInstanceRelaunch(componentInstance,
        componentInstanceEvent, false, containerDiag);
    verify(comp, never()).markAsSucceeded(any(ComponentInstance.class));
    verify(comp, times(1)).markAsFailed(any(ComponentInstance.class));
    verify(comp, times(0)).reInsertPendingInstance(
        any(ComponentInstance.class));
    verify(serviceScheduler.getTerminationHandler(), times(0)).terminate(
        anyInt());

    // Test case7: one component, 3 instances, restart policy = ON_FAILURE,
    // exit=1
    // 2 of the instances completed, it should continue run.

    comp = createComponent(serviceScheduler,
        org.apache.hadoop.yarn.service.api.records.Component
            .RestartPolicyEnum.ON_FAILURE,
        0, 1, 3, 0);

    Iterator<ComponentInstance> iter =
        comp.getAllComponentInstances().iterator();

    containerStatus.setExitStatus(1);
    ComponentInstance commponentInstance = iter.next();
    ComponentInstance.handleComponentInstanceRelaunch(commponentInstance,
        componentInstanceEvent, false, containerDiag);
    verify(comp, never()).markAsSucceeded(any(ComponentInstance.class));
    verify(comp, never()).markAsFailed(any(ComponentInstance.class));
    verify(comp, times(1)).reInsertPendingInstance(
        any(ComponentInstance.class));
    verify(serviceScheduler.getTerminationHandler(), times(0)).terminate(
        anyInt());

    // Test case8: 2 components, 2 instances for each
    // comp2 already finished.
    // comp1 has a new instance finish, we should terminate the service

    comp = createComponent(serviceScheduler,
        org.apache.hadoop.yarn.service.api.records.Component
            .RestartPolicyEnum.NEVER,
        2, 0);
    Collection<ComponentInstance> component1Instances =
        comp.getAllComponentInstances();

    containerStatus.setExitStatus(-1);

    Component comp2 = createComponent(
        componentInstance.getComponent().getScheduler(),
        org.apache.hadoop.yarn.service.api.records.Component
            .RestartPolicyEnum.NEVER,
        2, 1);

    Collection<ComponentInstance> component2Instances =
        comp2.getAllComponentInstances();

    Map<String, ComponentInstance> failed2Instances = new HashMap<>();

    for (ComponentInstance component2Instance : component2Instances) {
      failed2Instances.put(component2Instance.getCompInstanceName(),
          component2Instance);
      when(component2Instance.getComponent().getFailedInstances()).thenReturn(
          failed2Instances.values());
      when(component2Instance.getComponent().getNumFailedInstances())
          .thenReturn(new Long(failed2Instances.size()));
      ComponentInstance.handleComponentInstanceRelaunch(component2Instance,
          componentInstanceEvent, false, containerDiag);
    }

    Map<String, ComponentInstance> failed1Instances = new HashMap<>();

    // 2nd component, already finished.
    for (ComponentInstance component1Instance : component1Instances) {
      failed1Instances.put(component1Instance.getCompInstanceName(),
          component1Instance);
      when(component1Instance.getComponent().getFailedInstances()).thenReturn(
          failed1Instances.values());
      when(component1Instance.getComponent().getNumFailedInstances())
          .thenReturn(new Long(failed1Instances.size()));
      ComponentInstance.handleComponentInstanceRelaunch(component1Instance,
          componentInstanceEvent, false, containerDiag);
    }

    verify(comp, never()).markAsSucceeded(any(ComponentInstance.class));
    verify(comp, times(2)).markAsFailed(any(ComponentInstance.class));
    verify(comp, times(0)).reInsertPendingInstance(
        any(ComponentInstance.class));

    verify(serviceScheduler.getTerminationHandler(), times(1)).terminate(
        eq(-1));

    // Test case9: 2 components, 2 instances for each
    // comp2 already finished.
    // comp1 has a new instance finish, we should terminate the service
    // All instance finish with 0, service should exit with 0 as well.
    containerStatus.setExitStatus(0);

    comp = createComponent(serviceScheduler,
        org.apache.hadoop.yarn.service.api.records.Component
            .RestartPolicyEnum.ON_FAILURE,
        2, 0);
    component1Instances = comp.getAllComponentInstances();

    comp2 = createComponent(componentInstance.getComponent().getScheduler(),
        org.apache.hadoop.yarn.service.api.records.Component
            .RestartPolicyEnum.ON_FAILURE,
        2, 1);

    component2Instances = comp2.getAllComponentInstances();

    Map<String, ComponentInstance> succeeded2Instances = new HashMap<>();

    for (ComponentInstance component2Instance : component2Instances) {
      succeeded2Instances.put(component2Instance.getCompInstanceName(),
          component2Instance);
      when(component2Instance.getComponent().getSucceededInstances())
          .thenReturn(succeeded2Instances.values());
      when(component2Instance.getComponent().getNumSucceededInstances())
          .thenReturn(new Long(succeeded2Instances.size()));
      ComponentInstance.handleComponentInstanceRelaunch(component2Instance,
          componentInstanceEvent, false, containerDiag);
    }

    Map<String, ComponentInstance> succeeded1Instances = new HashMap<>();
    // 2nd component, already finished.
    for (ComponentInstance component1Instance : component1Instances) {
      succeeded1Instances.put(component1Instance.getCompInstanceName(),
          component1Instance);
      when(component1Instance.getComponent().getSucceededInstances())
          .thenReturn(succeeded1Instances.values());
      when(component1Instance.getComponent().getNumSucceededInstances())
          .thenReturn(new Long(succeeded1Instances.size()));
      ComponentInstance.handleComponentInstanceRelaunch(component1Instance,
          componentInstanceEvent, false, containerDiag);
    }

    verify(comp, times(2)).markAsSucceeded(any(ComponentInstance.class));
    verify(comp, never()).markAsFailed(any(ComponentInstance.class));
    verify(componentInstance.getComponent(), times(0)).reInsertPendingInstance(
        any(ComponentInstance.class));
    verify(serviceScheduler.getTerminationHandler(), times(1)).terminate(eq(0));

    // Test case10: 2 components, 2 instances for each
    // comp2 hasn't finished
    // comp1 finished.
    // Service should continue run.

    comp = createComponent(serviceScheduler,
        org.apache.hadoop.yarn.service.api.records.Component
            .RestartPolicyEnum.NEVER,
        2, 0);
    component1Instances = comp.getAllComponentInstances();

    comp2 = createComponent(componentInstance.getComponent().getScheduler(),
        org.apache.hadoop.yarn.service.api.records.Component
            .RestartPolicyEnum.NEVER,
        2, 1);

    component2Instances = comp2.getAllComponentInstances();

    for (ComponentInstance component2Instance : component2Instances) {
      ComponentInstance.handleComponentInstanceRelaunch(component2Instance,
          componentInstanceEvent, false, containerDiag);
    }

    succeeded1Instances = new HashMap<>();
    // 2nd component, already finished.
    for (ComponentInstance component1Instance : component1Instances) {
      succeeded1Instances.put(component1Instance.getCompInstanceName(),
          component1Instance);
      when(component1Instance.getComponent().getSucceededInstances())
          .thenReturn(succeeded1Instances.values());
      ComponentInstance.handleComponentInstanceRelaunch(component1Instance,
          componentInstanceEvent, false, containerDiag);
    }

    verify(comp, times(2)).markAsSucceeded(any(ComponentInstance.class));
    verify(comp, never()).markAsFailed(any(ComponentInstance.class));
    verify(componentInstance.getComponent(), times(0)).reInsertPendingInstance(
        any(ComponentInstance.class));
    verify(serviceScheduler.getTerminationHandler(), never()).terminate(eq(0));

  }
}
