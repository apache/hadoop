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
import org.apache.hadoop.yarn.service.MockRunningServiceContext;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.TestServiceManager;
import org.apache.hadoop.yarn.service.api.records.ComponentState;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEvent;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Iterator;

import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType.BECOME_READY;
import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType.START;
import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType.STOP;

import static org.apache.hadoop.yarn.service.conf.YarnServiceConstants
    .CONTAINER_STATE_REPORT_AS_SERVICE_STATE;

/**
 * Tests for {@link Component}.
 */
public class TestComponent {

  static final Logger LOG = Logger.getLogger(TestComponent.class);

  @Rule
  public ServiceTestUtils.ServiceFSWatcher rule =
      new ServiceTestUtils.ServiceFSWatcher();

  @Test
  public void testComponentUpgrade() throws Exception {
    ServiceContext context = createTestContext(rule, "testComponentUpgrade");
    Component comp = context.scheduler.getAllComponents().entrySet().iterator()
        .next().getValue();

    ComponentEvent upgradeEvent = new ComponentEvent(comp.getName(),
        ComponentEventType.UPGRADE);
    comp.handle(upgradeEvent);
    Assert.assertEquals("component not in need upgrade state",
        ComponentState.NEEDS_UPGRADE, comp.getComponentSpec().getState());
  }

  @Test
  public void testCheckState() throws Exception {
    String serviceName = "testCheckState";
    ServiceContext context = createTestContext(rule, serviceName);
    Component comp = context.scheduler.getAllComponents().entrySet().iterator()
        .next().getValue();

    comp.handle(new ComponentEvent(comp.getName(), ComponentEventType.UPGRADE)
        .setTargetSpec(createSpecWithEnv(serviceName, comp.getName(), "key1",
            "val1")).setUpgradeVersion("v2"));

    // one instance finished upgrading
    comp.getUpgradeStatus().decContainersThatNeedUpgrade();
    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CHECK_STABLE));
    Assert.assertEquals("component not in need upgrade state",
        ComponentState.NEEDS_UPGRADE, comp.getComponentSpec().getState());

    // second instance finished upgrading
    comp.getUpgradeStatus().decContainersThatNeedUpgrade();
    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CHECK_STABLE));

    Assert.assertEquals("component not in stable state",
        ComponentState.STABLE, comp.getComponentSpec().getState());
    Assert.assertEquals("component did not upgrade successfully", "val1",
        comp.getComponentSpec().getConfiguration().getEnv("key1"));
  }

  @Test
  public void testContainerCompletedWhenUpgrading() throws Exception {
    String serviceName = "testContainerCompletedWhenUpgrading";
    MockRunningServiceContext context = createTestContext(rule, serviceName);
    Component comp = context.scheduler.getAllComponents().entrySet().iterator()
        .next().getValue();

    comp.handle(new ComponentEvent(comp.getName(), ComponentEventType.UPGRADE)
        .setTargetSpec(createSpecWithEnv(serviceName, comp.getName(), "key1",
            "val1")).setUpgradeVersion("v2"));
    comp.getAllComponentInstances().forEach(instance ->
        instance.handle(new ComponentInstanceEvent(
        instance.getContainer().getId(), ComponentInstanceEventType.UPGRADE)));

    // reinitialization of a container failed
    for(ComponentInstance instance : comp.getAllComponentInstances()) {
      ComponentEvent stopEvent = new ComponentEvent(comp.getName(),
          ComponentEventType.CONTAINER_COMPLETED)
          .setInstance(instance)
          .setContainerId(instance.getContainer().getId());
      comp.handle(stopEvent);
      instance.handle(new ComponentInstanceEvent(
          instance.getContainer().getId(), STOP));
    }
    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CHECK_STABLE));

    Assert.assertEquals("component not in needs upgrade state",
        ComponentState.NEEDS_UPGRADE, comp.getComponentSpec().getState());
  }

  @Test
  public void testCancelUpgrade() throws Exception {
    ServiceContext context = createTestContext(rule, "testCancelUpgrade");
    Component comp = context.scheduler.getAllComponents().entrySet().iterator()
        .next().getValue();

    ComponentEvent upgradeEvent = new ComponentEvent(comp.getName(),
        ComponentEventType.CANCEL_UPGRADE);
    comp.handle(upgradeEvent);
    Assert.assertEquals("component not in need upgrade state",
        ComponentState.NEEDS_UPGRADE, comp.getComponentSpec().getState());

    Assert.assertEquals(
        org.apache.hadoop.yarn.service.component.ComponentState
            .CANCEL_UPGRADING, comp.getState());
  }

  @Test
  public void testContainerCompletedCancelUpgrade() throws Exception {
    String serviceName = "testContainerCompletedCancelUpgrade";
    MockRunningServiceContext context = createTestContext(rule, serviceName);
    Component comp = context.scheduler.getAllComponents().entrySet().iterator()
        .next().getValue();

    // upgrade completes
    comp.handle(new ComponentEvent(comp.getName(), ComponentEventType.UPGRADE)
        .setTargetSpec(createSpecWithEnv(serviceName, comp.getName(), "key1",
            "val1")).setUpgradeVersion("v2"));
    comp.getAllComponentInstances().forEach(instance ->
        instance.handle(new ComponentInstanceEvent(
            instance.getContainer().getId(),
            ComponentInstanceEventType.UPGRADE)));

    // reinitialization of a container done
    for(ComponentInstance instance : comp.getAllComponentInstances()) {
      instance.handle(new ComponentInstanceEvent(
          instance.getContainer().getId(), START));
      instance.handle(new ComponentInstanceEvent(
          instance.getContainer().getId(), BECOME_READY));
    }

    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CANCEL_UPGRADE)
        .setTargetSpec(createSpecWithEnv(serviceName, comp.getName(), "key1",
            "val0")).setUpgradeVersion("v1"));
    comp.getAllComponentInstances().forEach(instance ->
        instance.handle(new ComponentInstanceEvent(
            instance.getContainer().getId(),
            ComponentInstanceEventType.CANCEL_UPGRADE)));

    Iterator<ComponentInstance> iter = comp.getAllComponentInstances()
        .iterator();

    // cancel upgrade failed of a container
    ComponentInstance instance1 = iter.next();
    ComponentEvent stopEvent = new ComponentEvent(comp.getName(),
        ComponentEventType.CONTAINER_COMPLETED)
        .setInstance(instance1)
        .setContainerId(instance1.getContainer().getId());
    comp.handle(stopEvent);
    instance1.handle(new ComponentInstanceEvent(
        instance1.getContainer().getId(), STOP));
    Assert.assertEquals(
        org.apache.hadoop.yarn.service.component.ComponentState
            .CANCEL_UPGRADING, comp.getState());

    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CHECK_STABLE));

    Assert.assertEquals("component not in needs upgrade state",
        ComponentState.NEEDS_UPGRADE, comp.getComponentSpec().getState());
    Assert.assertEquals(
        org.apache.hadoop.yarn.service.component.ComponentState
            .CANCEL_UPGRADING, comp.getState());

    // second instance finished upgrading
    ComponentInstance instance2 = iter.next();
    instance2.handle(new ComponentInstanceEvent(
        instance2.getContainer().getId(), ComponentInstanceEventType.START));
    instance2.handle(new ComponentInstanceEvent(
        instance2.getContainer().getId(),
        ComponentInstanceEventType.BECOME_READY));

    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CHECK_STABLE));

    Assert.assertEquals("component not in flexing state",
        ComponentState.FLEXING, comp.getComponentSpec().getState());
    // new container get allocated
    context.assignNewContainer(context.attemptId, 10, comp);

    comp.handle(new ComponentEvent(comp.getName(),
            ComponentEventType.CHECK_STABLE));

    Assert.assertEquals("component not in stable state",
        ComponentState.STABLE, comp.getComponentSpec().getState());
    Assert.assertEquals("cancel upgrade failed", "val0",
        comp.getComponentSpec().getConfiguration().getEnv("key1"));
  }

  @Test
  public void testCancelUpgradeSuccessWhileUpgrading() throws Exception {
    String serviceName = "testCancelUpgradeWhileUpgrading";
    MockRunningServiceContext context = createTestContext(rule, serviceName);
    Component comp = context.scheduler.getAllComponents().entrySet().iterator()
        .next().getValue();
    cancelUpgradeWhileUpgrading(context, comp);

    // cancel upgrade successful for both instances
    for(ComponentInstance instance : comp.getAllComponentInstances()) {
      instance.handle(new ComponentInstanceEvent(
          instance.getContainer().getId(),
          ComponentInstanceEventType.START));
      instance.handle(new ComponentInstanceEvent(
          instance.getContainer().getId(),
          ComponentInstanceEventType.BECOME_READY));
    }

    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CHECK_STABLE));

    Assert.assertEquals("component not in stable state",
        ComponentState.STABLE, comp.getComponentSpec().getState());
    Assert.assertEquals("cancel upgrade failed", "val0",
        comp.getComponentSpec().getConfiguration().getEnv("key1"));
  }

  @Test
  public void testCancelUpgradeFailureWhileUpgrading() throws Exception {
    String serviceName = "testCancelUpgradeFailureWhileUpgrading";
    MockRunningServiceContext context = createTestContext(rule, serviceName);
    Component comp = context.scheduler.getAllComponents().entrySet().iterator()
        .next().getValue();
    cancelUpgradeWhileUpgrading(context, comp);

    // cancel upgrade failed for both instances
    for(ComponentInstance instance : comp.getAllComponentInstances()) {
      instance.handle(new ComponentInstanceEvent(
          instance.getContainer().getId(),
          ComponentInstanceEventType.STOP));
    }
    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CHECK_STABLE));

    Assert.assertEquals("component not in flexing state",
        ComponentState.FLEXING, comp.getComponentSpec().getState());

    for (ComponentInstance instance : comp.getAllComponentInstances()) {
      // new container get allocated
      context.assignNewContainer(context.attemptId, 10, comp);
    }

    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CHECK_STABLE));

    Assert.assertEquals("component not in stable state",
        ComponentState.STABLE, comp.getComponentSpec().getState());
    Assert.assertEquals("cancel upgrade failed", "val0",
        comp.getComponentSpec().getConfiguration().getEnv("key1"));
  }

  private void cancelUpgradeWhileUpgrading(
      MockRunningServiceContext context, Component comp)
      throws Exception {

    comp.handle(new ComponentEvent(comp.getName(), ComponentEventType.UPGRADE)
        .setTargetSpec(createSpecWithEnv(context.service.getName(),
            comp.getName(), "key1", "val1")).setUpgradeVersion("v0"));

    Iterator<ComponentInstance> iter = comp.getAllComponentInstances()
        .iterator();

    ComponentInstance instance1 = iter.next();

    // instance1 is triggered to upgrade
    instance1.handle(new ComponentInstanceEvent(
        instance1.getContainer().getId(), ComponentInstanceEventType.UPGRADE));

    // component upgrade is cancelled
    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CANCEL_UPGRADE)
        .setTargetSpec(createSpecWithEnv(context.service.getName(),
            comp.getName(), "key1",
            "val0")).setUpgradeVersion("v0"));

    // all instances upgrade is cancelled.
    comp.getAllComponentInstances().forEach(instance ->
        instance.handle(new ComponentInstanceEvent(
            instance.getContainer().getId(),
            ComponentInstanceEventType.CANCEL_UPGRADE)));

    // regular upgrade failed for instance 1
    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CONTAINER_COMPLETED).setInstance(instance1)
        .setContainerId(instance1.getContainer().getId()));
    instance1.handle(new ComponentInstanceEvent(
        instance1.getContainer().getId(), STOP));

    // component should be in cancel upgrade
    Assert.assertEquals(
        org.apache.hadoop.yarn.service.component.ComponentState
            .CANCEL_UPGRADING, comp.getState());

    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CHECK_STABLE));

    Assert.assertEquals("component not in needs upgrade state",
        ComponentState.NEEDS_UPGRADE, comp.getComponentSpec().getState());
    Assert.assertEquals(
        org.apache.hadoop.yarn.service.component.ComponentState
            .CANCEL_UPGRADING, comp.getState());
  }

  @Test
  public void testComponentStateReachesStableStateWithTerminatingComponents()
      throws
      Exception {
    final String serviceName =
        "testComponentStateUpdatesWithTerminatingComponents";

    Service testService = ServiceTestUtils.createTerminatingJobExample(
        serviceName);
    TestServiceManager.createDef(serviceName, testService);

    ServiceContext context = new MockRunningServiceContext(rule, testService);

    for (Component comp : context.scheduler.getAllComponents().values()) {

      Iterator<ComponentInstance> instanceIter = comp.
          getAllComponentInstances().iterator();

      ComponentInstance componentInstance = instanceIter.next();
      Container instanceContainer = componentInstance.getContainer();

      Assert.assertEquals(0, comp.getNumSucceededInstances());
      Assert.assertEquals(0, comp.getNumFailedInstances());
      Assert.assertEquals(2, comp.getNumRunningInstances());
      Assert.assertEquals(2, comp.getNumReadyInstances());
      Assert.assertEquals(0, comp.getPendingInstances().size());

      //stop 1 container
      ContainerStatus containerStatus = ContainerStatus.newInstance(
          instanceContainer.getId(),
          org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE,
          "successful", 0);
      comp.handle(new ComponentEvent(comp.getName(),
          ComponentEventType.CONTAINER_COMPLETED).setStatus(containerStatus)
          .setContainerId(instanceContainer.getId()));
      componentInstance.handle(
          new ComponentInstanceEvent(componentInstance.getContainer().getId(),
              ComponentInstanceEventType.STOP).setStatus(containerStatus));

      Assert.assertEquals(1, comp.getNumSucceededInstances());
      Assert.assertEquals(0, comp.getNumFailedInstances());
      Assert.assertEquals(1, comp.getNumRunningInstances());
      Assert.assertEquals(1, comp.getNumReadyInstances());
      Assert.assertEquals(0, comp.getPendingInstances().size());

      org.apache.hadoop.yarn.service.component.ComponentState componentState =
          Component.checkIfStable(comp);
      Assert.assertEquals(
          org.apache.hadoop.yarn.service.component.ComponentState.STABLE,
          componentState);
    }
  }

  @Test
  public void testComponentStateUpdatesWithTerminatingComponents()
      throws
      Exception {
    final String serviceName =
        "testComponentStateUpdatesWithTerminatingComponents";

    Service testService = ServiceTestUtils.createTerminatingJobExample(
        serviceName);
    TestServiceManager.createDef(serviceName, testService);

    ServiceContext context = new MockRunningServiceContext(rule, testService);

    for (Component comp : context.scheduler.getAllComponents().values()) {
      Iterator<ComponentInstance> instanceIter = comp.
          getAllComponentInstances().iterator();

      while (instanceIter.hasNext()) {

        ComponentInstance componentInstance = instanceIter.next();
        Container instanceContainer = componentInstance.getContainer();

        //stop 1 container
        ContainerStatus containerStatus = ContainerStatus.newInstance(
            instanceContainer.getId(),
            org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE,
            "successful", 0);
        comp.handle(new ComponentEvent(comp.getName(),
            ComponentEventType.CONTAINER_COMPLETED).setStatus(containerStatus)
            .setContainerId(instanceContainer.getId()));
        componentInstance.handle(
            new ComponentInstanceEvent(componentInstance.getContainer().getId(),
                ComponentInstanceEventType.STOP).setStatus(containerStatus));
      }

      ComponentState componentState =
          comp.getComponentSpec().getState();
      Assert.assertEquals(
          ComponentState.SUCCEEDED,
          componentState);
    }

    ServiceState serviceState =
        testService.getState();
    Assert.assertEquals(
        ServiceState.SUCCEEDED,
        serviceState);
  }

  @Test
  public void testComponentStateUpdatesWithTerminatingDominantComponents()
      throws Exception {
    final String serviceName =
        "testComponentStateUpdatesWithTerminatingServiceStateComponents";

    Service testService =
        ServiceTestUtils.createTerminatingDominantComponentJobExample(
            serviceName);
    TestServiceManager.createDef(serviceName, testService);

    ServiceContext context = new MockRunningServiceContext(rule, testService);

    for (Component comp : context.scheduler.getAllComponents().values()) {
      boolean componentIsDominant = comp.getComponentSpec()
          .getConfiguration().getPropertyBool(
              CONTAINER_STATE_REPORT_AS_SERVICE_STATE, false);
      if (componentIsDominant) {
        Iterator<ComponentInstance> instanceIter = comp.
            getAllComponentInstances().iterator();

        while (instanceIter.hasNext()) {

          ComponentInstance componentInstance = instanceIter.next();
          Container instanceContainer = componentInstance.getContainer();

          //stop 1 container
          ContainerStatus containerStatus = ContainerStatus.newInstance(
              instanceContainer.getId(),
              org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE,
              "successful", 0);
          comp.handle(new ComponentEvent(comp.getName(),
              ComponentEventType.CONTAINER_COMPLETED).setStatus(containerStatus)
              .setContainerId(instanceContainer.getId()));
          componentInstance.handle(
              new ComponentInstanceEvent(componentInstance.getContainer().
                  getId(), ComponentInstanceEventType.STOP).
                  setStatus(containerStatus));
        }
        ComponentState componentState =
            comp.getComponentSpec().getState();
        Assert.assertEquals(
            ComponentState.SUCCEEDED,
            componentState);
      }
    }

    ServiceState serviceState =
        testService.getState();
    Assert.assertEquals(
        ServiceState.SUCCEEDED,
        serviceState);
  }

  private static org.apache.hadoop.yarn.service.api.records.Component
      createSpecWithEnv(String serviceName, String compName, String key,
      String val) {
    Service service = TestServiceManager.createBaseDef(serviceName);
    org.apache.hadoop.yarn.service.api.records.Component spec =
        service.getComponent(compName);
    spec.getConfiguration().getEnv().put(key, val);
    return spec;
  }

  public static MockRunningServiceContext createTestContext(
      ServiceTestUtils.ServiceFSWatcher fsWatcher, String serviceName)
      throws Exception {
    return new MockRunningServiceContext(fsWatcher,
        TestServiceManager.createBaseDef(serviceName));
  }
}

