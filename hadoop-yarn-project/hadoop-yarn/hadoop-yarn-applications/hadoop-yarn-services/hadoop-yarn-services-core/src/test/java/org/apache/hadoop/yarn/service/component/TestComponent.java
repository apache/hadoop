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
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.TestServiceManager;
import org.apache.hadoop.yarn.service.api.records.ComponentState;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEvent;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType;
import org.apache.hadoop.yarn.service.MockRunningServiceContext;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Iterator;

import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType.STOP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    comp.decContainersThatNeedUpgrade();
    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CHECK_STABLE));
    Assert.assertEquals("component not in need upgrade state",
        ComponentState.NEEDS_UPGRADE, comp.getComponentSpec().getState());

    // second instance finished upgrading
    comp.decContainersThatNeedUpgrade();
    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CHECK_STABLE));

    Assert.assertEquals("component not in stable state",
        ComponentState.STABLE, comp.getComponentSpec().getState());
    Assert.assertEquals("component did not upgrade successfully", "val1",
        comp.getComponentSpec().getConfiguration().getEnv("key1"));
  }

  @Test
  public void testContainerCompletedWhenUpgrading() throws Exception {
    String serviceName = "testContainerComplete";
    MockRunningServiceContext context = createTestContext(rule, serviceName);
    Component comp = context.scheduler.getAllComponents().entrySet().iterator()
        .next().getValue();

    comp.handle(new ComponentEvent(comp.getName(), ComponentEventType.UPGRADE)
        .setTargetSpec(createSpecWithEnv(serviceName, comp.getName(), "key1",
            "val1")).setUpgradeVersion("v2"));
    comp.getAllComponentInstances().forEach(instance -> {
      instance.handle(new ComponentInstanceEvent(
          instance.getContainer().getId(), ComponentInstanceEventType.UPGRADE));
    });
    Iterator<ComponentInstance> instanceIter = comp.
        getAllComponentInstances().iterator();

    // reinitialization of a container failed
    ContainerStatus status = mock(ContainerStatus.class);
    when(status.getExitStatus()).thenReturn(ContainerExitStatus.ABORTED);
    ComponentInstance instance = instanceIter.next();
    ComponentEvent stopEvent = new ComponentEvent(comp.getName(),
        ComponentEventType.CONTAINER_COMPLETED)
        .setInstance(instance).setContainerId(instance.getContainer().getId())
        .setStatus(status);
    comp.handle(stopEvent);
    instance.handle(new ComponentInstanceEvent(instance.getContainer().getId(),
        STOP).setStatus(status));

    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CHECK_STABLE));

    Assert.assertEquals("component not in flexing state",
        ComponentState.FLEXING, comp.getComponentSpec().getState());

    // new container get allocated
    context.assignNewContainer(context.attemptId, 10, comp);

    // second instance finished upgrading
    ComponentInstance instance2 = instanceIter.next();
    instance2.handle(new ComponentInstanceEvent(
        instance2.getContainer().getId(),
        ComponentInstanceEventType.BECOME_READY));
    comp.handle(new ComponentEvent(comp.getName(),
        ComponentEventType.CHECK_STABLE));

    Assert.assertEquals("component not in stable state",
        ComponentState.STABLE, comp.getComponentSpec().getState());
    Assert.assertEquals("component did not upgrade successfully", "val1",
        comp.getComponentSpec().getConfiguration().getEnv("key1"));
  }

  @Test
  public void testComponentStateUpdatesWithTerminatingComponents() throws
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

