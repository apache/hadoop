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

import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.ServiceScheduler;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.TestServiceManager;
import org.apache.hadoop.yarn.service.api.records.ComponentState;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEvent;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType;
import org.apache.hadoop.yarn.service.containerlaunch.ContainerLaunchService;
import org.apache.hadoop.yarn.service.registry.YarnRegistryViewForProviders;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType.STOP;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Component}.
 */
public class TestComponent {

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
    ServiceContext context = createTestContext(rule, serviceName);
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
    assignNewContainer(context.attemptId, 10, context, comp);

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

  private static org.apache.hadoop.yarn.service.api.records.Component
      createSpecWithEnv(String serviceName, String compName, String key,
      String val) {
    Service service = TestServiceManager.createBaseDef(serviceName);
    org.apache.hadoop.yarn.service.api.records.Component spec =
        service.getComponent(compName);
    spec.getConfiguration().getEnv().put(key, val);
    return spec;
  }

  public static ServiceContext createTestContext(
      ServiceTestUtils.ServiceFSWatcher fsWatcher, String serviceName)
      throws Exception {
    ServiceContext context = new ServiceContext();
    context.service = TestServiceManager.createBaseDef(serviceName);
    context.fs = fsWatcher.getFs();

    ContainerLaunchService mockLaunchService = mock(
        ContainerLaunchService.class);

    context.scheduler = new ServiceScheduler(context) {
      @Override
      protected YarnRegistryViewForProviders createYarnRegistryOperations(
          ServiceContext context, RegistryOperations registryClient) {
        return mock(YarnRegistryViewForProviders.class);
      }

      @Override
      public NMClientAsync createNMClient() {
        NMClientAsync nmClientAsync = super.createNMClient();
        NMClient nmClient = mock(NMClient.class);
        try {
          when(nmClient.getContainerStatus(anyObject(), anyObject()))
              .thenAnswer((Answer<ContainerStatus>) invocation ->
                  ContainerStatus.newInstance(
                      (ContainerId) invocation.getArguments()[0],
                      org.apache.hadoop.yarn.api.records.ContainerState.RUNNING,
                      "", 0));
        } catch (YarnException | IOException e) {
          throw new RuntimeException(e);
        }
        nmClientAsync.setClient(nmClient);
        return nmClientAsync;
      }

      @Override
      public ContainerLaunchService getContainerLaunchService() {
        return mockLaunchService;
      }
    };
    context.scheduler.init(fsWatcher.getConf());

    doNothing().when(mockLaunchService).
        reInitCompInstance(anyObject(), anyObject(), anyObject(), anyObject());
    stabilizeComponents(context);
    return context;
  }

  private static void stabilizeComponents(ServiceContext context) {

    ApplicationId appId = ApplicationId.fromString(context.service.getId());
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    context.attemptId = attemptId;
    Map<String, Component>
        componentState = context.scheduler.getAllComponents();
    for (org.apache.hadoop.yarn.service.api.records.Component componentSpec :
        context.service.getComponents()) {
      Component component = new org.apache.hadoop.yarn.service.component.
          Component(componentSpec, 1L, context);
      componentState.put(component.getName(), component);
      component.handle(new ComponentEvent(component.getName(),
          ComponentEventType.FLEX));
      for (int i = 0; i < componentSpec.getNumberOfContainers(); i++) {
        assignNewContainer(attemptId, i + 1, context, component);
      }
      component.handle(new ComponentEvent(component.getName(),
          ComponentEventType.CHECK_STABLE));
    }
  }

  private static void assignNewContainer(
      ApplicationAttemptId attemptId, long containerNum,
      ServiceContext context, Component component) {
    Container container = org.apache.hadoop.yarn.api.records.Container
        .newInstance(ContainerId.newContainerId(attemptId, containerNum),
            NODE_ID, "localhost", null, null,
            null);
    component.handle(new ComponentEvent(component.getName(),
        ComponentEventType.CONTAINER_ALLOCATED)
        .setContainer(container).setContainerId(container.getId()));
    ComponentInstance instance = context.scheduler.getLiveInstances().get(
        container.getId());
    ComponentInstanceEvent startEvent = new ComponentInstanceEvent(
        container.getId(), ComponentInstanceEventType.START);
    instance.handle(startEvent);

    ComponentInstanceEvent readyEvent = new ComponentInstanceEvent(
        container.getId(), ComponentInstanceEventType.BECOME_READY);
    instance.handle(readyEvent);
  }

  private static final NodeId NODE_ID = NodeId.fromString("localhost:0");

}

