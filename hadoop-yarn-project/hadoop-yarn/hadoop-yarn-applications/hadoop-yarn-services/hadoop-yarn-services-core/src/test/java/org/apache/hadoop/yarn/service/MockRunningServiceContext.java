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

import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.component.ComponentEvent;
import org.apache.hadoop.yarn.service.component.ComponentEventType;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEvent;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType;
import org.apache.hadoop.yarn.service.containerlaunch.ContainerLaunchService;
import org.apache.hadoop.yarn.service.registry.YarnRegistryViewForProviders;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Mocked service context for a running service.
 */
public class MockRunningServiceContext extends ServiceContext {

  public MockRunningServiceContext(ServiceTestUtils.ServiceFSWatcher fsWatcher,
      Service serviceDef) throws Exception {
    super();
    this.service = serviceDef;
    this.fs = fsWatcher.getFs();

    ContainerLaunchService mockLaunchService = mock(
        ContainerLaunchService.class);

    this.scheduler = new ServiceScheduler(this) {
      @Override
      protected YarnRegistryViewForProviders
      createYarnRegistryOperations(
          ServiceContext context, RegistryOperations registryClient) {
        return mock(YarnRegistryViewForProviders.class);
      }

      @Override
      public NMClientAsync createNMClient() {
        NMClientAsync nmClientAsync = super.createNMClient();
        NMClient nmClient = mock(NMClient.class);
        try {
          when(nmClient.getContainerStatus(anyObject(), anyObject()))
              .thenAnswer(
                  (Answer<ContainerStatus>) invocation -> ContainerStatus
                      .newInstance((ContainerId) invocation.getArguments()[0],
                          org.apache.hadoop.yarn.api.records.ContainerState
                              .RUNNING,
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

      @Override
      public ServiceUtils.ProcessTerminationHandler getTerminationHandler() {
        return new
            ServiceUtils.ProcessTerminationHandler() {
              public void terminate(int exitCode) {
              }
            };
      }

      @Override
      protected ServiceManager createServiceManager() {
        return ServiceTestUtils.createServiceManager(
            MockRunningServiceContext.this);
      }
    };


    this.scheduler.init(fsWatcher.getConf());

    doNothing().when(mockLaunchService).
        reInitCompInstance(anyObject(), anyObject(), anyObject(), anyObject());
    stabilizeComponents(this);
  }

  private void stabilizeComponents(ServiceContext context) {

    ApplicationId appId = ApplicationId.fromString(context.service.getId());
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    context.attemptId = attemptId;
    Map<String, Component>
        componentState = context.scheduler.getAllComponents();

    int counter = 0;
    for (org.apache.hadoop.yarn.service.api.records.Component componentSpec :
        context.service.getComponents()) {
      Component component = new org.apache.hadoop.yarn.service.component.
          Component(componentSpec, 1L, context);
      componentState.put(component.getName(), component);
      component.handle(
          new ComponentEvent(component.getName(), ComponentEventType.FLEX)
              .setDesired(
                  component.getComponentSpec().getNumberOfContainers()));

      for (int i = 0; i < componentSpec.getNumberOfContainers(); i++) {
        counter++;
        assignNewContainer(attemptId, counter, component);
      }

      component.handle(new ComponentEvent(component.getName(),
          ComponentEventType.CHECK_STABLE));
    }
  }

  public void assignNewContainer(ApplicationAttemptId attemptId,
      long containerNum, Component component) {

    Container container = org.apache.hadoop.yarn.api.records.Container
        .newInstance(ContainerId.newContainerId(attemptId, containerNum),
            NODE_ID, "localhost", null, null,
            null);
    component.handle(new ComponentEvent(component.getName(),
        ComponentEventType.CONTAINER_ALLOCATED)
        .setContainer(container).setContainerId(container.getId()));
    ComponentInstance instance = this.scheduler.getLiveInstances().get(
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
