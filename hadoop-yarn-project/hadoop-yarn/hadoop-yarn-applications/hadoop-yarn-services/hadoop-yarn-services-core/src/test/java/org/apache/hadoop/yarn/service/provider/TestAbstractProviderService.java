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

package org.apache.hadoop.yarn.service.provider;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.service.MockRunningServiceContext;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.TestServiceManager;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.containerlaunch.AbstractLauncher;
import org.apache.hadoop.yarn.service.containerlaunch.ContainerLaunchService;
import org.apache.hadoop.yarn.service.provider.docker.DockerProviderService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractProviderService}
 */
public class TestAbstractProviderService {

  private ServiceContext serviceContext;
  private Service testService;
  private AbstractLauncher launcher;

  @RegisterExtension
  private ServiceTestUtils.ServiceFSWatcher rule =
      new ServiceTestUtils.ServiceFSWatcher();

  @BeforeEach
  public void setup() throws Exception {
    testService = TestServiceManager.createBaseDef("testService");
    serviceContext = new MockRunningServiceContext(rule, testService);
    launcher = new AbstractLauncher(serviceContext);
    rule.getFs().setAppDir(new Path("target/testAbstractProviderService"));
  }

  @AfterEach
  public void teardown() throws Exception {
    FileUtils.deleteQuietly(
        new File(rule.getFs().getAppDir().toUri().getPath()));
  }

  @Test
  public void testBuildContainerLaunchCommand() throws Exception {
    AbstractProviderService providerService = new DockerProviderService();
    Component component = serviceContext.scheduler.getAllComponents().entrySet()
        .iterator().next().getValue();
    ContainerLaunchService.ComponentLaunchContext clc =
        createEntryPointCLCFor(testService, component, "sleep,9000");

    ComponentInstance instance = component.getAllComponentInstances().iterator()
        .next();
    Container container = mock(Container.class);
    providerService.buildContainerLaunchCommand(launcher, testService, instance,
        rule.getFs(), serviceContext.scheduler.getConfig(), container, clc,
        null);

    assertEquals(Lists.newArrayList(clc.getLaunchCommand()),
        launcher.getCommands(), "commands");
  }

  @Test
  public void testBuildContainerLaunchCommandWithSpace() throws Exception {
    AbstractProviderService providerService = new DockerProviderService();
    Component component = serviceContext.scheduler.getAllComponents().entrySet()
        .iterator().next().getValue();
    ContainerLaunchService.ComponentLaunchContext clc =
        createEntryPointCLCFor(testService, component, "ls -l \" space\"");

    ComponentInstance instance = component.getAllComponentInstances().iterator()
        .next();
    Container container = mock(Container.class);
    providerService.buildContainerLaunchCommand(launcher, testService, instance,
        rule.getFs(), serviceContext.scheduler.getConfig(), container, clc,
        null);

    assertEquals(Lists.newArrayList("ls,-l, space"),
        launcher.getCommands(), "commands don't match.");
  }

  @Test
  public void testBuildContainerLaunchContext() throws Exception {
    AbstractProviderService providerService = new DockerProviderService();
    Component component = serviceContext.scheduler.getAllComponents().entrySet()
        .iterator().next().getValue();
    ContainerLaunchService.ComponentLaunchContext clc =
        createEntryPointCLCFor(testService, component, "sleep,9000");

    ComponentInstance instance = component.getAllComponentInstances().iterator()
        .next();
    Container container = mock(Container.class);
    ContainerId containerId = ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(
            System.currentTimeMillis(), 1), 1), 1L);
    when(container.getId()).thenReturn(containerId);
    providerService.buildContainerLaunchContext(launcher, testService, instance,
        rule.getFs(), serviceContext.scheduler.getConfig(), container, clc);

    assertEquals(clc.getArtifact().getId(),
        launcher.getDockerImage(), "artifact");
  }

  private static ContainerLaunchService.ComponentLaunchContext
      createEntryPointCLCFor(Service service, Component component,
          String launchCmd) {
    Artifact artifact = new Artifact();
    artifact.setType(Artifact.TypeEnum.DOCKER);
    artifact.setId("example");
    Map<String, String> env = new HashMap<>();
    env.put("YARN_CONTAINER_RUNTIME_DOCKER_DELAYED_REMOVAL", "true");
    env.put("YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE", "true");
    component.getComponentSpec().getConfiguration().setEnv(env);

    return new ContainerLaunchService.ComponentLaunchContext(
        component.getName(),
        service.getVersion())
        .setArtifact(artifact)
        .setConfiguration(component.getComponentSpec().getConfiguration())
        .setLaunchCommand(launchCmd);
  }
}
