/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.MockPrivilegedOperationCaptor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.TestDockerContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_ID_STR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerCommandExecutor.DockerContainerStatus;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test common docker commands.
 */
public class TestDockerCommandExecutor {

  private static final String MOCK_CONTAINER_ID = "container_id";
  private static final String MOCK_LOCAL_IMAGE_NAME = "local_image_name";
  private static final String MOCK_IMAGE_NAME = "image_name";

  private PrivilegedOperationExecutor mockExecutor;
  private CGroupsHandler mockCGroupsHandler;
  private Configuration configuration;
  private ContainerRuntimeContext.Builder builder;
  private DockerLinuxContainerRuntime runtime;
  private Container container;
  private ContainerId cId;
  private ContainerLaunchContext context;
  private HashMap<String, String> env;

  @Before
  public void setUp() throws Exception {
    mockExecutor = mock(PrivilegedOperationExecutor.class);
    mockCGroupsHandler = mock(CGroupsHandler.class);
    configuration = new Configuration();
    runtime = new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    container = mock(Container.class);
    cId = mock(ContainerId.class);
    context = mock(ContainerLaunchContext.class);
    env = new HashMap<>();
    builder = new ContainerRuntimeContext.Builder(container);

    when(container.getContainerId()).thenReturn(cId);
    when(cId.toString()).thenReturn(MOCK_CONTAINER_ID);
    when(container.getLaunchContext()).thenReturn(context);
    when(context.getEnvironment()).thenReturn(env);

    builder.setExecutionAttribute(CONTAINER_ID_STR, MOCK_CONTAINER_ID);
    runtime.initialize(
        TestDockerContainerRuntime.enableMockContainerExecutor(configuration),
        null);
  }

  @Test
  public void testExecuteDockerCommand() throws Exception {
    DockerStopCommand dockerStopCommand =
        new DockerStopCommand(MOCK_CONTAINER_ID);
    DockerCommandExecutor
        .executeDockerCommand(dockerStopCommand, cId.toString(), env,
            configuration, mockExecutor, false);
    List<PrivilegedOperation> ops = MockPrivilegedOperationCaptor
        .capturePrivilegedOperations(mockExecutor, 1, true);
    assertEquals(1, ops.size());
    assertEquals(PrivilegedOperation.OperationType.RUN_DOCKER_CMD.name(),
        ops.get(0).getOperationType().name());
  }

  @Test
  public void testExecuteDockerRm() throws Exception {
    DockerRmCommand dockerCommand = new DockerRmCommand(MOCK_CONTAINER_ID);
    DockerCommandExecutor
        .executeDockerCommand(dockerCommand, MOCK_CONTAINER_ID, env,
            configuration, mockExecutor, false);
    List<PrivilegedOperation> ops = MockPrivilegedOperationCaptor
        .capturePrivilegedOperations(mockExecutor, 1, true);
    List<String> dockerCommands = getValidatedDockerCommands(ops);
    assertEquals(1, ops.size());
    assertEquals(PrivilegedOperation.OperationType.RUN_DOCKER_CMD.name(),
        ops.get(0).getOperationType().name());
    assertEquals(3, dockerCommands.size());
    assertEquals("[docker-command-execution]", dockerCommands.get(0));
    assertEquals("  docker-command=rm", dockerCommands.get(1));
    assertEquals("  name=" + MOCK_CONTAINER_ID, dockerCommands.get(2));
  }

  @Test
  public void testExecuteDockerStop() throws Exception {
    DockerStopCommand dockerCommand = new DockerStopCommand(MOCK_CONTAINER_ID);
    DockerCommandExecutor
        .executeDockerCommand(dockerCommand, MOCK_CONTAINER_ID, env,
            configuration, mockExecutor, false);
    List<PrivilegedOperation> ops = MockPrivilegedOperationCaptor
        .capturePrivilegedOperations(mockExecutor, 1, true);
    List<String> dockerCommands = getValidatedDockerCommands(ops);
    assertEquals(1, ops.size());
    assertEquals(PrivilegedOperation.OperationType.RUN_DOCKER_CMD.name(),
        ops.get(0).getOperationType().name());
    assertEquals(3, dockerCommands.size());
    assertEquals("[docker-command-execution]", dockerCommands.get(0));
    assertEquals("  docker-command=stop", dockerCommands.get(1));
    assertEquals("  name=" + MOCK_CONTAINER_ID, dockerCommands.get(2));
  }

  @Test
  public void testExecuteDockerInspectStatus() throws Exception {
    DockerInspectCommand dockerCommand =
        new DockerInspectCommand(MOCK_CONTAINER_ID).getContainerStatus();
    DockerCommandExecutor
        .executeDockerCommand(dockerCommand, MOCK_CONTAINER_ID, env,
            configuration, mockExecutor, false);
    List<PrivilegedOperation> ops = MockPrivilegedOperationCaptor
        .capturePrivilegedOperations(mockExecutor, 1, true);
    List<String> dockerCommands = getValidatedDockerCommands(ops);
    assertEquals(1, ops.size());
    assertEquals(PrivilegedOperation.OperationType.RUN_DOCKER_CMD.name(),
        ops.get(0).getOperationType().name());
    assertEquals(4, dockerCommands.size());
    assertEquals("[docker-command-execution]", dockerCommands.get(0));
    assertEquals("  docker-command=inspect", dockerCommands.get(1));
    assertEquals("  format={{.State.Status}}", dockerCommands.get(2));
    assertEquals("  name=" + MOCK_CONTAINER_ID, dockerCommands.get(3));

  }

  @Test
  public void testExecuteDockerPull() throws Exception {
    DockerPullCommand dockerCommand =
        new DockerPullCommand(MOCK_IMAGE_NAME);
    DockerCommandExecutor
        .executeDockerCommand(dockerCommand, MOCK_CONTAINER_ID, env,
            configuration, mockExecutor, false);
    List<PrivilegedOperation> ops = MockPrivilegedOperationCaptor
        .capturePrivilegedOperations(mockExecutor, 1, true);
    List<String> dockerCommands = getValidatedDockerCommands(ops);
    assertEquals(1, ops.size());
    assertEquals(PrivilegedOperation.OperationType.RUN_DOCKER_CMD.name(),
        ops.get(0).getOperationType().name());
    assertEquals(3, dockerCommands.size());
    assertEquals("[docker-command-execution]", dockerCommands.get(0));
    assertEquals("  docker-command=pull", dockerCommands.get(1));
    assertEquals("  image=" + MOCK_IMAGE_NAME, dockerCommands.get(2));
  }

  @Test
  public void testExecuteDockerLoad() throws Exception {
    DockerLoadCommand dockerCommand =
        new DockerLoadCommand(MOCK_LOCAL_IMAGE_NAME);
    DockerCommandExecutor
        .executeDockerCommand(dockerCommand, MOCK_CONTAINER_ID, env,
            configuration, mockExecutor, false);
    List<PrivilegedOperation> ops = MockPrivilegedOperationCaptor
        .capturePrivilegedOperations(mockExecutor, 1, true);
    List<String> dockerCommands = getValidatedDockerCommands(ops);
    assertEquals(1, ops.size());
    assertEquals(PrivilegedOperation.OperationType.RUN_DOCKER_CMD.name(),
        ops.get(0).getOperationType().name());
    assertEquals(3, dockerCommands.size());
    assertEquals("[docker-command-execution]", dockerCommands.get(0));
    assertEquals("  docker-command=load", dockerCommands.get(1));
    assertEquals("  image=" + MOCK_LOCAL_IMAGE_NAME, dockerCommands.get(2));


  }

  @Test
  public void testGetContainerStatus() throws Exception {
    for (DockerContainerStatus status : DockerContainerStatus.values()) {
      when(mockExecutor.executePrivilegedOperation(eq(null),
          any(PrivilegedOperation.class), eq(null), any(), eq(true), eq(false)))
          .thenReturn(status.getName());
      assertEquals(status, DockerCommandExecutor
          .getContainerStatus(MOCK_CONTAINER_ID, configuration, mockExecutor));
    }
  }

  private List<String> getValidatedDockerCommands(
      List<PrivilegedOperation> ops) throws IOException {
    try {
      List<String> dockerCommands = new ArrayList<>();
      for (PrivilegedOperation op : ops) {
        Assert.assertEquals(op.getOperationType(),
            PrivilegedOperation.OperationType.RUN_DOCKER_CMD);
        String dockerCommandFile = op.getArguments().get(0);
        List<String> dockerCommandFileContents = Files
            .readAllLines(Paths.get(dockerCommandFile),
                Charset.forName("UTF-8"));
        dockerCommands.addAll(dockerCommandFileContents);
      }
      return dockerCommands;
    } catch (IOException e) {
      throw new IOException("Unable to read the docker command file.", e);
    }
  }
}
