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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
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
import java.util.concurrent.ConcurrentMap;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_ID_STR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerCommandExecutor.DockerContainerStatus;
import static org.eclipse.jetty.server.handler.gzip.GzipHttpOutputInterceptor.LOG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test common docker commands.
 */
public class TestDockerCommandExecutor {

  private static final String MOCK_CONTAINER_ID =
      "container_e11_1861047502093_13763105_01_000001";
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
  private Context nmContext;
  private ApplicationAttemptId appAttemptId;

  @Before
  public void setUp() throws Exception {
    mockExecutor = mock(PrivilegedOperationExecutor.class);
    mockCGroupsHandler = mock(CGroupsHandler.class);
    configuration = new Configuration();
    String tmpPath = new StringBuffer(System.getProperty("test.build.data"))
        .append('/').append("hadoop.tmp.dir").toString();
    configuration.set("hadoop.tmp.dir", tmpPath);
    runtime = new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    container = mock(Container.class);
    cId = mock(ContainerId.class);
    context = mock(ContainerLaunchContext.class);
    env = new HashMap<>();
    builder = new ContainerRuntimeContext.Builder(container);
    nmContext = createMockNMContext();
    appAttemptId = mock(ApplicationAttemptId.class);

    when(container.getContainerId()).thenReturn(cId);
    when(cId.toString()).thenReturn(MOCK_CONTAINER_ID);
    when(cId.getApplicationAttemptId()).thenReturn(appAttemptId);
    when(container.getLaunchContext()).thenReturn(context);
    when(context.getEnvironment()).thenReturn(env);

    builder.setExecutionAttribute(CONTAINER_ID_STR, MOCK_CONTAINER_ID);
    runtime.initialize(
        TestDockerContainerRuntime.enableMockContainerExecutor(configuration),
        null);
  }


  public Context createMockNMContext() {
    Context mockNMContext = mock(Context.class);
    LocalDirsHandlerService localDirsHandler =
        mock(LocalDirsHandlerService.class);

    String tmpPath = new StringBuffer(System.getProperty("test.build.data"))
        .append('/').append("hadoop.tmp.dir").toString();

    ConcurrentMap<ContainerId, Container> containerMap =
        mock(ConcurrentMap.class);

    when(mockNMContext.getLocalDirsHandler()).thenReturn(localDirsHandler);
    when(mockNMContext.getContainers()).thenReturn(containerMap);
    when(containerMap.get(any())).thenReturn(container);

    try {
      when(localDirsHandler.getLocalPathForWrite(anyString()))
          .thenReturn(new Path(tmpPath));
    } catch (IOException ioe) {
      LOG.info("LocalDirsHandler failed" + ioe);
    }
    return mockNMContext;
  }

  @Test
  public void testExecuteDockerCommand() throws Exception {
    DockerStopCommand dockerStopCommand =
        new DockerStopCommand(MOCK_CONTAINER_ID);
    DockerCommandExecutor.executeDockerCommand(dockerStopCommand,
        cId.toString(), env, configuration, mockExecutor, false, nmContext);
    List<PrivilegedOperation> ops = MockPrivilegedOperationCaptor
        .capturePrivilegedOperations(mockExecutor, 1, true);
    assertEquals(1, ops.size());
    assertEquals(PrivilegedOperation.OperationType.RUN_DOCKER_CMD.name(),
        ops.get(0).getOperationType().name());
  }

  @Test
  public void testExecuteDockerRm() throws Exception {
    DockerRmCommand dockerCommand = new DockerRmCommand(MOCK_CONTAINER_ID);
    DockerCommandExecutor.executeDockerCommand(dockerCommand, MOCK_CONTAINER_ID,
        env, configuration, mockExecutor, false, nmContext);
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
    DockerCommandExecutor.executeDockerCommand(dockerCommand, MOCK_CONTAINER_ID,
        env, configuration, mockExecutor, false, nmContext);
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
    DockerCommandExecutor.executeDockerCommand(dockerCommand, MOCK_CONTAINER_ID,
        env, configuration, mockExecutor, false, nmContext);
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
    DockerCommandExecutor.executeDockerCommand(dockerCommand, MOCK_CONTAINER_ID,
        env, configuration, mockExecutor, false, nmContext);
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
    DockerCommandExecutor.executeDockerCommand(dockerCommand, MOCK_CONTAINER_ID,
        env, configuration, mockExecutor, false, nmContext);
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
      assertEquals(status, DockerCommandExecutor.getContainerStatus(
          MOCK_CONTAINER_ID, configuration, mockExecutor, nmContext));
    }
  }

  @Test
  public void testExecuteDockerKillSIGQUIT() throws Exception {
    DockerKillCommand dockerKillCommand =
        new DockerKillCommand(MOCK_CONTAINER_ID)
            .setSignal(ContainerExecutor.Signal.QUIT.name());
    DockerCommandExecutor.executeDockerCommand(dockerKillCommand,
        MOCK_CONTAINER_ID, env, configuration, mockExecutor, false, nmContext);
    List<PrivilegedOperation> ops = MockPrivilegedOperationCaptor
        .capturePrivilegedOperations(mockExecutor, 1, true);
    List<String> dockerCommands = getValidatedDockerCommands(ops);
    assertEquals(1, ops.size());
    assertEquals(PrivilegedOperation.OperationType.RUN_DOCKER_CMD.name(),
        ops.get(0).getOperationType().name());
    assertEquals(4, dockerCommands.size());
    assertEquals("[docker-command-execution]", dockerCommands.get(0));
    assertEquals("  docker-command=kill", dockerCommands.get(1));
    assertEquals("  name=" + MOCK_CONTAINER_ID, dockerCommands.get(2));
    assertEquals("  signal=" + ContainerExecutor.Signal.QUIT.name(),
        dockerCommands.get(3));
  }

  @Test
  public void testExecuteDockerKillSIGKILL() throws Exception {
    DockerKillCommand dockerKillCommand =
        new DockerKillCommand(MOCK_CONTAINER_ID)
            .setSignal(ContainerExecutor.Signal.KILL.name());
    DockerCommandExecutor.executeDockerCommand(dockerKillCommand,
        MOCK_CONTAINER_ID, env, configuration, mockExecutor, false, nmContext);
    List<PrivilegedOperation> ops = MockPrivilegedOperationCaptor
        .capturePrivilegedOperations(mockExecutor, 1, true);
    List<String> dockerCommands = getValidatedDockerCommands(ops);
    assertEquals(1, ops.size());
    assertEquals(PrivilegedOperation.OperationType.RUN_DOCKER_CMD.name(),
        ops.get(0).getOperationType().name());
    assertEquals(4, dockerCommands.size());
    assertEquals("[docker-command-execution]", dockerCommands.get(0));
    assertEquals("  docker-command=kill", dockerCommands.get(1));
    assertEquals("  name=" + MOCK_CONTAINER_ID, dockerCommands.get(2));
    assertEquals("  signal=" + ContainerExecutor.Signal.KILL.name(),
        dockerCommands.get(3));
  }

  @Test
  public void testExecuteDockerKillSIGTERM() throws Exception {
    DockerKillCommand dockerKillCommand =
        new DockerKillCommand(MOCK_CONTAINER_ID)
            .setSignal(ContainerExecutor.Signal.TERM.name());
    DockerCommandExecutor.executeDockerCommand(dockerKillCommand,
        MOCK_CONTAINER_ID, env, configuration, mockExecutor, false, nmContext);
    List<PrivilegedOperation> ops = MockPrivilegedOperationCaptor
        .capturePrivilegedOperations(mockExecutor, 1, true);
    List<String> dockerCommands = getValidatedDockerCommands(ops);
    assertEquals(1, ops.size());
    assertEquals(PrivilegedOperation.OperationType.RUN_DOCKER_CMD.name(),
        ops.get(0).getOperationType().name());
    assertEquals(4, dockerCommands.size());
    assertEquals("[docker-command-execution]", dockerCommands.get(0));
    assertEquals("  docker-command=kill", dockerCommands.get(1));
    assertEquals("  name=" + MOCK_CONTAINER_ID, dockerCommands.get(2));
    assertEquals("  signal=" + ContainerExecutor.Signal.TERM.name(),
        dockerCommands.get(3));
  }

  @Test
  public void testIsStoppable() {
    assertTrue(DockerCommandExecutor.isStoppable(
        DockerContainerStatus.RUNNING));
    assertTrue(DockerCommandExecutor.isStoppable(
        DockerContainerStatus.RESTARTING));
    assertFalse(DockerCommandExecutor.isStoppable(
        DockerContainerStatus.EXITED));
    assertFalse(DockerCommandExecutor.isStoppable(
        DockerContainerStatus.CREATED));
    assertFalse(DockerCommandExecutor.isStoppable(
        DockerContainerStatus.DEAD));
    assertFalse(DockerCommandExecutor.isStoppable(
        DockerContainerStatus.NONEXISTENT));
    assertFalse(DockerCommandExecutor.isStoppable(
        DockerContainerStatus.REMOVING));
    assertFalse(DockerCommandExecutor.isStoppable(
        DockerContainerStatus.STOPPED));
    assertFalse(DockerCommandExecutor.isStoppable(
        DockerContainerStatus.UNKNOWN));
  }

  @Test
  public void testIsKIllable() {
    assertTrue(DockerCommandExecutor.isKillable(
        DockerContainerStatus.RUNNING));
    assertTrue(DockerCommandExecutor.isKillable(
        DockerContainerStatus.RESTARTING));
    assertFalse(DockerCommandExecutor.isKillable(
        DockerContainerStatus.EXITED));
    assertFalse(DockerCommandExecutor.isKillable(
        DockerContainerStatus.CREATED));
    assertFalse(DockerCommandExecutor.isKillable(
        DockerContainerStatus.DEAD));
    assertFalse(DockerCommandExecutor.isKillable(
        DockerContainerStatus.NONEXISTENT));
    assertFalse(DockerCommandExecutor.isKillable(
        DockerContainerStatus.REMOVING));
    assertFalse(DockerCommandExecutor.isKillable(
        DockerContainerStatus.STOPPED));
    assertFalse(DockerCommandExecutor.isKillable(
        DockerContainerStatus.UNKNOWN));
  }

  @Test
  public void testIsRemovable() {
    assertTrue(DockerCommandExecutor.isRemovable(
        DockerContainerStatus.STOPPED));
    assertTrue(DockerCommandExecutor.isRemovable(
        DockerContainerStatus.RESTARTING));
    assertTrue(DockerCommandExecutor.isRemovable(
        DockerContainerStatus.EXITED));
    assertTrue(DockerCommandExecutor.isRemovable(
        DockerContainerStatus.CREATED));
    assertTrue(DockerCommandExecutor.isRemovable(
        DockerContainerStatus.DEAD));
    assertFalse(DockerCommandExecutor.isRemovable(
        DockerContainerStatus.NONEXISTENT));
    assertFalse(DockerCommandExecutor.isRemovable(
        DockerContainerStatus.REMOVING));
    assertFalse(DockerCommandExecutor.isRemovable(
        DockerContainerStatus.UNKNOWN));
    assertFalse(DockerCommandExecutor.isRemovable(
        DockerContainerStatus.RUNNING));
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
