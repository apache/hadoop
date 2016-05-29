/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class TestDockerContainerRuntime {
  private static final Log LOG = LogFactory
      .getLog(TestDockerContainerRuntime.class);
  private Configuration conf;
  private PrivilegedOperationExecutor mockExecutor;
  private CGroupsHandler mockCGroupsHandler;
  private String containerId;
  private Container container;
  private ContainerId cId;
  private ContainerLaunchContext context;
  private HashMap<String, String> env;
  private String image;
  private String runAsUser;
  private String user;
  private String appId;
  private String containerIdStr = containerId;
  private Path containerWorkDir;
  private Path nmPrivateContainerScriptPath;
  private Path nmPrivateTokensPath;
  private Path pidFilePath;
  private List<String> localDirs;
  private List<String> logDirs;
  private List<String> containerLocalDirs;
  private List<String> containerLogDirs;
  private Map<Path, List<String>> localizedResources;
  private String resourcesOptions;
  private ContainerRuntimeContext.Builder builder;
  private final String submittingUser = "anakin";
  private final String whitelistedUser = "yoda";
  private String[] testCapabilities;

  @Before
  public void setup() {
    String tmpPath = new StringBuffer(System.getProperty("test.build.data"))
        .append('/').append("hadoop.tmp.dir").toString();

    conf = new Configuration();
    conf.set("hadoop.tmp.dir", tmpPath);

    mockExecutor = Mockito
        .mock(PrivilegedOperationExecutor.class);
    mockCGroupsHandler = Mockito.mock(CGroupsHandler.class);
    containerId = "container_id";
    container = mock(Container.class);
    cId = mock(ContainerId.class);
    context = mock(ContainerLaunchContext.class);
    env = new HashMap<String, String>();
    image = "busybox:latest";

    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_IMAGE, image);
    when(container.getContainerId()).thenReturn(cId);
    when(cId.toString()).thenReturn(containerId);
    when(container.getLaunchContext()).thenReturn(context);
    when(context.getEnvironment()).thenReturn(env);
    when(container.getUser()).thenReturn(submittingUser);

    runAsUser = "run_as_user";
    user = "user";
    appId = "app_id";
    containerIdStr = containerId;
    containerWorkDir = new Path("/test_container_work_dir");
    nmPrivateContainerScriptPath = new Path("/test_script_path");
    nmPrivateTokensPath = new Path("/test_private_tokens_path");
    pidFilePath = new Path("/test_pid_file_path");
    localDirs = new ArrayList<>();
    logDirs = new ArrayList<>();
    resourcesOptions = "cgroups=none";
    containerLocalDirs = new ArrayList<>();
    containerLogDirs = new ArrayList<>();
    localizedResources = new HashMap<>();

    localDirs.add("/test_local_dir");
    logDirs.add("/test_log_dir");
    containerLocalDirs.add("/test_container_local_dir");
    containerLogDirs.add("/test_container_log_dir");
    localizedResources.put(new Path("/test_local_dir/test_resource_file"),
        Collections.singletonList("test_dir/test_resource_file"));

    testCapabilities = new String[] {"NET_BIND_SERVICE", "SYS_CHROOT"};
    conf.setStrings(YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
        testCapabilities);

    builder = new ContainerRuntimeContext
        .Builder(container);

    builder.setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user)
        .setExecutionAttribute(APPID, appId)
        .setExecutionAttribute(CONTAINER_ID_STR, containerIdStr)
        .setExecutionAttribute(CONTAINER_WORK_DIR, containerWorkDir)
        .setExecutionAttribute(NM_PRIVATE_CONTAINER_SCRIPT_PATH,
            nmPrivateContainerScriptPath)
        .setExecutionAttribute(NM_PRIVATE_TOKENS_PATH, nmPrivateTokensPath)
        .setExecutionAttribute(PID_FILE_PATH, pidFilePath)
        .setExecutionAttribute(LOCAL_DIRS, localDirs)
        .setExecutionAttribute(LOG_DIRS, logDirs)
        .setExecutionAttribute(CONTAINER_LOCAL_DIRS, containerLocalDirs)
        .setExecutionAttribute(CONTAINER_LOG_DIRS, containerLogDirs)
        .setExecutionAttribute(LOCALIZED_RESOURCES, localizedResources)
        .setExecutionAttribute(RESOURCES_OPTIONS, resourcesOptions);
  }

  @Test
  public void testSelectDockerContainerType() {
    Map<String, String> envDockerType = new HashMap<>();
    Map<String, String> envOtherType = new HashMap<>();

    envDockerType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "docker");
    envOtherType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "other");

    Assert.assertEquals(false, DockerLinuxContainerRuntime
        .isDockerContainerRequested(null));
    Assert.assertEquals(true, DockerLinuxContainerRuntime
        .isDockerContainerRequested(envDockerType));
    Assert.assertEquals(false, DockerLinuxContainerRuntime
        .isDockerContainerRequested(envOtherType));
  }

  @SuppressWarnings("unchecked")
  private PrivilegedOperation capturePrivilegedOperationAndVerifyArgs()
      throws PrivilegedOperationException {
    ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass(
        PrivilegedOperation.class);

    //single invocation expected
    //due to type erasure + mocking, this verification requires a suppress
    // warning annotation on the entire method
    verify(mockExecutor, times(1))
        .executePrivilegedOperation(anyList(), opCaptor.capture(), any(
            File.class), any(Map.class), eq(false), eq(false));

    //verification completed. we need to isolate specific invications.
    // hence, reset mock here
    Mockito.reset(mockExecutor);

    PrivilegedOperation op = opCaptor.getValue();

    Assert.assertEquals(PrivilegedOperation.OperationType
        .LAUNCH_DOCKER_CONTAINER, op.getOperationType());

    List<String> args = op.getArguments();

    //This invocation of container-executor should use 13 arguments in a
    // specific order (sigh.)
    Assert.assertEquals(13, args.size());

    //verify arguments
    Assert.assertEquals(runAsUser, args.get(0));
    Assert.assertEquals(user, args.get(1));
    Assert.assertEquals(Integer.toString(PrivilegedOperation.RunAsUserCommand
        .LAUNCH_DOCKER_CONTAINER.getValue()), args.get(2));
    Assert.assertEquals(appId, args.get(3));
    Assert.assertEquals(containerId, args.get(4));
    Assert.assertEquals(containerWorkDir.toString(), args.get(5));
    Assert.assertEquals(nmPrivateContainerScriptPath.toUri()
        .toString(), args.get(6));
    Assert.assertEquals(nmPrivateTokensPath.toUri().getPath(), args.get(7));
    Assert.assertEquals(pidFilePath.toString(), args.get(8));
    Assert.assertEquals(localDirs.get(0), args.get(9));
    Assert.assertEquals(logDirs.get(0), args.get(10));
    Assert.assertEquals(resourcesOptions, args.get(12));

    return op;
  }

  private String getExpectedTestCapabilitiesArgumentString()  {
    /* Ordering of capabilities depends on HashSet ordering. */
    Set<String> capabilitySet = new HashSet<>(Arrays.asList(testCapabilities));
    StringBuilder expectedCapabilitiesString = new StringBuilder(
        "--cap-drop=ALL ");

    for(String capability : capabilitySet) {
      expectedCapabilitiesString.append("--cap-add=").append(capability)
          .append(" ");
    }

    return expectedCapabilitiesString.toString();
  }

  @Test
  public void testDockerContainerLaunch()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);
    runtime.launchContainer(builder.build());

    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    //This is the expected docker invocation for this case
    StringBuffer expectedCommandTemplate = new StringBuffer("run --name=%1$s ")
        .append("--user=%2$s -d ")
        .append("--workdir=%3$s ")
        .append("--net=host ")
        .append(getExpectedTestCapabilitiesArgumentString())
        .append("-v /etc/passwd:/etc/password:ro ")
        .append("-v %4$s:%4$s ")
        .append("-v %5$s:%5$s ")
        .append("-v %6$s:%6$s ")
        .append("%7$s ")
        .append("bash %8$s/launch_container.sh");

    String expectedCommand = String.format(expectedCommandTemplate.toString(),
        containerId, runAsUser, containerWorkDir, containerLocalDirs.get(0),
        containerWorkDir, containerLogDirs.get(0), image, containerWorkDir);

    List<String> dockerCommands = Files.readAllLines(Paths.get
            (dockerCommandFile), Charset.forName("UTF-8"));

    Assert.assertEquals(1, dockerCommands.size());
    Assert.assertEquals(expectedCommand, dockerCommands.get(0));
  }

  @Test
  public void testAllowedNetworksConfiguration() throws
      ContainerExecutionException {
    //the default network configuration should cause
    // no exception should be thrown.

    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);

    //invalid default network configuration - sdn2 is included in allowed
    // networks

    String[] networks = {"host", "none", "bridge", "sdn1"};
    String invalidDefaultNetwork = "sdn2";

    conf.setStrings(YarnConfiguration.NM_DOCKER_ALLOWED_CONTAINER_NETWORKS,
        networks);
    conf.set(YarnConfiguration.NM_DOCKER_DEFAULT_CONTAINER_NETWORK,
        invalidDefaultNetwork);

    try {
      runtime =
          new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
      runtime.initialize(conf);
      Assert.fail("Invalid default network configuration should did not "
          + "trigger initialization failure.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }

    //valid default network configuration - sdn1 is included in allowed
    // networks - no exception should be thrown.

    String validDefaultNetwork = "sdn1";

    conf.set(YarnConfiguration.NM_DOCKER_DEFAULT_CONTAINER_NETWORK,
        validDefaultNetwork);
    runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testContainerLaunchWithNetworkingDefaults()
      throws ContainerExecutionException, IOException,
      PrivilegedOperationException {
    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);

    Random randEngine = new Random();
    String disallowedNetwork = "sdn" + Integer.toString(randEngine.nextInt());

    try {
      env.put("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK",
          disallowedNetwork);
      runtime.launchContainer(builder.build());
      Assert.fail("Network was expected to be disallowed: " +
          disallowedNetwork);
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception: " + e);
    }

    int size = YarnConfiguration
        .DEFAULT_NM_DOCKER_ALLOWED_CONTAINER_NETWORKS.length;
    String allowedNetwork = YarnConfiguration
        .DEFAULT_NM_DOCKER_ALLOWED_CONTAINER_NETWORKS[randEngine.nextInt(size)];
    env.put("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK",
        allowedNetwork);

    //this should cause no failures.

    runtime.launchContainer(builder.build());
    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    //This is the expected docker invocation for this case
    StringBuffer expectedCommandTemplate =
        new StringBuffer("run --name=%1$s ").append("--user=%2$s -d ")
            .append("--workdir=%3$s ")
            .append("--net=" + allowedNetwork + " ")
            .append(getExpectedTestCapabilitiesArgumentString())
            .append("-v /etc/passwd:/etc/password:ro ")
            .append("-v %4$s:%4$s ").append("-v %5$s:%5$s ")
            .append("-v %6$s:%6$s ").append("%7$s ")
            .append("bash %8$s/launch_container.sh");

    String expectedCommand = String
        .format(expectedCommandTemplate.toString(), containerId, runAsUser,
            containerWorkDir, containerLocalDirs.get(0), containerWorkDir,
            containerLogDirs.get(0), image, containerWorkDir);

    List<String> dockerCommands = Files
        .readAllLines(Paths.get(dockerCommandFile), Charset.forName("UTF-8"));

    Assert.assertEquals(1, dockerCommands.size());
    Assert.assertEquals(expectedCommand, dockerCommands.get(0));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testContainerLaunchWithCustomNetworks()
      throws ContainerExecutionException, IOException,
      PrivilegedOperationException {
    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);

    String customNetwork1 = "sdn1";
    String customNetwork2 = "sdn2";
    String customNetwork3 = "sdn3";

    String[] networks = {"host", "none", "bridge", customNetwork1,
        customNetwork2};

    //customized set of allowed networks
    conf.setStrings(YarnConfiguration.NM_DOCKER_ALLOWED_CONTAINER_NETWORKS,
        networks);
    //default network is "sdn1"
    conf.set(YarnConfiguration.NM_DOCKER_DEFAULT_CONTAINER_NETWORK,
        customNetwork1);

    //this should cause no failures.
    runtime.initialize(conf);
    runtime.launchContainer(builder.build());
    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    //This is the expected docker invocation for this case. customNetwork1
    // ("sdn1") is the expected network to be used in this case
    StringBuffer expectedCommandTemplate =
        new StringBuffer("run --name=%1$s ").append("--user=%2$s -d ")
            .append("--workdir=%3$s ")
            .append("--net=" + customNetwork1 + " ")
            .append(getExpectedTestCapabilitiesArgumentString())
            .append("-v /etc/passwd:/etc/password:ro ")
            .append("-v %4$s:%4$s ").append("-v %5$s:%5$s ")
            .append("-v %6$s:%6$s ").append("%7$s ")
            .append("bash %8$s/launch_container.sh");

    String expectedCommand = String
        .format(expectedCommandTemplate.toString(), containerId, runAsUser,
            containerWorkDir, containerLocalDirs.get(0), containerWorkDir,
            containerLogDirs.get(0), image, containerWorkDir);

    List<String> dockerCommands = Files
        .readAllLines(Paths.get(dockerCommandFile), Charset.forName("UTF-8"));

    Assert.assertEquals(1, dockerCommands.size());
    Assert.assertEquals(expectedCommand, dockerCommands.get(0));


    //now set an explicit (non-default) allowedNetwork and ensure that it is
    // used.

    env.put("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK",
        customNetwork2);
    runtime.launchContainer(builder.build());

    op = capturePrivilegedOperationAndVerifyArgs();
    args = op.getArguments();
    dockerCommandFile = args.get(11);

    //This is the expected docker invocation for this case. customNetwork2
    // ("sdn2") is the expected network to be used in this case
    expectedCommandTemplate =
        new StringBuffer("run --name=%1$s ").append("--user=%2$s -d ")
            .append("--workdir=%3$s ")
            .append("--net=" + customNetwork2 + " ")
            .append(getExpectedTestCapabilitiesArgumentString())
            .append("-v /etc/passwd:/etc/password:ro ")
            .append("-v %4$s:%4$s ").append("-v %5$s:%5$s ")
            .append("-v %6$s:%6$s ").append("%7$s ")
            .append("bash %8$s/launch_container.sh");

    expectedCommand = String
        .format(expectedCommandTemplate.toString(), containerId, runAsUser,
            containerWorkDir, containerLocalDirs.get(0), containerWorkDir,
            containerLogDirs.get(0), image, containerWorkDir);

    dockerCommands = Files
        .readAllLines(Paths.get(dockerCommandFile), Charset.forName("UTF-8"));

    Assert.assertEquals(1, dockerCommands.size());
    Assert.assertEquals(expectedCommand, dockerCommands.get(0));

    //disallowed network should trigger a launch failure

    env.put("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK",
        customNetwork3);
    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Disallowed network : " + customNetwork3
          + "did not trigger launch failure.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testLaunchPrivilegedContainersInvalidEnvVar()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);

    env.put("YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER",
        "invalid-value");
    runtime.launchContainer(builder.build());

    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    List<String> dockerCommands = Files.readAllLines(Paths.get
        (dockerCommandFile), Charset.forName("UTF-8"));

    Assert.assertEquals(1, dockerCommands.size());

    String command = dockerCommands.get(0);

    //ensure --privileged isn't in the invocation
    Assert.assertTrue("Unexpected --privileged in docker run args : " + command,
        !command.contains("--privileged"));
  }

  @Test
  public void testLaunchPrivilegedContainersWithDisabledSetting()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);

    env.put("YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER",
        "true");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a privileged launch container failure.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testLaunchPrivilegedContainersWithEnabledSettingAndDefaultACL()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    //Enable privileged containers.
    conf.setBoolean(YarnConfiguration.NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS,
        true);

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);

    env.put("YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER",
        "true");
    //By default
    // yarn.nodemanager.runtime.linux.docker.privileged-containers.acl
    // is empty. So we expect this launch to fail.

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a privileged launch container failure.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void
  testLaunchPrivilegedContainersEnabledAndUserNotInWhitelist()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    //Enable privileged containers.
    conf.setBoolean(YarnConfiguration.NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS,
        true);
    //set whitelist of users.
    conf.set(YarnConfiguration.NM_DOCKER_PRIVILEGED_CONTAINERS_ACL,
        whitelistedUser);

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);

    env.put("YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER",
        "true");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a privileged launch container failure.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void
  testLaunchPrivilegedContainersEnabledAndUserInWhitelist()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    //Enable privileged containers.
    conf.setBoolean(YarnConfiguration.NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS,
        true);
    //Add submittingUser to whitelist.
    conf.set(YarnConfiguration.NM_DOCKER_PRIVILEGED_CONTAINERS_ACL,
        submittingUser);

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);

    env.put("YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER",
        "true");

    runtime.launchContainer(builder.build());
    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    List<String> dockerCommands = Files.readAllLines(Paths.get
        (dockerCommandFile), Charset.forName("UTF-8"));

    Assert.assertEquals(1, dockerCommands.size());

    String command = dockerCommands.get(0);

    //submitting user is whitelisted. ensure --privileged is in the invocation
    Assert.assertTrue("Did not find expected '--privileged' in docker run args "
        + ": " + command, command.contains("--privileged"));
  }

  @Test
  public void testCGroupParent() throws ContainerExecutionException {
    String hierarchy = "hadoop-yarn-test";
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY,
        hierarchy);

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime
        (mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);

    String resourceOptionsNone = "cgroups=none";
    DockerRunCommand command = Mockito.mock(DockerRunCommand.class);

    Mockito.when(mockCGroupsHandler.getRelativePathForCGroup(containerId))
        .thenReturn(hierarchy + "/" + containerIdStr);
    runtime.addCGroupParentIfRequired(resourceOptionsNone, containerIdStr,
        command);

    //no --cgroup-parent should be added here
    Mockito.verifyZeroInteractions(command);

    String resourceOptionsCpu = "/sys/fs/cgroup/cpu/" + hierarchy +
        containerIdStr;
    runtime.addCGroupParentIfRequired(resourceOptionsCpu, containerIdStr,
        command);

    //--cgroup-parent should be added for the containerId in question
    String expectedPath = "/" + hierarchy + "/" + containerIdStr;
    Mockito.verify(command).setCGroupParent(expectedPath);

    //create a runtime with a 'null' cgroups handler - i.e no
    // cgroup-based resource handlers are in use.

    runtime = new DockerLinuxContainerRuntime
        (mockExecutor, null);
    runtime.initialize(conf);

    runtime.addCGroupParentIfRequired(resourceOptionsNone, containerIdStr,
        command);
    runtime.addCGroupParentIfRequired(resourceOptionsCpu, containerIdStr,
        command);

    //no --cgroup-parent should be added in either case
    Mockito.verifyZeroInteractions(command);
  }

  @Test
  public void testMountSourceOnly()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_LOCAL_RESOURCE_MOUNTS,
        "source");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a launch container failure due to invalid mount.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testMountSourceTarget()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_LOCAL_RESOURCE_MOUNTS,
        "test_dir/test_resource_file:test_mount");

    runtime.launchContainer(builder.build());
    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    List<String> dockerCommands = Files.readAllLines(Paths.get
        (dockerCommandFile), Charset.forName("UTF-8"));

    Assert.assertEquals(1, dockerCommands.size());

    String command = dockerCommands.get(0);

    Assert.assertTrue("Did not find expected " +
        "/test_local_dir/test_resource_file:test_mount mount in docker " +
        "run args : " + command,
        command.contains(" -v /test_local_dir/test_resource_file:test_mount" +
            ":ro "));
  }

  @Test
  public void testMountInvalid()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_LOCAL_RESOURCE_MOUNTS,
        "source:target:other");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a launch container failure due to invalid mount.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testMountMultiple()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_LOCAL_RESOURCE_MOUNTS,
        "test_dir/test_resource_file:test_mount1," +
            "test_dir/test_resource_file:test_mount2");

    runtime.launchContainer(builder.build());
    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    List<String> dockerCommands = Files.readAllLines(Paths.get
        (dockerCommandFile), Charset.forName("UTF-8"));

    Assert.assertEquals(1, dockerCommands.size());

    String command = dockerCommands.get(0);

    Assert.assertTrue("Did not find expected " +
        "/test_local_dir/test_resource_file:test_mount1 mount in docker " +
        "run args : " + command,
        command.contains(" -v /test_local_dir/test_resource_file:test_mount1" +
            ":ro "));
    Assert.assertTrue("Did not find expected " +
        "/test_local_dir/test_resource_file:test_mount2 mount in docker " +
        "run args : " + command,
        command.contains(" -v /test_local_dir/test_resource_file:test_mount2" +
            ":ro "));
  }

}
