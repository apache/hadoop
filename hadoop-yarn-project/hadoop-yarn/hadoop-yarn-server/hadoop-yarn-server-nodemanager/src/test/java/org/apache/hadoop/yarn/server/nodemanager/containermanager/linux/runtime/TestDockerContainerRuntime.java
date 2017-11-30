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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.APPID;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_ID_STR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_LOCAL_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_LOG_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_WORK_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.FILECACHE_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOCALIZED_RESOURCES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOCAL_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOG_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.NM_PRIVATE_CONTAINER_SCRIPT_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.NM_PRIVATE_TOKENS_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.PID;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.PID_FILE_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.RESOURCES_OPTIONS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.RUN_AS_USER;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.SIGNAL;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.USER;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.USER_LOCAL_DIRS;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestDockerContainerRuntime {
  private static final Logger LOG =
       LoggerFactory.getLogger(TestDockerContainerRuntime.class);
  private Configuration conf;
  private PrivilegedOperationExecutor mockExecutor;
  private CGroupsHandler mockCGroupsHandler;
  private String containerId;
  private String defaultHostname;
  private Container container;
  private ContainerId cId;
  private ContainerLaunchContext context;
  private HashMap<String, String> env;
  private String image;
  private String uidGidPair;
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
  private List<String> filecacheDirs;
  private List<String> userLocalDirs;
  private List<String> containerLocalDirs;
  private List<String> containerLogDirs;
  private Map<Path, List<String>> localizedResources;
  private String resourcesOptions;
  private ContainerRuntimeContext.Builder builder;
  private final String submittingUser = "anakin";
  private final String whitelistedUser = "yoda";
  private String[] testCapabilities;
  private final String signalPid = "1234";

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
    defaultHostname = RegistryPathUtils.encodeYarnID(containerId);
    container = mock(Container.class);
    cId = mock(ContainerId.class);
    context = mock(ContainerLaunchContext.class);
    env = new HashMap<String, String>();
    env.put("FROM_CLIENT", "1");
    image = "busybox:latest";

    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_IMAGE, image);
    when(container.getContainerId()).thenReturn(cId);
    when(cId.toString()).thenReturn(containerId);
    when(container.getLaunchContext()).thenReturn(context);
    when(context.getEnvironment()).thenReturn(env);
    when(container.getUser()).thenReturn(submittingUser);

    uidGidPair = "";
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
    filecacheDirs = new ArrayList<>();
    resourcesOptions = "cgroups=none";
    userLocalDirs = new ArrayList<>();
    containerLocalDirs = new ArrayList<>();
    containerLogDirs = new ArrayList<>();
    localizedResources = new HashMap<>();

    localDirs.add("/test_local_dir");
    logDirs.add("/test_log_dir");
    filecacheDirs.add("/test_filecache_dir");
    userLocalDirs.add("/test_user_local_dir");
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
        .setExecutionAttribute(FILECACHE_DIRS, filecacheDirs)
        .setExecutionAttribute(USER_LOCAL_DIRS, userLocalDirs)
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
  private PrivilegedOperation capturePrivilegedOperation()
      throws PrivilegedOperationException {
    ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass(
        PrivilegedOperation.class);

    //single invocation expected
    //due to type erasure + mocking, this verification requires a suppress
    // warning annotation on the entire method
    verify(mockExecutor, times(1))
        .executePrivilegedOperation(anyList(), opCaptor.capture(), any(
            File.class), eq(null), eq(false), eq(false));

    //verification completed. we need to isolate specific invications.
    // hence, reset mock here
    Mockito.reset(mockExecutor);

    return opCaptor.getValue();
  }

    @SuppressWarnings("unchecked")
  private PrivilegedOperation capturePrivilegedOperationAndVerifyArgs()
      throws PrivilegedOperationException {

    PrivilegedOperation op = capturePrivilegedOperation();

    Assert.assertEquals(PrivilegedOperation.OperationType
        .LAUNCH_DOCKER_CONTAINER, op.getOperationType());

    List<String> args = op.getArguments();

    //This invocation of container-executor should use 13 arguments in a
    // specific order (sigh.)
    Assert.assertEquals(13, args.size());

    //verify arguments
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

  private String getExpectedCGroupsMountString() {
    CGroupsHandler cgroupsHandler = ResourceHandlerModule.getCGroupsHandler();
    if(cgroupsHandler == null) {
      return "";
    }

    String cgroupMountPath = cgroupsHandler.getCGroupMountPath();
    boolean cGroupsMountExists = new File(
        cgroupMountPath).exists();

    if(cGroupsMountExists) {
      return "-v " + cgroupMountPath
          + ":" + cgroupMountPath + ":ro ";
    } else {
      return "";
    }
  }

  @Test
  public void testDockerContainerLaunch()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, null);
    runtime.launchContainer(builder.build());

    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    List<String> dockerCommands = Files.readAllLines(Paths.get
            (dockerCommandFile), Charset.forName("UTF-8"));

    int expected = 13;
    int counter = 0;
    Assert.assertEquals(expected, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-add=SYS_CHROOT,NET_BIND_SERVICE",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-drop=ALL", dockerCommands.get(counter++));
    Assert.assertEquals("  detach=true", dockerCommands.get(counter++));
    Assert.assertEquals("  docker-command=run", dockerCommands.get(counter++));
    Assert.assertEquals("  hostname=ctr-id", dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  name=container_id", dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  rw-mounts=/test_container_local_dir:/test_container_local_dir,"
            + "/test_filecache_dir:/test_filecache_dir,"
            + "/test_container_work_dir:/test_container_work_dir,"
            + "/test_container_log_dir:/test_container_log_dir,"
            + "/test_user_local_dir:/test_user_local_dir",
        dockerCommands.get(counter++));
    Assert.assertEquals("  user=run_as_user", dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter++));
  }

  @Test
  public void testContainerLaunchWithUserRemapping()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    conf.setBoolean(YarnConfiguration.NM_DOCKER_ENABLE_USER_REMAPPING,
        true);
    Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
        new String[]{"whoami"});
    shexec.execute();
    // get rid of newline at the end
    runAsUser = shexec.getOutput().replaceAll("\n$", "");
    builder.setExecutionAttribute(RUN_AS_USER, runAsUser);

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, null);
    runtime.launchContainer(builder.build());

    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    String uid = "";
    String gid = "";
    String[] groups = {};
    Shell.ShellCommandExecutor shexec1 = new Shell.ShellCommandExecutor(
        new String[]{"id", "-u", runAsUser});
    Shell.ShellCommandExecutor shexec2 = new Shell.ShellCommandExecutor(
        new String[]{"id", "-g", runAsUser});
    Shell.ShellCommandExecutor shexec3 = new Shell.ShellCommandExecutor(
        new String[]{"id", "-G", runAsUser});
    try {
      shexec1.execute();
      // get rid of newline at the end
      uid = shexec1.getOutput().replaceAll("\n$", "");
    } catch (Exception e) {
      LOG.info("Could not run id -u command: " + e);
    }
    try {
      shexec2.execute();
      // get rid of newline at the end
      gid = shexec2.getOutput().replaceAll("\n$", "");
    } catch (Exception e) {
      LOG.info("Could not run id -g command: " + e);
    }
    try {
      shexec3.execute();
      groups = shexec3.getOutput().replace("\n", " ").split(" ");
    } catch (Exception e) {
      LOG.info("Could not run id -G command: " + e);
    }
    uidGidPair = uid + ":" + gid;

    List<String> dockerCommands = Files.readAllLines(
        Paths.get(dockerCommandFile), Charset.forName("UTF-8"));

    Assert.assertEquals(14, dockerCommands.size());
    int counter = 0;
    Assert.assertEquals("[docker-command-execution]",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-add=SYS_CHROOT,NET_BIND_SERVICE",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-drop=ALL", dockerCommands.get(counter++));
    Assert.assertEquals("  detach=true", dockerCommands.get(counter++));
    Assert.assertEquals("  docker-command=run", dockerCommands.get(counter++));
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert.assertEquals("  hostname=ctr-id",
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  name=container_id", dockerCommands.get(counter++));
    Assert
        .assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  rw-mounts=/test_container_local_dir:/test_container_local_dir,"
            + "/test_filecache_dir:/test_filecache_dir,"
            + "/test_container_work_dir:/test_container_work_dir,"
            + "/test_container_log_dir:/test_container_log_dir,"
            + "/test_user_local_dir:/test_user_local_dir",
        dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter++));
  }

  @Test
  public void testAllowedNetworksConfiguration() throws
      ContainerExecutionException {
    //the default network configuration should cause
    // no exception should be thrown.

    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, null);

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
      runtime.initialize(conf, null);
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
    runtime.initialize(conf, null);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testContainerLaunchWithNetworkingDefaults()
      throws ContainerExecutionException, IOException,
      PrivilegedOperationException {
    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, null);

    Random randEngine = new Random();
    String disallowedNetwork = "sdn" + Integer.toString(randEngine.nextInt());

    try {
      env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_NETWORK,
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
    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_NETWORK,
        allowedNetwork);
    String expectedHostname = "test.hostname";
    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_HOSTNAME,
        expectedHostname);

    //this should cause no failures.

    runtime.launchContainer(builder.build());
    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    //This is the expected docker invocation for this case
    List<String> dockerCommands = Files
        .readAllLines(Paths.get(dockerCommandFile), Charset.forName("UTF-8"));
    int expected = 13;
    int counter = 0;
    Assert.assertEquals(expected, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-add=SYS_CHROOT,NET_BIND_SERVICE",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-drop=ALL", dockerCommands.get(counter++));
    Assert.assertEquals("  detach=true", dockerCommands.get(counter++));
    Assert.assertEquals("  docker-command=run", dockerCommands.get(counter++));
    Assert.assertEquals("  hostname=test.hostname",
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  name=container_id", dockerCommands.get(counter++));
    Assert
        .assertEquals("  net=" + allowedNetwork, dockerCommands.get(counter++));
    Assert.assertEquals(
        "  rw-mounts=/test_container_local_dir:/test_container_local_dir,"
            + "/test_filecache_dir:/test_filecache_dir,"
            + "/test_container_work_dir:/test_container_work_dir,"
            + "/test_container_log_dir:/test_container_log_dir,"
            + "/test_user_local_dir:/test_user_local_dir",
        dockerCommands.get(counter++));
    Assert.assertEquals("  user=run_as_user", dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter++));
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
    runtime.initialize(conf, null);
    runtime.launchContainer(builder.build());
    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    //This is the expected docker invocation for this case. customNetwork1
    // ("sdn1") is the expected network to be used in this case
    List<String> dockerCommands = Files
        .readAllLines(Paths.get(dockerCommandFile), Charset.forName("UTF-8"));

    int expected = 13;
    int counter = 0;
    Assert.assertEquals(expected, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-add=SYS_CHROOT,NET_BIND_SERVICE",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-drop=ALL", dockerCommands.get(counter++));
    Assert.assertEquals("  detach=true", dockerCommands.get(counter++));
    Assert.assertEquals("  docker-command=run", dockerCommands.get(counter++));
    Assert.assertEquals("  hostname=ctr-id", dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  name=container_id", dockerCommands.get(counter++));
    Assert.assertEquals("  net=sdn1", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  rw-mounts=/test_container_local_dir:/test_container_local_dir,"
            + "/test_filecache_dir:/test_filecache_dir,"
            + "/test_container_work_dir:/test_container_work_dir,"
            + "/test_container_log_dir:/test_container_log_dir,"
            + "/test_user_local_dir:/test_user_local_dir",
        dockerCommands.get(counter++));
    Assert.assertEquals("  user=run_as_user", dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter++));

    //now set an explicit (non-default) allowedNetwork and ensure that it is
    // used.

    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_NETWORK,
        customNetwork2);
    runtime.launchContainer(builder.build());

    op = capturePrivilegedOperationAndVerifyArgs();
    args = op.getArguments();
    dockerCommandFile = args.get(11);

    //This is the expected docker invocation for this case. customNetwork2
    // ("sdn2") is the expected network to be used in this case
    dockerCommands = Files
        .readAllLines(Paths.get(dockerCommandFile), Charset.forName("UTF-8"));
    counter = 0;
    Assert.assertEquals(expected, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-add=SYS_CHROOT,NET_BIND_SERVICE",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-drop=ALL", dockerCommands.get(counter++));
    Assert.assertEquals("  detach=true", dockerCommands.get(counter++));
    Assert.assertEquals("  docker-command=run", dockerCommands.get(counter++));
    Assert.assertEquals("  hostname=ctr-id", dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));

    Assert.assertEquals("  name=container_id", dockerCommands.get(counter++));
    Assert.assertEquals("  net=sdn2", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  rw-mounts=/test_container_local_dir:/test_container_local_dir,"
            + "/test_filecache_dir:/test_filecache_dir,"
            + "/test_container_work_dir:/test_container_work_dir,"
            + "/test_container_log_dir:/test_container_log_dir,"
            + "/test_user_local_dir:/test_user_local_dir",
        dockerCommands.get(counter++));
    Assert.assertEquals("  user=run_as_user", dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter++));


    //disallowed network should trigger a launch failure

    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_NETWORK,
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
    runtime.initialize(conf, null);

    env.put(DockerLinuxContainerRuntime
            .ENV_DOCKER_CONTAINER_RUN_PRIVILEGED_CONTAINER, "invalid-value");
    runtime.launchContainer(builder.build());

    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    List<String> dockerCommands = Files.readAllLines(Paths.get
        (dockerCommandFile), Charset.forName("UTF-8"));

    int expected = 13;
    Assert.assertEquals(expected, dockerCommands.size());

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
    runtime.initialize(conf, null);

    env.put(DockerLinuxContainerRuntime
            .ENV_DOCKER_CONTAINER_RUN_PRIVILEGED_CONTAINER, "true");

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
    runtime.initialize(conf, null);

    env.put(DockerLinuxContainerRuntime
            .ENV_DOCKER_CONTAINER_RUN_PRIVILEGED_CONTAINER, "true");
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
    runtime.initialize(conf, null);

    env.put(DockerLinuxContainerRuntime
            .ENV_DOCKER_CONTAINER_RUN_PRIVILEGED_CONTAINER, "true");

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
    runtime.initialize(conf, null);

    env.put(DockerLinuxContainerRuntime
            .ENV_DOCKER_CONTAINER_RUN_PRIVILEGED_CONTAINER, "true");

    runtime.launchContainer(builder.build());
    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    List<String> dockerCommands = Files.readAllLines(Paths.get
        (dockerCommandFile), Charset.forName("UTF-8"));

    int expected = 14;
    int counter = 0;
    Assert.assertEquals(expected, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-add=SYS_CHROOT,NET_BIND_SERVICE",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-drop=ALL", dockerCommands.get(counter++));
    Assert.assertEquals("  detach=true", dockerCommands.get(counter++));
    Assert.assertEquals("  docker-command=run", dockerCommands.get(counter++));
    Assert.assertEquals("  hostname=ctr-id", dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  name=container_id", dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  privileged=true", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  rw-mounts=/test_container_local_dir:/test_container_local_dir,"
            + "/test_filecache_dir:/test_filecache_dir,"
            + "/test_container_work_dir:/test_container_work_dir,"
            + "/test_container_log_dir:/test_container_log_dir,"
            + "/test_user_local_dir:/test_user_local_dir",
        dockerCommands.get(counter++));
    Assert.assertEquals("  user=run_as_user", dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter++));
  }

  @Test
  public void testCGroupParent() throws ContainerExecutionException {
    String hierarchy = "hadoop-yarn-test";
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY,
        hierarchy);

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime
        (mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, null);

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
    runtime.initialize(conf, null);

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
    runtime.initialize(conf, null);

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
    runtime.initialize(conf, null);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_LOCAL_RESOURCE_MOUNTS,
        "test_dir/test_resource_file:test_mount");

    runtime.launchContainer(builder.build());
    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    List<String> dockerCommands = Files.readAllLines(Paths.get
        (dockerCommandFile), Charset.forName("UTF-8"));

    Assert.assertEquals(14, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]", dockerCommands.get(0));
    Assert.assertEquals("  cap-add=SYS_CHROOT,NET_BIND_SERVICE",
        dockerCommands.get(1));
    Assert.assertEquals("  cap-drop=ALL", dockerCommands.get(2));
    Assert.assertEquals("  detach=true", dockerCommands.get(3));
    Assert.assertEquals("  docker-command=run", dockerCommands.get(4));
    Assert.assertEquals("  hostname=ctr-id", dockerCommands.get(5));
    Assert.assertEquals("  image=busybox:latest", dockerCommands.get(6));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(7));
    Assert.assertEquals("  name=container_id", dockerCommands.get(8));
    Assert.assertEquals("  net=host", dockerCommands.get(9));
    Assert.assertEquals(
        "  ro-mounts=/test_local_dir/test_resource_file:test_mount",
        dockerCommands.get(10));
    Assert.assertEquals(
        "  rw-mounts=/test_container_local_dir:/test_container_local_dir,"
            + "/test_filecache_dir:/test_filecache_dir,"
            + "/test_container_work_dir:/test_container_work_dir,"
            + "/test_container_log_dir:/test_container_log_dir,"
            + "/test_user_local_dir:/test_user_local_dir",
        dockerCommands.get(11));
    Assert.assertEquals("  user=run_as_user", dockerCommands.get(12));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(13));
  }

  @Test
  public void testMountInvalid()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, null);

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
    runtime.initialize(conf, null);

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

    Assert.assertEquals(14, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]", dockerCommands.get(0));
    Assert.assertEquals("  cap-add=SYS_CHROOT,NET_BIND_SERVICE",
        dockerCommands.get(1));
    Assert.assertEquals("  cap-drop=ALL", dockerCommands.get(2));
    Assert.assertEquals("  detach=true", dockerCommands.get(3));
    Assert.assertEquals("  docker-command=run", dockerCommands.get(4));
    Assert.assertEquals("  hostname=ctr-id", dockerCommands.get(5));
    Assert.assertEquals("  image=busybox:latest", dockerCommands.get(6));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(7));
    Assert.assertEquals("  name=container_id", dockerCommands.get(8));
    Assert.assertEquals("  net=host", dockerCommands.get(9));
    Assert.assertEquals(
        "  ro-mounts=/test_local_dir/test_resource_file:test_mount1,"
            + "/test_local_dir/test_resource_file:test_mount2",
        dockerCommands.get(10));
    Assert.assertEquals(
        "  rw-mounts=/test_container_local_dir:/test_container_local_dir,"
            + "/test_filecache_dir:/test_filecache_dir,"
            + "/test_container_work_dir:/test_container_work_dir,"
            + "/test_container_log_dir:/test_container_log_dir,"
            + "/test_user_local_dir:/test_user_local_dir",
        dockerCommands.get(11));
    Assert.assertEquals("  user=run_as_user", dockerCommands.get(12));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(13));

  }

  @Test
  public void testUserMounts()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, null);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_MOUNTS,
        "/tmp/foo:/tmp/foo:ro,/tmp/bar:/tmp/bar:rw");

    runtime.launchContainer(builder.build());
    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    List<String> dockerCommands = Files.readAllLines(
        Paths.get(dockerCommandFile), Charset.forName("UTF-8"));

    Assert.assertEquals(14, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]", dockerCommands.get(0));
    Assert.assertEquals("  cap-add=SYS_CHROOT,NET_BIND_SERVICE",
        dockerCommands.get(1));
    Assert.assertEquals("  cap-drop=ALL", dockerCommands.get(2));
    Assert.assertEquals("  detach=true", dockerCommands.get(3));
    Assert.assertEquals("  docker-command=run", dockerCommands.get(4));
    Assert.assertEquals("  hostname=ctr-id", dockerCommands.get(5));
    Assert.assertEquals("  image=busybox:latest", dockerCommands.get(6));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(7));
    Assert.assertEquals("  name=container_id", dockerCommands.get(8));
    Assert.assertEquals("  net=host", dockerCommands.get(9));
    Assert.assertEquals("  ro-mounts=/tmp/foo:/tmp/foo",
        dockerCommands.get(10));
    Assert.assertEquals(
        "  rw-mounts=/test_container_local_dir:/test_container_local_dir,"
            + "/test_filecache_dir:/test_filecache_dir,"
            + "/test_container_work_dir:/test_container_work_dir,"
            + "/test_container_log_dir:/test_container_log_dir,"
            + "/test_user_local_dir:/test_user_local_dir,"
            + "/tmp/bar:/tmp/bar",
        dockerCommands.get(11));
    Assert.assertEquals("  user=run_as_user", dockerCommands.get(12));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(13));
  }

  @Test
  public void testUserMountInvalid()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, null);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_MOUNTS,
        "source:target");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a launch container failure due to invalid mount.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testUserMountModeInvalid()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, null);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_MOUNTS,
        "source:target:other");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a launch container failure due to invalid mode.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testUserMountModeNulInvalid()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException{
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, null);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_MOUNTS,
        "s\0ource:target:ro");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a launch container failure due to NUL in mount.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testContainerLivelinessCheck()
      throws ContainerExecutionException, PrivilegedOperationException {

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    builder.setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user)
        .setExecutionAttribute(PID, signalPid)
        .setExecutionAttribute(SIGNAL, ContainerExecutor.Signal.NULL);
    runtime.initialize(enableMockContainerExecutor(conf), null);
    runtime.signalContainer(builder.build());

    PrivilegedOperation op = capturePrivilegedOperation();
    Assert.assertEquals(op.getOperationType(),
        PrivilegedOperation.OperationType.SIGNAL_CONTAINER);
    Assert.assertEquals("run_as_user", op.getArguments().get(0));
    Assert.assertEquals("user", op.getArguments().get(1));
    Assert.assertEquals("2", op.getArguments().get(2));
    Assert.assertEquals("1234", op.getArguments().get(3));
    Assert.assertEquals("0", op.getArguments().get(4));
  }

  @Test
  public void testDockerStopOnTermSignal()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    List<String> dockerCommands = getDockerCommandsForSignal(
        ContainerExecutor.Signal.TERM);
    Assert.assertEquals(3, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]", dockerCommands.get(0));
    Assert.assertEquals("  docker-command=stop", dockerCommands.get(1));
    Assert.assertEquals("  name=container_id", dockerCommands.get(2));
  }

  @Test
  public void testDockerStopOnKillSignal()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    List<String> dockerCommands = getDockerCommandsForSignal(
        ContainerExecutor.Signal.KILL);
    Assert.assertEquals(3, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]", dockerCommands.get(0));
    Assert.assertEquals("  docker-command=stop", dockerCommands.get(1));
    Assert.assertEquals("  name=container_id", dockerCommands.get(2));
  }

  @Test
  public void testDockerStopOnQuitSignal()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    List<String> dockerCommands = getDockerCommandsForSignal(
        ContainerExecutor.Signal.QUIT);
    Assert.assertEquals(3, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]", dockerCommands.get(0));
    Assert.assertEquals("  docker-command=stop", dockerCommands.get(1));
    Assert.assertEquals("  name=container_id", dockerCommands.get(2));
  }

  private List<String> getDockerCommandsForSignal(
      ContainerExecutor.Signal signal)
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    builder.setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user)
        .setExecutionAttribute(PID, signalPid)
        .setExecutionAttribute(SIGNAL, signal);
    runtime.initialize(enableMockContainerExecutor(conf), null);
    runtime.signalContainer(builder.build());

    PrivilegedOperation op = capturePrivilegedOperation();
    Assert.assertEquals(op.getOperationType(),
        PrivilegedOperation.OperationType.RUN_DOCKER_CMD);
    String dockerCommandFile = op.getArguments().get(0);
    return Files.readAllLines(Paths.get(dockerCommandFile),
        Charset.forName("UTF-8"));
  }

  /**
   * Return a configuration object with the mock container executor binary
   * preconfigured.
   *
   * @param conf The hadoop configuration.
   * @return The hadoop configuration.
   */
  public static Configuration enableMockContainerExecutor(Configuration conf) {
    File f = new File("./src/test/resources/mock-container-executor");
    if(!FileUtil.canExecute(f)) {
      FileUtil.setExecutable(f, true);
    }
    String executorPath = f.getAbsolutePath();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, executorPath);
    return conf;
  }

  @Test
  public void testDockerImageNamePattern() throws Exception {
    String[] validNames =
        { "ubuntu", "fedora/httpd:version1.0",
            "fedora/httpd:version1.0.test",
            "fedora/httpd:version1.0.TEST",
            "myregistryhost:5000/ubuntu",
            "myregistryhost:5000/fedora/httpd:version1.0",
            "myregistryhost:5000/fedora/httpd:version1.0.test",
            "myregistryhost:5000/fedora/httpd:version1.0.TEST"};

    String[] invalidNames = { "Ubuntu", "ubuntu || fedora", "ubuntu#",
        "myregistryhost:50AB0/ubuntu", "myregistry#host:50AB0/ubuntu",
        ":8080/ubuntu"
    };

    for (String name : validNames) {
      DockerLinuxContainerRuntime.validateImageName(name);
    }

    for (String name : invalidNames) {
      try {
        DockerLinuxContainerRuntime.validateImageName(name);
        Assert.fail(name + " is an invalid name and should fail the regex");
      } catch (ContainerExecutionException ce) {
        continue;
      }
    }
  }

  @Test
  public void testDockerHostnamePattern() throws Exception {
    String[] validNames = {"ab", "a.b.c.d", "a1-b.cd.ef", "0AB.", "C_D-"};

    String[] invalidNames = {"a", "a#.b.c", "-a.b.c", "a@b.c", "a/b/c"};

    for (String name : validNames) {
      DockerLinuxContainerRuntime.validateHostname(name);
    }

    for (String name : invalidNames) {
      try {
        DockerLinuxContainerRuntime.validateHostname(name);
        Assert.fail(name + " is an invalid hostname and should fail the regex");
      } catch (ContainerExecutionException ce) {
        continue;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void checkVolumeCreateCommand()
      throws PrivilegedOperationException, IOException {
    ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass(
        PrivilegedOperation.class);

    //single invocation expected
    //due to type erasure + mocking, this verification requires a suppress
    // warning annotation on the entire method
    verify(mockExecutor, times(1))
        .executePrivilegedOperation(anyList(), opCaptor.capture(), any(
            File.class), anyMap(), anyBoolean(), anyBoolean());

    //verification completed. we need to isolate specific invications.
    // hence, reset mock here
    Mockito.reset(mockExecutor);

    PrivilegedOperation op = opCaptor.getValue();
    Assert.assertEquals(PrivilegedOperation.OperationType
        .RUN_DOCKER_CMD, op.getOperationType());

    File commandFile = new File(StringUtils.join(",", op.getArguments()));
    FileInputStream fileInputStream = new FileInputStream(commandFile);
    String fileContent = new String(IOUtils.toByteArray(fileInputStream));
    Assert.assertEquals("[docker-command-execution]\n"
        + "  docker-command=volume\n" + "  sub-command=create\n"
        + "  volume=volume1\n", fileContent);
  }

  @Test
  public void testDockerCommandPlugin() throws Exception {
    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);

    Context nmContext = mock(Context.class);
    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    Map<String, ResourcePlugin> pluginsMap = new HashMap<>();
    ResourcePlugin plugin1 = mock(ResourcePlugin.class);

    // Create the docker command plugin logic, which will set volume driver
    DockerCommandPlugin dockerCommandPlugin = new DockerCommandPlugin() {
      @Override
      public void updateDockerRunCommand(DockerRunCommand dockerRunCommand,
          Container container) throws ContainerExecutionException {
        dockerRunCommand.setVolumeDriver("driver-1");
        dockerRunCommand.addReadOnlyMountLocation("/source/path",
            "/destination/path", true);
      }

      @Override
      public DockerVolumeCommand getCreateDockerVolumeCommand(Container container)
          throws ContainerExecutionException {
        return new DockerVolumeCommand("create").setVolumeName("volume1");
      }

      @Override
      public DockerVolumeCommand getCleanupDockerVolumesCommand(Container container)
          throws ContainerExecutionException {
        return null;
      }
    };

    when(plugin1.getDockerCommandPluginInstance()).thenReturn(
        dockerCommandPlugin);
    ResourcePlugin plugin2 = mock(ResourcePlugin.class);
    pluginsMap.put("plugin1", plugin1);
    pluginsMap.put("plugin2", plugin2);

    when(rpm.getNameToPlugins()).thenReturn(pluginsMap);

    when(nmContext.getResourcePluginManager()).thenReturn(rpm);

    runtime.initialize(conf, nmContext);

    ContainerRuntimeContext containerRuntimeContext = builder.build();

    runtime.prepareContainer(containerRuntimeContext);
    checkVolumeCreateCommand();

    runtime.launchContainer(containerRuntimeContext);
    PrivilegedOperation op = capturePrivilegedOperationAndVerifyArgs();
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get(11);

    List<String> dockerCommands = Files.readAllLines(Paths.get
        (dockerCommandFile), Charset.forName("UTF-8"));

    int expected = 15;
    int counter = 0;
    Assert.assertEquals(expected, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-add=SYS_CHROOT,NET_BIND_SERVICE",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-drop=ALL", dockerCommands.get(counter++));
    Assert.assertEquals("  detach=true", dockerCommands.get(counter++));
    Assert.assertEquals("  docker-command=run", dockerCommands.get(counter++));
    Assert.assertEquals("  hostname=ctr-id", dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  name=container_id", dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  ro-mounts=/source/path:/destination/path",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  rw-mounts=/test_container_local_dir:/test_container_local_dir,"
            + "/test_filecache_dir:/test_filecache_dir,"
            + "/test_container_work_dir:/test_container_work_dir,"
            + "/test_container_log_dir:/test_container_log_dir,"
            + "/test_user_local_dir:/test_user_local_dir",
        dockerCommands.get(counter++));
    Assert.assertEquals("  user=run_as_user", dockerCommands.get(counter++));

    // Verify volume-driver is set to expected value.
    Assert.assertEquals("  volume-driver=driver-1",
        dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter++));
  }

  @Test
  public void testDockerCapabilities()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    try {
      conf.setStrings(YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
          "none", "CHOWN", "DAC_OVERRIDE");
      runtime.initialize(conf, null);
      Assert.fail("Initialize didn't fail with invalid capabilities " +
          "'none', 'CHOWN', 'DAC_OVERRIDE'");
    } catch (ContainerExecutionException e) {
    }

    try {
      conf.setStrings(YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
          "CHOWN", "DAC_OVERRIDE", "NONE");
      runtime.initialize(conf, null);
      Assert.fail("Initialize didn't fail with invalid capabilities " +
          "'CHOWN', 'DAC_OVERRIDE', 'NONE'");
    } catch (ContainerExecutionException e) {
    }

    conf.setStrings(YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
        "NONE");
    runtime.initialize(conf, null);
    Assert.assertEquals(0, runtime.getCapabilities().size());

    conf.setStrings(YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
        "none");
    runtime.initialize(conf, null);
    Assert.assertEquals(0, runtime.getCapabilities().size());

    conf.setStrings(YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
        "CHOWN", "DAC_OVERRIDE");
    runtime.initialize(conf, null);
    Iterator<String> it = runtime.getCapabilities().iterator();
    Assert.assertEquals("CHOWN", it.next());
    Assert.assertEquals("DAC_OVERRIDE", it.next());
  }
}
