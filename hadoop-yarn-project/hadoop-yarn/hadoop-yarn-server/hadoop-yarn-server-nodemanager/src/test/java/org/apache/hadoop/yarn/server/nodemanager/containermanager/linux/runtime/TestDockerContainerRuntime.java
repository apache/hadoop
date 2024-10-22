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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.util.DockerClientConfigHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.TestDockerClientConfigHandler;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerCommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalizedResource;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_DOCKER_DEFAULT_RO_MOUNTS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_DOCKER_DEFAULT_RW_MOUNTS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_DOCKER_DEFAULT_TMPFS_MOUNTS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.APPID;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.APPLICATION_LOCAL_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_ID_STR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_LOG_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_WORK_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.FILECACHE_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOCALIZED_RESOURCES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOCAL_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOG_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.NM_PRIVATE_CONTAINER_SCRIPT_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.NM_PRIVATE_KEYSTORE_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.NM_PRIVATE_TOKENS_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.NM_PRIVATE_TRUSTSTORE_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.PID;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.PID_FILE_PATH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.PROCFS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.RESOURCES_OPTIONS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.RUN_AS_USER;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.SIGNAL;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.USER;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.USER_FILECACHE_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.OCIContainerRuntime.CONTAINER_PID_NAMESPACE_SUFFIX;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.OCIContainerRuntime.RUN_PRIVILEGED_CONTAINER_SUFFIX;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.OCIContainerRuntime.formatOciEnvKey;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class TestDockerContainerRuntime {
  private static final Logger LOG =
       LoggerFactory.getLogger(TestDockerContainerRuntime.class);
  private Configuration conf;
  private PrivilegedOperationExecutor mockExecutor;
  private CGroupsHandler mockCGroupsHandler;
  private String containerId;
  private Container container;
  private ContainerId cId;
  private ApplicationAttemptId appAttemptId;
  private ApplicationId mockApplicationId;
  private ContainerLaunchContext context;
  private Context nmContext;
  private HashMap<String, String> env;
  private String image;
  private String uidGidPair;
  private String runAsUser = System.getProperty("user.name");
  private String[] groups = {};
  private String user;
  private String appId;
  private String containerIdStr = containerId;
  private Path containerWorkDir;
  private Path nmPrivateContainerScriptPath;
  private Path nmPrivateTokensPath;
  private Path nmPrivateKeystorePath;
  private Path nmPrivateTruststorePath;
  private Path pidFilePath;
  private List<String> localDirs;
  private List<String> logDirs;
  private List<String> filecacheDirs;
  private List<String> userFilecacheDirs;
  private List<String> applicationLocalDirs;
  private List<String> containerLogDirs;
  private Map<Path, List<String>> localizedResources;
  private String resourcesOptions;
  private ContainerRuntimeContext.Builder builder;
  private final String submittingUser = "anakin";
  private final String whitelistedUser = "yoda";
  private String[] testCapabilities;
  private final String signalPid = "1234";
  private final String tmpPath =
      new StringBuilder(System.getProperty("test.build.data"))
      .append('/').append("hadoop.tmp.dir").toString();

  private static final String RUNTIME_TYPE = "DOCKER";
  private final static String ENV_OCI_CONTAINER_PID_NAMESPACE =
      formatOciEnvKey(RUNTIME_TYPE, CONTAINER_PID_NAMESPACE_SUFFIX);
  private final static String ENV_OCI_CONTAINER_RUN_PRIVILEGED_CONTAINER =
      formatOciEnvKey(RUNTIME_TYPE, RUN_PRIVILEGED_CONTAINER_SUFFIX);

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Parameterized.Parameters(name = "https={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {true}, {false}
    });
  }

  @Parameterized.Parameter
  public boolean https;

  @Before
  public void setup() {

    conf = new Configuration();
    conf.set("hadoop.tmp.dir", tmpPath);

    mockExecutor = Mockito
        .mock(PrivilegedOperationExecutor.class);
    mockCGroupsHandler = Mockito.mock(CGroupsHandler.class);
    containerId = "container_e11_1518975676334_14532816_01_000001";
    container = mock(Container.class);
    cId = mock(ContainerId.class);
    appAttemptId = mock(ApplicationAttemptId.class);
    mockApplicationId = mock(ApplicationId.class);
    context = mock(ContainerLaunchContext.class);
    env = new HashMap<String, String>();
    env.put("FROM_CLIENT", "1");
    image = "busybox:latest";
    nmContext = createMockNMContext();

    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_IMAGE, image);
    when(container.getContainerId()).thenReturn(cId);
    when(cId.toString()).thenReturn(containerId);
    when(mockApplicationId.toString()).thenReturn("applicationId");
    when(appAttemptId.getApplicationId()).thenReturn(mockApplicationId);
    when(cId.getApplicationAttemptId()).thenReturn(appAttemptId);
    when(container.getLaunchContext()).thenReturn(context);
    when(context.getEnvironment()).thenReturn(env);
    when(container.getUser()).thenReturn(submittingUser);

    // Get the running user's uid and gid for remap
    String uid = "";
    String gid = "";
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
    // Prevent gid threshold failures for these tests
    conf.setInt(YarnConfiguration.NM_DOCKER_USER_REMAPPING_GID_THRESHOLD, 0);

    user = submittingUser;
    appId = "app_id";
    containerIdStr = containerId;
    containerWorkDir = new Path("/test_container_work_dir");
    nmPrivateContainerScriptPath = new Path("/test_script_path");
    nmPrivateTokensPath = new Path("/test_private_tokens_path");
    if (https) {
      nmPrivateKeystorePath = new Path("/test_private_keystore_path");
      nmPrivateTruststorePath = new Path("/test_private_truststore_path");
    } else {
      nmPrivateKeystorePath = null;
      nmPrivateTruststorePath = null;
    }
    pidFilePath = new Path("/test_pid_file_path");
    localDirs = new ArrayList<>();
    logDirs = new ArrayList<>();
    filecacheDirs = new ArrayList<>();
    resourcesOptions = "cgroups=none";
    userFilecacheDirs = new ArrayList<>();
    applicationLocalDirs = new ArrayList<>();
    containerLogDirs = new ArrayList<>();
    localizedResources = new HashMap<>();

    localDirs.add("/test_local_dir");
    logDirs.add("/test_log_dir");
    filecacheDirs.add("/test_filecache_dir");
    userFilecacheDirs.add("/test_user_filecache_dir");
    applicationLocalDirs.add("/test_application_local_dir");
    containerLogDirs.add("/test_container_log_dir");
    localizedResources.put(new Path("/test_local_dir/test_resource_file"),
        Collections.singletonList("test_dir/test_resource_file"));

    File tmpDir = new File(tmpPath);
    tmpDir.mkdirs();

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
        .setExecutionAttribute(NM_PRIVATE_KEYSTORE_PATH, nmPrivateKeystorePath)
        .setExecutionAttribute(NM_PRIVATE_TRUSTSTORE_PATH,
            nmPrivateTruststorePath)
        .setExecutionAttribute(PID_FILE_PATH, pidFilePath)
        .setExecutionAttribute(LOCAL_DIRS, localDirs)
        .setExecutionAttribute(LOG_DIRS, logDirs)
        .setExecutionAttribute(FILECACHE_DIRS, filecacheDirs)
        .setExecutionAttribute(USER_FILECACHE_DIRS, userFilecacheDirs)
        .setExecutionAttribute(APPLICATION_LOCAL_DIRS, applicationLocalDirs)
        .setExecutionAttribute(CONTAINER_LOG_DIRS, containerLogDirs)
        .setExecutionAttribute(LOCALIZED_RESOURCES, localizedResources)
        .setExecutionAttribute(RESOURCES_OPTIONS, resourcesOptions);
  }

  @After
  public void cleanUp() throws IOException {
    File tmpDir = new File(tmpPath);
    FileUtils.deleteDirectory(tmpDir);
  }

  public Context createMockNMContext() {
    Context mockNMContext = mock(Context.class);
    LocalDirsHandlerService localDirsHandler =
        mock(LocalDirsHandlerService.class);
    ResourcePluginManager resourcePluginManager =
        mock(ResourcePluginManager.class);

    ConcurrentMap<ContainerId, Container> containerMap =
        mock(ConcurrentMap.class);

    when(mockNMContext.getLocalDirsHandler()).thenReturn(localDirsHandler);
    when(mockNMContext.getResourcePluginManager())
        .thenReturn(resourcePluginManager);
    when(mockNMContext.getContainers()).thenReturn(containerMap);
    when(containerMap.get(any())).thenReturn(container);

    ContainerManager mockContainerManager = mock(ContainerManager.class);
    ResourceLocalizationService mockLocalzationService =
        mock(ResourceLocalizationService.class);

    LocalizedResource mockLocalizedResource = mock(LocalizedResource.class);

    when(mockLocalizedResource.getLocalPath()).thenReturn(
        new Path("/local/layer1"));
    when(mockLocalzationService.getLocalizedResource(any(), anyString(), any()))
        .thenReturn(mockLocalizedResource);
    when(mockContainerManager.getResourceLocalizationService())
        .thenReturn(mockLocalzationService);
    when(mockNMContext.getContainerManager()).thenReturn(mockContainerManager);

    try {
      when(localDirsHandler.getLocalPathForWrite(anyString()))
          .thenReturn(new Path(tmpPath));
    } catch (IOException ioe) {
      LOG.info("LocalDirsHandler failed" + ioe);
    }
    return mockNMContext;
  }

  @Test
  public void testSelectDockerContainerType() {
    Map<String, String> envDockerType = new HashMap<>();
    Map<String, String> envOtherType = new HashMap<>();

    envDockerType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE,
        ContainerRuntimeConstants.CONTAINER_RUNTIME_DOCKER);
    envOtherType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "other");

    Assert.assertEquals(false, DockerLinuxContainerRuntime
        .isDockerContainerRequested(conf, null));
    Assert.assertEquals(true, DockerLinuxContainerRuntime
        .isDockerContainerRequested(conf, envDockerType));
    Assert.assertEquals(false, DockerLinuxContainerRuntime
        .isDockerContainerRequested(conf, envOtherType));
  }

  @Test
  public void testSelectDockerContainerTypeWithDockerAsDefault() {
    Map<String, String> envDockerType = new HashMap<>();
    Map<String, String> envOtherType = new HashMap<>();

    conf.set(YarnConfiguration.LINUX_CONTAINER_RUNTIME_TYPE,
        ContainerRuntimeConstants.CONTAINER_RUNTIME_DOCKER);
    envDockerType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE,
        ContainerRuntimeConstants.CONTAINER_RUNTIME_DOCKER);
    envOtherType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "other");

    Assert.assertEquals(true, DockerLinuxContainerRuntime
        .isDockerContainerRequested(conf, null));
    Assert.assertEquals(true, DockerLinuxContainerRuntime
        .isDockerContainerRequested(conf, envDockerType));
    Assert.assertEquals(false, DockerLinuxContainerRuntime
        .isDockerContainerRequested(conf, envOtherType));
  }

  @Test
  public void testSelectDockerContainerTypeWithDefaultSet() {
    Map<String, String> envDockerType = new HashMap<>();
    Map<String, String> envOtherType = new HashMap<>();

    conf.set(YarnConfiguration.LINUX_CONTAINER_RUNTIME_TYPE, "default");
    envDockerType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE,
        ContainerRuntimeConstants.CONTAINER_RUNTIME_DOCKER);
    envOtherType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "other");

    Assert.assertEquals(false, DockerLinuxContainerRuntime
        .isDockerContainerRequested(conf, null));
    Assert.assertEquals(true, DockerLinuxContainerRuntime
        .isDockerContainerRequested(conf, envDockerType));
    Assert.assertEquals(false, DockerLinuxContainerRuntime
        .isDockerContainerRequested(conf, envOtherType));
  }

  private PrivilegedOperation capturePrivilegedOperation()
      throws PrivilegedOperationException {
    return capturePrivilegedOperation(1);
  }

  private PrivilegedOperation capturePrivilegedOperation(int invocations)
      throws PrivilegedOperationException {
    ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass(
        PrivilegedOperation.class);

    verify(mockExecutor, times(invocations))
        .executePrivilegedOperation(any(), opCaptor.capture(), any(),
            any(), anyBoolean(), anyBoolean());

    //verification completed. we need to isolate specific invocations.
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

    //This invocation of container-executor should use 15 or 13 arguments in a
    // specific order
    int expected = (https) ? 15 : 13;
    int counter = 1;
    Assert.assertEquals(expected, args.size());
    Assert.assertEquals(user, args.get(counter++));
    Assert.assertEquals(Integer.toString(PrivilegedOperation.RunAsUserCommand
        .LAUNCH_DOCKER_CONTAINER.getValue()), args.get(counter++));
    Assert.assertEquals(appId, args.get(counter++));
    Assert.assertEquals(containerId, args.get(counter++));
    Assert.assertEquals(containerWorkDir.toString(), args.get(counter++));
    Assert.assertEquals(nmPrivateContainerScriptPath.toUri().toString(),
        args.get(counter++));
    Assert.assertEquals(nmPrivateTokensPath.toUri().getPath(),
        args.get(counter++));
    if (https) {
      Assert.assertEquals("--https", args.get(counter++));
      Assert.assertEquals(nmPrivateKeystorePath.toUri().toString(),
          args.get(counter++));
      Assert.assertEquals(nmPrivateTruststorePath.toUri().toString(),
          args.get(counter++));
    } else {
      Assert.assertEquals("--http", args.get(counter++));
    }
    Assert.assertEquals(pidFilePath.toString(), args.get(counter++));
    Assert.assertEquals(localDirs.get(0), args.get(counter++));
    Assert.assertEquals(logDirs.get(0), args.get(counter++));

    return op;
  }

  @Test
  public void testDockerContainerLaunch()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  public void testDockerContainerLaunchWithDefaultImage()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    conf.set(YarnConfiguration.NM_DOCKER_IMAGE_NAME, "busybox:1.2.3");
    env.remove(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_IMAGE);

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());

    List<String> dockerCommands = readDockerCommands();

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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:1.2.3", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  public void testDockerContainerLaunchWithoutDefaultImageUpdate()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    conf.setBoolean(YarnConfiguration.NM_DOCKER_IMAGE_UPDATE, false);

    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();
    Assert.assertEquals(false,
        conf.getBoolean(YarnConfiguration.NM_DOCKER_IMAGE_UPDATE, false));

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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  public void testDockerContainerLaunchWithDefaultImageUpdate()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    conf.setBoolean(YarnConfiguration.NM_DOCKER_IMAGE_UPDATE, true);

    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());

    ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass(
        PrivilegedOperation.class);

    //Two invocations expected.
    verify(mockExecutor, times(2))
        .executePrivilegedOperation(any(), opCaptor.capture(), any(),
            any(), anyBoolean(), anyBoolean());

    List<PrivilegedOperation> allCaptures = opCaptor.getAllValues();

    // pull image from remote hub firstly
    PrivilegedOperation op = allCaptures.get(0);
    Assert.assertEquals(PrivilegedOperation.OperationType
        .RUN_DOCKER_CMD, op.getOperationType());

    File commandFile = new File(StringUtils.join(",", op.getArguments()));
    FileInputStream fileInputStream = new FileInputStream(commandFile);
    String fileContent = new String(IOUtils.toByteArray(fileInputStream));
    Assert.assertEquals("[docker-command-execution]\n"
        + "  docker-command=pull\n"
        + "  image=busybox:latest\n", fileContent);
    fileInputStream.close();

    // launch docker container
    List<String> dockerCommands = readDockerCommands(2);

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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  public void testContainerLaunchWithUserRemapping()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    conf.setBoolean(YarnConfiguration.NM_DOCKER_ENABLE_USER_REMAPPING,
        true);
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

    Assert.assertEquals(13, dockerCommands.size());
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
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  public void testAllowedNetworksConfiguration() throws
      ContainerExecutionException {
    //the default network configuration should cause
    // no exception should be thrown.

    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

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
      runtime.initialize(conf, nmContext);
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
    runtime.initialize(conf, nmContext);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testContainerLaunchWithNetworkingDefaults()
      throws ContainerExecutionException, IOException,
      PrivilegedOperationException {
    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

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

    String allowedNetwork = "bridge";
    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_NETWORK,
        allowedNetwork);
    String expectedHostname = "test.hostname";
    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_HOSTNAME,
        expectedHostname);

    //this should cause no failures.

    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

    //This is the expected docker invocation for this case
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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert.assertEquals("  hostname=test.hostname",
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  net=" + allowedNetwork, dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testContainerLaunchWithHostDnsNetwork()
      throws ContainerExecutionException, IOException,
      PrivilegedOperationException {
    // Make it look like Registry DNS is enabled so we can test whether
    // hostname goes through
    conf.setBoolean(RegistryConstants.KEY_DNS_ENABLED, true);
    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    String expectedHostname = "test.hostname";
    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_HOSTNAME,
        expectedHostname);

    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

    //This is the expected docker invocation for this case
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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert.assertEquals("  hostname=test.hostname",
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
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
    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

    //This is the expected docker invocation for this case. customNetwork1
    // ("sdn1") is the expected network to be used in this case
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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  hostname=ctr-e11-1518975676334-14532816-01-000001",
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=sdn1", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));

    //now set an explicit (non-default) allowedNetwork and ensure that it is
    // used.

    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_NETWORK,
        customNetwork2);
    runtime.launchContainer(builder.build());
    dockerCommands = readDockerCommands();

    //This is the expected docker invocation for this case. customNetwork2
    // ("sdn2") is the expected network to be used in this case
    counter = 0;
    Assert.assertEquals(expected, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-add=SYS_CHROOT,NET_BIND_SERVICE",
        dockerCommands.get(counter++));
    Assert.assertEquals("  cap-drop=ALL", dockerCommands.get(counter++));
    Assert.assertEquals("  detach=true", dockerCommands.get(counter++));
    Assert.assertEquals("  docker-command=run", dockerCommands.get(counter++));
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  hostname=ctr-e11-1518975676334-14532816-01-000001",
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=sdn2", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));


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
  public void testLaunchPidNamespaceContainersInvalidEnvVar()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(ENV_OCI_CONTAINER_PID_NAMESPACE, "invalid-value");
    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

    int expected = 13;
    Assert.assertEquals(expected, dockerCommands.size());

    String command = dockerCommands.get(0);

    //ensure --pid isn't in the invocation
    Assert.assertTrue("Unexpected --pid in docker run args : " + command,
        !command.contains("--pid"));
  }

  @Test
  public void testLaunchPidNamespaceContainersWithDisabledSetting()
      throws ContainerExecutionException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(ENV_OCI_CONTAINER_PID_NAMESPACE, "host");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a pid host disabled container failure.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testLaunchPidNamespaceContainersEnabled()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    //Enable host pid namespace containers.
    conf.setBoolean(YarnConfiguration.NM_DOCKER_ALLOW_HOST_PID_NAMESPACE,
        true);

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(ENV_OCI_CONTAINER_PID_NAMESPACE, "host");

    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  pid=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  public void testLaunchPrivilegedContainersInvalidEnvVar()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(ENV_OCI_CONTAINER_RUN_PRIVILEGED_CONTAINER, "invalid-value");
    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

    int expected = 13;
    Assert.assertEquals(expected, dockerCommands.size());

    String command = dockerCommands.get(0);

    //ensure --privileged isn't in the invocation
    Assert.assertTrue("Unexpected --privileged in docker run args : " + command,
        !command.contains("--privileged"));
  }

  @Test
  public void testLaunchPrivilegedContainersWithDisabledSetting()
      throws ContainerExecutionException {
    conf.setBoolean(YarnConfiguration.NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS,
        false);
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(ENV_OCI_CONTAINER_RUN_PRIVILEGED_CONTAINER, "true");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a privileged launch container failure.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testLaunchPrivilegedContainersWithEnabledSettingAndDefaultACL()
      throws ContainerExecutionException {
    //Enable privileged containers.
    conf.setBoolean(YarnConfiguration.NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS,
        true);
    conf.set(YarnConfiguration.NM_DOCKER_PRIVILEGED_CONTAINERS_ACL, "");

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(ENV_OCI_CONTAINER_RUN_PRIVILEGED_CONTAINER, "true");
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
      throws ContainerExecutionException {
    //Enable privileged containers.
    conf.setBoolean(YarnConfiguration.NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS,
        true);
    //set whitelist of users.
    conf.set(YarnConfiguration.NM_DOCKER_PRIVILEGED_CONTAINERS_ACL,
        whitelistedUser);

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(ENV_OCI_CONTAINER_RUN_PRIVILEGED_CONTAINER, "true");

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
      IOException {
    //Enable privileged containers.
    conf.setBoolean(YarnConfiguration.NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS,
        true);
    //Add submittingUser to whitelist.
    conf.set(YarnConfiguration.NM_DOCKER_PRIVILEGED_CONTAINERS_ACL,
        submittingUser);

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(ENV_OCI_CONTAINER_RUN_PRIVILEGED_CONTAINER, "true");

    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

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
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  privileged=true", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + submittingUser,
        dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  public void testCGroupParent() throws ContainerExecutionException {
    String hierarchy = "hadoop-yarn-test";
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY,
        hierarchy);

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime
        (mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

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
    runtime.initialize(conf, nmContext);

    runtime.addCGroupParentIfRequired(resourceOptionsNone, containerIdStr,
        command);
    runtime.addCGroupParentIfRequired(resourceOptionsCpu, containerIdStr,
        command);

    //no --cgroup-parent should be added in either case
    Mockito.verifyZeroInteractions(command);
  }

  @Test
  public void testMountSourceOnly() throws ContainerExecutionException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_MOUNTS,
        "/source");

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
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_MOUNTS,
        "test_dir/test_resource_file:test_mount:ro");

    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert.assertEquals("  image=busybox:latest",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro,"
            + "/test_local_dir/test_resource_file:test_mount:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  public void testMountMultiple()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_MOUNTS,
        "test_dir/test_resource_file:test_mount1:ro;" +
            "test_dir/test_resource_file:test_mount2:ro");

    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert.assertEquals("  image=busybox:latest",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro,"
            + "/test_local_dir/test_resource_file:test_mount1:ro,"
            + "/test_local_dir/test_resource_file:test_mount2:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  public void testUserMounts()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_MOUNTS,
        "/tmp/foo:/tmp/foo:ro;/tmp/bar:/tmp/bar:rw;/tmp/baz:/tmp/baz;" +
            "/a:/a:shared;/b:/b:ro+shared;/c:/c:rw+rshared;/d:/d:private");

    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert.assertEquals("  image=busybox:latest",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro,"
            + "/tmp/foo:/tmp/foo:ro,"
            + "/tmp/bar:/tmp/bar:rw,/tmp/baz:/tmp/baz:rw,/a:/a:rw+shared,"
            + "/b:/b:ro+shared,/c:/c:rw+rshared,/d:/d:rw+private",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  public void testUserMountInvalid() throws ContainerExecutionException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_MOUNTS,
        "/source:target:ro;/source:target:other;/source:target:rw");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a launch container failure due to invalid mount.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testUserMountModeInvalid() throws ContainerExecutionException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_MOUNTS,
        "/source:target:other");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a launch container failure due to invalid mode.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testUserMountModeNulInvalid() throws ContainerExecutionException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_MOUNTS,
        "/s\0ource:target:ro");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a launch container failure due to NUL in mount.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testTmpfsMount()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_TMPFS_MOUNTS,
        "/run");

    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

    Assert.assertTrue(dockerCommands.contains("  tmpfs=/run"));
  }

  @Test
  public void testTmpfsMountMulti()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_TMPFS_MOUNTS,
        "/run,/tmp");

    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

    Assert.assertTrue(dockerCommands.contains("  tmpfs=/run,/tmp"));
  }

  @Test
  public void testDefaultTmpfsMounts()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    conf.setStrings(NM_DOCKER_DEFAULT_TMPFS_MOUNTS, "/run,/var/run");
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_TMPFS_MOUNTS,
        "/tmpfs");

    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

    Assert.assertTrue(dockerCommands.contains("  tmpfs=/tmpfs,/run,/var/run"));
  }

  @Test
  public void testDefaultTmpfsMountsInvalid()
      throws ContainerExecutionException {
    conf.setStrings(NM_DOCKER_DEFAULT_TMPFS_MOUNTS, "run,var/run");
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_TMPFS_MOUNTS,
        "/tmpfs");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail(
          "Expected a launch container failure due to non-absolute path.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testTmpfsRelativeInvalid() throws ContainerExecutionException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_TMPFS_MOUNTS,
        "run");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail(
          "Expected a launch container failure due to non-absolute path.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testTmpfsColonInvalid() throws ContainerExecutionException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_TMPFS_MOUNTS,
        "/run:");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail(
          "Expected a launch container failure due to invalid character.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testTmpfsNulInvalid() throws ContainerExecutionException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
            mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(
        DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_TMPFS_MOUNTS,
        "/ru\0n");

    try {
      runtime.launchContainer(builder.build());
      Assert.fail(
          "Expected a launch container failure due to NUL in tmpfs mount.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testDefaultROMounts()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    conf.setStrings(NM_DOCKER_DEFAULT_RO_MOUNTS,
        "/tmp/foo:/tmp/foo,/tmp/bar:/tmp/bar");
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert.assertEquals("  image=busybox:latest",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro,"
            + "/tmp/foo:/tmp/foo:ro,/tmp/bar:/tmp/bar:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  public void testDefaultROMountsInvalid() throws ContainerExecutionException {
    conf.setStrings(NM_DOCKER_DEFAULT_RO_MOUNTS,
        "source,target");
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a launch container failure due to invalid mount.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testDefaultRWMounts()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    conf.setStrings(NM_DOCKER_DEFAULT_RW_MOUNTS,
        "/tmp/foo:/tmp/foo,/tmp/bar:/tmp/bar");
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert.assertEquals("  image=busybox:latest",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro,"
            + "/tmp/foo:/tmp/foo:rw,/tmp/bar:/tmp/bar:rw",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  public void testDefaultRWMountsInvalid() throws ContainerExecutionException {
    conf.setStrings(NM_DOCKER_DEFAULT_RW_MOUNTS,
        "source,target");
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    try {
      runtime.launchContainer(builder.build());
      Assert.fail("Expected a launch container failure due to invalid mount.");
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception : " + e);
    }
  }

  @Test
  public void testContainerLivelinessFileExistsNoException() throws Exception {
    File testTempDir = tempDir.newFolder();
    File procPidPath = new File(testTempDir + File.separator + signalPid);
    procPidPath.createNewFile();
    procPidPath.deleteOnExit();
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    builder.setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user)
        .setExecutionAttribute(PID, signalPid)
        .setExecutionAttribute(SIGNAL, ContainerExecutor.Signal.NULL)
        .setExecutionAttribute(PROCFS, testTempDir.getAbsolutePath());
    runtime.initialize(enableMockContainerExecutor(conf), null);
    runtime.signalContainer(builder.build());
  }

  @Test
  public void testContainerLivelinessNoFileException() throws Exception {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    builder.setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user)
        .setExecutionAttribute(PID, signalPid)
        .setExecutionAttribute(SIGNAL, ContainerExecutor.Signal.NULL);
    runtime.initialize(enableMockContainerExecutor(conf), null);
    try {
      runtime.signalContainer(builder.build());
    } catch (ContainerExecutionException e) {
      Assert.assertEquals(
          PrivilegedOperation.ResultCode.INVALID_CONTAINER_PID.getValue(),
          e.getExitCode());
    }
  }

  @Test
  public void testDockerStopOnTermSignalWhenRunning()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    when(mockExecutor
        .executePrivilegedOperation(any(), any(PrivilegedOperation.class),
        any(), any(), anyBoolean(), anyBoolean())).thenReturn(
        DockerCommandExecutor.DockerContainerStatus.RUNNING.getName());
    List<String> dockerCommands = getDockerCommandsForDockerStop(
        ContainerExecutor.Signal.TERM);
    verifyStopCommand(dockerCommands, ContainerExecutor.Signal.TERM.toString());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDockerStopWithQuitSignalWhenRunning()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    when(mockExecutor
        .executePrivilegedOperation(any(), any(PrivilegedOperation.class),
            any(), any(), anyBoolean(), anyBoolean())).thenReturn(
        DockerCommandExecutor.DockerContainerStatus.RUNNING.getName() +
            ",SIGQUIT");

    List<String> dockerCommands = getDockerCommandsForDockerStop(
        ContainerExecutor.Signal.TERM);
    verifyStopCommand(dockerCommands, "SIGQUIT");
  }

  @Test
  public void testDockerStopOnKillSignalWhenRunning()
      throws ContainerExecutionException, PrivilegedOperationException {
    List<String> dockerCommands = getDockerCommandsForSignal(
        ContainerExecutor.Signal.KILL);
    Assert.assertEquals(5, dockerCommands.size());
    Assert.assertEquals(runAsUser, dockerCommands.get(0));
    Assert.assertEquals(user, dockerCommands.get(1));
    Assert.assertEquals(
        Integer.toString(PrivilegedOperation.RunAsUserCommand
        .SIGNAL_CONTAINER.getValue()),
        dockerCommands.get(2));
    Assert.assertEquals(signalPid, dockerCommands.get(3));
    Assert.assertEquals(
        Integer.toString(ContainerExecutor.Signal.KILL.getValue()),
        dockerCommands.get(4));
  }

  @Test
  public void testDockerKillOnQuitSignalWhenRunning() throws Exception {
    List<String> dockerCommands = getDockerCommandsForSignal(
        ContainerExecutor.Signal.QUIT);

    Assert.assertEquals(5, dockerCommands.size());
    Assert.assertEquals(runAsUser, dockerCommands.get(0));
    Assert.assertEquals(user, dockerCommands.get(1));
    Assert.assertEquals(
        Integer.toString(PrivilegedOperation.RunAsUserCommand
        .SIGNAL_CONTAINER.getValue()),
        dockerCommands.get(2));
    Assert.assertEquals(signalPid, dockerCommands.get(3));
    Assert.assertEquals(
        Integer.toString(ContainerExecutor.Signal.QUIT.getValue()),
        dockerCommands.get(4));
  }

  @Test
  public void testDockerStopOnTermSignalWhenRunningPrivileged()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    conf.set(YarnConfiguration.NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS, "true");
    conf.set(YarnConfiguration.NM_DOCKER_PRIVILEGED_CONTAINERS_ACL,
        submittingUser);
    env.put(ENV_OCI_CONTAINER_RUN_PRIVILEGED_CONTAINER, "true");
    when(mockExecutor
        .executePrivilegedOperation(any(), any(PrivilegedOperation.class),
        any(), any(), anyBoolean(), anyBoolean())).thenReturn(
        DockerCommandExecutor.DockerContainerStatus.RUNNING.getName());
    List<String> dockerCommands = getDockerCommandsForDockerStop(
        ContainerExecutor.Signal.TERM);
    verifyStopCommand(dockerCommands, ContainerExecutor.Signal.TERM.toString());
  }

  @Test
  public void testDockerStopOnKillSignalWhenRunningPrivileged()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    conf.set(YarnConfiguration.NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS, "true");
    conf.set(YarnConfiguration.NM_DOCKER_PRIVILEGED_CONTAINERS_ACL,
        submittingUser);
    env.put(ENV_OCI_CONTAINER_RUN_PRIVILEGED_CONTAINER, "true");
    when(mockExecutor
        .executePrivilegedOperation(any(), any(PrivilegedOperation.class),
        any(), any(), anyBoolean(), anyBoolean())).thenReturn(
        DockerCommandExecutor.DockerContainerStatus.RUNNING.getName());
    List<String> dockerCommands = getDockerCommandsForDockerStop(
        ContainerExecutor.Signal.KILL);
    Assert.assertEquals(4, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]", dockerCommands.get(0));
    Assert.assertEquals("  docker-command=kill", dockerCommands.get(1));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(2));
    Assert.assertEquals("  signal=KILL", dockerCommands.get(3));
  }

  @Test
  public void testDockerKillOnQuitSignalWhenRunningPrivileged()
      throws Exception {
    conf.set(YarnConfiguration.NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS, "true");
    conf.set(YarnConfiguration.NM_DOCKER_PRIVILEGED_CONTAINERS_ACL,
        submittingUser);
    env.put(ENV_OCI_CONTAINER_RUN_PRIVILEGED_CONTAINER, "true");
    when(mockExecutor
        .executePrivilegedOperation(any(), any(PrivilegedOperation.class),
        any(), any(), anyBoolean(), anyBoolean())).thenReturn(
        DockerCommandExecutor.DockerContainerStatus.RUNNING.getName());
    List<String> dockerCommands = getDockerCommandsForDockerStop(
        ContainerExecutor.Signal.QUIT);

    Assert.assertEquals(4, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]", dockerCommands.get(0));
    Assert.assertEquals("  docker-command=kill", dockerCommands.get(1));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(2));
    Assert.assertEquals("  signal=QUIT", dockerCommands.get(3));
  }

  @Test
  public void testDockerRmOnWhenExited() throws Exception {
    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_DELAYED_REMOVAL,
        "false");
    conf.set(YarnConfiguration.NM_DOCKER_ALLOW_DELAYED_REMOVAL, "true");
    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    builder.setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user);
    runtime.initialize(enableMockContainerExecutor(conf), null);
    runtime.reapContainer(builder.build());
    verify(mockExecutor, times(1))
        .executePrivilegedOperation(any(), any(), any(),
            any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testNoDockerRmWhenDelayedDeletionEnabled()
      throws Exception {
    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_DELAYED_REMOVAL,
        "true");
    conf.set(YarnConfiguration.NM_DOCKER_ALLOW_DELAYED_REMOVAL, "true");
    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    builder.setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user);
    runtime.initialize(enableMockContainerExecutor(conf), null);
    runtime.reapContainer(builder.build());
    verify(mockExecutor, never())
        .executePrivilegedOperation(any(), any(), any(),
            anyMap(), anyBoolean(), anyBoolean());
  }

  private List<String> getDockerCommandsForDockerStop(
      ContainerExecutor.Signal signal)
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {

    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    builder.setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user)
        .setExecutionAttribute(PID, signalPid)
        .setExecutionAttribute(SIGNAL, signal);
    runtime.initialize(enableMockContainerExecutor(conf), nmContext);
    runtime.signalContainer(builder.build());

    PrivilegedOperation op = capturePrivilegedOperation(2);
    Assert.assertEquals(op.getOperationType(),
        PrivilegedOperation.OperationType.RUN_DOCKER_CMD);
    String dockerCommandFile = op.getArguments().get(0);
    return Files.readAllLines(Paths.get(dockerCommandFile),
        StandardCharsets.UTF_8);
  }

  private List<String> getDockerCommandsForSignal(
      ContainerExecutor.Signal signal)
      throws ContainerExecutionException, PrivilegedOperationException {

    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    builder.setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user)
        .setExecutionAttribute(PID, signalPid)
        .setExecutionAttribute(SIGNAL, signal);
    runtime.initialize(enableMockContainerExecutor(conf), null);
    runtime.signalContainer(builder.build());

    PrivilegedOperation op = capturePrivilegedOperation();
    Assert.assertEquals(op.getOperationType(),
        PrivilegedOperation.OperationType.SIGNAL_CONTAINER);
    return op.getArguments();
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
    String[] validNames = {"ubuntu", "fedora/httpd:version1.0", "fedora/httpd:version1.0.test",
        "fedora/httpd:version1.0.TEST", "myregistryhost:5000/ubuntu",
        "myregistryhost:5000/fedora/httpd:version1.0",
        "myregistryhost:5000/fedora/httpd:version1.0.test",
        "myregistryhost:5000/fedora/httpd:version1.0.TEST",
        "123456789123.dkr.ecr.us-east-1.amazonaws.com/emr-docker-examples:pyspark-example"
            + "@sha256:f1d4ae3f7261a72e98c6ebefe9985cf10a0ea5bd762585a43e0700ed99863807"};

    String[] invalidNames = {"Ubuntu", "ubuntu || fedora", "ubuntu#", "myregistryhost:50AB0/ubuntu",
        "myregistry#host:50AB0/ubuntu", ":8080/ubuntu",

        // Invalid: contains "@sha256" but doesn't really contain a digest.
        "123456789123.dkr.ecr.us-east-1.amazonaws.com/emr-docker-examples:pyspark-example@sha256",

        // Invalid: digest is too short.
        "123456789123.dkr.ecr.us-east-1.amazonaws.com/emr-docker-examples:pyspark-example"
            + "@sha256:f1d4",

        // Invalid: digest is too long
        "123456789123.dkr.ecr.us-east-1.amazonaws.com/emr-docker-examples:pyspark-example"
            + "@sha256:f1d4ae3f7261a72e98c6ebefe9985cf10a0ea5bd762585a43e0700ed99863807f"};

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

  @Test
  public void testValidDockerHostnameLength() throws Exception {
    String validLength = "example.test.site";
    DockerLinuxContainerRuntime.validateHostname(validLength);
  }

  @Test(expected = ContainerExecutionException.class)
  public void testInvalidDockerHostnameLength() throws Exception {
    String invalidLength =
        "exampleexampleexampleexampleexampleexampleexampleexample.test.site";
    DockerLinuxContainerRuntime.validateHostname(invalidLength);
  }

  @SuppressWarnings("unchecked")
  private void checkVolumeCreateCommand()
      throws PrivilegedOperationException, IOException {
    ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass(
        PrivilegedOperation.class);

    //Three invocations expected (volume creation, volume check, run container)
    //due to type erasure + mocking, this verification requires a suppress
    // warning annotation on the entire method
    verify(mockExecutor, times(3))
        .executePrivilegedOperation(any(), opCaptor.capture(), any(),
            any(), anyBoolean(), anyBoolean());

    //verification completed. we need to isolate specific invications.
    // hence, reset mock here
    //Mockito.reset(mockExecutor);

    List<PrivilegedOperation> allCaptures = opCaptor.getAllValues();

    PrivilegedOperation op = allCaptures.get(0);
    Assert.assertEquals(PrivilegedOperation.OperationType
        .RUN_DOCKER_CMD, op.getOperationType());

    File commandFile = new File(StringUtils.join(",", op.getArguments()));
    FileInputStream fileInputStream = new FileInputStream(commandFile);
    String fileContent = new String(IOUtils.toByteArray(fileInputStream));
    Assert.assertEquals("[docker-command-execution]\n"
        + "  docker-command=volume\n" + "  driver=local\n"
        + "  sub-command=create\n" + "  volume=volume1\n", fileContent);
    fileInputStream.close();

    op = allCaptures.get(1);
    Assert.assertEquals(PrivilegedOperation.OperationType
        .RUN_DOCKER_CMD, op.getOperationType());

    commandFile = new File(StringUtils.join(",", op.getArguments()));
    fileInputStream = new FileInputStream(commandFile);
    fileContent = new String(IOUtils.toByteArray(fileInputStream));
    Assert.assertEquals(
        "[docker-command-execution]\n" + "  docker-command=volume\n"
            + "  sub-command=ls\n", fileContent);
    fileInputStream.close();
  }

  private static class MockDockerCommandPlugin implements DockerCommandPlugin {
    private final String volume;
    private final String driver;

    public MockDockerCommandPlugin(String volume, String driver) {
      this.volume = volume;
      this.driver = driver;
    }

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
      return new DockerVolumeCommand("create").setVolumeName(volume)
          .setDriverName(driver);
    }

    @Override
    public DockerVolumeCommand getCleanupDockerVolumesCommand(
        Container container) throws ContainerExecutionException {
      return null;
    }
  }

  private void testDockerCommandPluginWithVolumesOutput(
      String dockerVolumeListOutput, boolean expectFail)
      throws PrivilegedOperationException, ContainerExecutionException,
      IOException {
    mockExecutor = Mockito
        .mock(PrivilegedOperationExecutor.class);

    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    when(mockExecutor
        .executePrivilegedOperation(any(), any(PrivilegedOperation.class),
            any(), any(), anyBoolean(), anyBoolean())).thenReturn(
        null, dockerVolumeListOutput);

    Context mockNMContext = createMockNMContext();
    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    Map<String, ResourcePlugin> pluginsMap = new HashMap<>();
    ResourcePlugin plugin1 = mock(ResourcePlugin.class);

    // Create the docker command plugin logic, which will set volume driver
    DockerCommandPlugin dockerCommandPlugin = new MockDockerCommandPlugin(
        "volume1", "local");

    when(plugin1.getDockerCommandPluginInstance()).thenReturn(
        dockerCommandPlugin);
    ResourcePlugin plugin2 = mock(ResourcePlugin.class);
    pluginsMap.put("plugin1", plugin1);
    pluginsMap.put("plugin2", plugin2);

    when(rpm.getNameToPlugins()).thenReturn(pluginsMap);

    when(mockNMContext.getResourcePluginManager()).thenReturn(rpm);

    runtime.initialize(conf, mockNMContext);

    ContainerRuntimeContext containerRuntimeContext = builder.build();

    try {
      runtime.prepareContainer(containerRuntimeContext);
      runtime.launchContainer(containerRuntimeContext);
      checkVolumeCreateCommand();
    } catch (ContainerExecutionException e) {
      if (expectFail) {
        // Expected
        return;
      } else{
        Assert.fail("Should successfully prepareContainers" + e);
      }
    }
    if (expectFail) {
      Assert.fail(
          "Should fail because output is illegal");
    }
  }

  @Test
  public void testDockerCommandPluginCheckVolumeAfterCreation()
      throws Exception {
    // For following tests, we expect to have volume1,local in output

    // Failure cases
    testDockerCommandPluginWithVolumesOutput(
        "DRIVER              VOLUME NAME\n", true);
    testDockerCommandPluginWithVolumesOutput("", true);
    testDockerCommandPluginWithVolumesOutput("volume1", true);
    testDockerCommandPluginWithVolumesOutput(
        "DRIVER              VOLUME NAME\n" +
        "nvidia-docker       nvidia_driver_375.66\n", true);
    testDockerCommandPluginWithVolumesOutput(
        "DRIVER              VOLUME NAME\n" +
        "                    volume1\n", true);
    testDockerCommandPluginWithVolumesOutput("local", true);
    testDockerCommandPluginWithVolumesOutput("volume2,local", true);
    testDockerCommandPluginWithVolumesOutput(
        "DRIVER              VOLUME NAME\n" +
        "local               volume2\n", true);
    testDockerCommandPluginWithVolumesOutput("volum1,something", true);
    testDockerCommandPluginWithVolumesOutput(
        "DRIVER              VOLUME NAME\n" +
        "something               volume1\n", true);
    testDockerCommandPluginWithVolumesOutput("volum1,something\nvolum2,local",
        true);

    // Success case
    testDockerCommandPluginWithVolumesOutput(
        "DRIVER              VOLUME NAME\n" +
        "nvidia-docker       nvidia_driver_375.66\n" +
        "local               volume1\n", false);
    testDockerCommandPluginWithVolumesOutput(
        "volume_xyz,nvidia\nvolume1,local\n\n", false);
    testDockerCommandPluginWithVolumesOutput(" volume1,  local \n", false);
    testDockerCommandPluginWithVolumesOutput(
        "volume_xyz,\tnvidia\n   volume1,\tlocal\n\n", false);
  }


  @Test
  public void testDockerCommandPlugin() throws Exception {
    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    when(mockExecutor
        .executePrivilegedOperation(any(), any(PrivilegedOperation.class),
            any(), any(), anyBoolean(), anyBoolean())).thenReturn(
        null, "volume1,local");

    Context mockNMContext = createMockNMContext();
    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    Map<String, ResourcePlugin> pluginsMap = new HashMap<>();
    ResourcePlugin plugin1 = mock(ResourcePlugin.class);

    // Create the docker command plugin logic, which will set volume driver
    DockerCommandPlugin dockerCommandPlugin = new MockDockerCommandPlugin(
        "volume1", "local");

    when(plugin1.getDockerCommandPluginInstance()).thenReturn(
        dockerCommandPlugin);
    ResourcePlugin plugin2 = mock(ResourcePlugin.class);
    pluginsMap.put("plugin1", plugin1);
    pluginsMap.put("plugin2", plugin2);

    when(rpm.getNameToPlugins()).thenReturn(pluginsMap);

    when(mockNMContext.getResourcePluginManager()).thenReturn(rpm);

    runtime.initialize(conf, mockNMContext);

    ContainerRuntimeContext containerRuntimeContext = builder.build();

    runtime.prepareContainer(containerRuntimeContext);

    runtime.launchContainer(containerRuntimeContext);
    checkVolumeCreateCommand();

    List<String> dockerCommands = readDockerCommands(3);

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
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert
        .assertEquals("  image=busybox:latest", dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro,"
            + "/source/path:/destination/path:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));

    // Verify volume-driver is set to expected value.
    Assert.assertEquals("  volume-driver=driver-1",
        dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter));
  }

  @Test
  public void testDockerCapabilities() throws ContainerExecutionException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    try {
      conf.setStrings(YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
          "none", "CHOWN", "DAC_OVERRIDE");
      runtime.initialize(conf, nmContext);
      Assert.fail("Initialize didn't fail with invalid capabilities " +
          "'none', 'CHOWN', 'DAC_OVERRIDE'");
    } catch (ContainerExecutionException e) {
    }

    try {
      conf.setStrings(YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
          "CHOWN", "DAC_OVERRIDE", "NONE");
      runtime.initialize(conf, nmContext);
      Assert.fail("Initialize didn't fail with invalid capabilities " +
          "'CHOWN', 'DAC_OVERRIDE', 'NONE'");
    } catch (ContainerExecutionException e) {
    }

    conf.setStrings(YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
        "NONE");
    runtime.initialize(conf, nmContext);
    Assert.assertEquals(0, runtime.getCapabilities().size());

    conf.setStrings(YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
        "none");
    runtime.initialize(conf, nmContext);
    Assert.assertEquals(0, runtime.getCapabilities().size());

    conf.setStrings(YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
        "CHOWN", "DAC_OVERRIDE");
    runtime.initialize(conf, nmContext);
    Iterator<String> it = runtime.getCapabilities().iterator();
    Assert.assertEquals("CHOWN", it.next());
    Assert.assertEquals("DAC_OVERRIDE", it.next());
  }

  @Test
  public void testLaunchContainerWithDockerTokens()
      throws ContainerExecutionException, PrivilegedOperationException, IOException {

    // Get the credentials object with the Tokens.
    Credentials credentials = DockerClientConfigHandler.readCredentialsFromConfigFile(
        new Path(getDockerClientConfigFile().toURI()), conf, appId);
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    ByteBuffer tokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    testLaunchContainer(tokens, null);
  }

  @Test
  public void testLaunchContainerWithAdditionalDockerClientConfig()
      throws ContainerExecutionException, PrivilegedOperationException, IOException {
    testLaunchContainer(null, getDockerClientConfigFile());
  }

  public void testLaunchContainer(ByteBuffer tokens, File dockerConfigFile)
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    if (dockerConfigFile != null) {
      // load the docker client config file from system environment
      env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_CLIENT_CONFIG,
          dockerConfigFile.getPath());
    }

    if (tokens != null) {
      // Configure the runtime and launch the container
      when(context.getTokens()).thenReturn(tokens);
    }
    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    Set<PosixFilePermission> perms =
        PosixFilePermissions.fromString("rwxr-xr--");
    FileAttribute<Set<PosixFilePermission>> attr =
        PosixFilePermissions.asFileAttribute(perms);
    Path outDir = new Path(
        Files.createTempDirectory("docker-client-config-out", attr).toUri()
            .getPath() + "/launch_container.sh");
    builder.setExecutionAttribute(NM_PRIVATE_CONTAINER_SCRIPT_PATH, outDir);
    runtime.launchContainer(builder.build());
    PrivilegedOperation op = capturePrivilegedOperation();
    Assert.assertEquals(
        PrivilegedOperation.OperationType.LAUNCH_DOCKER_CONTAINER,
            op.getOperationType());

    List<String> args = op.getArguments();

    int expectedArgs = (https) ? 15 : 13;
    int argsCounter = 0;
    Assert.assertEquals(expectedArgs, args.size());
    Assert.assertEquals(runAsUser, args.get(argsCounter++));
    Assert.assertEquals(user, args.get(argsCounter++));
    Assert.assertEquals(Integer.toString(
        PrivilegedOperation.RunAsUserCommand.LAUNCH_DOCKER_CONTAINER
            .getValue()), args.get(argsCounter++));
    Assert.assertEquals(appId, args.get(argsCounter++));
    Assert.assertEquals(containerId, args.get(argsCounter++));
    Assert.assertEquals(containerWorkDir.toString(), args.get(argsCounter++));
    Assert.assertEquals(outDir.toUri().getPath(), args.get(argsCounter++));
    Assert.assertEquals(nmPrivateTokensPath.toUri().getPath(),
        args.get(argsCounter++));
    if (https) {
      Assert.assertEquals("--https", args.get(argsCounter++));
      Assert.assertEquals(nmPrivateKeystorePath.toUri().toString(),
          args.get(argsCounter++));
      Assert.assertEquals(nmPrivateTruststorePath.toUri().toString(),
          args.get(argsCounter++));
    } else {
      Assert.assertEquals("--http", args.get(argsCounter++));
    }
    Assert.assertEquals(pidFilePath.toString(), args.get(argsCounter++));
    Assert.assertEquals(localDirs.get(0), args.get(argsCounter++));
    Assert.assertEquals(logDirs.get(0), args.get(argsCounter++));
    String dockerCommandFile = args.get(argsCounter++);

    List<String> dockerCommands = Files
        .readAllLines(Paths.get(dockerCommandFile), StandardCharsets.UTF_8);

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
    Assert.assertEquals("  docker-config=" + outDir.getParent(),
        dockerCommands.get(counter++));
    Assert.assertEquals("  group-add=" + String.join(",", groups),
        dockerCommands.get(counter++));
    Assert.assertEquals("  image=busybox:latest",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  launch-command=bash,/test_container_work_dir/launch_container.sh",
        dockerCommands.get(counter++));
    Assert.assertEquals("  mounts="
            + "/test_container_log_dir:/test_container_log_dir:rw,"
            + "/test_application_local_dir:/test_application_local_dir:rw,"
            + "/test_filecache_dir:/test_filecache_dir:ro,"
            + "/test_user_filecache_dir:/test_user_filecache_dir:ro",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter++));
    Assert.assertEquals("  net=host", dockerCommands.get(counter++));
    Assert.assertEquals("  user=" + uidGidPair, dockerCommands.get(counter++));
    Assert.assertEquals("  workdir=/test_container_work_dir",
        dockerCommands.get(counter++));
  }

  private File getDockerClientConfigFile() throws IOException {
    // Write the JSOn to a temp file.
    File file = File.createTempFile("docker-client-config", "runtime-test");
    file.deleteOnExit();
    BufferedWriter bw = new BufferedWriter(new FileWriter(file));
    bw.write(TestDockerClientConfigHandler.JSON);
    bw.close();
    return file;
  }

  @Test
  public void testDockerContainerRelaunch()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    when(mockExecutor
        .executePrivilegedOperation(any(), any(PrivilegedOperation.class),
        any(), any(), anyBoolean(), anyBoolean())).thenReturn(
        DockerCommandExecutor.DockerContainerStatus.STOPPED.getName());
    runtime.initialize(conf, nmContext);
    runtime.relaunchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands(2);

    int expected = 3;
    int counter = 0;
    Assert.assertEquals(expected, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]",
        dockerCommands.get(counter++));
    Assert.assertEquals("  docker-command=start",
        dockerCommands.get(counter++));
    Assert.assertEquals(
        "  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(counter));
  }

  @Test
  public void testLaunchContainersWithSpecificDockerRuntime()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    DockerLinuxContainerRuntime runtime = new DockerLinuxContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    env.put(DockerLinuxContainerRuntime
            .ENV_DOCKER_CONTAINER_DOCKER_RUNTIME, "runc");
    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();
    Assert.assertEquals(14, dockerCommands.size());
    Assert.assertEquals("  runtime=runc", dockerCommands.get(11));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testContainerLaunchWithAllowedRuntimes()
      throws ContainerExecutionException, IOException,
      PrivilegedOperationException {
    DockerLinuxContainerRuntime runtime =
        new DockerLinuxContainerRuntime(mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);

    String disallowedRuntime = "runc2";

    try {
      env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_DOCKER_RUNTIME,
          disallowedRuntime);
      runtime.launchContainer(builder.build());
      Assert.fail("Runtime was expected to be disallowed: " +
          disallowedRuntime);
    } catch (ContainerExecutionException e) {
      LOG.info("Caught expected exception: " + e);
    }

    String allowedRuntime = "runc";
    env.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_DOCKER_RUNTIME,
        allowedRuntime);
    //this should cause no failures.

    runtime.launchContainer(builder.build());
    List<String> dockerCommands = readDockerCommands();

    //This is the expected docker invocation for this case
    Assert.assertEquals(14, dockerCommands.size());
    Assert.assertEquals("  runtime=runc", dockerCommands.get(11));
  }

  private static void verifyStopCommand(List<String> dockerCommands,
      String signal) {
    Assert.assertEquals(4, dockerCommands.size());
    Assert.assertEquals("[docker-command-execution]", dockerCommands.get(0));
    Assert.assertEquals("  docker-command=kill", dockerCommands.get(1));
    Assert.assertEquals("  name=container_e11_1518975676334_14532816_01_000001",
        dockerCommands.get(2));
    Assert.assertEquals("  signal=" + signal, dockerCommands.get(3));
  }

  private List<String> readDockerCommands() throws IOException,
      PrivilegedOperationException {
    return readDockerCommands(1);
  }

  private List<String> readDockerCommands(int invocations) throws IOException,
      PrivilegedOperationException {
    PrivilegedOperation op = (invocations == 1)
        ? capturePrivilegedOperationAndVerifyArgs()
        : capturePrivilegedOperation(invocations);
    List<String> args = op.getArguments();
    String dockerCommandFile = args.get((https) ? 14 : 12);

    List<String> dockerCommands = Files.readAllLines(
        Paths.get(dockerCommandFile), StandardCharsets.UTF_8);
    return dockerCommands;
  }
}
