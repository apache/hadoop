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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.RuncContainerRuntime.RuncRuntimeObject;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.HdfsManifestToResourcesPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.ImageManifest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.ImageTagToManifestPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCILayer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCIRuntimeConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCIRuntimeConfig.OCIMount;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCIRuntimeConfig.OCIProcessConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncImageTagToManifestPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncManifestToResourcesPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalizedResource;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_DEFAULT_RO_MOUNTS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_DEFAULT_RW_MOUNTS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_LAYER_MOUNTS_TO_KEEP;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.RuncContainerRuntime.ENV_RUNC_CONTAINER_MOUNTS;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This class tests the {@link RuncContainerRuntime}.
 */
@RunWith(Parameterized.class)
public class TestRuncContainerRuntime {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRuncContainerRuntime.class);
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
  private String runAsUser = System.getProperty("user.name");
  private String user;
  private String appId;
  private String containerIdStr;
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
  private ObjectMapper mapper;
  private RuncContainerRuntime.RuncRuntimeObject runcRuntimeObject;
  private LocalResource localResource;
  private URL mockUrl;
  private Resource resource;
  private int layersToKeep;
  private int cpuShares;
  private List<OCIMount> expectedMounts;
  private String tmpPath;
  private LocalResource config;
  private List<LocalResource> layers;

  private RuncImageTagToManifestPlugin mockRuncImageTagToManifestPlugin =
      mock(ImageTagToManifestPlugin.class);
  private RuncManifestToResourcesPlugin mockRuncManifestToResourcesPlugin =
      mock(HdfsManifestToResourcesPlugin.class);

  @Parameterized.Parameters(name = "https={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {true}, {false}
    });
  }

  @Parameterized.Parameter
  public boolean https;

  @Before
  public void setup() throws ContainerExecutionException {
    mockExecutor = Mockito
        .mock(PrivilegedOperationExecutor.class);
    mockCGroupsHandler = Mockito.mock(CGroupsHandler.class);
    tmpPath = new StringBuffer(System.getProperty("test.build.data"))
      .append('/').append("hadoop.tmp.dir").toString();
    containerId = "container_e11_1518975676334_14532816_01_000001";
    container = mock(Container.class);
    cId = mock(ContainerId.class);
    appAttemptId = mock(ApplicationAttemptId.class);
    mockApplicationId = mock(ApplicationId.class);
    context = mock(ContainerLaunchContext.class);
    env = new HashMap<>();
    env.put("FROM_CLIENT", "1");
    image = "busybox:latest";
    nmContext = createMockNMContext();
    runcRuntimeObject =
        mock(RuncContainerRuntime.RuncRuntimeObject.class);
    localResource = mock(LocalResource.class);
    mockUrl = mock(URL.class);
    resource = mock(Resource.class);
    appId = "app_id";
    layersToKeep = 5;
    cpuShares = 10;


    conf = new Configuration();
    conf.set("hadoop.tmp.dir", tmpPath);
    conf.setInt(NM_RUNC_LAYER_MOUNTS_TO_KEEP, layersToKeep);

    env.put(RuncContainerRuntime.ENV_RUNC_CONTAINER_IMAGE, image);
    when(container.getContainerId()).thenReturn(cId);
    when(cId.toString()).thenReturn(containerId);
    when(mockApplicationId.toString()).thenReturn(appId);
    when(appAttemptId.getApplicationId()).thenReturn(mockApplicationId);
    when(cId.getApplicationAttemptId()).thenReturn(appAttemptId);
    when(container.getLaunchContext()).thenReturn(context);
    when(context.getEnvironment()).thenReturn(env);
    when(container.getUser()).thenReturn(submittingUser);
    when(container.getContainerRuntimeData(any()))
        .thenReturn(runcRuntimeObject);
    when(container.getResource()).thenReturn(resource);
    when(resource.getVirtualCores()).thenReturn(cpuShares);
    when(runcRuntimeObject.getConfig()).thenReturn(localResource);
    when(localResource.getResource()).thenReturn(mockUrl);
    try {
      when(mockUrl.toPath()).thenReturn(new Path("/test_user_filecache_dir"));
    } catch (URISyntaxException use) {
      throw new RuntimeException(use);
    }

    user = submittingUser;
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
    expectedMounts = new ArrayList<>();

    String filecachePath = tmpPath + "/filecache";
    String userFilecachePath = tmpPath + "/userFilecache";

    localDirs.add("/test_local_dir");
    logDirs.add("/test_log_dir");
    filecacheDirs.add(filecachePath);
    userFilecacheDirs.add(userFilecachePath);
    applicationLocalDirs.add("/test_application_local_dir");
    containerLogDirs.add("/test_container_log_dir");
    localizedResources.put(new Path("/test_local_dir/test_resource_file"),
        Collections.singletonList("test_dir/test_resource_file"));

    File tmpDir = new File(tmpPath);
    tmpDir.mkdirs();

    List<String> rwOptions = new ArrayList<>();
    rwOptions.add("rw");
    rwOptions.add("rbind");
    rwOptions.add("rprivate");

    List<String> roOptions = new ArrayList<>();
    roOptions.add("ro");
    roOptions.add("rbind");
    roOptions.add("rprivate");

    for (String containerLogDir : containerLogDirs) {
      expectedMounts.add(new OCIMount(
          containerLogDir, "bind", containerLogDir, rwOptions));
    }

    for (String applicationLocalDir : applicationLocalDirs) {
      expectedMounts.add(new OCIMount(
          applicationLocalDir, "bind", applicationLocalDir, rwOptions));
    }

    for (String filecacheDir : filecacheDirs) {
      File filecacheDirFile = new File(filecacheDir);
      filecacheDirFile.mkdirs();
      expectedMounts.add(new OCIMount(
          filecacheDir, "bind", filecacheDir, roOptions));
    }
    for (String userFilecacheDir : userFilecacheDirs) {
      File userFilecacheDirFile = new File(userFilecacheDir);
      userFilecacheDirFile.mkdirs();
      expectedMounts.add(new OCIMount(
          userFilecachePath, "bind", userFilecachePath, roOptions));
    }

    expectedMounts.add(new OCIMount(
        "/tmp", "bind", containerWorkDir + "/private_slash_tmp", rwOptions));
    expectedMounts.add(new OCIMount(
        "/var/tmp", "bind", containerWorkDir + "/private_var_slash_tmp",
        rwOptions));

    mapper = new ObjectMapper();

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

  /**
   * This class mocks out the {@link RuncContainerRuntime}.
   */
  public class MockRuncContainerRuntime extends RuncContainerRuntime {
    MockRuncContainerRuntime(
        PrivilegedOperationExecutor privilegedOperationExecutor,
        CGroupsHandler cGroupsHandler) {
      super(privilegedOperationExecutor, cGroupsHandler);
    }

    @Override
    protected RuncImageTagToManifestPlugin chooseImageTagToManifestPlugin()
        throws ContainerExecutionException {
      ImageManifest mockImageManifest = mock(ImageManifest.class);
      try {
        when(mockRuncImageTagToManifestPlugin.getManifestFromImageTag(any()))
            .thenReturn(mockImageManifest);
      } catch (IOException ioe) {
        throw new ContainerExecutionException(ioe);
      }
      return mockRuncImageTagToManifestPlugin;
    }

    @Override
    protected RuncManifestToResourcesPlugin chooseManifestToResourcesPlugin()
        throws ContainerExecutionException {
      URL configUrl = URL.fromPath(new Path(tmpPath + "config"));
      URL layer1Url = URL.fromPath(new Path(tmpPath + "layer1"));
      URL layer2Url = URL.fromPath(new Path(tmpPath + "layer2"));

      long size = 1234;
      long timestamp = 5678;

      config = LocalResource.newInstance(configUrl,
          LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
          size, timestamp);

      LocalResource layer1 = LocalResource.newInstance(layer1Url,
          LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
          size, timestamp);
      LocalResource layer2 = LocalResource.newInstance(layer2Url,
          LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
          size, timestamp);

      layers = new ArrayList<>();

      layers.add(layer1);
      layers.add(layer2);

      try {
        when(mockRuncManifestToResourcesPlugin.getConfigResource(any()))
            .thenReturn(config);
        when(mockRuncManifestToResourcesPlugin.getLayerResources(any()))
            .thenReturn(layers);
      } catch (IOException ioe) {
        throw new ContainerExecutionException(ioe);
      }
      return mockRuncManifestToResourcesPlugin;
    }

    @Override
    protected List<String> extractImageEnv(File configFile) {
      return new ArrayList<>();
    }

    @Override
    protected List<String> extractImageEntrypoint(File configFile) {
      return new ArrayList<>();
    }
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

  private File captureRuncConfigFile()
      throws PrivilegedOperationException {
    PrivilegedOperation op = capturePrivilegedOperation(1);

    Assert.assertEquals(PrivilegedOperation.OperationType
        .RUN_RUNC_CONTAINER, op.getOperationType());
    return new File(op.getArguments().get(0));
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

  private RuncRuntimeObject captureRuncRuntimeObject(
      int invocations) {

    ArgumentCaptor<RuncRuntimeObject> opCaptor = ArgumentCaptor.forClass(
        RuncRuntimeObject.class);

    verify(container, times(invocations))
        .setContainerRuntimeData(opCaptor.capture());

    //verification completed. we need to isolate specific invocations.
    // hence, reset mock here
    Mockito.reset(container);

    return opCaptor.getValue();
  }

  @SuppressWarnings("unchecked")
  private RuncContainerExecutorConfig verifyRuncConfig(File configFile)
      throws IOException {
    int configSize;
    String configVersion;
    String configRunAsUser;
    String configUser;
    String configContainerId;
    String configAppId;
    String configPidFile;
    String configContainerScriptPath;
    String configContainerCredentialsPath;
    int configHttps;
    String configKeystorePath;
    String configTruststorePath;
    List<String> configLocalDirsList;
    List<String> configLogDirsList;
    List<OCILayer> configLayersList;
    int configLayersToKeep;
    String configContainerWorkDir;
    int expectedConfigSize;
    long configCpuShares;

    JsonNode configNode = mapper.readTree(configFile);

    RuncContainerExecutorConfig runcContainerExecutorConfig =
        mapper.readValue(configNode, RuncContainerExecutorConfig.class);
    configSize = configNode.size();

    OCIRuntimeConfig ociRuntimeConfig =
        runcContainerExecutorConfig.getOciRuntimeConfig();
    OCIProcessConfig ociProcessConfig = ociRuntimeConfig.getProcess();

    configVersion = runcContainerExecutorConfig.getVersion();
    configRunAsUser = runcContainerExecutorConfig.getRunAsUser();
    configUser = runcContainerExecutorConfig.getUsername();
    configContainerId = runcContainerExecutorConfig.getContainerId();
    configAppId = runcContainerExecutorConfig.getApplicationId();
    configPidFile = runcContainerExecutorConfig.getPidFile();
    configContainerScriptPath =
        runcContainerExecutorConfig.getContainerScriptPath();
    configContainerCredentialsPath =
        runcContainerExecutorConfig.getContainerCredentialsPath();
    configHttps = runcContainerExecutorConfig.getHttps();
    configKeystorePath = runcContainerExecutorConfig.getKeystorePath();
    configTruststorePath = runcContainerExecutorConfig.getTruststorePath();
    configLocalDirsList = runcContainerExecutorConfig.getLocalDirs();
    configLogDirsList = runcContainerExecutorConfig.getLogDirs();
    configLayersList = runcContainerExecutorConfig.getLayers();
    configLayersToKeep = runcContainerExecutorConfig.getReapLayerKeepCount();
    configContainerWorkDir = ociRuntimeConfig.getProcess().getCwd();
    configCpuShares =
        ociRuntimeConfig.getLinux().getResources().getCPU().getShares();

    expectedConfigSize = (https) ? 16 : 13;

    Assert.assertEquals(expectedConfigSize, configSize);
    Assert.assertEquals("0.1", configVersion);
    Assert.assertEquals(runAsUser, configRunAsUser);
    Assert.assertEquals(user, configUser);
    Assert.assertEquals(containerId, configContainerId);
    Assert.assertEquals(appId, configAppId);
    Assert.assertEquals(pidFilePath.toString(), configPidFile);
    Assert.assertEquals(nmPrivateContainerScriptPath.toUri().toString(),
        configContainerScriptPath);
    Assert.assertEquals(nmPrivateTokensPath.toUri().getPath(),
        configContainerCredentialsPath);

    if (https) {
      Assert.assertEquals(1, configHttps);
      Assert.assertEquals(nmPrivateKeystorePath.toUri().toString(),
          configKeystorePath);
      Assert.assertEquals(nmPrivateTruststorePath.toUri().toString(),
          configTruststorePath);
    } else {
      Assert.assertEquals(0, configHttps);
      Assert.assertNull(configKeystorePath);
      Assert.assertNull(configTruststorePath);
    }

    Assert.assertEquals(localDirs, configLocalDirsList);
    Assert.assertEquals(logDirs, configLogDirsList);
    Assert.assertEquals(0, configLayersList.size());
    Assert.assertEquals(layersToKeep, configLayersToKeep);

    List<OCIMount> configMounts = ociRuntimeConfig.getMounts();
    verifyRuncMounts(expectedMounts, configMounts);

    List<String> processArgsList = ociProcessConfig.getArgs();
    String configArgs = "".join(",", processArgsList);

    Assert.assertEquals(containerWorkDir.toString(), configContainerWorkDir);
    Assert.assertEquals("bash," + containerWorkDir + "/launch_container.sh",
        configArgs);
    Assert.assertEquals(cpuShares, configCpuShares);

    return runcContainerExecutorConfig;
  }


  private void verifyRuncMounts(List<OCIMount> expectedRuncMounts,
      List<OCIMount> configMounts) throws IOException {
    Assert.assertEquals(expectedRuncMounts.size(), configMounts.size());
    boolean found;
    for (OCIMount expectedMount : expectedRuncMounts) {
      found = false;

      for (OCIMount configMount : configMounts) {
        if (expectedMount.getDestination().equals(configMount.getDestination())
            && expectedMount.getSource().equals(configMount.getSource())
            && expectedMount.getType().equals(configMount.getType())
            && expectedMount.getOptions().
            containsAll(configMount.getOptions())) {
          found = true;
          break;
        }
      }

      if (!found) {
        String expectedMountString = expectedMount.getSource() + ":"
            + expectedMount.getDestination() + ", " + expectedMount.getType()
            + ", " + expectedMount.getOptions().toString();
        throw new IOException("Expected mount not found: "
            + expectedMountString);
      }
    }
  }

  @Test
  public void testSelectRuncContainerType() {
    Map<String, String> envRuncType = new HashMap<>();
    Map<String, String> envOtherType = new HashMap<>();

    envRuncType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE,
        ContainerRuntimeConstants.CONTAINER_RUNTIME_RUNC);
    envOtherType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "other");

    Assert.assertFalse(RuncContainerRuntime
        .isRuncContainerRequested(conf, null));
    Assert.assertTrue(RuncContainerRuntime
        .isRuncContainerRequested(conf, envRuncType));
    Assert.assertFalse(RuncContainerRuntime
        .isRuncContainerRequested(conf, envOtherType));
  }

  @Test
  public void testSelectRuncContainerTypeWithRuncAsDefault() {
    Map<String, String> envRuncType = new HashMap<>();
    Map<String, String> envOtherType = new HashMap<>();

    conf.set(YarnConfiguration.LINUX_CONTAINER_RUNTIME_TYPE,
        ContainerRuntimeConstants.CONTAINER_RUNTIME_RUNC);
    envRuncType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE,
        ContainerRuntimeConstants.CONTAINER_RUNTIME_RUNC);
    envOtherType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "other");

    Assert.assertTrue(RuncContainerRuntime
        .isRuncContainerRequested(conf, null));
    Assert.assertTrue(RuncContainerRuntime
        .isRuncContainerRequested(conf, envRuncType));
    Assert.assertFalse(RuncContainerRuntime
        .isRuncContainerRequested(conf, envOtherType));
  }

  @Test
  public void testSelectRuncContainerTypeWithDefaultSet() {
    Map<String, String> envRuncType = new HashMap<>();
    Map<String, String> envOtherType = new HashMap<>();

    conf.set(YarnConfiguration.LINUX_CONTAINER_RUNTIME_TYPE, "default");
    envRuncType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE,
        ContainerRuntimeConstants.CONTAINER_RUNTIME_RUNC);
    envOtherType.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "other");

    Assert.assertFalse(RuncContainerRuntime
        .isRuncContainerRequested(conf, null));
    Assert.assertTrue(RuncContainerRuntime
        .isRuncContainerRequested(conf, envRuncType));
    Assert.assertFalse(RuncContainerRuntime
        .isRuncContainerRequested(conf, envOtherType));
  }

  @Test
  public void testRuncContainerLaunch()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    MockRuncContainerRuntime runtime = new MockRuncContainerRuntime(
        mockExecutor, mockCGroupsHandler);

    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());

    File configFile = captureRuncConfigFile();
    verifyRuncConfig(configFile);
  }

  @Test
  public void testRuncContainerLaunchWithDefaultImage()
      throws ContainerExecutionException, IOException {
    String runcImage = "busybox:1.2.3";
    conf.set(YarnConfiguration.NM_RUNC_IMAGE_NAME, runcImage);
    env.remove(RuncContainerRuntime.ENV_RUNC_CONTAINER_IMAGE);

    MockRuncContainerRuntime runtime = new MockRuncContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);
    runtime.getLocalResources(container);

    Mockito.verify(mockRuncImageTagToManifestPlugin)
        .getManifestFromImageTag(runcImage);
  }

  @Test
  public void testCGroupParent() throws ContainerExecutionException,
      PrivilegedOperationException, IOException {
    // Case 1: neither hierarchy nor resource options set,
    // so cgroup should not be set
    MockRuncContainerRuntime runtime = new MockRuncContainerRuntime(
        mockExecutor, mockCGroupsHandler);

    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());

    File configFile = captureRuncConfigFile();
    RuncContainerExecutorConfig runcContainerExecutorConfig =
        verifyRuncConfig(configFile);

    String configCgroupsPath = runcContainerExecutorConfig
        .getOciRuntimeConfig().getLinux().getCgroupsPath();
    Assert.assertNull(configCgroupsPath);

    // Case 2: hierarchy set, but resource options not,
    // so cgroup should not be set
    String hierarchy = "hadoop-yarn-test";
    when(mockCGroupsHandler.getRelativePathForCGroup(any()))
        .thenReturn(hierarchy);

    runtime = new MockRuncContainerRuntime(
        mockExecutor, mockCGroupsHandler);

    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());

    configFile = captureRuncConfigFile();
    runcContainerExecutorConfig = verifyRuncConfig(configFile);

    configCgroupsPath = runcContainerExecutorConfig.getOciRuntimeConfig()
        .getLinux().getCgroupsPath();
    Assert.assertNull(configCgroupsPath);

    // Case 3: resource options set, so cgroup should be set
    String resourceOptionsCpu = "/sys/fs/cgroup/cpu/" + hierarchy +
        containerIdStr;

    builder.setExecutionAttribute(RESOURCES_OPTIONS, resourceOptionsCpu);

    runtime = new MockRuncContainerRuntime(
        mockExecutor, mockCGroupsHandler);

    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());

    configFile = captureRuncConfigFile();
    runcContainerExecutorConfig = verifyRuncConfig(configFile);

    configCgroupsPath = runcContainerExecutorConfig.getOciRuntimeConfig()
        .getLinux().getCgroupsPath();
    Assert.assertEquals("/" + hierarchy, configCgroupsPath);

    // Case 4: cgroupsHandler is null, so cgroup should not be set
    resourceOptionsCpu = "/sys/fs/cgroup/cpu/" + hierarchy +
        containerIdStr;

    builder.setExecutionAttribute(RESOURCES_OPTIONS, resourceOptionsCpu);

    runtime = new MockRuncContainerRuntime(
        mockExecutor, null);

    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());

    configFile = captureRuncConfigFile();
    runcContainerExecutorConfig = verifyRuncConfig(configFile);

    configCgroupsPath = runcContainerExecutorConfig.getOciRuntimeConfig()
        .getLinux().getCgroupsPath();
    Assert.assertNull(configCgroupsPath);
  }


  @Test
  public void testDefaultROMounts()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    String roMount1 = tmpPath + "/foo";
    File roMountFile1 = new File(roMount1);
    roMountFile1.mkdirs();

    String roMount2 = tmpPath + "/bar";
    File roMountFile2 = new File(roMount2);
    roMountFile2.mkdirs();

    conf.setStrings(NM_RUNC_DEFAULT_RO_MOUNTS,
        roMount1 + ":" + roMount1 + "," + roMount2 + ":" + roMount2);

    List<String> roOptions = new ArrayList<>();
    roOptions.add("ro");
    roOptions.add("rbind");
    roOptions.add("rprivate");

    expectedMounts.add(new OCIMount(
        roMount1, "bind", roMount1, roOptions));
    expectedMounts.add(new OCIMount(
        roMount2, "bind", roMount2, roOptions));

    MockRuncContainerRuntime runtime = new MockRuncContainerRuntime(
        mockExecutor, mockCGroupsHandler);

    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());

    File configFile = captureRuncConfigFile();
    verifyRuncConfig(configFile);
  }

  @Test
  public void testDefaultROMountsInvalid() throws ContainerExecutionException {
    conf.setStrings(NM_RUNC_DEFAULT_RO_MOUNTS,
        "source,target");
    RuncContainerRuntime runtime = new MockRuncContainerRuntime(
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
    String rwMount1 = tmpPath + "/foo";
    File rwMountFile1 = new File(rwMount1);
    rwMountFile1.mkdirs();

    String rwMount2 = tmpPath + "/bar";
    File rwMountFile2 = new File(rwMount2);
    rwMountFile2.mkdirs();

    conf.setStrings(NM_RUNC_DEFAULT_RW_MOUNTS,
        rwMount1 + ":" + rwMount1 + "," + rwMount2 + ":" + rwMount2);

    List<String> rwOptions = new ArrayList<>();
    rwOptions.add("rw");
    rwOptions.add("rbind");
    rwOptions.add("rprivate");

    expectedMounts.add(new OCIMount(
        rwMount1, "bind", rwMount1, rwOptions));
    expectedMounts.add(new OCIMount(
        rwMount2, "bind", rwMount2, rwOptions));

    MockRuncContainerRuntime runtime = new MockRuncContainerRuntime(
        mockExecutor, mockCGroupsHandler);

    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());

    File configFile = captureRuncConfigFile();
    verifyRuncConfig(configFile);
  }

  @Test
  public void testDefaultRWMountsInvalid() throws ContainerExecutionException {
    conf.setStrings(NM_RUNC_DEFAULT_RW_MOUNTS,
        "source,target");
    RuncContainerRuntime runtime = new MockRuncContainerRuntime(
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
  public void testUserMounts()
      throws ContainerExecutionException, PrivilegedOperationException,
      IOException {
    String roMount = tmpPath + "/foo";
    File roMountFile = new File(roMount);
    roMountFile.mkdirs();

    String rwMount = tmpPath + "/bar";
    File rwMountFile = new File(rwMount);
    rwMountFile.mkdirs();

    env.put(ENV_RUNC_CONTAINER_MOUNTS,
        roMount + ":" + roMount + ":ro," + rwMount + ":" + rwMount + ":rw");

    List<String> rwOptions = new ArrayList<>();
    rwOptions.add("rw");
    rwOptions.add("rbind");
    rwOptions.add("rprivate");

    List<String> roOptions = new ArrayList<>();
    roOptions.add("ro");
    roOptions.add("rbind");
    roOptions.add("rprivate");

    expectedMounts.add(new OCIMount(
        roMount, "bind", roMount, roOptions));
    expectedMounts.add(new OCIMount(
        rwMount, "bind", rwMount, rwOptions));

    MockRuncContainerRuntime runtime = new MockRuncContainerRuntime(
        mockExecutor, mockCGroupsHandler);

    runtime.initialize(conf, nmContext);
    runtime.launchContainer(builder.build());

    File configFile = captureRuncConfigFile();
    verifyRuncConfig(configFile);
  }

  @Test
  public void testUserMountsInvalid() throws ContainerExecutionException {
    env.put(ENV_RUNC_CONTAINER_MOUNTS,
        "source:target");
    RuncContainerRuntime runtime = new MockRuncContainerRuntime(
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
  public void testUserMountsModeInvalid() throws ContainerExecutionException {
    env.put(ENV_RUNC_CONTAINER_MOUNTS,
        "source:target:other");
    RuncContainerRuntime runtime = new MockRuncContainerRuntime(
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
  public void testUserMountsModeNullInvalid()
      throws ContainerExecutionException {
    env.put(ENV_RUNC_CONTAINER_MOUNTS,
        "s\0ource:target:ro");
    RuncContainerRuntime runtime = new MockRuncContainerRuntime(
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
  public void testRuncHostnamePattern() throws Exception {
    String[] validNames = {"ab", "a.b.c.d", "a1-b.cd.ef", "0AB.", "C_D-"};

    String[] invalidNames = {"a", "a#.b.c", "-a.b.c", "a@b.c", "a/b/c"};

    for (String name : validNames) {
      RuncContainerRuntime.validateHostname(name);
    }

    for (String name : invalidNames) {
      try {
        RuncContainerRuntime.validateHostname(name);
        Assert.fail(name + " is an invalid hostname and should fail the regex");
      } catch (ContainerExecutionException ce) {
        continue;
      }
    }
  }

  @Test
  public void testValidRuncHostnameLength() throws Exception {
    String validLength = "example.test.site";
    RuncContainerRuntime.validateHostname(validLength);
  }

  @Test(expected = ContainerExecutionException.class)
  public void testInvalidRuncHostnameLength() throws Exception {
    String invalidLength =
        "exampleexampleexampleexampleexampleexampleexampleexample.test.site";
    RuncContainerRuntime.validateHostname(invalidLength);
  }

  @Test
  public void testGetLocalResources() throws Exception {
    RuncContainerRuntime runtime = new MockRuncContainerRuntime(
        mockExecutor, mockCGroupsHandler);
    runtime.initialize(conf, nmContext);
    runtime.getLocalResources(container);

    RuncRuntimeObject runtimeObject =
        captureRuncRuntimeObject(1);

    LocalResource testConfig = runtimeObject.getConfig();
    List<LocalResource> testLayers = runtimeObject.getOCILayers();

    Assert.assertEquals(config, testConfig);
    Assert.assertEquals(layers, testLayers);

  }
}
