/*
 *
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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.ImageManifest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCILayer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCIRuntimeConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCIRuntimeConfig.OCILinuxConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCIRuntimeConfig.OCIMount;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCIRuntimeConfig.OCIProcessConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncImageTagToManifestPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncManifestToResourcesPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalizedResource;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.volume.csi.ContainerVolumePublisher;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_IMAGE_TAG_TO_MANIFEST_PLUGIN;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_LAYER_MOUNTS_TO_KEEP;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_REAP_RUNC_LAYER_MOUNTS_INTERVAL;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_MANIFEST_TO_RESOURCES_PLUGIN;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_IMAGE_TAG_TO_MANIFEST_PLUGIN;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_LAYER_MOUNTS_TO_KEEP;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_MANIFEST_TO_RESOURCES_PLUGIN;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_REAP_RUNC_LAYER_MOUNTS_INTERVAL;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;
/**
 * <p>This class is an extension of {@link OCIContainerRuntime} that uses the
 * native {@code container-executor} binary via a
 * {@link PrivilegedOperationExecutor} instance to launch processes inside
 * Runc containers.</p>
 *
 * <p>The following environment variables are used to configure the Runc
 * engine:</p>
 *
 * <ul>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_TYPE} ultimately determines whether a
 *     runC container will be used. If the value is {@code runc}, a runC
 *     container will be used. Otherwise a regular process tree container will
 *     be used. This environment variable is checked by the
 *     {@link #isRuncContainerRequested} method, which is called by the
 *     {@link DelegatingLinuxContainerRuntime}.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_RUNC_IMAGE} names which image
 *     will be used to launch the Runc container.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_RUNC_MOUNTS} allows users to specify
 *     additional volume mounts for the runC container. The value of the
 *     environment variable should be a comma-separated list of mounts.
 *     All such mounts must be given as {@code source:dest[:mode]} and the mode
 *     must be "ro" (read-only) or "rw" (read-write) to specify the type of
 *     access being requested. If neither is specified, read-write will be
 *     assumed. The requested mounts will be validated by
 *     container-executor based on the values set in container-executor.cfg for
 *     {@code runc.allowed.ro-mounts} and {@code runc.allowed.rw-mounts}.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_RUNC_CONTAINER_HOSTNAME} sets the
 *     hostname to be used by the Runc container. If not specified, a
 *     hostname will be derived from the container ID and set as default
 *     hostname for networks other than 'host'.
 *   </li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RuncContainerRuntime extends OCIContainerRuntime {

  private static final Log LOG = LogFactory.getLog(
      RuncContainerRuntime.class);

  @InterfaceAudience.Private
  private static final String RUNTIME_TYPE = "RUNC";

  @InterfaceAudience.Private
  public static final String ENV_RUNC_CONTAINER_IMAGE =
      "YARN_CONTAINER_RUNTIME_RUNC_IMAGE";
  @InterfaceAudience.Private
  public static final String ENV_RUNC_CONTAINER_MOUNTS =
      "YARN_CONTAINER_RUNTIME_RUNC_MOUNTS";
  @InterfaceAudience.Private
  public static final String ENV_RUNC_CONTAINER_HOSTNAME =
      "YARN_CONTAINER_RUNTIME_RUNC_CONTAINER_HOSTNAME";

  @InterfaceAudience.Private
  public final static String ENV_RUNC_CONTAINER_PID_NAMESPACE =
      formatOciEnvKey(RUNTIME_TYPE, CONTAINER_PID_NAMESPACE_SUFFIX);
  @InterfaceAudience.Private
  public final static String ENV_RUNC_CONTAINER_RUN_PRIVILEGED_CONTAINER =
      formatOciEnvKey(RUNTIME_TYPE, RUN_PRIVILEGED_CONTAINER_SUFFIX);

  private Configuration conf;
  private Context nmContext;
  private PrivilegedOperationExecutor privilegedOperationExecutor;
  private CGroupsHandler cGroupsHandler;
  private RuncImageTagToManifestPlugin imageTagToManifestPlugin;
  private RuncManifestToResourcesPlugin manifestToResourcesPlugin;
  private ObjectMapper mapper;
  private String seccomp;
  private int layersToKeep;
  private String defaultRuncImage;
  private ScheduledExecutorService exec;
  private String seccompProfile;
  private Set<String> defaultROMounts = new HashSet<>();
  private Set<String> defaultRWMounts = new HashSet<>();
  private Set<String> allowedNetworks = new HashSet<>();
  private Set<String> allowedRuntimes = new HashSet<>();
  private AccessControlList privilegedContainersAcl;

  public RuncContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor) {
    this(privilegedOperationExecutor, ResourceHandlerModule
        .getCGroupsHandler());
  }

  //A constructor with an injected cGroupsHandler primarily used for testing.
  @VisibleForTesting
  public RuncContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor, CGroupsHandler cGroupsHandler) {
    super(privilegedOperationExecutor, cGroupsHandler);
    this.privilegedOperationExecutor = privilegedOperationExecutor;

    if (cGroupsHandler == null) {
      LOG.info("cGroupsHandler is null - cgroups not in use.");
    } else {
      this.cGroupsHandler = cGroupsHandler;
    }
  }

  @Override
  public void initialize(Configuration configuration, Context nmCtx)
      throws ContainerExecutionException {
    super.initialize(configuration, nmCtx);
    this.conf = configuration;
    this.nmContext = nmCtx;
    imageTagToManifestPlugin = chooseImageTagToManifestPlugin();
    imageTagToManifestPlugin.init(conf);
    manifestToResourcesPlugin = chooseManifestToResourcesPlugin();
    manifestToResourcesPlugin.init(conf);
    mapper = new ObjectMapper();
    defaultRuncImage = conf.get(YarnConfiguration.NM_RUNC_IMAGE_NAME);

    allowedNetworks.clear();
    allowedRuntimes.clear();

    allowedNetworks.addAll(Arrays.asList(
        conf.getTrimmedStrings(
        YarnConfiguration.NM_RUNC_ALLOWED_CONTAINER_NETWORKS,
        YarnConfiguration.DEFAULT_NM_RUNC_ALLOWED_CONTAINER_NETWORKS)));

    allowedRuntimes.addAll(Arrays.asList(
        conf.getTrimmedStrings(
        YarnConfiguration.NM_RUNC_ALLOWED_CONTAINER_RUNTIMES,
        YarnConfiguration.DEFAULT_NM_RUNC_ALLOWED_CONTAINER_RUNTIMES)));

    privilegedContainersAcl = new AccessControlList(conf.getTrimmed(
        YarnConfiguration.NM_RUNC_PRIVILEGED_CONTAINERS_ACL,
        YarnConfiguration.DEFAULT_NM_RUNC_PRIVILEGED_CONTAINERS_ACL));

    seccompProfile = conf.get(YarnConfiguration.NM_RUNC_SECCOMP_PROFILE);

    defaultROMounts.addAll(Arrays.asList(
        conf.getTrimmedStrings(
        YarnConfiguration.NM_RUNC_DEFAULT_RO_MOUNTS)));

    defaultRWMounts.addAll(Arrays.asList(
        conf.getTrimmedStrings(
        YarnConfiguration.NM_RUNC_DEFAULT_RW_MOUNTS)));

    try {
      //TODO Remove whitespace in seccomp that gets output to config.json
      if (seccompProfile != null) {
        seccomp = new String(Files.readAllBytes(Paths.get(seccompProfile)),
            StandardCharsets.UTF_8);
      }
    } catch (IOException ioe) {
      throw new ContainerExecutionException(ioe);
    }

    layersToKeep = conf.getInt(NM_RUNC_LAYER_MOUNTS_TO_KEEP,
        DEFAULT_NM_RUNC_LAYER_MOUNTS_TO_KEEP);

  }

  @Override
  public void start() {
    int reapRuncLayerMountsInterval =
        conf.getInt(NM_REAP_RUNC_LAYER_MOUNTS_INTERVAL,
        DEFAULT_NM_REAP_RUNC_LAYER_MOUNTS_INTERVAL);
    exec = HadoopExecutors.newScheduledThreadPool(1);
    exec.scheduleAtFixedRate(
        new Runnable() {
          @Override
          public void run() {
            try {
              PrivilegedOperation launchOp = new PrivilegedOperation(
                  PrivilegedOperation.OperationType.REAP_RUNC_LAYER_MOUNTS);
              launchOp.appendArgs(Integer.toString(layersToKeep));
              try {
                String stdout = privilegedOperationExecutor
                    .executePrivilegedOperation(null,
                    launchOp, null, null, false, false);
                if(stdout != null) {
                  LOG.info("Reap layer mounts thread: " + stdout);
                }
              } catch (PrivilegedOperationException e) {
                LOG.warn("Failed to reap old runc layer mounts", e);
              }
            } catch (Exception e) {
              LOG.warn("Reap layer mount thread caught an exception: ", e);
            }
          }
        }, 0, reapRuncLayerMountsInterval, TimeUnit.SECONDS);
    imageTagToManifestPlugin.start();
    manifestToResourcesPlugin.start();
  }

  @Override
  public void stop() {
    exec.shutdownNow();
    imageTagToManifestPlugin.stop();
    manifestToResourcesPlugin.stop();
  }

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    List<String> env = new ArrayList<>();
    Container container = ctx.getContainer();
    String runAsUser = ctx.getExecutionAttribute(RUN_AS_USER);
    String user = ctx.getExecutionAttribute(USER);
    ContainerId containerId = container.getContainerId();
    ApplicationId appId = containerId.getApplicationAttemptId()
        .getApplicationId();

    Map<String, String> environment = container.getLaunchContext()
        .getEnvironment();
    ArrayList<OCIMount> mounts = new ArrayList<>();
    ArrayList<OCILayer> layers = new ArrayList<>();
    String hostname = environment.get(ENV_RUNC_CONTAINER_HOSTNAME);

    validateHostname(hostname);

    String containerIdStr = containerId.toString();
    String applicationId = appId.toString();
    Path containerWorkDir = ctx.getExecutionAttribute(CONTAINER_WORK_DIR);

    RuncRuntimeObject runcRuntimeObject =
        container.getContainerRuntimeData(RuncRuntimeObject.class);
    List<LocalResource> layerResources = runcRuntimeObject.getOCILayers();

    ResourceLocalizationService localizationService =
        nmContext.getContainerManager().getResourceLocalizationService();

    List<String> args = new ArrayList<>();

    try {
      try {
        LocalResource rsrc = runcRuntimeObject.getConfig();
        LocalResourceRequest req = new LocalResourceRequest(rsrc);
        LocalizedResource localRsrc = localizationService
            .getLocalizedResource(req, user, appId);
        if (localRsrc == null) {
          throw new ContainerExecutionException("Could not successfully " +
              "localize layers. rsrc: " + rsrc.getResource().getFile());
        }

        File file = new File(localRsrc.getLocalPath().toString());
        List<String> imageEnv = extractImageEnv(file);
        if (imageEnv != null && !imageEnv.isEmpty()) {
          env.addAll(imageEnv);
        }
        List<String> entrypoint = extractImageEntrypoint(file);
        if (entrypoint != null && !entrypoint.isEmpty()) {
          args.addAll(entrypoint);
        }
      } catch (IOException ioe) {
        throw new ContainerExecutionException(ioe);
      }

      for (LocalResource rsrc : layerResources) {
        LocalResourceRequest req = new LocalResourceRequest(rsrc);
        LocalizedResource localRsrc = localizationService
            .getLocalizedResource(req, user, appId);

        OCILayer layer = new OCILayer("application/vnd.squashfs",
            localRsrc.getLocalPath().toString());
        layers.add(layer);
      }
    } catch (URISyntaxException e) {
      throw new ContainerExecutionException(e);
    }

    setContainerMounts(mounts, ctx, containerWorkDir, environment);

    String resourcesOpts = ctx.getExecutionAttribute(RESOURCES_OPTIONS);

    Path nmPrivateContainerScriptPath = ctx.getExecutionAttribute(
        NM_PRIVATE_CONTAINER_SCRIPT_PATH);

    Path nmPrivateTokensPath =
        ctx.getExecutionAttribute(NM_PRIVATE_TOKENS_PATH);

    int cpuShares = container.getResource().getVirtualCores();

    // Zero sets to default of 1024.  2 is the minimum value otherwise
    if (cpuShares < 2) {
      cpuShares = 2;
    }

    Path launchDst =
        new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);

    args.add("bash");
    args.add(launchDst.toUri().getPath());

    String cgroupPath = getCgroupPath(resourcesOpts, "runc-" + containerIdStr);

    String pidFile = ctx.getExecutionAttribute(PID_FILE_PATH).toString();

    @SuppressWarnings("unchecked")
    List<String> localDirs = ctx.getExecutionAttribute(LOCAL_DIRS);
    @SuppressWarnings("unchecked")
    List<String> logDirs = ctx.getExecutionAttribute(LOG_DIRS);

    Path keystorePath = ctx.getExecutionAttribute(NM_PRIVATE_KEYSTORE_PATH);
    Path truststorePath = ctx.getExecutionAttribute(NM_PRIVATE_TRUSTSTORE_PATH);

    int https = 0;
    String keystore = null;
    String truststore = null;

    if (keystorePath != null && truststorePath != null) {
      https = 1;
      keystore = keystorePath.toUri().getPath();
      truststore = truststorePath.toUri().getPath();
    }

    OCIProcessConfig processConfig = createOCIProcessConfig(
        containerWorkDir.toString(), env, args);
    OCILinuxConfig linuxConfig = createOCILinuxConfig(cpuShares,
        cgroupPath, seccomp);

    OCIRuntimeConfig ociRuntimeConfig = new OCIRuntimeConfig(null, mounts,
        processConfig, hostname, null, null, linuxConfig);

    RuncContainerExecutorConfig runcContainerExecutorConfig =
        createRuncContainerExecutorConfig(runAsUser, user, containerIdStr,
        applicationId, pidFile, nmPrivateContainerScriptPath.toString(),
        nmPrivateTokensPath.toString(), https, keystore, truststore,
        localDirs, logDirs, layers,
        ociRuntimeConfig);

    String commandFile = writeCommandToFile(
        runcContainerExecutorConfig, container);
    PrivilegedOperation launchOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.RUN_RUNC_CONTAINER);

    launchOp.appendArgs(commandFile);

    try {
      privilegedOperationExecutor.executePrivilegedOperation(null,
          launchOp, null, null, false, false);
    } catch (PrivilegedOperationException e) {
      LOG.info("Launch container failed: ", e);
      try {
        LOG.debug("config.json used: " +
            mapper.writeValueAsString(runcContainerExecutorConfig));
      } catch (IOException ioe) {
        LOG.info("Json Generation Exception", ioe);
      }

      throw new ContainerExecutionException("Launch container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  private String getCgroupPath(String resourcesOptions, String containerIdStr) {
    if (cGroupsHandler == null) {
      LOG.debug("cGroupsHandler is null. cgroups are not in use. nothing to"
          + " do.");
      return null;
    }

    if (resourcesOptions.equals(
        (PrivilegedOperation.CGROUP_ARG_PREFIX + PrivilegedOperation
        .CGROUP_ARG_NO_TASKS))) {
      LOG.debug("no resource restrictions specified. not using runc's "
          + "cgroup options");
    } else {
      LOG.debug("using runc's cgroups options");

      String cGroupPath = "/" + cGroupsHandler.getRelativePathForCGroup(
          containerIdStr);

      LOG.debug("using cgroup parent: " + cGroupPath);

      return cGroupPath;
    }
    return null;
  }

  private void addUserMounts(List<OCIMount> mounts,
      Map<String, String> environment,
      Map<Path, List<String>> localizedResources)
      throws ContainerExecutionException {
    if (environment.containsKey(ENV_RUNC_CONTAINER_MOUNTS)) {
      Matcher parsedMounts = USER_MOUNT_PATTERN.matcher(
          environment.get(ENV_RUNC_CONTAINER_MOUNTS));
      if (!parsedMounts.find()) {
        throw new ContainerExecutionException(
            "Unable to parse user supplied mount list: "
            + environment.get(ENV_RUNC_CONTAINER_MOUNTS));
      }
      parsedMounts.reset();
      long mountCount = 0;
      while (parsedMounts.find()) {
        mountCount++;
        String src = parsedMounts.group(1);
        java.nio.file.Path srcPath = java.nio.file.Paths.get(src);
        if (!srcPath.isAbsolute()) {
          src = mountReadOnlyPath(src, localizedResources);
        }
        String dst = parsedMounts.group(2);
        String mode = parsedMounts.group(4);
        boolean isReadWrite;
        if (mode == null) {
          isReadWrite = true;
        } else if (mode.equals("rw")) {
          isReadWrite = true;
        } else if (mode.equals("ro")) {
          isReadWrite = false;
        } else {
          throw new ContainerExecutionException(
              "Unable to parse mode of some mounts in user supplied "
              + "mount list: "
              + environment.get(ENV_RUNC_CONTAINER_MOUNTS));
        }
        addRuncMountLocation(mounts, src, dst, false, isReadWrite);
      }
      long commaCount = environment.get(ENV_RUNC_CONTAINER_MOUNTS).chars()
          .filter(c -> c == ',').count();
      if (mountCount != commaCount + 1) {
        // this means the matcher skipped an improperly formatted mount
        throw new ContainerExecutionException(
            "Unable to parse some mounts in user supplied mount list: "
            + environment.get(ENV_RUNC_CONTAINER_MOUNTS));
      }
    }
  }

  private void addDefaultMountLocation(List<OCIMount> mounts,
      Set<String> defaultMounts, boolean createSource, boolean isReadWrite)
      throws ContainerExecutionException {
    if(defaultMounts != null && !defaultMounts.isEmpty()) {
      for (String mount : defaultMounts) {
        String[] dir = StringUtils.split(mount, ':');
        if (dir.length != 2) {
          throw new ContainerExecutionException("Invalid mount : " +
              mount);
        }
        String src = dir[0];
        String dst = dir[1];
        addRuncMountLocation(mounts, src, dst, createSource, isReadWrite);
      }
    }
  }

  private void addRuncMountLocation(List<OCIMount> mounts, String srcPath,
      String dstPath, boolean createSource, boolean isReadWrite) {
    if (!createSource) {
      boolean sourceExists = new File(srcPath).exists();
      if (!sourceExists) {
        return;
      }
    }

    ArrayList<String> options = new ArrayList<>();
    if (isReadWrite) {
      options.add("rw");
    } else {
      options.add("ro");
    }
    options.add("rbind");
    options.add("rprivate");
    mounts.add(new OCIMount(dstPath, "bind", srcPath, options));
  }

  private void addAllRuncMountLocations(List<OCIMount> mounts,
      List<String> paths, boolean createSource, boolean isReadWrite) {
    for (String dir: paths) {
      this.addRuncMountLocation(mounts, dir, dir, createSource, isReadWrite);
    }
  }

  public Map<String, LocalResource> getLocalResources(
      Container container) throws IOException {
    Map<String, LocalResource> containerLocalRsrc =
        container.getLaunchContext().getLocalResources();
    long layerCount = 0;
    Map<String, String> environment =
        container.getLaunchContext().getEnvironment();
    String imageName = environment.get(ENV_RUNC_CONTAINER_IMAGE);
    if (imageName == null || imageName.isEmpty()) {
      environment.put(ENV_RUNC_CONTAINER_IMAGE,
          defaultRuncImage);
      imageName = defaultRuncImage;
    }

    ImageManifest manifest =
        imageTagToManifestPlugin.getManifestFromImageTag(imageName);
    LocalResource config =
        manifestToResourcesPlugin.getConfigResource(manifest);
    List<LocalResource> layers =
        manifestToResourcesPlugin.getLayerResources(manifest);

    RuncRuntimeObject runcRuntimeObject =
        new RuncRuntimeObject(config, layers);
    container.setContainerRuntimeData(runcRuntimeObject);

    for (LocalResource localRsrc : layers) {
      while(containerLocalRsrc.putIfAbsent("runc-layer" +
          Long.toString(layerCount), localRsrc) != null) {
        layerCount++;
      }
    }

    while(containerLocalRsrc.putIfAbsent("runc-config" +
      Long.toString(layerCount), config) != null) {
      layerCount++;
    }

    return containerLocalRsrc;
  }

  protected RuncImageTagToManifestPlugin chooseImageTagToManifestPlugin()
      throws ContainerExecutionException {
    String pluginName =
        conf.get(NM_RUNC_IMAGE_TAG_TO_MANIFEST_PLUGIN,
        DEFAULT_NM_RUNC_IMAGE_TAG_TO_MANIFEST_PLUGIN);
    RuncImageTagToManifestPlugin runcImageTagToManifestPlugin;
    try {
      Class<?> clazz = Class.forName(pluginName);
      runcImageTagToManifestPlugin =
          (RuncImageTagToManifestPlugin) clazz.newInstance();
    } catch (Exception e) {
      throw new ContainerExecutionException(e);
    }
    return runcImageTagToManifestPlugin;
  }

  protected RuncManifestToResourcesPlugin chooseManifestToResourcesPlugin()
      throws ContainerExecutionException {
    String pluginName =
        conf.get(NM_RUNC_MANIFEST_TO_RESOURCES_PLUGIN,
        DEFAULT_NM_RUNC_MANIFEST_TO_RESOURCES_PLUGIN);
    LOG.info("pluginName = " + pluginName);
    RuncManifestToResourcesPlugin runcManifestToResourcesPlugin;
    try {
      Class<?> clazz = Class.forName(pluginName);
      runcManifestToResourcesPlugin =
          (RuncManifestToResourcesPlugin) clazz.newInstance();
    } catch (Exception e) {
      throw new ContainerExecutionException(e);
    }
    return runcManifestToResourcesPlugin;
  }

  @SuppressWarnings("unchecked")
  protected List<String> extractImageEnv(File config) throws IOException {
    JsonNode node = mapper.readTree(config);
    JsonNode envNode = node.path("config").path("Env");
    if (envNode.isMissingNode()) {
      return null;
    }
    return mapper.readValue(envNode, List.class);
  }

  @SuppressWarnings("unchecked")
  protected List<String> extractImageEntrypoint(File config)
      throws IOException {
    JsonNode node = mapper.readTree(config);
    JsonNode entrypointNode = node.path("config").path("Entrypoint");
    if (entrypointNode.isMissingNode()) {
      return null;
    }
    return mapper.readValue(entrypointNode, List.class);
  }

  private RuncContainerExecutorConfig createRuncContainerExecutorConfig(
      String runAsUser, String username, String containerId,
      String applicationId, String pidFile,
      String containerScriptPath, String containerCredentialsPath,
      int https, String keystorePath, String truststorePath,
      List<String> localDirs, List<String> logDirs,
      List<OCILayer> layers, OCIRuntimeConfig ociRuntimeConfig) {

    return new RuncContainerExecutorConfig(runAsUser, username, containerId,
        applicationId, pidFile, containerScriptPath, containerCredentialsPath,
        https, keystorePath, truststorePath,
        localDirs, logDirs, layers, layersToKeep, ociRuntimeConfig);
  }

  private OCIProcessConfig createOCIProcessConfig(String cwd,
      List<String> env, List<String> args) {
    return new OCIProcessConfig(false, null, cwd, env,
        args, null, null, null, false, 0, null, null);
  }

  private OCILinuxConfig createOCILinuxConfig(long cpuShares,
      String cgroupsPath, String seccompProf) {
    OCILinuxConfig.Resources.CPU cgroupCPU =
        new OCILinuxConfig.Resources.CPU(cpuShares, 0, 0, 0, 0,
        null, null);
    OCILinuxConfig.Resources cgroupResources =
        new OCILinuxConfig.Resources(null, null, cgroupCPU, null, null, null,
        null, null);

    return new OCILinuxConfig(null, null, null, null,
        cgroupsPath, cgroupResources, null, null, seccompProf, null, null,
        null, null);
  }

  private void setContainerMounts(ArrayList<OCIMount> mounts,
      ContainerRuntimeContext ctx, Path containerWorkDir,
      Map<String, String> environment)
      throws ContainerExecutionException {
    @SuppressWarnings("unchecked")
    List<String> filecacheDirs = ctx.getExecutionAttribute(FILECACHE_DIRS);
    @SuppressWarnings("unchecked")
    List<String> containerLogDirs = ctx.getExecutionAttribute(
        CONTAINER_LOG_DIRS);
    @SuppressWarnings("unchecked")
    List<String> userFilecacheDirs =
        ctx.getExecutionAttribute(USER_FILECACHE_DIRS);
    @SuppressWarnings("unchecked")
    List<String> applicationLocalDirs =
        ctx.getExecutionAttribute(APPLICATION_LOCAL_DIRS);
    @SuppressWarnings("unchecked")
    Map<Path, List<String>> localizedResources = ctx.getExecutionAttribute(
        LOCALIZED_RESOURCES);

    addRuncMountLocation(mounts, containerWorkDir.toString() +
        "/private_slash_tmp", "/tmp", true, true);
    addRuncMountLocation(mounts, containerWorkDir.toString() +
        "/private_var_slash_tmp", "/var/tmp", true, true);

    addAllRuncMountLocations(mounts, containerLogDirs, true, true);
    addAllRuncMountLocations(mounts, applicationLocalDirs, true, true);
    addAllRuncMountLocations(mounts, filecacheDirs, false, false);
    addAllRuncMountLocations(mounts, userFilecacheDirs, false, false);
    addDefaultMountLocation(mounts, defaultROMounts, false, false);
    addDefaultMountLocation(mounts, defaultRWMounts, false, true);
    addUserMounts(mounts, environment, localizedResources);
  }

  public String writeCommandToFile(
      RuncContainerExecutorConfig runcContainerExecutorConfig,
      Container container)
      throws ContainerExecutionException {
    ContainerId containerId = container.getContainerId();
    String filePrefix = containerId.toString();
    ApplicationId appId = containerId.getApplicationAttemptId()
        .getApplicationId();
    File commandFile;
    try {
      File cmdDir = null;

      if(nmContext != null && nmContext.getLocalDirsHandler() != null) {
        String cmdDirStr = nmContext.getLocalDirsHandler().getLocalPathForWrite(
            ResourceLocalizationService.NM_PRIVATE_DIR + Path.SEPARATOR +
            appId + Path.SEPARATOR + filePrefix + Path.SEPARATOR).toString();
        cmdDir = new File(cmdDirStr);
        if (!cmdDir.mkdirs() && !cmdDir.exists()) {
          throw new IOException("Cannot create container private directory "
              + cmdDir);
        }
      }
      commandFile = new File(cmdDir + "/runc-config.json");

      try {
        mapper.writeValue(commandFile, runcContainerExecutorConfig);
      } catch (IOException ioe) {
        throw new ContainerExecutionException(ioe);
      }

      return commandFile.getAbsolutePath();
    } catch (IOException e) {
      LOG.warn("Unable to write runc config.json to temporary file!");
      throw new ContainerExecutionException(e);
    }
  }

  public String getExposedPorts(Container container) {
    return null;
  }

  public String[] getIpAndHost(Container container) {
    return null;
  }

  public IOStreamPair execContainer(ContainerExecContext ctx)
      throws ContainerExecutionException {
    return null;
  }

  public void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
  }

  public void relaunchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
  }

  /**
   * Return whether the given environment variables indicate that the operation
   * is requesting a Runc container.  If the environment contains a key
   * called {@code YARN_CONTAINER_RUNTIME_TYPE} whose value is {@code runc},
   * this method will return true.  Otherwise it will return false.
   *
   * @param daemonConf the NodeManager daemon configuration
   * @param env the environment variable settings for the operation
   * @return whether a Runc container is requested
   */
  public static boolean isRuncContainerRequested(Configuration daemonConf,
      Map<String, String> env) {
    String type = (env == null)
        ? null : env.get(ContainerRuntimeConstants.ENV_CONTAINER_TYPE);
    if (type == null) {
      type = daemonConf.get(YarnConfiguration.LINUX_CONTAINER_RUNTIME_TYPE);
    }
    return type != null && type.equals(
        ContainerRuntimeConstants.CONTAINER_RUNTIME_RUNC);
  }


  @Override
  public boolean isRuntimeRequested(Map<String, String> env) {
    return isRuncContainerRequested(conf, env);
  }

  @Override
  public void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    ContainerExecutor.Signal signal = ctx.getExecutionAttribute(SIGNAL);
    Container container = ctx.getContainer();

    if (signal == ContainerExecutor.Signal.KILL ||
        signal == ContainerExecutor.Signal.TERM) {
      ContainerVolumePublisher publisher = new ContainerVolumePublisher(
          container, container.getCsiVolumesRootDir(), this);
      try {
        publisher.unpublishVolumes();
      } catch (YarnException | IOException e) {
        throw new ContainerExecutionException(e);
      }
    }

    PrivilegedOperation signalOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.SIGNAL_CONTAINER);

    signalOp.appendArgs(ctx.getExecutionAttribute(RUN_AS_USER),
        ctx.getExecutionAttribute(USER),
        Integer.toString(PrivilegedOperation.RunAsUserCommand
        .SIGNAL_CONTAINER.getValue()),
        ctx.getExecutionAttribute(PID),
        Integer.toString(signal.getValue()));

    //Some failures here are acceptable. Let the calling executor decide.
    signalOp.disableFailureLogging();

    try {
      PrivilegedOperationExecutor executor = PrivilegedOperationExecutor
          .getInstance(conf);

      executor.executePrivilegedOperation(null,
          signalOp, null, null, false, false);
    } catch (PrivilegedOperationException e) {
      //Don't log the failure here. Some kinds of signaling failures are
      // acceptable. Let the calling executor decide what to do.
      throw new ContainerExecutionException("Signal container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @InterfaceStability.Unstable
  static class RuncRuntimeObject {
    private final List<LocalResource> layers;
    private final LocalResource config;

    RuncRuntimeObject(LocalResource config,
        List<LocalResource> layers) {
      this.config = config;
      this.layers = layers;
    }

    public LocalResource getConfig() {
      return this.config;
    }

    public List<LocalResource> getOCILayers() {
      return this.layers;
    }
  }

  boolean getHostPidNamespaceEnabled() {
    return conf.getBoolean(
        YarnConfiguration.NM_RUNC_ALLOW_HOST_PID_NAMESPACE,
        YarnConfiguration.DEFAULT_NM_RUNC_ALLOW_HOST_PID_NAMESPACE);
  }

  boolean getPrivilegedContainersEnabledOnCluster() {
    return conf.getBoolean(
        YarnConfiguration.NM_RUNC_ALLOW_PRIVILEGED_CONTAINERS,
        YarnConfiguration.DEFAULT_NM_RUNC_ALLOW_PRIVILEGED_CONTAINERS);
  }

  Set<String> getAllowedNetworks() {
    return allowedNetworks;
  }

  Set<String> getAllowedRuntimes() {
    return allowedRuntimes;
  }

  AccessControlList getPrivilegedContainersAcl() {
    return privilegedContainersAcl;
  }

  String getEnvOciContainerPidNamespace() {
    return ENV_RUNC_CONTAINER_PID_NAMESPACE;
  }

  String getEnvOciContainerRunPrivilegedContainer() {
    return ENV_RUNC_CONTAINER_RUN_PRIVILEGED_CONTAINER;
  }
}
