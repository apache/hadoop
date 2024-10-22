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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerCommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerExecCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerKillCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerPullCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRmCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerStartCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.volume.csi.ContainerVolumePublisher;
import org.apache.hadoop.yarn.util.DockerClientConfigHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerClient;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerInspectCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;

/**
 * <p>This class is an extension of {@link OCIContainerRuntime} that uses the
 * native {@code container-executor} binary via a
 * {@link PrivilegedOperationExecutor} instance to launch processes inside
 * Docker containers.</p>
 *
 * <p>The following environment variables are used to configure the Docker
 * engine:</p>
 *
 * <ul>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_TYPE} ultimately determines whether a
 *     Docker container will be used. If the value is {@code docker}, a Docker
 *     container will be used. Otherwise a regular process tree container will
 *     be used. This environment variable is checked by the
 *     {@link #isDockerContainerRequested} method, which is called by the
 *     {@link DelegatingLinuxContainerRuntime}.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_IMAGE} names which image
 *     will be used to launch the Docker container.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE} controls
 *     whether the Docker container's default command is overridden.  When set
 *     to {@code true}, the Docker container's command will be
 *     {@code bash <path_to_launch_script>}. When unset or set to {@code false}
 *     the Docker container's default command is used.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK} sets the
 *     network type to be used by the Docker container. It must be a valid
 *     value as determined by the
 *     {@code yarn.nodemanager.runtime.linux.docker.allowed-container-networks}
 *     property.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_PORTS_MAPPING} allows users to
 *     specify ports mapping for the bridge network Docker container. The value
 *     of the environment variable should be a comma-separated list of ports
 *     mapping. It's the same to "-p" option for the Docker run command. If the
 *     value is empty, "-P" will be added.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_PID_NAMESPACE}
 *     controls which PID namespace will be used by the Docker container. By
 *     default, each Docker container has its own PID namespace. To share the
 *     namespace of the host, the
 *     {@code yarn.nodemanager.runtime.linux.docker.host-pid-namespace.allowed}
 *     property must be set to {@code true}. If the host PID namespace is
 *     allowed and this environment variable is set to {@code host}, the
 *     Docker container will share the host's PID namespace. No other value is
 *     allowed.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_HOSTNAME} sets the
 *     hostname to be used by the Docker container. If not specified, a
 *     hostname will be derived from the container ID and set as default
 *     hostname for networks other than 'host'.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER}
 *     controls whether the Docker container is a privileged container. In order
 *     to use privileged containers, the
 *     {@code yarn.nodemanager.runtime.linux.docker.privileged-containers.allowed}
 *     property must be set to {@code true}, and the application owner must
 *     appear in the value of the
 *     {@code yarn.nodemanager.runtime.linux.docker.privileged-containers.acl}
 *     property. If this environment variable is set to {@code true}, a
 *     privileged Docker container will be used if allowed. No other value is
 *     allowed, so the environment variable should be left unset rather than
 *     setting it to false.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS} allows users to specify
 *     additional volume mounts for the Docker container. The value of the
 *     environment variable should be a comma-separated list of mounts.
 *     All such mounts must be given as {@code source:dest[:mode]} and the mode
 *     must be "ro" (read-only) or "rw" (read-write) to specify the type of
 *     access being requested. If neither is specified, read-write will be
 *     assumed. The mode may include a bind propagation option. In that case,
 *     the mode should either be of the form [option], rw+[option], or
 *     ro+[option]. Valid bind propagation options are shared, rshared, slave,
 *     rslave, private, and rprivate. The requested mounts will be validated by
 *     container-executor based on the values set in container-executor.cfg for
 *     {@code docker.allowed.ro-mounts} and {@code docker.allowed.rw-mounts}.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_TMPFS_MOUNTS} allows users to
 *     specify additional tmpfs mounts for the Docker container. The value of
 *     the environment variable should be a comma-separated list of mounts.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_DELAYED_REMOVAL} allows a user
 *     to request delayed deletion of the Docker containers on a per
 *     container basis. If true, Docker containers will not be removed until
 *     the duration defined by {@code yarn.nodemanager.delete.debug-delay-sec}
 *     has elapsed. Administrators can disable this feature through the
 *     yarn-site property
 *     {@code yarn.nodemanager.runtime.linux.docker.delayed-removal.allowed}.
 *     This feature is disabled by default. When this feature is disabled or set
 *     to false, the container will be removed as soon as it exits.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_YARN_SYSFS_ENABLE} allows export yarn
 *     service json to docker container.  This feature is disabled by default.
 *     When this feature is set, app.json will be available in
 *     /hadoop/yarn/sysfs/app.json.
 *   </li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DockerLinuxContainerRuntime extends OCIContainerRuntime {
  private static final Logger LOG =
      LoggerFactory.getLogger(DockerLinuxContainerRuntime.class);

  // This validates that the image is a proper docker image
  public static final String DOCKER_IMAGE_PATTERN =
      "^(([a-zA-Z0-9.-]+)(:\\d+)?/)?([a-z0-9_./-]+)(:[\\w.-]+)?$";
  private static final Pattern dockerImagePattern =
      Pattern.compile(DOCKER_IMAGE_PATTERN);

  private static final Pattern DOCKER_DIGEST_PATTERN = Pattern.compile("^sha256:[a-z0-9]{12,64}$");

  private static final String DEFAULT_PROCFS = "/proc";

  @InterfaceAudience.Private
  private static final String RUNTIME_TYPE = "DOCKER";

  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_IMAGE =
      "YARN_CONTAINER_RUNTIME_DOCKER_IMAGE";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_CLIENT_CONFIG =
      "YARN_CONTAINER_RUNTIME_DOCKER_CLIENT_CONFIG";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_NETWORK =
      "YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_HOSTNAME =
      "YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_HOSTNAME";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_MOUNTS =
      "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_TMPFS_MOUNTS =
      "YARN_CONTAINER_RUNTIME_DOCKER_TMPFS_MOUNTS";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_DELAYED_REMOVAL =
      "YARN_CONTAINER_RUNTIME_DOCKER_DELAYED_REMOVAL";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_PORTS_MAPPING =
      "YARN_CONTAINER_RUNTIME_DOCKER_PORTS_MAPPING";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_YARN_SYSFS =
      "YARN_CONTAINER_RUNTIME_YARN_SYSFS_ENABLE";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_DOCKER_RUNTIME =
      "YARN_CONTAINER_RUNTIME_DOCKER_RUNTIME";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_DOCKER_SERVICE_MODE =
      "YARN_CONTAINER_RUNTIME_DOCKER_SERVICE_MODE";

  @InterfaceAudience.Private
  public final static String ENV_OCI_CONTAINER_PID_NAMESPACE =
      formatOciEnvKey(RUNTIME_TYPE, CONTAINER_PID_NAMESPACE_SUFFIX);
  @InterfaceAudience.Private
  public final static String ENV_OCI_CONTAINER_RUN_PRIVILEGED_CONTAINER =
      formatOciEnvKey(RUNTIME_TYPE, RUN_PRIVILEGED_CONTAINER_SUFFIX);

  private Configuration conf;
  private Context nmContext;
  private DockerClient dockerClient;
  private PrivilegedOperationExecutor privilegedOperationExecutor;
  private String defaultImageName;
  private Boolean defaultImageUpdate;
  private Set<String> allowedNetworks = new HashSet<>();
  private Set<String> allowedRuntimes = new HashSet<>();
  private String defaultNetwork;
  private CGroupsHandler cGroupsHandler;
  private AccessControlList privilegedContainersAcl;
  private boolean enableUserReMapping;
  private int userRemappingUidThreshold;
  private int userRemappingGidThreshold;
  private Set<String> capabilities;
  private boolean delayedRemovalAllowed;
  private Set<String> defaultROMounts = new HashSet<>();
  private Set<String> defaultRWMounts = new HashSet<>();
  private Set<String> defaultTmpfsMounts = new HashSet<>();

  /**
   * Return whether the given environment variables indicate that the operation
   * is requesting a Docker container.  If the environment contains a key
   * called {@code YARN_CONTAINER_RUNTIME_TYPE} whose value is {@code docker},
   * this method will return true.  Otherwise it will return false.
   *
   * @param daemonConf the NodeManager daemon configuration
   * @param env the environment variable settings for the operation
   * @return whether a Docker container is requested
   */
  public static boolean isDockerContainerRequested(Configuration daemonConf,
      Map<String, String> env) {
    String type = (env == null)
        ? null : env.get(ContainerRuntimeConstants.ENV_CONTAINER_TYPE);
    if (type == null) {
      type = daemonConf.get(YarnConfiguration.LINUX_CONTAINER_RUNTIME_TYPE);
    }
    return type != null && type.equals(
        ContainerRuntimeConstants.CONTAINER_RUNTIME_DOCKER);
  }

  /**
   * Create an instance using the given {@link PrivilegedOperationExecutor}
   * instance for performing operations.
   *
   * @param privilegedOperationExecutor the {@link PrivilegedOperationExecutor}
   * instance
   */
  public DockerLinuxContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor) {
    this(privilegedOperationExecutor,
        ResourceHandlerModule.getCGroupsHandler());
  }

  /**
   * Create an instance using the given {@link PrivilegedOperationExecutor}
   * instance for performing operations and the given {@link CGroupsHandler}
   * instance. This constructor is intended for use in testing.
   *  @param privilegedOperationExecutor the {@link PrivilegedOperationExecutor}
   * instance
   * @param cGroupsHandler the {@link CGroupsHandler} instance
   */
  @VisibleForTesting
  public DockerLinuxContainerRuntime(
      PrivilegedOperationExecutor privilegedOperationExecutor,
      CGroupsHandler cGroupsHandler) {
    super(privilegedOperationExecutor, cGroupsHandler);

    this.privilegedOperationExecutor = privilegedOperationExecutor;

    if (cGroupsHandler == null) {
      LOG.info("cGroupsHandler is null - cgroups not in use.");
    } else {
      this.cGroupsHandler = cGroupsHandler;
    }
  }

  @Override
  public void initialize(Configuration conf, Context nmContext)
      throws ContainerExecutionException {
    super.initialize(conf, nmContext);
    this.nmContext = nmContext;
    this.conf = conf;

    dockerClient = new DockerClient();
    allowedNetworks.clear();
    allowedRuntimes.clear();
    defaultROMounts.clear();
    defaultRWMounts.clear();
    defaultTmpfsMounts.clear();
    defaultImageName = conf.getTrimmed(
        YarnConfiguration.NM_DOCKER_IMAGE_NAME, "");
    defaultImageUpdate = conf.getBoolean(
        YarnConfiguration.NM_DOCKER_IMAGE_UPDATE, false);
    allowedNetworks.addAll(Arrays.asList(
        conf.getTrimmedStrings(
            YarnConfiguration.NM_DOCKER_ALLOWED_CONTAINER_NETWORKS,
            YarnConfiguration.DEFAULT_NM_DOCKER_ALLOWED_CONTAINER_NETWORKS)));
    defaultNetwork = conf.getTrimmed(
        YarnConfiguration.NM_DOCKER_DEFAULT_CONTAINER_NETWORK,
        YarnConfiguration.DEFAULT_NM_DOCKER_DEFAULT_CONTAINER_NETWORK);
    allowedRuntimes.addAll(Arrays.asList(
        conf.getTrimmedStrings(
            YarnConfiguration.NM_DOCKER_ALLOWED_CONTAINER_RUNTIMES,
            YarnConfiguration.DEFAULT_NM_DOCKER_ALLOWED_CONTAINER_RUNTIMES)));

    if(!allowedNetworks.contains(defaultNetwork)) {
      String message = "Default network: " + defaultNetwork
          + " is not in the set of allowed networks: " + allowedNetworks;

      if (LOG.isWarnEnabled()) {
        LOG.warn(message + ". Please check configuration");
      }

      throw new ContainerExecutionException(message);
    }

    // initialize csi adaptors if necessary
    initiateCsiClients(conf);

    privilegedContainersAcl = new AccessControlList(conf.getTrimmed(
        YarnConfiguration.NM_DOCKER_PRIVILEGED_CONTAINERS_ACL,
        YarnConfiguration.DEFAULT_NM_DOCKER_PRIVILEGED_CONTAINERS_ACL));

    enableUserReMapping = conf.getBoolean(
      YarnConfiguration.NM_DOCKER_ENABLE_USER_REMAPPING,
      YarnConfiguration.DEFAULT_NM_DOCKER_ENABLE_USER_REMAPPING);

    userRemappingUidThreshold = conf.getInt(
      YarnConfiguration.NM_DOCKER_USER_REMAPPING_UID_THRESHOLD,
      YarnConfiguration.DEFAULT_NM_DOCKER_USER_REMAPPING_UID_THRESHOLD);

    userRemappingGidThreshold = conf.getInt(
      YarnConfiguration.NM_DOCKER_USER_REMAPPING_GID_THRESHOLD,
      YarnConfiguration.DEFAULT_NM_DOCKER_USER_REMAPPING_GID_THRESHOLD);

    capabilities = getDockerCapabilitiesFromConf();

    delayedRemovalAllowed = conf.getBoolean(
        YarnConfiguration.NM_DOCKER_ALLOW_DELAYED_REMOVAL,
        YarnConfiguration.DEFAULT_NM_DOCKER_ALLOW_DELAYED_REMOVAL);

    defaultROMounts.addAll(Arrays.asList(
        conf.getTrimmedStrings(
        YarnConfiguration.NM_DOCKER_DEFAULT_RO_MOUNTS)));

    defaultRWMounts.addAll(Arrays.asList(
        conf.getTrimmedStrings(
        YarnConfiguration.NM_DOCKER_DEFAULT_RW_MOUNTS)));

    defaultTmpfsMounts.addAll(Arrays.asList(
        conf.getTrimmedStrings(
        YarnConfiguration.NM_DOCKER_DEFAULT_TMPFS_MOUNTS)));
  }

  @Override
  public boolean isRuntimeRequested(Map<String, String> env) {
    return isDockerContainerRequested(conf, env);
  }

  private Set<String> getDockerCapabilitiesFromConf() throws
      ContainerExecutionException {
    Set<String> caps = new HashSet<>(Arrays.asList(
        conf.getTrimmedStrings(
        YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
        YarnConfiguration.DEFAULT_NM_DOCKER_CONTAINER_CAPABILITIES)));
    if(caps.contains("none") || caps.contains("NONE")) {
      if(caps.size() > 1) {
        String msg = "Mixing capabilities with the none keyword is" +
            " not supported";
        throw new ContainerExecutionException(msg);
      }
      caps = Collections.emptySet();
    }

    return caps;
  }

  public Set<String> getCapabilities() {
    return capabilities;
  }

  private String runDockerVolumeCommand(DockerVolumeCommand dockerVolumeCommand,
      Container container) throws ContainerExecutionException {
    try {
      String commandFile = dockerClient.writeCommandToTempFile(
          dockerVolumeCommand, container.getContainerId(), nmContext);
      PrivilegedOperation privOp = new PrivilegedOperation(
          PrivilegedOperation.OperationType.RUN_DOCKER_CMD);
      privOp.appendArgs(commandFile);
      String output = privilegedOperationExecutor
          .executePrivilegedOperation(null, privOp, null,
              null, true, false);
      LOG.info("ContainerId=" + container.getContainerId()
          + ", docker volume output for " + dockerVolumeCommand + ": "
          + output);
      return output;
    } catch (ContainerExecutionException e) {
      LOG.error("Error when writing command to temp file, command="
              + dockerVolumeCommand,
          e);
      throw e;
    } catch (PrivilegedOperationException e) {
      LOG.error("Error when executing command, command="
          + dockerVolumeCommand, e);
      throw new ContainerExecutionException(e);
    }
  }

  private void checkDockerVolumeCreated(
      DockerVolumeCommand dockerVolumeCreationCommand, Container container)
      throws ContainerExecutionException {
    DockerVolumeCommand dockerVolumeInspectCommand = new DockerVolumeCommand(
        DockerVolumeCommand.VOLUME_LS_SUB_COMMAND);
    String output = runDockerVolumeCommand(dockerVolumeInspectCommand,
        container);

    // Parse output line by line and check if it matches
    String volumeName = dockerVolumeCreationCommand.getVolumeName();
    String driverName = dockerVolumeCreationCommand.getDriverName();
    if (driverName == null) {
      driverName = "local";
    }

    for (String line : output.split("\n")) {
      line = line.trim();
      if (line.contains(volumeName) && line.contains(driverName)) {
        // Good we found it.
        LOG.info(
            "Docker volume-name=" + volumeName + " driver-name=" + driverName
                + " already exists for container=" + container
                .getContainerId() + ", continue...");
        return;
      }
    }

    // Couldn't find the volume
    String message =
        " Couldn't find volume=" + volumeName + " driver=" + driverName
            + " for container=" + container.getContainerId()
            + ", please check error message in log to understand "
            + "why this happens.";
    LOG.error(message);
    LOG.debug("All docker volumes in the system, command={}",
        dockerVolumeInspectCommand);

    throw new ContainerExecutionException(message);
  }

  /** Set a DNS friendly hostname.
   *  Only add hostname if network is not host or if hostname is
   *  specified via YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_HOSTNAME
   *  in host network mode
   */
  private void setHostname(DockerRunCommand runCommand,
      String containerIdStr, String network, String name)
      throws ContainerExecutionException {

    if (network.equalsIgnoreCase("host")) {
      if (name != null && !name.isEmpty()) {
        LOG.info("setting hostname in container to: " + name);
        runCommand.setHostname(name);
      }
    } else {
      //get default hostname
      if (name == null || name.isEmpty()) {
        name = RegistryPathUtils.encodeYarnID(containerIdStr);

        String domain = conf.get(RegistryConstants.KEY_DNS_DOMAIN);
        if (domain != null) {
          name += ("." + domain);
        }
        validateHostname(name);
      }
      LOG.info("setting hostname in container to: " + name);
      runCommand.setHostname(name);
    }
  }

  /**
   * If CGROUPS in enabled and not set to none, then set the CGROUP parent for
   * the command instance.
   *
   * @param resourcesOptions the resource options to check for "cgroups=none"
   * @param containerIdStr the container ID
   * @param runCommand the command to set with the CGROUP parent
   */
  @VisibleForTesting
  protected void addCGroupParentIfRequired(String resourcesOptions,
      String containerIdStr, DockerRunCommand runCommand) {
    if (cGroupsHandler == null) {
      LOG.debug("cGroupsHandler is null. cgroups are not in use. nothing to"
            + " do.");
      return;
    }

    if (resourcesOptions.equals(PrivilegedOperation.CGROUP_ARG_PREFIX
            + PrivilegedOperation.CGROUP_ARG_NO_TASKS)) {
      LOG.debug("no resource restrictions specified. not using docker's "
          + "cgroup options");
    } else {
      LOG.debug("using docker's cgroups options");

      String cGroupPath = "/"
          + cGroupsHandler.getRelativePathForCGroup(containerIdStr);

      LOG.debug("using cgroup parent: {}", cGroupPath);

      runCommand.setCGroupParent(cGroupPath);
    }
  }

  /**
   * Check if system is default to disable docker override or
   * user requested a Docker container with ENTRY_POINT support.
   *
   * @param environment - Docker container environment variables
   * @return true if Docker launch command override is disabled
   */
  private boolean checkUseEntryPoint(Map<String, String> environment) {
    boolean overrideDisable = false;
    String overrideDisableKey = Environment.
        YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE.
            name();
    String overrideDisableValue = (environment.get(overrideDisableKey) != null)
        ? environment.get(overrideDisableKey) :
            System.getenv(overrideDisableKey);
    overrideDisable = Boolean.parseBoolean(overrideDisableValue);
    return overrideDisable;
  }

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();
    Map<String, String> environment = container.getLaunchContext()
        .getEnvironment();
    String imageName = environment.get(ENV_DOCKER_CONTAINER_IMAGE);
    String network = environment.get(ENV_DOCKER_CONTAINER_NETWORK);
    String hostname = environment.get(ENV_DOCKER_CONTAINER_HOSTNAME);
    String runtime = environment.get(ENV_DOCKER_CONTAINER_DOCKER_RUNTIME);
    boolean serviceMode = Boolean.parseBoolean(environment.get(
        ENV_DOCKER_CONTAINER_DOCKER_SERVICE_MODE));
    boolean useEntryPoint = serviceMode || checkUseEntryPoint(environment);
    String clientConfig = environment.get(ENV_DOCKER_CONTAINER_CLIENT_CONFIG);

    if (imageName == null || imageName.isEmpty()) {
      imageName = defaultImageName;
    }
    if(network == null || network.isEmpty()) {
      network = defaultNetwork;
    }

    validateContainerNetworkType(network);

    validateHostname(hostname);

    validateImageName(imageName);

    validateContainerRuntimeType(runtime);

    if (defaultImageUpdate) {
      pullImageFromRemote(containerIdStr, imageName);
    }

    String runAsUser = ctx.getExecutionAttribute(RUN_AS_USER);
    String dockerRunAsUser = runAsUser;
    Path containerWorkDir = ctx.getExecutionAttribute(CONTAINER_WORK_DIR);
    String[] groups = null;

    if (enableUserReMapping) {
      String uid = getUserIdInfo(runAsUser);
      groups = getGroupIdInfo(runAsUser);
      String gid = groups[0];
      if(Integer.parseInt(uid) < userRemappingUidThreshold) {
        String message = "uid: " + uid + " below threshold: "
            + userRemappingUidThreshold;
        throw new ContainerExecutionException(message);
      }
      for(int i = 0; i < groups.length; i++) {
        String group = groups[i];
        if (Integer.parseInt(group) < userRemappingGidThreshold) {
          String message = "gid: " + group
              + " below threshold: " + userRemappingGidThreshold;
          throw new ContainerExecutionException(message);
        }
      }
      if (!allowPrivilegedContainerExecution(container)) {
        dockerRunAsUser = uid + ":" + gid;
      } else {
        dockerRunAsUser = ctx.getExecutionAttribute(USER);
      }
    }

    //List<String> -> stored as List -> fetched/converted to List<String>
    //we can't do better here thanks to type-erasure
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

    @SuppressWarnings("unchecked")
    DockerRunCommand runCommand = new DockerRunCommand(containerIdStr,
        dockerRunAsUser, imageName)
        .setNetworkType(network);

    setHostname(runCommand, containerIdStr, network, hostname);

    // Add ports mapping value.
    if (environment.containsKey(ENV_DOCKER_CONTAINER_PORTS_MAPPING)) {
      String portsMapping = environment.get(ENV_DOCKER_CONTAINER_PORTS_MAPPING);
      for (String mapping:portsMapping.split(",")) {
        if (!Pattern.matches(PORTS_MAPPING_PATTERN, mapping)) {
          throw new ContainerExecutionException(
              "Invalid port mappings: " + mapping);
        }
        runCommand.addPortsMapping(mapping);
      }
    }

    runCommand.setCapabilities(capabilities);
    if (runtime != null && !runtime.isEmpty()) {
      runCommand.addRuntime(runtime);
    }

    if (!serviceMode) {
      runCommand.addAllReadWriteMountLocations(containerLogDirs);
      runCommand.addAllReadWriteMountLocations(applicationLocalDirs);
      runCommand.addAllReadOnlyMountLocations(filecacheDirs);
      runCommand.addAllReadOnlyMountLocations(userFilecacheDirs);
    }

    if (environment.containsKey(ENV_DOCKER_CONTAINER_MOUNTS)) {
      Matcher parsedMounts = USER_MOUNT_PATTERN.matcher(
          environment.get(ENV_DOCKER_CONTAINER_MOUNTS));
      if (!parsedMounts.find()) {
        throw new ContainerExecutionException(
            "Unable to parse user supplied mount list: "
                + environment.get(ENV_DOCKER_CONTAINER_MOUNTS));
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
        if (mode == null) {
          mode = "rw";
        } else if (!mode.startsWith("ro") && !mode.startsWith("rw")) {
          mode = "rw+" + mode;
        }
        runCommand.addMountLocation(src, dst, mode);
      }
      long semicolonCount = environment.get(ENV_DOCKER_CONTAINER_MOUNTS).chars()
          .filter(c -> c == ';').count();
      if (mountCount != semicolonCount + 1) {
        // this means the matcher skipped an improperly formatted mount
        throw new ContainerExecutionException(
            "Unable to parse some mounts in user supplied mount list: "
                + environment.get(ENV_DOCKER_CONTAINER_MOUNTS));
      }
    }

    if(defaultROMounts != null && !defaultROMounts.isEmpty()) {
      for (String mount : defaultROMounts) {
        String[] dir = StringUtils.split(mount, ':');
        if (dir.length != 2) {
          throw new ContainerExecutionException("Invalid mount : " +
              mount);
        }
        String src = dir[0];
        String dst = dir[1];
        runCommand.addReadOnlyMountLocation(src, dst);
      }
    }

    if(defaultRWMounts != null && !defaultRWMounts.isEmpty()) {
      for (String mount : defaultRWMounts) {
        String[] dir = StringUtils.split(mount, ':');
        if (dir.length != 2) {
          throw new ContainerExecutionException("Invalid mount : " +
              mount);
        }
        String src = dir[0];
        String dst = dir[1];
        runCommand.addReadWriteMountLocation(src, dst);
      }
    }

    ContainerVolumePublisher publisher = new ContainerVolumePublisher(
        container, container.getCsiVolumesRootDir(), this);
    try {
      Map<String, String> volumeMounts = publisher.publishVolumes();
      volumeMounts.forEach((local, remote) ->
          runCommand.addReadWriteMountLocation(local, remote));
    } catch (YarnException | IOException e) {
      throw new ContainerExecutionException(
          "Container requests for volume resource but we are failed"
              + " to publish volumes on this node");
    }

    if (environment.containsKey(ENV_DOCKER_CONTAINER_TMPFS_MOUNTS)) {
      String[] tmpfsMounts = environment.get(ENV_DOCKER_CONTAINER_TMPFS_MOUNTS)
          .split(",");
      for (String mount : tmpfsMounts) {
        if (!TMPFS_MOUNT_PATTERN.matcher(mount).matches()) {
          throw new ContainerExecutionException("Invalid tmpfs mount : " +
              mount);
        }
        runCommand.addTmpfsMount(mount);
      }
    }

    if (defaultTmpfsMounts != null && !defaultTmpfsMounts.isEmpty()) {
      for (String mount : defaultTmpfsMounts) {
        if (!TMPFS_MOUNT_PATTERN.matcher(mount).matches()) {
          throw new ContainerExecutionException("Invalid tmpfs mount : " +
              mount);
        }
        runCommand.addTmpfsMount(mount);
      }
    }

    if (allowHostPidNamespace(container)) {
      runCommand.setPidNamespace("host");
    }

    if (allowPrivilegedContainerExecution(container)) {
      runCommand.setPrivileged();
    }

    addDockerClientConfigToRunCommand(ctx, runCommand,
        getAdditionalDockerClientCredentials(clientConfig, containerIdStr));

    String resourcesOpts = ctx.getExecutionAttribute(RESOURCES_OPTIONS);

    addCGroupParentIfRequired(resourcesOpts, containerIdStr, runCommand);

    if(environment.containsKey(ENV_DOCKER_CONTAINER_YARN_SYSFS) &&
        Boolean.parseBoolean(environment
            .get(ENV_DOCKER_CONTAINER_YARN_SYSFS))) {
      runCommand.setYarnSysFS(true);
    }

    // In service mode, the YARN log dirs are not mounted into the container.
    // As a result, the container fails to start due to stdout and stderr output
    // being sent to a file in a directory that does not exist. In service mode,
    // only supply the command with no stdout or stderr redirection.
    List<String> commands = container.getLaunchContext().getCommands();
    if (serviceMode) {
      commands = Arrays.asList(
          String.join(" ", commands).split("1>")[0].split(" "));
    }

    if (useEntryPoint) {
      runCommand.setOverrideDisabled(true);
      runCommand.addEnv(environment);
      runCommand.setOverrideCommandWithArgs(commands);
      runCommand.disableDetach();
      runCommand.setLogDir(container.getLogDir());
    } else {
      List<String> overrideCommands = new ArrayList<>();
      Path launchDst =
          new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);
      overrideCommands.add("bash");
      overrideCommands.add(launchDst.toUri().getPath());
      runCommand.setContainerWorkDir(containerWorkDir.toString());
      runCommand.setOverrideCommandWithArgs(overrideCommands);
      runCommand.detachOnRun();
    }

    if (serviceMode) {
      runCommand.setServiceMode(serviceMode);
    }

    if(enableUserReMapping) {
      if (!allowPrivilegedContainerExecution(container)) {
        runCommand.groupAdd(groups);
      }
    }

    // use plugins to create volume and update docker run command.
    if (nmContext != null
        && nmContext.getResourcePluginManager().getNameToPlugins() != null) {
      for (ResourcePlugin plugin : nmContext.getResourcePluginManager()
          .getNameToPlugins().values()) {
        DockerCommandPlugin dockerCommandPlugin =
            plugin.getDockerCommandPluginInstance();

        if (dockerCommandPlugin != null) {
          // Create volumes when needed.
          DockerVolumeCommand dockerVolumeCommand =
              dockerCommandPlugin.getCreateDockerVolumeCommand(
                  ctx.getContainer());
          if (dockerVolumeCommand != null) {
            runDockerVolumeCommand(dockerVolumeCommand, container);

            // After volume created, run inspect to make sure volume properly
            // created.
            if (dockerVolumeCommand.getSubCommand().equals(
                DockerVolumeCommand.VOLUME_CREATE_SUB_COMMAND)) {
              checkDockerVolumeCreated(dockerVolumeCommand, container);
            }
          }
          // Update cmd
          dockerCommandPlugin.updateDockerRunCommand(runCommand, container);
        }
      }
    }

    String commandFile = dockerClient.writeCommandToTempFile(runCommand,
        containerId, nmContext);
    PrivilegedOperation launchOp = buildLaunchOp(ctx,
        commandFile, runCommand);

    // Some failures here are acceptable. Let the calling executor decide.
    launchOp.disableFailureLogging();

    try {
      privilegedOperationExecutor.executePrivilegedOperation(null,
          launchOp, null, null, false, false);
    } catch (PrivilegedOperationException e) {
      throw new ContainerExecutionException("Launch container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  private Credentials getAdditionalDockerClientCredentials(String clientConfig,
      String containerIdStr) {
    Credentials additionalDockerCredentials = null;
    if (clientConfig != null && !clientConfig.isEmpty()) {
      try {
        additionalDockerCredentials =
            DockerClientConfigHandler.readCredentialsFromConfigFile(new Path(clientConfig), conf,
                containerIdStr);
      } catch (IOException e) {
        throw new RuntimeException(
            "Fail to read additional docker client config file from " + clientConfig);
      }
    }
    return additionalDockerCredentials;
  }

  @Override
  public void relaunchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    ContainerId containerId = ctx.getContainer().getContainerId();
    String containerIdStr = containerId.toString();
    // Check to see if the container already exists for relaunch
    DockerCommandExecutor.DockerContainerStatus containerStatus =
        DockerCommandExecutor.getContainerStatus(containerIdStr,
            privilegedOperationExecutor, nmContext);
    if (containerStatus != null &&
        DockerCommandExecutor.isStartable(containerStatus)) {
      DockerStartCommand startCommand = new DockerStartCommand(containerIdStr);
      String commandFile = dockerClient.writeCommandToTempFile(startCommand,
          containerId, nmContext);
      PrivilegedOperation launchOp = buildLaunchOp(ctx, commandFile,
          startCommand);

      // Some failures here are acceptable. Let the calling executor decide.
      launchOp.disableFailureLogging();

      try {
        privilegedOperationExecutor.executePrivilegedOperation(null,
            launchOp, null, null, false, false);
      } catch (PrivilegedOperationException e) {
        throw new ContainerExecutionException("Relaunch container failed", e
            .getExitCode(), e.getOutput(), e.getErrorOutput());
      }
    } else {
      throw new ContainerExecutionException("Container is not in a startable "
          + "state, unable to relaunch: " + containerIdStr);
    }

  }
  /**
   * Signal the docker container.
   *
   * Signals are used to check the liveliness of the container as well as to
   * stop/kill the container. The following outlines the docker container
   * signal handling.
   *
   * <ol>
   *     <li>If the null signal is sent, run kill -0 on the pid. This is used
   *     to check if the container is still alive, which is necessary for
   *     reacquiring containers on NM restart.</li>
   *     <li>If SIGTERM, SIGKILL is sent, attempt to stop and remove the docker
   *     container.</li>
   *     <li>If the docker container exists and is running, execute docker
   *     stop.</li>
   *     <li>If any other signal is sent, signal the container using docker
   *     kill.</li>
   * </ol>
   *
   * @param ctx the {@link ContainerRuntimeContext}.
   * @throws ContainerExecutionException if the signaling fails.
   */
  @Override
  public void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    ContainerExecutor.Signal signal = ctx.getExecutionAttribute(SIGNAL);
    Map<String, String> env =
        ctx.getContainer().getLaunchContext().getEnvironment();
    try {
      if (ContainerExecutor.Signal.NULL.equals(signal)) {
        executeLivelinessCheck(ctx);
      } else if (ContainerExecutor.Signal.TERM.equals(signal)) {
        ContainerId containerId = ctx.getContainer().getContainerId();
        handleContainerStop(containerId, env);
      } else {
        handleContainerKill(ctx, env, signal);
      }
    } catch (ContainerExecutionException e) {
      throw new ContainerExecutionException("Signal docker container failed",
          e.getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  /**
   * Reap the docker container.
   *
   * @param ctx the {@link ContainerRuntimeContext}.
   * @throws ContainerExecutionException if the removal fails.
   */
  @Override
  public void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    // Clean up the Docker container
    handleContainerRemove(ctx.getContainer().getContainerId().toString(),
        ctx.getContainer().getLaunchContext().getEnvironment());

    // Cleanup volumes when needed.
    if (nmContext != null
        && nmContext.getResourcePluginManager().getNameToPlugins() != null) {
      for (ResourcePlugin plugin : nmContext.getResourcePluginManager()
          .getNameToPlugins().values()) {
        DockerCommandPlugin dockerCommandPlugin =
            plugin.getDockerCommandPluginInstance();
        if (dockerCommandPlugin != null) {
          DockerVolumeCommand dockerVolumeCommand =
              dockerCommandPlugin.getCleanupDockerVolumesCommand(
                  ctx.getContainer());
          if (dockerVolumeCommand != null) {
            runDockerVolumeCommand(dockerVolumeCommand, ctx.getContainer());
          }
        }
      }
    }
  }

  /**
   * Perform docker exec command into running container.
   *
   * @param ctx container exec context
   * @return IOStreams of docker exec
   * @throws ContainerExecutionException
   */
  @Override
  public IOStreamPair execContainer(ContainerExecContext ctx)
      throws ContainerExecutionException {
    String containerId = ctx.getContainer().getContainerId().toString();
    DockerExecCommand dockerExecCommand = new DockerExecCommand(containerId);
    dockerExecCommand.setInteractive();
    dockerExecCommand.setTTY();
    List<String> command = new ArrayList<String>();
    StringBuilder sb = new StringBuilder();
    sb.append("/bin/");
    sb.append(ctx.getShell());
    command.add(sb.toString());
    command.add("-i");
    dockerExecCommand.setOverrideCommandWithArgs(command);
    String commandFile = dockerClient.writeCommandToTempFile(dockerExecCommand,
        ContainerId.fromString(containerId), nmContext);
    PrivilegedOperation privOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.EXEC_CONTAINER);
    privOp.appendArgs(commandFile);
    privOp.disableFailureLogging();

    IOStreamPair output;
    try {
      output =
          privilegedOperationExecutor.executePrivilegedInteractiveOperation(
              null, privOp);
      LOG.info("ContainerId=" + containerId + ", docker exec output for "
          + dockerExecCommand + ": " + output);
    } catch (PrivilegedOperationException e) {
      throw new ContainerExecutionException(
          "Execute container interactive shell failed", e.getExitCode(),
          e.getOutput(), e.getErrorOutput());
    } catch (InterruptedException ie) {
      LOG.warn("InterruptedException executing command: ", ie);
      throw new ContainerExecutionException(ie.getMessage());
    }
    return output;
  }

  // ipAndHost[0] contains comma separated list of IPs
  // ipAndHost[1] contains the hostname.
  @Override
  public String[] getIpAndHost(Container container) {
    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();
    DockerInspectCommand inspectCommand =
        new DockerInspectCommand(containerIdStr).getIpAndHost();
    try {
      String output = executeDockerInspect(containerId, inspectCommand);
      LOG.info("Docker inspect output for " + containerId + ": " + output);
      // strip off quotes if any
      output = output.replaceAll("['\"]", "");
      int index = output.lastIndexOf(',');
      if (index == -1) {
        LOG.error("Incorrect format for ip and host");
        return null;
      }
      String ips = output.substring(0, index).trim();
      String host = output.substring(index+1).trim();
      if (ips.equals("")) {
        String network;
        try {
          network = container.getLaunchContext().getEnvironment()
              .get(ENV_DOCKER_CONTAINER_NETWORK);
          if (network == null || network.isEmpty()) {
            network = defaultNetwork;
          }
        } catch (NullPointerException e) {
          network = defaultNetwork;
        }
        boolean useHostNetwork = network.equalsIgnoreCase("host");
        if (useHostNetwork) {
          // Report back node manager IP in the event where docker
          // inspect reports no IP address.  This is for bridging a gap for
          // docker environment to run with host network.
          InetAddress address;
          try {
            address = InetAddress.getLocalHost();
            ips = address.getHostAddress();
          } catch (UnknownHostException e) {
            LOG.error("Can not determine IP for container:"
                + containerId);
          }
        }
      }
      String[] ipAndHost = new String[2];
      ipAndHost[0] = ips;
      ipAndHost[1] = host;
      return ipAndHost;
    } catch (ContainerExecutionException e) {
      LOG.error("Error when writing command to temp file", e);
    } catch (PrivilegedOperationException e) {
      LOG.error("Error when executing command.", e);
    }
    return null;
  }

  @Override
  public String getExposedPorts(Container container) {
    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();
    DockerInspectCommand inspectCommand =
        new DockerInspectCommand(containerIdStr).getExposedPorts();
    try {
      String output = executeDockerInspect(containerId, inspectCommand);
      return output;
    } catch (ContainerExecutionException e) {
      LOG.error("Error when writing command to temp file", e);
    } catch (PrivilegedOperationException e) {
      LOG.error("Error when executing command.", e);
    }
    return null;
  }

  private PrivilegedOperation buildLaunchOp(ContainerRuntimeContext ctx,
      String commandFile, DockerCommand command) {

    String runAsUser = ctx.getExecutionAttribute(RUN_AS_USER);
    String containerIdStr = ctx.getContainer().getContainerId().toString();
    Path nmPrivateContainerScriptPath = ctx.getExecutionAttribute(
            NM_PRIVATE_CONTAINER_SCRIPT_PATH);
    Path containerWorkDir = ctx.getExecutionAttribute(CONTAINER_WORK_DIR);
    //we can't do better here thanks to type-erasure
    @SuppressWarnings("unchecked")
    List<String> localDirs = ctx.getExecutionAttribute(LOCAL_DIRS);
    @SuppressWarnings("unchecked")
    List<String> logDirs = ctx.getExecutionAttribute(LOG_DIRS);

    PrivilegedOperation launchOp = new PrivilegedOperation(
            PrivilegedOperation.OperationType.LAUNCH_DOCKER_CONTAINER);

    launchOp.appendArgs(runAsUser, ctx.getExecutionAttribute(USER),
        Integer.toString(PrivilegedOperation
            .RunAsUserCommand.LAUNCH_DOCKER_CONTAINER.getValue()),
        ctx.getExecutionAttribute(APPID),
        containerIdStr,
        containerWorkDir.toString(),
        nmPrivateContainerScriptPath.toUri().getPath(),
        ctx.getExecutionAttribute(NM_PRIVATE_TOKENS_PATH).toUri().getPath());
    Path keystorePath = ctx.getExecutionAttribute(NM_PRIVATE_KEYSTORE_PATH);
    Path truststorePath = ctx.getExecutionAttribute(NM_PRIVATE_TRUSTSTORE_PATH);
    if (keystorePath != null && truststorePath != null) {
      launchOp.appendArgs("--https",
          keystorePath.toUri().getPath(),
          truststorePath.toUri().getPath());
    } else {
      launchOp.appendArgs("--http");
    }
    launchOp.appendArgs(
        ctx.getExecutionAttribute(PID_FILE_PATH).toString(),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            localDirs),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            logDirs),
        commandFile);

    String tcCommandFile = ctx.getExecutionAttribute(TC_COMMAND_FILE);

    if (tcCommandFile != null) {
      launchOp.appendArgs(tcCommandFile);
    }
    LOG.debug("Launching container with cmd: {}", command);

    return launchOp;
  }

  public static void validateImageName(String imageName)
      throws ContainerExecutionException {
    if (imageName == null || imageName.isEmpty()) {
      throw new ContainerExecutionException(
          ENV_DOCKER_CONTAINER_IMAGE + " not set!");
    }
    // check if digest is part of imageName, extract and validate it.
    String digest = null;
    if (imageName.contains("@sha256")) {
      String[] digestParts = imageName.split("@");
      digest = digestParts[1];
      imageName = digestParts[0];
    }
    if (!dockerImagePattern.matcher(imageName).matches() || (digest != null
        && !DOCKER_DIGEST_PATTERN.matcher(digest).matches())) {
      throw new ContainerExecutionException(
          "Image name '" + imageName + "' doesn't match docker image name pattern");
    }
  }

  public void pullImageFromRemote(String containerIdStr, String imageName)
      throws ContainerExecutionException {
    long start = System.currentTimeMillis();
    DockerPullCommand dockerPullCommand = new DockerPullCommand(imageName);
    LOG.debug("now pulling docker image. image name: {}, container: {}",
        imageName, containerIdStr);

    DockerCommandExecutor.executeDockerCommand(dockerPullCommand,
        containerIdStr, null,
        privilegedOperationExecutor, false, nmContext);

    long end = System.currentTimeMillis();
    long pullImageTimeMs = end - start;

    LOG.debug("pull docker image done with {}ms specnt. image name: {},"
        + " container: {}", pullImageTimeMs, imageName, containerIdStr);
  }

  private void executeLivelinessCheck(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    String procFs = ctx.getExecutionAttribute(PROCFS);
    if (procFs == null || procFs.isEmpty()) {
      procFs = DEFAULT_PROCFS;
    }
    String pid = ctx.getExecutionAttribute(PID);
    if (!new File(procFs + File.separator + pid).exists()) {
      String msg = "Liveliness check failed for PID: " + pid
          + ". Container may have already completed.";
      throw new ContainerExecutionException(msg,
          PrivilegedOperation.ResultCode.INVALID_CONTAINER_PID.getValue());
    }
  }

  /**
   * Handles a docker container stop by first finding the {@code STOPSIGNAL}
   * using docker inspect and then executing
   * {@code docker kill --signal=<STOPSIGNAL>}.
   * It doesn't rely on the docker stop because that sends a {@code SIGKILL}
   * to the root process in the container after the {@code STOPSIGNAL}.The grace
   * period which the docker stop uses has granularity in seconds. However, NM
   * is designed to explicitly send a {@code SIGKILL} to the containers after a
   * grace period which has a granularity of millis. It doesn't want the docker
   * stop to send {@code SIGKILL} but docker stop has no option to disallow
   * that.
   *
   * @param containerId container id
   * @param env         env
   * @throws ContainerExecutionException
   */
  private void handleContainerStop(ContainerId containerId,
      Map<String, String> env)
      throws ContainerExecutionException {

    DockerCommandExecutor.DockerContainerStatus containerStatus =
        DockerCommandExecutor.DockerContainerStatus.UNKNOWN;
    String stopSignal = ContainerExecutor.Signal.TERM.toString();
    char delimiter = ',';
    DockerInspectCommand inspectCommand =
        new DockerInspectCommand(containerId.toString()).get(new String[] {
            DockerInspectCommand.STATUS_TEMPLATE,
            DockerInspectCommand.STOPSIGNAL_TEMPLATE}, delimiter);
    try {
      String output = executeDockerInspect(containerId, inspectCommand).trim();

      if (!output.isEmpty()) {
        String[] statusAndSignal = StringUtils.split(output, delimiter);
        containerStatus = DockerCommandExecutor.parseContainerStatus(
            statusAndSignal[0]);
        if (statusAndSignal.length > 1) {
          stopSignal = statusAndSignal[1];
        }
      }
    } catch (ContainerExecutionException | PrivilegedOperationException e) {
      LOG.debug("{} inspect failed, skipping stop", containerId, e);
      return;
    }

    if (DockerCommandExecutor.isStoppable(containerStatus)) {
      DockerKillCommand dockerStopCommand = new DockerKillCommand(
          containerId.toString()).setSignal(stopSignal);
      DockerCommandExecutor.executeDockerCommand(dockerStopCommand,
          containerId.toString(), env, privilegedOperationExecutor, false,
          nmContext);
    } else {
      LOG.debug("{} status is {}, skipping stop", containerId, containerStatus);
    }
  }

  private String executeDockerInspect(ContainerId containerId,
      DockerInspectCommand inspectCommand) throws ContainerExecutionException,
      PrivilegedOperationException {
    String commandFile = dockerClient.writeCommandToTempFile(inspectCommand,
        containerId, nmContext);
    PrivilegedOperation privOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.RUN_DOCKER_CMD);
    privOp.appendArgs(commandFile);

    String output = privilegedOperationExecutor.executePrivilegedOperation(null,
        privOp, null, null, true, false);
    LOG.info("{} : docker inspect output {} ", containerId, output);
    return output;
  }

  private void handleContainerKill(ContainerRuntimeContext ctx,
      Map<String, String> env,
      ContainerExecutor.Signal signal) throws ContainerExecutionException {
    Container container = ctx.getContainer();

    ContainerVolumePublisher publisher = new ContainerVolumePublisher(
        container, container.getCsiVolumesRootDir(), this);
    try {
      publisher.unpublishVolumes();
    } catch (YarnException | IOException e) {
      throw new ContainerExecutionException(e);
    }

    boolean serviceMode = Boolean.parseBoolean(env.get(
        ENV_DOCKER_CONTAINER_DOCKER_SERVICE_MODE));

    // Only need to check whether the container was asked to be privileged.
    // If the container had failed the permissions checks upon launch, it
    // would have never been launched and thus we wouldn't be here
    // attempting to signal it.
    if (isContainerRequestedAsPrivileged(container) || serviceMode) {
      String containerId = container.getContainerId().toString();
      DockerCommandExecutor.DockerContainerStatus containerStatus =
          DockerCommandExecutor.getContainerStatus(containerId,
          privilegedOperationExecutor, nmContext);
      if (DockerCommandExecutor.isKillable(containerStatus)) {
        DockerKillCommand dockerKillCommand =
            new DockerKillCommand(containerId).setSignal(signal.name());
        DockerCommandExecutor.executeDockerCommand(dockerKillCommand,
            containerId, env, privilegedOperationExecutor, false, nmContext);
      } else {
        LOG.debug(
            "Container status is {}, skipping kill - {}",
            containerStatus.getName(), containerId);
      }
    } else {
      PrivilegedOperation privOp = new PrivilegedOperation(
          PrivilegedOperation.OperationType.SIGNAL_CONTAINER);
      privOp.appendArgs(ctx.getExecutionAttribute(RUN_AS_USER),
          ctx.getExecutionAttribute(USER),
          Integer.toString(PrivilegedOperation.RunAsUserCommand
          .SIGNAL_CONTAINER.getValue()),
          ctx.getExecutionAttribute(PID),
          Integer.toString(ctx.getExecutionAttribute(SIGNAL).getValue()));
      privOp.disableFailureLogging();
      try {
        privilegedOperationExecutor.executePrivilegedOperation(null,
            privOp, null, null, false, false);
      } catch (PrivilegedOperationException e) {
        //Don't log the failure here. Some kinds of signaling failures are
        // acceptable. Let the calling executor decide what to do.
        throw new ContainerExecutionException("Signal container failed using "
            + "signal: " + signal.name(), e
            .getExitCode(), e.getOutput(), e.getErrorOutput());
      }
    }
  }

  private void handleContainerRemove(String containerId,
      Map<String, String> env) throws ContainerExecutionException {
    String delayedRemoval = env.get(ENV_DOCKER_CONTAINER_DELAYED_REMOVAL);
    if (delayedRemovalAllowed && delayedRemoval != null
        && delayedRemoval.equalsIgnoreCase("true")) {
      LOG.info("Delayed removal requested and allowed, skipping removal - "
          + containerId);
    } else {
      DockerCommandExecutor.DockerContainerStatus containerStatus =
          DockerCommandExecutor.getContainerStatus(containerId,
              privilegedOperationExecutor, nmContext);
      if (DockerCommandExecutor.isRemovable(containerStatus)) {
        DockerRmCommand dockerRmCommand = new DockerRmCommand(containerId,
            ResourceHandlerModule.getCgroupsRelativeRoot());
        DockerCommandExecutor.executeDockerCommand(dockerRmCommand, containerId,
            env, privilegedOperationExecutor, false, nmContext);
      }
    }
  }

  private void addDockerClientConfigToRunCommand(ContainerRuntimeContext ctx,
      DockerRunCommand dockerRunCommand, Credentials additionDockerCredentials)
      throws ContainerExecutionException {
    ByteBuffer tokens = ctx.getContainer().getLaunchContext().getTokens();
    Credentials credentials = new Credentials();
    if (tokens != null) {
      tokens.rewind();
      if (tokens.hasRemaining()) {
        try {
          credentials.addAll(DockerClientConfigHandler
              .getCredentialsFromTokensByteBuffer(tokens));
        } catch (IOException e) {
          throw new ContainerExecutionException("Unable to read tokens.");
        }
      }
    }

    if (additionDockerCredentials != null) {
      credentials.addAll(additionDockerCredentials);
    }

    if (credentials.numberOfTokens() > 0) {
      Path nmPrivateDir =
          ctx.getExecutionAttribute(NM_PRIVATE_CONTAINER_SCRIPT_PATH)
              .getParent();
      File dockerConfigPath = new File(nmPrivateDir + "/config.json");
      try {
        DockerClientConfigHandler
            .writeDockerCredentialsToPath(dockerConfigPath, credentials);
      } catch (IOException e) {
        throw new ContainerExecutionException(
            "Unable to write Docker client credentials to "
                + dockerConfigPath);
      }
      dockerRunCommand.setClientConfigDir(dockerConfigPath.getParent());
    }
  }

  boolean getHostPidNamespaceEnabled() {
    return conf.getBoolean(
      YarnConfiguration.NM_DOCKER_ALLOW_HOST_PID_NAMESPACE,
      YarnConfiguration.DEFAULT_NM_DOCKER_ALLOW_HOST_PID_NAMESPACE);
  }

  boolean getPrivilegedContainersEnabledOnCluster() {
    return conf.getBoolean(
        YarnConfiguration.NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS,
        YarnConfiguration.DEFAULT_NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS);
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
    return ENV_OCI_CONTAINER_PID_NAMESPACE;
  }

  String getEnvOciContainerRunPrivilegedContainer() {
    return ENV_OCI_CONTAINER_RUN_PRIVILEGED_CONTAINER;
  }

}
