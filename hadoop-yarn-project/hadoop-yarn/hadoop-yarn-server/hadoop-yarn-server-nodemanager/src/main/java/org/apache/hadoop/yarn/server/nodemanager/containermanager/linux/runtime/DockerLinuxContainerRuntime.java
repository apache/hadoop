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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerCommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerKillCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRmCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerStartCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.util.DockerClientConfigHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.Shell;
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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerStopCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;

/**
 * <p>This class is a {@link ContainerRuntime} implementation that uses the
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
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_IMAGE_FILE} is currently ignored.
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
 +     additional volume mounts for the Docker container. The value of the
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
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DockerLinuxContainerRuntime implements LinuxContainerRuntime {
  private static final Logger LOG =
       LoggerFactory.getLogger(DockerLinuxContainerRuntime.class);

  // This validates that the image is a proper docker image
  public static final String DOCKER_IMAGE_PATTERN =
      "^(([a-zA-Z0-9.-]+)(:\\d+)?/)?([a-z0-9_./-]+)(:[\\w.-]+)?$";
  private static final Pattern dockerImagePattern =
      Pattern.compile(DOCKER_IMAGE_PATTERN);
  public static final String HOSTNAME_PATTERN =
      "^[a-zA-Z0-9][a-zA-Z0-9_.-]+$";
  private static final Pattern hostnamePattern = Pattern.compile(
      HOSTNAME_PATTERN);
  private static final Pattern USER_MOUNT_PATTERN = Pattern.compile(
      "(?<=^|,)([^:\\x00]+):([^:\\x00]+)" +
          "(:(r[ow]|(r[ow][+])?(r?shared|r?slave|r?private)))?(?:,|$)");
  private static final Pattern TMPFS_MOUNT_PATTERN = Pattern.compile(
      "^/[^:\\x00]+$");
  public static final String PORTS_MAPPING_PATTERN =
      "^:[0-9]+|^[0-9]+:[0-9]+|^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]" +
          "|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])" +
          ":[0-9]+:[0-9]+$";
  private static final int HOST_NAME_LENGTH = 64;
  private static final String DEFAULT_PROCFS = "/proc";

  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_IMAGE =
      "YARN_CONTAINER_RUNTIME_DOCKER_IMAGE";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_IMAGE_FILE =
      "YARN_CONTAINER_RUNTIME_DOCKER_IMAGE_FILE";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_RUN_OVERRIDE_DISABLE =
      "YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_NETWORK =
      "YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_PID_NAMESPACE =
      "YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_PID_NAMESPACE";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_HOSTNAME =
      "YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_HOSTNAME";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_RUN_PRIVILEGED_CONTAINER =
      "YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_RUN_ENABLE_USER_REMAPPING =
      "YARN_CONTAINER_RUNTIME_DOCKER_RUN_ENABLE_USER_REMAPPING";
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
  private Configuration conf;
  private Context nmContext;
  private DockerClient dockerClient;
  private PrivilegedOperationExecutor privilegedOperationExecutor;
  private Set<String> allowedNetworks = new HashSet<>();
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
   * @param env the environment variable settings for the operation
   * @return whether a Docker container is requested
   */
  public static boolean isDockerContainerRequested(
      Map<String, String> env) {
    if (env == null) {
      return false;
    }

    String type = env.get(ContainerRuntimeConstants.ENV_CONTAINER_TYPE);

    return type != null && type.equals("docker");
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
    this.nmContext = nmContext;
    this.conf = conf;
    dockerClient = new DockerClient();
    allowedNetworks.clear();
    defaultROMounts.clear();
    defaultRWMounts.clear();
    defaultTmpfsMounts.clear();
    allowedNetworks.addAll(Arrays.asList(
        conf.getTrimmedStrings(
            YarnConfiguration.NM_DOCKER_ALLOWED_CONTAINER_NETWORKS,
            YarnConfiguration.DEFAULT_NM_DOCKER_ALLOWED_CONTAINER_NETWORKS)));
    defaultNetwork = conf.getTrimmed(
        YarnConfiguration.NM_DOCKER_DEFAULT_CONTAINER_NETWORK,
        YarnConfiguration.DEFAULT_NM_DOCKER_DEFAULT_CONTAINER_NETWORK);

    if(!allowedNetworks.contains(defaultNetwork)) {
      String message = "Default network: " + defaultNetwork
          + " is not in the set of allowed networks: " + allowedNetworks;

      if (LOG.isWarnEnabled()) {
        LOG.warn(message + ". Please check "
            + "configuration");
      }

      throw new ContainerExecutionException(message);
    }

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
    return isDockerContainerRequested(env);
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

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();

    // Create volumes when needed.
    if (nmContext != null
        && nmContext.getResourcePluginManager().getNameToPlugins() != null) {
      for (ResourcePlugin plugin : nmContext.getResourcePluginManager()
          .getNameToPlugins().values()) {
        DockerCommandPlugin dockerCommandPlugin =
            plugin.getDockerCommandPluginInstance();
        if (dockerCommandPlugin != null) {
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
        }
      }
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

    if (LOG.isDebugEnabled()) {
      LOG.debug("All docker volumes in the system, command="
          + dockerVolumeInspectCommand.toString());
    }

    throw new ContainerExecutionException(message);
  }

  private void validateContainerNetworkType(String network)
      throws ContainerExecutionException {
    if (allowedNetworks.contains(network)) {
      return;
    }

    String msg = "Disallowed network:  '" + network
        + "' specified. Allowed networks: are " + allowedNetworks
        .toString();
    throw new ContainerExecutionException(msg);
  }

  /**
   * Return whether the YARN container is allowed to run using the host's PID
   * namespace for the Docker container. For this to be allowed, the submitting
   * user must request the feature and the feature must be enabled on the
   * cluster.
   *
   * @param container the target YARN container
   * @return whether host pid namespace is requested and allowed
   * @throws ContainerExecutionException if host pid namespace is requested
   * but is not allowed
   */
  private boolean allowHostPidNamespace(Container container)
      throws ContainerExecutionException {
    Map<String, String> environment = container.getLaunchContext()
        .getEnvironment();
    String pidNamespace = environment.get(ENV_DOCKER_CONTAINER_PID_NAMESPACE);

    if (pidNamespace == null) {
      return false;
    }

    if (!pidNamespace.equalsIgnoreCase("host")) {
      LOG.warn("NOT requesting PID namespace. Value of " +
          ENV_DOCKER_CONTAINER_PID_NAMESPACE + "is invalid: " + pidNamespace);
      return false;
    }

    boolean hostPidNamespaceEnabled = conf.getBoolean(
        YarnConfiguration.NM_DOCKER_ALLOW_HOST_PID_NAMESPACE,
        YarnConfiguration.DEFAULT_NM_DOCKER_ALLOW_HOST_PID_NAMESPACE);

    if (!hostPidNamespaceEnabled) {
      String message = "Host pid namespace being requested but this is not "
          + "enabled on this cluster";
      LOG.warn(message);
      throw new ContainerExecutionException(message);
    }

    return true;
  }

  public static void validateHostname(String hostname) throws
      ContainerExecutionException {
    if (hostname != null && !hostname.isEmpty()) {
      if (!hostnamePattern.matcher(hostname).matches()) {
        throw new ContainerExecutionException("Hostname '" + hostname
            + "' doesn't match docker hostname pattern");
      }
      if (hostname.length() > HOST_NAME_LENGTH) {
        throw new ContainerExecutionException(
            "Hostname can not be greater than " + HOST_NAME_LENGTH
                + " characters: " + hostname);
      }
    }
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("cGroupsHandler is null. cgroups are not in use. nothing to"
            + " do.");
      }
      return;
    }

    if (resourcesOptions.equals(PrivilegedOperation.CGROUP_ARG_PREFIX
            + PrivilegedOperation.CGROUP_ARG_NO_TASKS)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("no resource restrictions specified. not using docker's "
            + "cgroup options");
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("using docker's cgroups options");
      }

      String cGroupPath = "/"
          + cGroupsHandler.getRelativePathForCGroup(containerIdStr);

      if (LOG.isDebugEnabled()) {
        LOG.debug("using cgroup parent: " + cGroupPath);
      }

      runCommand.setCGroupParent(cGroupPath);
    }
  }

  /**
   * Return whether the YARN container is allowed to run in a privileged
   * Docker container. For a privileged container to be allowed all of the
   * following three conditions must be satisfied:
   *
   * <ol>
   *   <li>Submitting user must request for a privileged container</li>
   *   <li>Privileged containers must be enabled on the cluster</li>
   *   <li>Submitting user must be white-listed to run a privileged
   *   container</li>
   * </ol>
   *
   * @param container the target YARN container
   * @return whether privileged container execution is allowed
   * @throws ContainerExecutionException if privileged container execution
   * is requested but is not allowed
   */
  private boolean allowPrivilegedContainerExecution(Container container)
      throws ContainerExecutionException {

    if(!isContainerRequestedAsPrivileged(container)) {
      return false;
    }

    LOG.info("Privileged container requested for : " + container
        .getContainerId().toString());

    //Ok, so we have been asked to run a privileged container. Security
    // checks need to be run. Each violation is an error.

    //check if privileged containers are enabled.
    boolean privilegedContainersEnabledOnCluster = conf.getBoolean(
        YarnConfiguration.NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS,
            YarnConfiguration.DEFAULT_NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS);

    if (!privilegedContainersEnabledOnCluster) {
      String message = "Privileged container being requested but privileged "
          + "containers are not enabled on this cluster";
      LOG.warn(message);
      throw new ContainerExecutionException(message);
    }

    //check if submitting user is in the whitelist.
    String submittingUser = container.getUser();
    UserGroupInformation submitterUgi = UserGroupInformation
        .createRemoteUser(submittingUser);

    if (!privilegedContainersAcl.isUserAllowed(submitterUgi)) {
      String message = "Cannot launch privileged container. Submitting user ("
          + submittingUser + ") fails ACL check.";
      LOG.warn(message);
      throw new ContainerExecutionException(message);
    }

    LOG.info("All checks pass. Launching privileged container for : "
        + container.getContainerId().toString());

    return true;
  }

  /**
   * This function only returns whether a privileged container was requested,
   * not whether the container was or will be launched as privileged.
   * @param container
   * @return
   */
  private boolean isContainerRequestedAsPrivileged(
      Container container) {
    String runPrivilegedContainerEnvVar = container.getLaunchContext()
        .getEnvironment().get(ENV_DOCKER_CONTAINER_RUN_PRIVILEGED_CONTAINER);
    return Boolean.parseBoolean(runPrivilegedContainerEnvVar);
  }

  @VisibleForTesting
  private String mountReadOnlyPath(String mount,
      Map<Path, List<String>> localizedResources)
      throws ContainerExecutionException {
    for (Entry<Path, List<String>> resource : localizedResources.entrySet()) {
      if (resource.getValue().contains(mount)) {
        java.nio.file.Path path = Paths.get(resource.getKey().toString());
        if (!path.isAbsolute()) {
          throw new ContainerExecutionException("Mount must be absolute: " +
              mount);
        }
        if (Files.isSymbolicLink(path)) {
          throw new ContainerExecutionException("Mount cannot be a symlink: " +
              mount);
        }
        return path.toString();
      }
    }
    throw new ContainerExecutionException("Mount must be a localized " +
        "resource: " + mount);
  }

  private String getUserIdInfo(String userName)
      throws ContainerExecutionException {
    String id = "";
    Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
        new String[]{"id", "-u", userName});
    try {
      shexec.execute();
      id = shexec.getOutput().replaceAll("[^0-9]", "");
    } catch (Exception e) {
      throw new ContainerExecutionException(e);
    }
    return id;
  }

  private String[] getGroupIdInfo(String userName)
      throws ContainerExecutionException {
    String[] id = null;
    Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
        new String[]{"id", "-G", userName});
    try {
      shexec.execute();
      id = shexec.getOutput().replace("\n", "").split(" ");
    } catch (Exception e) {
      throw new ContainerExecutionException(e);
    }
    return id;
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
    Map<String, String> environment = container.getLaunchContext()
        .getEnvironment();
    String imageName = environment.get(ENV_DOCKER_CONTAINER_IMAGE);
    String network = environment.get(ENV_DOCKER_CONTAINER_NETWORK);
    String hostname = environment.get(ENV_DOCKER_CONTAINER_HOSTNAME);
    boolean useEntryPoint = checkUseEntryPoint(environment);

    if(network == null || network.isEmpty()) {
      network = defaultNetwork;
    }

    validateContainerNetworkType(network);

    validateHostname(hostname);

    validateImageName(imageName);

    String containerIdStr = containerId.toString();
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

    runCommand.addAllReadWriteMountLocations(containerLogDirs);
    runCommand.addAllReadWriteMountLocations(applicationLocalDirs);
    runCommand.addAllReadOnlyMountLocations(filecacheDirs);
    runCommand.addAllReadOnlyMountLocations(userFilecacheDirs);

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
      long commaCount = environment.get(ENV_DOCKER_CONTAINER_MOUNTS).chars()
          .filter(c -> c == ',').count();
      if (mountCount != commaCount + 1) {
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

    addDockerClientConfigToRunCommand(ctx, runCommand);

    String resourcesOpts = ctx.getExecutionAttribute(RESOURCES_OPTIONS);

    addCGroupParentIfRequired(resourcesOpts, containerIdStr, runCommand);

    if (useEntryPoint) {
      runCommand.setOverrideDisabled(true);
      runCommand.addEnv(environment);
      runCommand.setOverrideCommandWithArgs(container.getLaunchContext()
          .getCommands());
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

    if(enableUserReMapping) {
      if (!allowPrivilegedContainerExecution(container)) {
        runCommand.groupAdd(groups);
      }
    }

    // use plugins to update docker run command.
    if (nmContext != null
        && nmContext.getResourcePluginManager().getNameToPlugins() != null) {
      for (ResourcePlugin plugin : nmContext.getResourcePluginManager()
          .getNameToPlugins().values()) {
        DockerCommandPlugin dockerCommandPlugin =
            plugin.getDockerCommandPluginInstance();
        if (dockerCommandPlugin != null) {
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
        String containerId = ctx.getContainer().getContainerId().toString();
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


  // ipAndHost[0] contains comma separated list of IPs
  // ipAndHost[1] contains the hostname.
  @Override
  public String[] getIpAndHost(Container container) {
    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();
    DockerInspectCommand inspectCommand =
        new DockerInspectCommand(containerIdStr).getIpAndHost();
    try {
      String commandFile = dockerClient.writeCommandToTempFile(inspectCommand,
          containerId, nmContext);
      PrivilegedOperation privOp = new PrivilegedOperation(
          PrivilegedOperation.OperationType.RUN_DOCKER_CMD);
      privOp.appendArgs(commandFile);
      String output = privilegedOperationExecutor
          .executePrivilegedOperation(null, privOp, null,
              null, true, false);
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
              .get("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK");
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
            ctx.getExecutionAttribute(NM_PRIVATE_TOKENS_PATH).toUri().getPath(),
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Launching container with cmd: " + command);
    }

    return launchOp;
  }

  public static void validateImageName(String imageName)
      throws ContainerExecutionException {
    if (imageName == null || imageName.isEmpty()) {
      throw new ContainerExecutionException(
          ENV_DOCKER_CONTAINER_IMAGE + " not set!");
    }
    if (!dockerImagePattern.matcher(imageName).matches()) {
      throw new ContainerExecutionException("Image name '" + imageName
          + "' doesn't match docker image name pattern");
    }
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

  private void handleContainerStop(String containerId, Map<String, String> env)
      throws ContainerExecutionException {
    DockerCommandExecutor.DockerContainerStatus containerStatus =
        DockerCommandExecutor.getContainerStatus(containerId,
            privilegedOperationExecutor, nmContext);
    if (DockerCommandExecutor.isStoppable(containerStatus)) {
      DockerStopCommand dockerStopCommand = new DockerStopCommand(containerId);
      DockerCommandExecutor.executeDockerCommand(dockerStopCommand, containerId,
          env, privilegedOperationExecutor, false, nmContext);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Container status is " + containerStatus.getName()
                + ", skipping stop - " + containerId);
      }
    }
  }

  private void handleContainerKill(ContainerRuntimeContext ctx,
      Map<String, String> env,
      ContainerExecutor.Signal signal) throws ContainerExecutionException {
    Container container = ctx.getContainer();

    // Only need to check whether the container was asked to be privileged.
    // If the container had failed the permissions checks upon launch, it
    // would have never been launched and thus we wouldn't be here
    // attempting to signal it.
    if (isContainerRequestedAsPrivileged(container)) {
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
      DockerRunCommand dockerRunCommand) throws ContainerExecutionException {
    ByteBuffer tokens = ctx.getContainer().getLaunchContext().getTokens();
    Credentials credentials;
    if (tokens != null) {
      tokens.rewind();
      if (tokens.hasRemaining()) {
        try {
          credentials = DockerClientConfigHandler
              .getCredentialsFromTokensByteBuffer(tokens);
        } catch (IOException e) {
          throw new ContainerExecutionException("Unable to read tokens.");
        }
        if (credentials.numberOfTokens() > 0) {
          Path nmPrivateDir =
              ctx.getExecutionAttribute(NM_PRIVATE_CONTAINER_SCRIPT_PATH)
                  .getParent();
          File dockerConfigPath = new File(nmPrivateDir + "/config.json");
          try {
            if (DockerClientConfigHandler
                .writeDockerCredentialsToPath(dockerConfigPath, credentials)) {
              dockerRunCommand.setClientConfigDir(dockerConfigPath.getParent());
            }
          } catch (IOException e) {
            throw new ContainerExecutionException(
                "Unable to write Docker client credentials to "
                    + dockerConfigPath);
          }
        }
      }
    }
  }
}
