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
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerCommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerKillCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRmCommand;
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
 *     hostname will be derived from the container ID.
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
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_LOCAL_RESOURCE_MOUNTS} adds
 *     additional volume mounts to the Docker container. The value of the
 *     environment variable should be a comma-separated list of mounts.
 *     All such mounts must be given as {@code source:dest}, where the
 *     source is an absolute path that is not a symlink and that points to a
 *     localized resource.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS} allows users to specify
 +     additional volume mounts for the Docker container. The value of the
 *     environment variable should be a comma-separated list of mounts.
 *     All such mounts must be given as {@code source:dest:mode}, and the mode
 *     must be "ro" (read-only) or "rw" (read-write) to specify the type of
 *     access being requested. The requested mounts will be validated by
 *     container-executor based on the values set in container-executor.cfg for
 *     {@code docker.allowed.ro-mounts} and {@code docker.allowed.rw-mounts}.
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
      "(?<=^|,)([^:\\x00]+):([^:\\x00]+):([a-z]+)");

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
  public static final String ENV_DOCKER_CONTAINER_LOCAL_RESOURCE_MOUNTS =
      "YARN_CONTAINER_RUNTIME_DOCKER_LOCAL_RESOURCE_MOUNTS";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_MOUNTS =
      "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_DELAYED_REMOVAL =
      "YARN_CONTAINER_RUNTIME_DOCKER_DELAYED_REMOVAL";

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
    dockerClient = new DockerClient(conf);
    allowedNetworks.clear();
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
          dockerVolumeCommand, container.getContainerId().toString());
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
    }
  }

  /** Set a DNS friendly hostname. */
  private void setHostname(DockerRunCommand runCommand, String
      containerIdStr, String name)
      throws ContainerExecutionException {
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
    Map<String, String> environment = container.getLaunchContext()
        .getEnvironment();
    String runPrivilegedContainerEnvVar = environment
        .get(ENV_DOCKER_CONTAINER_RUN_PRIVILEGED_CONTAINER);

    if (runPrivilegedContainerEnvVar == null) {
      return false;
    }

    if (!runPrivilegedContainerEnvVar.equalsIgnoreCase("true")) {
      LOG.warn("NOT running a privileged container. Value of " +
          ENV_DOCKER_CONTAINER_RUN_PRIVILEGED_CONTAINER
          + "is invalid: " + runPrivilegedContainerEnvVar);
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

  @VisibleForTesting
  protected String validateMount(String mount,
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

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    Map<String, String> environment = container.getLaunchContext()
        .getEnvironment();
    String imageName = environment.get(ENV_DOCKER_CONTAINER_IMAGE);
    String network = environment.get(ENV_DOCKER_CONTAINER_NETWORK);
    String hostname = environment.get(ENV_DOCKER_CONTAINER_HOSTNAME);

    if(network == null || network.isEmpty()) {
      network = defaultNetwork;
    }

    validateContainerNetworkType(network);

    validateHostname(hostname);

    validateImageName(imageName);

    String containerIdStr = container.getContainerId().toString();
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
      dockerRunAsUser = uid + ":" + gid;
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
        .detachOnRun()
        .setContainerWorkDir(containerWorkDir.toString())
        .setNetworkType(network);
    setHostname(runCommand, containerIdStr, hostname);
    runCommand.setCapabilities(capabilities);

    runCommand.addAllReadWriteMountLocations(containerLogDirs);
    runCommand.addAllReadWriteMountLocations(applicationLocalDirs);
    runCommand.addAllReadOnlyMountLocations(filecacheDirs);
    runCommand.addAllReadOnlyMountLocations(userFilecacheDirs);

    if (environment.containsKey(ENV_DOCKER_CONTAINER_LOCAL_RESOURCE_MOUNTS)) {
      String mounts = environment.get(
          ENV_DOCKER_CONTAINER_LOCAL_RESOURCE_MOUNTS);
      if (!mounts.isEmpty()) {
        for (String mount : StringUtils.split(mounts)) {
          String[] dir = StringUtils.split(mount, ':');
          if (dir.length != 2) {
            throw new ContainerExecutionException("Invalid mount : " +
                mount);
          }
          String src = validateMount(dir[0], localizedResources);
          String dst = dir[1];
          runCommand.addReadOnlyMountLocation(src, dst, true);
        }
      }
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
      while (parsedMounts.find()) {
        String src = parsedMounts.group(1);
        String dst = parsedMounts.group(2);
        String mode = parsedMounts.group(3);
        if (!mode.equals("ro") && !mode.equals("rw")) {
          throw new ContainerExecutionException(
              "Invalid mount mode requested for mount: "
                  + parsedMounts.group());
        }
        if (mode.equals("ro")) {
          runCommand.addReadOnlyMountLocation(src, dst);
        } else {
          runCommand.addReadWriteMountLocation(src, dst);
        }
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

    String disableOverride = environment.get(
        ENV_DOCKER_CONTAINER_RUN_OVERRIDE_DISABLE);

    if (disableOverride != null && disableOverride.equals("true")) {
      LOG.info("command override disabled");
    } else {
      List<String> overrideCommands = new ArrayList<>();
      Path launchDst =
          new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);

      overrideCommands.add("bash");
      overrideCommands.add(launchDst.toUri().getPath());
      runCommand.setOverrideCommandWithArgs(overrideCommands);
    }

    if(enableUserReMapping) {
      runCommand.groupAdd(groups);
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
        containerIdStr);
    PrivilegedOperation launchOp = buildLaunchOp(ctx,
        commandFile, runCommand);

    try {
      privilegedOperationExecutor.executePrivilegedOperation(null,
          launchOp, null, null, false, false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Launch container failed. Exception: ", e);
      LOG.info("Docker command used: " + runCommand);

      throw new ContainerExecutionException("Launch container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
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
    String containerId = ctx.getContainer().getContainerId().toString();
    Map<String, String> env =
        ctx.getContainer().getLaunchContext().getEnvironment();
    try {
      if (ContainerExecutor.Signal.NULL.equals(signal)) {
        executeLivelinessCheck(ctx);
      } else {
        if (ContainerExecutor.Signal.KILL.equals(signal)
            || ContainerExecutor.Signal.TERM.equals(signal)) {
          handleContainerStop(containerId, env);
        } else {
          handleContainerKill(containerId, env, signal);
        }
      }
    } catch (ContainerExecutionException e) {
      LOG.warn("Signal docker container failed. Exception: ", e);
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
    String containerId = container.getContainerId().toString();
    DockerInspectCommand inspectCommand =
        new DockerInspectCommand(containerId).getIpAndHost();
    try {
      String commandFile = dockerClient.writeCommandToTempFile(inspectCommand,
          containerId);
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
      String commandFile, DockerRunCommand runCommand) {

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
    String resourcesOpts = ctx.getExecutionAttribute(RESOURCES_OPTIONS);

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
            commandFile,
            resourcesOpts);

    String tcCommandFile = ctx.getExecutionAttribute(TC_COMMAND_FILE);

    if (tcCommandFile != null) {
      launchOp.appendArgs(tcCommandFile);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Launching container with cmd: " + runCommand);
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
    PrivilegedOperation signalOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.SIGNAL_CONTAINER);
    signalOp.appendArgs(ctx.getExecutionAttribute(RUN_AS_USER),
        ctx.getExecutionAttribute(USER), Integer.toString(
            PrivilegedOperation.RunAsUserCommand.SIGNAL_CONTAINER.getValue()),
        ctx.getExecutionAttribute(PID),
        Integer.toString(ctx.getExecutionAttribute(SIGNAL).getValue()));
    signalOp.disableFailureLogging();
    try {
      privilegedOperationExecutor.executePrivilegedOperation(null, signalOp,
          null, ctx.getContainer().getLaunchContext().getEnvironment(), false,
          false);
    } catch (PrivilegedOperationException e) {
      String msg = "Liveliness check failed for PID: "
          + ctx.getExecutionAttribute(PID)
          + ". Container may have already completed.";
      throw new ContainerExecutionException(msg, e.getExitCode(), e.getOutput(),
          e.getErrorOutput());
    }
  }

  private void handleContainerStop(String containerId, Map<String, String> env)
      throws ContainerExecutionException {
    DockerCommandExecutor.DockerContainerStatus containerStatus =
        DockerCommandExecutor.getContainerStatus(containerId, conf,
            privilegedOperationExecutor);
    if (DockerCommandExecutor.isStoppable(containerStatus)) {
      DockerStopCommand dockerStopCommand = new DockerStopCommand(containerId);
      DockerCommandExecutor.executeDockerCommand(dockerStopCommand, containerId,
          env, conf, privilegedOperationExecutor, false);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Container status is " + containerStatus.getName()
                + ", skipping stop - " + containerId);
      }
    }
  }

  private void handleContainerKill(String containerId, Map<String, String> env,
      ContainerExecutor.Signal signal) throws ContainerExecutionException {
    DockerCommandExecutor.DockerContainerStatus containerStatus =
        DockerCommandExecutor.getContainerStatus(containerId, conf,
            privilegedOperationExecutor);
    if (DockerCommandExecutor.isKillable(containerStatus)) {
      DockerKillCommand dockerKillCommand =
          new DockerKillCommand(containerId).setSignal(signal.name());
      DockerCommandExecutor.executeDockerCommand(dockerKillCommand, containerId,
          env, conf, privilegedOperationExecutor, false);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Container status is " + containerStatus.getName()
                + ", skipping kill - " + containerId);
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
          DockerCommandExecutor.getContainerStatus(containerId, conf,
              privilegedOperationExecutor);
      if (DockerCommandExecutor.isRemovable(containerStatus)) {
        DockerRmCommand dockerRmCommand = new DockerRmCommand(containerId);
        DockerCommandExecutor.executeDockerCommand(dockerRmCommand, containerId,
            env, conf, privilegedOperationExecutor, false);
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
    }
  }
}
