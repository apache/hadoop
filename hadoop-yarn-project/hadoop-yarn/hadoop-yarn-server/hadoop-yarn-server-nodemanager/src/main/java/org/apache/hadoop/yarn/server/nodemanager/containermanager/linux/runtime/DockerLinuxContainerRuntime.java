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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerClient;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DockerLinuxContainerRuntime implements LinuxContainerRuntime {
  private static final Log LOG = LogFactory.getLog(
      DockerLinuxContainerRuntime.class);

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
  public static final String ENV_DOCKER_CONTAINER_RUN_PRIVILEGED_CONTAINER =
      "YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_LOCAL_RESOURCE_MOUNTS =
      "YARN_CONTAINER_RUNTIME_DOCKER_LOCAL_RESOURCE_MOUNTS";

  private Configuration conf;
  private DockerClient dockerClient;
  private PrivilegedOperationExecutor privilegedOperationExecutor;
  private Set<String> allowedNetworks = new HashSet<>();
  private String defaultNetwork;
  private CGroupsHandler cGroupsHandler;
  private AccessControlList privilegedContainersAcl;

  public static boolean isDockerContainerRequested(
      Map<String, String> env) {
    if (env == null) {
      return false;
    }

    String type = env.get(ContainerRuntimeConstants.ENV_CONTAINER_TYPE);

    return type != null && type.equals("docker");
  }

  public DockerLinuxContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor) {
    this(privilegedOperationExecutor, ResourceHandlerModule
        .getCGroupsHandler());
  }

  //A constructor with an injected cGroupsHandler primarily used for testing.
  @VisibleForTesting
  public DockerLinuxContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor, CGroupsHandler cGroupsHandler) {
    this.privilegedOperationExecutor = privilegedOperationExecutor;

    if (cGroupsHandler == null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("cGroupsHandler is null - cgroups not in use.");
      }
    } else {
      this.cGroupsHandler = cGroupsHandler;
    }
  }

  @Override
  public void initialize(Configuration conf)
      throws ContainerExecutionException {
    this.conf = conf;
    dockerClient = new DockerClient(conf);
    allowedNetworks.clear();
    allowedNetworks.addAll(Arrays.asList(
        conf.getStrings(YarnConfiguration.NM_DOCKER_ALLOWED_CONTAINER_NETWORKS,
            YarnConfiguration.DEFAULT_NM_DOCKER_ALLOWED_CONTAINER_NETWORKS)));
    defaultNetwork = conf.get(
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

    privilegedContainersAcl = new AccessControlList(conf.get(
        YarnConfiguration.NM_DOCKER_PRIVILEGED_CONTAINERS_ACL,
        YarnConfiguration.DEFAULT_NM_DOCKER_PRIVILEGED_CONTAINERS_ACL));
  }

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {

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

  public void addCGroupParentIfRequired(String resourcesOptions,
      String containerIdStr, DockerRunCommand runCommand)
      throws ContainerExecutionException {
    if (cGroupsHandler == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("cGroupsHandler is null. cgroups are not in use. nothing to"
            + " do.");
      }
      return;
    }

    if (resourcesOptions.equals(
        (PrivilegedOperation.CGROUP_ARG_PREFIX + PrivilegedOperation
            .CGROUP_ARG_NO_TASKS))) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("no resource restrictions specified. not using docker's "
            + "cgroup options");
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("using docker's cgroups options");
      }

      String cGroupPath = "/" + cGroupsHandler.getRelativePathForCGroup(
          containerIdStr);

      if (LOG.isDebugEnabled()) {
        LOG.debug("using cgroup parent: " + cGroupPath);
      }

      runCommand.setCGroupParent(cGroupPath);
    }
  }

  private boolean allowPrivilegedContainerExecution(Container container)
      throws ContainerExecutionException {
    //For a privileged container to be run all of the following three conditions
    // must be satisfied:
    //1) Submitting user must request for a privileged container
    //2) Privileged containers must be enabled on the cluster
    //3) Submitting user must be whitelisted to run a privileged container

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

    if (LOG.isInfoEnabled()) {
      LOG.info("Privileged container requested for : " + container
          .getContainerId().toString());
    }

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

    if (LOG.isInfoEnabled()) {
      LOG.info("All checks pass. Launching privileged container for : "
          + container.getContainerId().toString());
    }

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

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    Map<String, String> environment = container.getLaunchContext()
        .getEnvironment();
    String imageName = environment.get(ENV_DOCKER_CONTAINER_IMAGE);
    String network = environment.get(ENV_DOCKER_CONTAINER_NETWORK);

    if(network == null || network.isEmpty()) {
      network = defaultNetwork;
    }

    validateContainerNetworkType(network);

    if (imageName == null) {
      throw new ContainerExecutionException(ENV_DOCKER_CONTAINER_IMAGE
          + " not set!");
    }

    String containerIdStr = container.getContainerId().toString();
    String runAsUser = ctx.getExecutionAttribute(RUN_AS_USER);
    Path containerWorkDir = ctx.getExecutionAttribute(CONTAINER_WORK_DIR);
    //List<String> -> stored as List -> fetched/converted to List<String>
    //we can't do better here thanks to type-erasure
    @SuppressWarnings("unchecked")
    List<String> localDirs = ctx.getExecutionAttribute(LOCAL_DIRS);
    @SuppressWarnings("unchecked")
    List<String> logDirs = ctx.getExecutionAttribute(LOG_DIRS);
    @SuppressWarnings("unchecked")
    List<String> containerLocalDirs = ctx.getExecutionAttribute(
        CONTAINER_LOCAL_DIRS);
    @SuppressWarnings("unchecked")
    List<String> containerLogDirs = ctx.getExecutionAttribute(
        CONTAINER_LOG_DIRS);
    @SuppressWarnings("unchecked")
    Map<Path, List<String>> localizedResources = ctx.getExecutionAttribute(
        LOCALIZED_RESOURCES);
    Set<String> capabilities = new HashSet<>(Arrays.asList(conf.getStrings(
        YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
        YarnConfiguration.DEFAULT_NM_DOCKER_CONTAINER_CAPABILITIES)));

    @SuppressWarnings("unchecked")
    DockerRunCommand runCommand = new DockerRunCommand(containerIdStr,
        runAsUser, imageName)
        .detachOnRun()
        .setContainerWorkDir(containerWorkDir.toString())
        .setNetworkType(network)
        .setCapabilities(capabilities)
        .addMountLocation("/etc/passwd", "/etc/password:ro");
    List<String> allDirs = new ArrayList<>(containerLocalDirs);

    allDirs.add(containerWorkDir.toString());
    allDirs.addAll(containerLogDirs);
    for (String dir: allDirs) {
      runCommand.addMountLocation(dir, dir);
    }

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
          runCommand.addMountLocation(src, dst + ":ro");
        }
      }
    }

    if (allowPrivilegedContainerExecution(container)) {
      runCommand.setPrivileged();
    }

    String resourcesOpts = ctx.getExecutionAttribute(RESOURCES_OPTIONS);

    addCGroupParentIfRequired(resourcesOpts, containerIdStr, runCommand);

   Path nmPrivateContainerScriptPath = ctx.getExecutionAttribute(
        NM_PRIVATE_CONTAINER_SCRIPT_PATH);

    String disableOverride = environment.get(
        ENV_DOCKER_CONTAINER_RUN_OVERRIDE_DISABLE);

    if (disableOverride != null && disableOverride.equals("true")) {
      if (LOG.isInfoEnabled()) {
        LOG.info("command override disabled");
      }
    } else {
      List<String> overrideCommands = new ArrayList<>();
      Path launchDst =
          new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);

      overrideCommands.add("bash");
      overrideCommands.add(launchDst.toUri().getPath());
      runCommand.setOverrideCommandWithArgs(overrideCommands);
    }

    String commandFile = dockerClient.writeCommandToTempFile(runCommand,
        containerIdStr);
    PrivilegedOperation launchOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.LAUNCH_DOCKER_CONTAINER);

    launchOp.appendArgs(runAsUser, ctx.getExecutionAttribute(USER),
        Integer.toString(PrivilegedOperation
            .RunAsUserCommand.LAUNCH_DOCKER_CONTAINER.getValue()),
        ctx.getExecutionAttribute(APPID),
        containerIdStr, containerWorkDir.toString(),
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

    try {
      privilegedOperationExecutor.executePrivilegedOperation(null,
          launchOp, null, container.getLaunchContext().getEnvironment(),
          false, false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Launch container failed. Exception: ", e);

      throw new ContainerExecutionException("Launch container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  public void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    PrivilegedOperation signalOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.SIGNAL_CONTAINER);

    signalOp.appendArgs(ctx.getExecutionAttribute(RUN_AS_USER),
        ctx.getExecutionAttribute(USER),
        Integer.toString(PrivilegedOperation
            .RunAsUserCommand.SIGNAL_CONTAINER.getValue()),
        ctx.getExecutionAttribute(PID),
        Integer.toString(ctx.getExecutionAttribute(SIGNAL).getValue()));

    try {
      PrivilegedOperationExecutor executor = PrivilegedOperationExecutor
          .getInstance(conf);

      executor.executePrivilegedOperation(null,
          signalOp, null, container.getLaunchContext().getEnvironment(),
          false, true);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Signal container failed. Exception: ", e);

      throw new ContainerExecutionException("Signal container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  public void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {

  }
}
