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
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.impl.pb.client.CsiAdaptorProtocolPBClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.util.csi.CsiConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime.isDockerContainerRequested;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.RuncContainerRuntime.isRuncContainerRequested;

/**
 * <p>This class is a {@link ContainerRuntime} implementation that uses the
 * native {@code container-executor} binary via a
 * {@link PrivilegedOperationExecutor} instance to launch processes inside
 * OCI-compliant containers.</p>
 *
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class OCIContainerRuntime implements LinuxContainerRuntime {
  private static final Logger LOG =
      LoggerFactory.getLogger(OCIContainerRuntime.class);

  private static final Pattern HOSTNAME_PATTERN = Pattern.compile(
      "^[a-zA-Z0-9][a-zA-Z0-9_.-]+$");
  static final Pattern USER_MOUNT_PATTERN = Pattern.compile(
      "(?<=^|,)([^:\\x00]+):([^:\\x00]+)" +
      "(:(r[ow]|(r[ow][+])?(r?shared|r?slave|r?private)))?(?:,|$)");
  static final Pattern TMPFS_MOUNT_PATTERN = Pattern.compile(
      "^/[^:\\x00]+$");
  static final String PORTS_MAPPING_PATTERN =
      "^:[0-9]+|^[0-9]+:[0-9]+|^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]" +
      "|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])" +
      ":[0-9]+:[0-9]+$";
  private static final int HOST_NAME_LENGTH = 64;

  @InterfaceAudience.Private
  public static final String RUNTIME_PREFIX = "YARN_CONTAINER_RUNTIME_%s_%s";
  @InterfaceAudience.Private
  public static final String CONTAINER_PID_NAMESPACE_SUFFIX =
      "CONTAINER_PID_NAMESPACE";
  @InterfaceAudience.Private
  public static final String RUN_PRIVILEGED_CONTAINER_SUFFIX =
      "RUN_PRIVILEGED_CONTAINER";

  private Map<String, CsiAdaptorProtocol> csiClients = new HashMap<>();

  abstract Set<String> getAllowedNetworks();
  abstract Set<String> getAllowedRuntimes();
  abstract boolean getHostPidNamespaceEnabled();
  abstract boolean getPrivilegedContainersEnabledOnCluster();
  abstract AccessControlList getPrivilegedContainersAcl();
  abstract String getEnvOciContainerPidNamespace();
  abstract String getEnvOciContainerRunPrivilegedContainer();

  public OCIContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor) {
    this(privilegedOperationExecutor, ResourceHandlerModule
        .getCGroupsHandler());
  }

  public OCIContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor, CGroupsHandler cGroupsHandler) {
  }

  public void initialize(Configuration conf, Context nmContext)
      throws ContainerExecutionException {

  }

  public static boolean isOCICompliantContainerRequested(
      Configuration daemonConf, Map<String, String> env) {
    return isDockerContainerRequested(daemonConf, env) ||
        isRuncContainerRequested(daemonConf, env);
  }

  @VisibleForTesting
  protected String mountReadOnlyPath(String mount,
      Map<Path, List<String>> localizedResources)
      throws ContainerExecutionException {
    for (Map.Entry<Path, List<String>> resource :
        localizedResources.entrySet()) {
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
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
  }

  protected String getUserIdInfo(String userName)
      throws ContainerExecutionException {
    String id;
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

  protected String[] getGroupIdInfo(String userName)
      throws ContainerExecutionException {
    String[] id;
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

  protected void validateContainerNetworkType(String network)
      throws ContainerExecutionException {
    Set<String> allowedNetworks = getAllowedNetworks();
    if (allowedNetworks.contains(network)) {
      return;
    }

    String msg = "Disallowed network:  '" + network
        + "' specified. Allowed networks: are " + allowedNetworks
        .toString();
    throw new ContainerExecutionException(msg);
  }

  protected void validateContainerRuntimeType(String runtime)
      throws ContainerExecutionException {
    Set<String> allowedRuntimes = getAllowedRuntimes();
    if (runtime == null || runtime.isEmpty()
        || allowedRuntimes.contains(runtime)) {
      return;
    }

    String msg = "Disallowed runtime:  '" + runtime
            + "' specified. Allowed runtimes: are " + allowedRuntimes
            .toString();
    throw new ContainerExecutionException(msg);
  }

  /**
   * Return whether the YARN container is allowed to run using the host's PID
   * namespace for the OCI-compliant container. For this to be allowed, the
   * submitting user must request the feature and the feature must be enabled
   * on the cluster.
   *
   * @param container the target YARN container
   * @return whether host pid namespace is requested and allowed
   * @throws ContainerExecutionException if host pid namespace is requested
   * but is not allowed
   */
  protected boolean allowHostPidNamespace(Container container)
      throws ContainerExecutionException {
    Map<String, String> environment = container.getLaunchContext()
        .getEnvironment();
    String envOciContainerPidNamespace = getEnvOciContainerPidNamespace();

    String pidNamespace = environment.get(envOciContainerPidNamespace);

    if (pidNamespace == null) {
      return false;
    }

    if (!pidNamespace.equalsIgnoreCase("host")) {
      LOG.warn("NOT requesting PID namespace. Value of " +
          envOciContainerPidNamespace
          + "is invalid: " + pidNamespace);
      return false;
    }

    boolean hostPidNamespaceEnabled = getHostPidNamespaceEnabled();

    if (!hostPidNamespaceEnabled) {
      String message = "Host pid namespace being requested but this is not "
          + "enabled on this cluster";
      LOG.warn(message);
      throw new ContainerExecutionException(message);
    }

    return true;
  }


  protected static void validateHostname(String hostname) throws
      ContainerExecutionException {
    if (hostname != null && !hostname.isEmpty()) {
      if (!HOSTNAME_PATTERN.matcher(hostname).matches()) {
        throw new ContainerExecutionException("Hostname '" + hostname
            + "' doesn't match OCI-compliant hostname pattern");
      }
      if (hostname.length() > HOST_NAME_LENGTH) {
        throw new ContainerExecutionException(
            "Hostname can not be greater than " + HOST_NAME_LENGTH
                + " characters: " + hostname);
      }
    }
  }

  /**
   * Return whether the YARN container is allowed to run in a privileged
   * OCI-compliant container. For a privileged container to be allowed all of
   * the following three conditions must be satisfied:
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
  protected boolean allowPrivilegedContainerExecution(Container container)
      throws ContainerExecutionException {

    if(!isContainerRequestedAsPrivileged(container)) {
      return false;
    }

    LOG.info("Privileged container requested for : " + container
        .getContainerId().toString());

    //Ok, so we have been asked to run a privileged container. Security
    // checks need to be run. Each violation is an error.

    //check if privileged containers are enabled.
    boolean privilegedContainersEnabledOnCluster =
        getPrivilegedContainersEnabledOnCluster();

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

    if (!getPrivilegedContainersAcl().isUserAllowed(submitterUgi)) {
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
   * @return true if container is requested as privileged
   */
  protected boolean isContainerRequestedAsPrivileged(
      Container container) {
    String envOciContainerRunPrivilegedContainer =
        getEnvOciContainerRunPrivilegedContainer();
    String runPrivilegedContainerEnvVar = container.getLaunchContext()
        .getEnvironment().get(envOciContainerRunPrivilegedContainer);
    return Boolean.parseBoolean(runPrivilegedContainerEnvVar);
  }

  public Map<String, CsiAdaptorProtocol> getCsiClients() {
    return csiClients;
  }

   /**
   * Initiate CSI clients to talk to the CSI adaptors on this node and
   * cache the clients for easier fetch.
   * @param config configuration
   * @throws ContainerExecutionException
   */
  protected void initiateCsiClients(Configuration config)
      throws ContainerExecutionException {
    String[] driverNames = CsiConfigUtils.getCsiDriverNames(config);
    if (driverNames != null && driverNames.length > 0) {
      for (String driverName : driverNames) {
        try {
          // find out the adaptors service address
          InetSocketAddress adaptorServiceAddress =
              CsiConfigUtils.getCsiAdaptorAddressForDriver(driverName, config);
          LOG.info("Initializing a csi-adaptor-client for csi-adaptor {},"
              + " csi-driver {}", adaptorServiceAddress.toString(), driverName);
          CsiAdaptorProtocolPBClientImpl client =
              new CsiAdaptorProtocolPBClientImpl(1L, adaptorServiceAddress,
                  config);
          csiClients.put(driverName, client);
        } catch (IOException | YarnException e1) {
          throw new ContainerExecutionException(e1.getMessage());
        }
      }
    }
  }

  public static String formatOciEnvKey(String runtimeTypeUpper,
      String envKeySuffix) {
    return String.format(RUNTIME_PREFIX, runtimeTypeUpper, envKeySuffix);
  }
}
