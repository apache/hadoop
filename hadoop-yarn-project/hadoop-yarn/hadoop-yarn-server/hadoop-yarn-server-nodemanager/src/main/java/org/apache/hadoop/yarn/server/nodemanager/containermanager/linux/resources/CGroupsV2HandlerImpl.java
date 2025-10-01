/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Support for interacting with various CGroup v2 subsystems. Thread-safe.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
class CGroupsV2HandlerImpl extends AbstractCGroupsHandler {
  private static final Logger LOG =
          LoggerFactory.getLogger(CGroupsV2HandlerImpl.class);

  private static final String CGROUP2_FSTYPE = "cgroup2";

  /**
   * Create cgroup v2 handler object.
   * @param conf configuration
   * @param privilegedOperationExecutor provides mechanisms to execute
   *                                    PrivilegedContainerOperations
   * @param mtab mount file location
   * @throws ResourceHandlerException if initialization failed
   */
  CGroupsV2HandlerImpl(Configuration conf, PrivilegedOperationExecutor
          privilegedOperationExecutor, String mtab)
          throws ResourceHandlerException {
    super(conf, privilegedOperationExecutor, mtab);
  }

  /**
   * Create cgroup v2 handler object.
   * @param conf configuration
   * @param privilegedOperationExecutor provides mechanisms to execute
   *                                    PrivilegedContainerOperations
   * @throws ResourceHandlerException if initialization failed
   */
  CGroupsV2HandlerImpl(Configuration conf, PrivilegedOperationExecutor
          privilegedOperationExecutor) throws ResourceHandlerException {
    this(conf, privilegedOperationExecutor, MTAB_FILE);
  }

  @Override
  public Set<String> getValidCGroups() {
    return CGroupController.getValidV2CGroups();
  }

  @Override
  protected List<CGroupController> getCGroupControllers() {
    return Arrays.stream(CGroupController.values()).filter(CGroupController::isInV2)
            .collect(Collectors.toList());
  }

  @Override
  protected Map<String, Set<String>> parsePreConfiguredMountPath() {
    Map<String, Set<String>> controllerMappings = new HashMap<>();
    try {
      controllerMappings.put(this.cGroupsMountConfig.getV2MountPath(),
          readControllersFile(this.cGroupsMountConfig.getV2MountPath()));
    } catch (IOException e) {
      // Failing to read the cgroup.controllers file in the preconfigured might mean
      // that the node is not using cgroup v2, or no cgroup v2 hierarchy is mounted
      // under the specified path. If the node is using v1 we will fall back to cgroup v1
      // in ResourceHandlerModule.initializeCGroupHandlers. If the cgroup v2 hierarchy is
      // not mounted and no cgroup v1 hierarchy is mounted, we will fail to start the NM.
      LOG.info("Failed to read the cgroup controllers file in the preconfigured directory: {}. " +
          "The cgroup v2 hierarchy may not be mounted under the specified path, or the node" +
          " might be using cgroup v1.", this.cGroupsMountConfig.getV2MountPath());
      LOG.debug("Exception while reading the cgroup.controllers file: ", e);
    }
    return controllerMappings;
  }

  @Override
  protected Set<String> handleMtabEntry(String path, String type, String options)
      throws IOException {
    if (type.equals(CGROUP2_FSTYPE)) {
      return readControllersFile(path);
    }

    return null;
  }

  @Override
  protected void mountCGroupController(CGroupController controller) {
    throw new UnsupportedOperationException("Mounting cgroup controllers is not supported in " +
        "cgroup v2");
  }

  /**
   * Parse the cgroup v2 controllers file (cgroup.controllers) to check the enabled controllers.
   * @param cgroupPath path to the cgroup directory
   * @return set of enabled and YARN supported controllers.
   * @throws IOException if the file is not found or cannot be read
   */
  public Set<String> readControllersFile(String cgroupPath) throws IOException {
    File cgroupControllersFile = new File(cgroupPath + Path.SEPARATOR + CGROUP_CONTROLLERS_FILE);
    if (!cgroupControllersFile.exists()) {
      throw new IOException("No cgroup controllers file found in the directory specified: " +
              cgroupPath);
    }

    String enabledControllers = FileUtils.readFileToString(cgroupControllersFile,
        StandardCharsets.UTF_8);
    Set<String> validCGroups = getValidCGroups();
    Set<String> controllerSet =
            new HashSet<>(Arrays.asList(enabledControllers.split(" ")));
    // Collect the valid subsystem names
    controllerSet.retainAll(validCGroups);
    if (controllerSet.isEmpty()) {
      LOG.warn("The following cgroup directory doesn't contain any supported controllers: " +
              cgroupPath);
    }

    return controllerSet;
  }

  /**
   * The cgroup.subtree_control file is used to enable controllers for a subtree of the cgroup
   * hierarchy (the current level excluded).
   * From the documentation: A read-write space separated values file which exists on all
   *  cgroups. Starts out empty. When read, it shows space separated list of the controllers which
   *  are enabled to control resource distribution from the cgroup to its children.
   *  Space separated list of controllers prefixed with '+' or '-'
   *  can be written to enable or disable controllers.
   * Since YARN will create a sub-cgroup for each container, we need to enable the controllers
   * for the subtree. Update the subtree_control file to enable subsequent container based cgroups
   * to use the same controllers.
   * If a cgroup.subtree_control file is present, but it doesn't contain all the controllers
   * enabled in the cgroup.controllers file, this method will update the subtree_control file
   * to include all the controllers.
   * @param yarnHierarchy path to the yarn cgroup under which the container cgroups will be created
   * @throws ResourceHandlerException if the controllers file cannot be updated
   */
  @Override
  protected void updateEnabledControllersInHierarchy(
      File yarnHierarchy, CGroupController controller) throws ResourceHandlerException {
    try {
      Set<String> enabledControllers = readControllersFile(yarnHierarchy.getAbsolutePath());
      if (!enabledControllers.contains(controller.getName())) {
        String errorMsg = String.format(
            "The controller %s is not enabled in the cgroup hierarchy: %s. Please enable it in " +
                "in the %s/cgroup.subtree_control file.",
            controller.getName(), yarnHierarchy.getAbsolutePath(),
            yarnHierarchy.getParentFile().getAbsolutePath());

        throw new ResourceHandlerException(getErrorWithDetails(
            errorMsg, controller.getName(),
            yarnHierarchy.getAbsolutePath()));
      }

      File subtreeControlFile = new File(yarnHierarchy.getAbsolutePath()
          + Path.SEPARATOR + CGROUP_SUBTREE_CONTROL_FILE);
      if (!subtreeControlFile.exists()) {
        String errorMsg = "No subtree control file found in the cgroup hierarchy: " +
            yarnHierarchy.getAbsolutePath();
        throw new ResourceHandlerException(getErrorWithDetails(
            errorMsg, controller.getName(),
            yarnHierarchy.getAbsolutePath()));
      }
      if (!subtreeControlFile.canWrite()) {
        String errorMsg = "Cannot write the cgroup.subtree_control file in the " +
            "cgroup hierarchy: " + yarnHierarchy.getAbsolutePath();
        throw new ResourceHandlerException(getErrorWithDetails(
            errorMsg, controller.getName(),
            yarnHierarchy.getAbsolutePath()));
      }

      Writer w = new OutputStreamWriter(Files.newOutputStream(subtreeControlFile.toPath(),
          StandardOpenOption.APPEND), StandardCharsets.UTF_8);
      try(PrintWriter pw = new PrintWriter(w)) {
        LOG.info("Appending the following controller to the cgroup.subtree_control file: {}, " +
                "for the cgroup hierarchy: {}", controller.getName(),
            yarnHierarchy.getAbsolutePath());
        pw.write("+" + controller.getName());
        if (pw.checkError()) {
          String errorMsg = "Failed to add the controller to the " +
              "cgroup.subtree_control file in the cgroup hierarchy: " +
              yarnHierarchy.getAbsolutePath();
          throw new ResourceHandlerException(getErrorWithDetails(
              errorMsg, controller.getName(),
              yarnHierarchy.getAbsolutePath()));
        }
      }
    } catch (IOException e) {
      String errorMsg = "Failed to update the cgroup.subtree_control file in the " +
          "cgroup hierarchy: " + yarnHierarchy.getAbsolutePath();
      throw new ResourceHandlerException(getErrorWithDetails(
          errorMsg, controller.getName(),
          yarnHierarchy.getAbsolutePath()));
    }
  }
}