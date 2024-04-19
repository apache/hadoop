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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
  protected Map<String, Set<String>> parsePreConfiguredMountPath() throws IOException {
    Map<String, Set<String>> controllerMappings = new HashMap<>();
    String controllerPath = this.cGroupsMountConfig.getMountPath() + Path.SEPARATOR + this.cGroupPrefix;
    controllerMappings.put(this.cGroupsMountConfig.getMountPath(), readControllersFile(controllerPath));
    return controllerMappings;
  }

  @Override
  protected Set<String> handleMtabEntry(String path, String type, String options) throws IOException {
    if (type.equals(CGROUP2_FSTYPE)) {
      return readControllersFile(path);
    }

    return null;
  }

  @Override
  protected void mountCGroupController(CGroupController controller) {
    throw new UnsupportedOperationException("Mounting cgroup controllers is not supported in cgroup v2");
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

    String enabledControllers = FileUtils.readFileToString(cgroupControllersFile, StandardCharsets.UTF_8);
    Set<String> validCGroups =
            CGroupsHandler.CGroupController.getValidCGroups();
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
   * Update the subtree_control file to enable subsequent container based cgroups to use the same controllers.
   * @param yarnHierarchy path to the yarn cgroup under which the container cgroups will be created
   * @throws ResourceHandlerException if the controllers file cannot be updated
   */
  @Override
  protected void updateEnabledControllersInHierarchy(File yarnHierarchy) throws ResourceHandlerException {
    try {
      Set<String> enabledControllers = readControllersFile(yarnHierarchy.getAbsolutePath());
      if (enabledControllers.isEmpty()) {
        throw new ResourceHandlerException("No valid controllers found in the cgroup hierarchy: " +
                yarnHierarchy.getAbsolutePath());
      }

      File subtreeControlFile = new File(yarnHierarchy.getAbsolutePath()
          + Path.SEPARATOR + CGROUP_SUBTREE_CONTROL_FILE);
      if (!subtreeControlFile.exists()) {
        throw new ResourceHandlerException("No subtree control file found in the cgroup hierarchy: " +
                yarnHierarchy.getAbsolutePath());
      }

      String subtreeControllers = FileUtils.readFileToString(subtreeControlFile, StandardCharsets.UTF_8);
      Set<String> subtreeControllerSet = new HashSet<>(Arrays.asList(subtreeControllers.split(" ")));
      subtreeControllerSet.retainAll(CGroupsHandler.CGroupController.getValidCGroups());

      if (subtreeControllerSet.containsAll(enabledControllers)) {
        return;
      }
      Writer w = new OutputStreamWriter(Files.newOutputStream(subtreeControlFile.toPath()), StandardCharsets.UTF_8);
      try(PrintWriter pw = new PrintWriter(w)) {
        pw.write(String.join(" ", enabledControllers));
      }
    } catch (IOException e) {
      throw new ResourceHandlerException("Failed to update the controllers file in the cgroup hierarchy: " +
              yarnHierarchy.getAbsolutePath(), e);
    }
  }

}