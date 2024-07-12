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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Support for interacting with various CGroup v1 subsystems. Thread-safe.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
class CGroupsHandlerImpl extends AbstractCGroupsHandler {
  private static final Logger LOG =
          LoggerFactory.getLogger(CGroupsHandlerImpl.class);
  private static final String CGROUP_FSTYPE = "cgroup";

  /**
   * Create cgroup v1 handler object.
   * @param conf configuration
   * @param privilegedOperationExecutor provides mechanisms to execute
   *                                    PrivilegedContainerOperations
   * @param mtab mount file location
   * @throws ResourceHandlerException if initialization failed
   */
  CGroupsHandlerImpl(Configuration conf, PrivilegedOperationExecutor
          privilegedOperationExecutor, String mtab)
          throws ResourceHandlerException {
    super(conf, privilegedOperationExecutor, mtab);
  }

  /**
   * Create cgroup v1 handler object.
   * @param conf configuration
   * @param privilegedOperationExecutor provides mechanisms to execute
   *                                    PrivilegedContainerOperations
   * @throws ResourceHandlerException if initialization failed
   */
  CGroupsHandlerImpl(Configuration conf, PrivilegedOperationExecutor
          privilegedOperationExecutor) throws ResourceHandlerException {
    this(conf, privilegedOperationExecutor, MTAB_FILE);
  }

  @Override
  public Set<String> getValidCGroups() {
    return CGroupController.getValidV1CGroups();
  }

  @Override
  protected List<CGroupController> getCGroupControllers() {
    return Arrays.stream(CGroupController.values()).filter(CGroupController::isInV1)
        .collect(Collectors.toList());
  }

  @Override
  protected Map<String, Set<String>> parsePreConfiguredMountPath() throws IOException {
    return ResourceHandlerModule.
            parseConfiguredCGroupPath(this.cGroupsMountConfig.getMountPath());
  }

  @Override
  protected Set<String> handleMtabEntry(String path, String type, String options) {
    Set<String> validCgroups = getValidCGroups();

    if (type.equals(CGROUP_FSTYPE)) {
      Set<String> controllerSet =
              new HashSet<>(Arrays.asList(options.split(",")));
      // Collect the valid subsystem names
      controllerSet.retainAll(validCgroups);
      return controllerSet;
    }

    return null;
  }

  @Override
  protected void mountCGroupController(CGroupController controller)
          throws ResourceHandlerException {
    String existingMountPath = getControllerPath(controller);
    String requestedMountPath =
            new File(cGroupsMountConfig.getMountPath(),
                    controller.getName()).getAbsolutePath();

    if (!requestedMountPath.equals(existingMountPath)) {
      //lock out other readers/writers till we are done
      rwLock.writeLock().lock();
      try {
        // If the controller was already mounted we have to mount it
        // with the same options to clone the mount point otherwise
        // the operation will fail
        String mountOptions;
        if (existingMountPath != null) {
          mountOptions = Joiner.on(',')
                  .join(parsedMtab.get(existingMountPath));
        } else {
          mountOptions = controller.getName();
        }

        String cGroupKV =
                mountOptions + "=" + requestedMountPath;
        PrivilegedOperation.OperationType opType = PrivilegedOperation
                .OperationType.MOUNT_CGROUPS;
        PrivilegedOperation op = new PrivilegedOperation(opType);

        op.appendArgs(cGroupPrefix, cGroupKV);
        LOG.info("Mounting controller " + controller.getName() + " at " +
                requestedMountPath);
        privilegedOperationExecutor.executePrivilegedOperation(op, false);

        //if privileged operation succeeds, update controller paths
        controllerPaths.put(controller, requestedMountPath);
      } catch (PrivilegedOperationException e) {
        LOG.error("Failed to mount controller: " + controller.getName());
        throw new ResourceHandlerException("Failed to mount controller: "
                + controller.getName());
      } finally {
        rwLock.writeLock().unlock();
      }
    } else {
      LOG.info("CGroup controller already mounted at: " + existingMountPath);
    }
  }

  @Override
  protected void updateEnabledControllersInHierarchy(
      File yarnHierarchy, CGroupController controller) {
    // no-op in cgroup v1
  }
}