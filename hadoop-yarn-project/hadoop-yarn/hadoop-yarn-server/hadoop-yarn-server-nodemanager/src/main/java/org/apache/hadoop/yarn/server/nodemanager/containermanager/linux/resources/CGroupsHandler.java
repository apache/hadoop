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

import java.util.HashSet;
import java.util.Set;

/**
 * Provides CGroups functionality. Implementations are expected to be
 * thread-safe
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface CGroupsHandler {

  /**
   * List of supported cgroup subsystem types.
   */
  enum CGroupController {
    CPU("cpu"),
    NET_CLS("net_cls"),
    BLKIO("blkio"),
    MEMORY("memory"),
    CPUACCT("cpuacct"),
    CPUSET("cpuset"),
    FREEZER("freezer"),
    DEVICES("devices");

    private final String name;

    CGroupController(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    /**
     * Get the list of valid cgroup names.
     * @return The set of cgroup name strings
     */
    public static Set<String> getValidCGroups() {
      HashSet<String> validCgroups = new HashSet<>();
      for (CGroupController controller : CGroupController.values()) {
        validCgroups.add(controller.getName());
      }
      return validCgroups;
    }
  }

  String CGROUP_PROCS_FILE = "cgroup.procs";
  String CGROUP_PARAM_CLASSID = "classid";
  String CGROUP_PARAM_BLKIO_WEIGHT = "weight";

  String CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES = "limit_in_bytes";
  String CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES = "memsw.limit_in_bytes";
  String CGROUP_PARAM_MEMORY_SOFT_LIMIT_BYTES = "soft_limit_in_bytes";
  String CGROUP_PARAM_MEMORY_OOM_CONTROL = "oom_control";
  String CGROUP_PARAM_MEMORY_SWAPPINESS = "swappiness";
  String CGROUP_PARAM_MEMORY_USAGE_BYTES = "usage_in_bytes";
  String CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES = "memsw.usage_in_bytes";
  String CGROUP_NO_LIMIT = "-1";
  String UNDER_OOM = "under_oom 1";


  String CGROUP_CPU_PERIOD_US = "cfs_period_us";
  String CGROUP_CPU_QUOTA_US = "cfs_quota_us";
  String CGROUP_CPU_SHARES = "shares";

  /**
   * Mounts or initializes a cgroup controller.
   * @param controller - the controller being initialized
   * @throws ResourceHandlerException the initialization failed due to the
   * environment
   */
  void initializeCGroupController(CGroupController controller)
      throws ResourceHandlerException;

  /**
   * Creates a cgroup for a given controller.
   * @param controller - controller type for which the cgroup is being created
   * @param cGroupId - id of the cgroup being created
   * @return full path to created cgroup
   * @throws ResourceHandlerException creation failed
   */
  String createCGroup(CGroupController controller, String cGroupId)
      throws ResourceHandlerException;

  /**
   * Deletes the specified cgroup.
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup being deleted
   * @throws ResourceHandlerException deletion failed
   */
  void deleteCGroup(CGroupController controller, String cGroupId) throws
      ResourceHandlerException;

  /**
   * Gets the absolute path to the specified cgroup controller.
   * @param controller - controller type for the cgroup
   * @return the root of the controller.
   */
  String getControllerPath(CGroupController controller);

  /**
   * Gets the relative path for the cgroup, independent of a controller, for a
   * given cgroup id.
   * @param cGroupId - id of the cgroup
   * @return path for the cgroup relative to the root of (any) controller.
   */
  String getRelativePathForCGroup(String cGroupId);

  /**
   * Gets the full path for the cgroup, given a controller and a cgroup id.
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @return full path for the cgroup
   */
  String getPathForCGroup(CGroupController controller, String
      cGroupId);

  /**
   * Gets the full path for the cgroup's tasks file, given a controller and a
   * cgroup id.
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @return full path for the cgroup's tasks file
   */
  String getPathForCGroupTasks(CGroupController controller, String
      cGroupId);

  /**
   * Gets the full path for a cgroup parameter, given a controller,
   * cgroup id and parameter name.
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @param param - cgroup parameter ( e.g classid )
   * @return full path for the cgroup parameter
   */
  String getPathForCGroupParam(CGroupController controller, String
      cGroupId, String param);

  /**
   * updates a cgroup parameter, given a controller, cgroup id, parameter name.
   * and a parameter value
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @param param - cgroup parameter ( e.g classid )
   * @param value - value to be written to the parameter file
   * @throws ResourceHandlerException the operation failed
   */
  void updateCGroupParam(CGroupController controller, String cGroupId,
      String param, String value) throws ResourceHandlerException;

  /**
   * reads a cgroup parameter value, given a controller, cgroup id, parameter.
   * name
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @param param - cgroup parameter ( e.g classid )
   * @return parameter value as read from the parameter file
   * @throws ResourceHandlerException the operation failed
   */
  String getCGroupParam(CGroupController controller, String cGroupId,
      String param) throws ResourceHandlerException;

  /**
   * Returns CGroup Mount Path.
   * @return parameter value as read from the parameter file
   */
  String getCGroupMountPath();
}
