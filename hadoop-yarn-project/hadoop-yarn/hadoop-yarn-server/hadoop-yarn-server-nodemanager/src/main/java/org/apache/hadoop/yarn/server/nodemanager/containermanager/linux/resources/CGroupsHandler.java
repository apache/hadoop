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
   * List of supported cgroup controller types. The two boolean variables denote whether
   * the controller is valid in v1, v2 or both.
   */
  enum CGroupController {
    NET_CLS("net_cls", true, false),
    BLKIO("blkio", true, false),
    CPUACCT("cpuacct", true, false),
    FREEZER("freezer", true, false),
    DEVICES("devices", true, false),

    // v2 specific
    IO("io", false, true),

    // present in v1 and v2
    CPU("cpu", true, true),
    CPUSET("cpuset", true, true),
    MEMORY("memory", true, true);

    private final String name;
    private final boolean inV1;
    private final boolean inV2;

    CGroupController(String name, boolean inV1, boolean inV2) {
      this.name = name;
      this.inV1 = inV1;
      this.inV2 = inV2;
    }

    public String getName() {
      return name;
    }

    public boolean isInV1() {
      return inV1;
    }

    public boolean isInV2() {
      return inV2;
    }

    /**
     * Returns a set of valid cgroup controller names for v1.
     * @return a set of valid cgroup controller names for v1.
     */
    public static Set<String> getValidV1CGroups() {
      HashSet<String> validCgroups = new HashSet<>();
      for (CGroupController controller : CGroupController.values()) {
        if (controller.isInV1()) {
          validCgroups.add(controller.getName());
        }
      }
      return validCgroups;
    }

    /**
     * Returns a set of valid cgroup controller names for v2.
     * @return a set of valid cgroup controller names for v2.
     */
    public static Set<String> getValidV2CGroups() {
      HashSet<String> validCgroups = new HashSet<>();
      for (CGroupController controller : CGroupController.values()) {
        if (controller.isInV2()) {
          validCgroups.add(controller.getName());
        }
      }
      return validCgroups;
    }
  }

  // v1 specific params
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

  // v2 specific params
  String CGROUP_CONTROLLERS_FILE = "cgroup.controllers";
  String CGROUP_SUBTREE_CONTROL_FILE = "cgroup.subtree_control";
  String CGROUP_CPU_MAX = "max";
  String CGROUP_MEMORY_MAX = "max";
  String CGROUP_MEMORY_LOW = "low";

  // present in v1 and v2
  String CGROUP_PROCS_FILE = "cgroup.procs";
  String CGROUP_PARAM_CLASSID = "classid";
  String CGROUP_PARAM_WEIGHT = "weight";

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
   * Gets the valid cgroup controller names based on the version used.
   * @return a set containing the valid controller names for the used cgroup version.
   */
  Set<String> getValidCGroups();

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

  /**
   * Returns CGroupV2 Mount Path.
   * @return parameter value as read from the parameter file
   */
  String getCGroupV2MountPath();
}
