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

/**
 * Provides CGroups functionality. Implementations are expected to be
 * thread-safe
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface CGroupsHandler {

  public enum CGroupController {
    CPU("cpu"),
    NET_CLS("net_cls"),
    BLKIO("blkio"),
    MEMORY("memory");

    private final String name;

    CGroupController(String name) {
      this.name = name;
    }

    String getName() {
      return name;
    }
  }

  public static final String CGROUP_FILE_TASKS = "tasks";
  public static final String CGROUP_PARAM_CLASSID = "classid";
  public static final String CGROUP_PARAM_BLKIO_WEIGHT = "weight";

  String CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES = "limit_in_bytes";
  String CGROUP_PARAM_MEMORY_SOFT_LIMIT_BYTES = "soft_limit_in_bytes";
  String CGROUP_PARAM_MEMORY_SWAPPINESS = "swappiness";


  String CGROUP_CPU_PERIOD_US = "cfs_period_us";
  String CGROUP_CPU_QUOTA_US = "cfs_quota_us";
  String CGROUP_CPU_SHARES = "shares";

  /**
   * Mounts a cgroup controller
   * @param controller - the controller being mounted
   * @throws ResourceHandlerException
   */
  public void mountCGroupController(CGroupController controller)
      throws ResourceHandlerException;

  /**
   * Creates a cgroup for a given controller
   * @param controller - controller type for which the cgroup is being created
   * @param cGroupId - id of the cgroup being created
   * @return full path to created cgroup
   * @throws ResourceHandlerException
   */
  public String createCGroup(CGroupController controller, String cGroupId)
      throws ResourceHandlerException;

  /**
   * Deletes the specified cgroup
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup being deleted
   * @throws ResourceHandlerException
   */
  public void deleteCGroup(CGroupController controller, String cGroupId) throws
      ResourceHandlerException;

  /**
   * Gets the relative path for the cgroup, independent of a controller, for a
   * given cgroup id.
   * @param cGroupId - id of the cgroup
   * @return path for the cgroup relative to the root of (any) controller.
   */
  public String getRelativePathForCGroup(String cGroupId);

  /**
   * Gets the full path for the cgroup, given a controller and a cgroup id
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @return full path for the cgroup
   */
  public String getPathForCGroup(CGroupController controller, String
      cGroupId);

  /**
   * Gets the full path for the cgroup's tasks file, given a controller and a
   * cgroup id
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @return full path for the cgroup's tasks file
   */
  public String getPathForCGroupTasks(CGroupController controller, String
      cGroupId);

  /**
   * Gets the full path for a cgroup parameter, given a controller,
   * cgroup id and parameter name
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @param param - cgroup parameter ( e.g classid )
   * @return full path for the cgroup parameter
   */
  public String getPathForCGroupParam(CGroupController controller, String
      cGroupId, String param);

  /**
   * updates a cgroup parameter, given a controller, cgroup id, parameter name
   * and a parameter value
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @param param - cgroup parameter ( e.g classid )
   * @param value - value to be written to the parameter file
   * @throws ResourceHandlerException
   */
  public void updateCGroupParam(CGroupController controller, String cGroupId,
      String param, String value) throws ResourceHandlerException;

  /**
   * reads a cgroup parameter value, given a controller, cgroup id, parameter
   * name
   * @param controller - controller type for the cgroup
   * @param cGroupId - id of the cgroup
   * @param param - cgroup parameter ( e.g classid )
   * @return parameter value as read from the parameter file
   * @throws ResourceHandlerException
   */
  public String getCGroupParam(CGroupController controller, String cGroupId,
      String param) throws ResourceHandlerException;
}
