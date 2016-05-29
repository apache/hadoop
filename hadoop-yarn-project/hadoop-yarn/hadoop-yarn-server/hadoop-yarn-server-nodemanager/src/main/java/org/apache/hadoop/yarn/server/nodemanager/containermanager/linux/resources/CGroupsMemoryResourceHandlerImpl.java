/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.util.ArrayList;
import java.util.List;

/**
 * Handler class to handle the memory controller. YARN already ships a
 * physical memory monitor in Java but it isn't as
 * good as CGroups. This handler sets the soft and hard memory limits. The soft
 * limit is set to 90% of the hard limit.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CGroupsMemoryResourceHandlerImpl implements MemoryResourceHandler {

  static final Log LOG = LogFactory.getLog(
      CGroupsMemoryResourceHandlerImpl.class);
  private static final CGroupsHandler.CGroupController MEMORY =
      CGroupsHandler.CGroupController.MEMORY;

  private CGroupsHandler cGroupsHandler;
  private int swappiness = 0;
  // multiplier to set the soft limit - value should be between 0 and 1
  private float softLimit = 0.0f;

  CGroupsMemoryResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration conf)
      throws ResourceHandlerException {
    boolean pmemEnabled =
        conf.getBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED,
            YarnConfiguration.DEFAULT_NM_PMEM_CHECK_ENABLED);
    boolean vmemEnabled =
        conf.getBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED,
            YarnConfiguration.DEFAULT_NM_VMEM_CHECK_ENABLED);
    if (pmemEnabled || vmemEnabled) {
      String msg = "The default YARN physical and/or virtual memory health"
          + " checkers as well as the CGroups memory controller are enabled. "
          + "If you wish to use the Cgroups memory controller, please turn off"
          + " the default physical/virtual memory checkers by setting "
          + YarnConfiguration.NM_PMEM_CHECK_ENABLED + " and "
          + YarnConfiguration.NM_VMEM_CHECK_ENABLED + " to false.";
      throw new ResourceHandlerException(msg);
    }
    this.cGroupsHandler.mountCGroupController(MEMORY);
    swappiness = conf
        .getInt(YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS,
            YarnConfiguration.DEFAULT_NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS);
    if (swappiness < 0 || swappiness > 100) {
      throw new ResourceHandlerException(
          "Illegal value '" + swappiness + "' for "
              + YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS
              + ". Value must be between 0 and 100.");
    }
    float softLimitPerc = conf.getFloat(
      YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE,
      YarnConfiguration.DEFAULT_NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE);
    softLimit = softLimitPerc / 100.0f;
    if (softLimitPerc < 0.0f || softLimitPerc > 100.0f) {
      throw new ResourceHandlerException(
          "Illegal value '" + softLimitPerc + "' "
              + YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE
              + ". Value must be between 0 and 100.");
    }
    return null;
  }

  @VisibleForTesting
  int getSwappiness() {
    return swappiness;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {

    String cgroupId = container.getContainerId().toString();
    //memory is in MB
    long containerSoftLimit =
        (long) (container.getResource().getMemorySize() * this.softLimit);
    long containerHardLimit = container.getResource().getMemorySize();
    cGroupsHandler.createCGroup(MEMORY, cgroupId);
    try {
      cGroupsHandler.updateCGroupParam(MEMORY, cgroupId,
          CGroupsHandler.CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES,
          String.valueOf(containerHardLimit) + "M");
      cGroupsHandler.updateCGroupParam(MEMORY, cgroupId,
          CGroupsHandler.CGROUP_PARAM_MEMORY_SOFT_LIMIT_BYTES,
          String.valueOf(containerSoftLimit) + "M");
      cGroupsHandler.updateCGroupParam(MEMORY, cgroupId,
          CGroupsHandler.CGROUP_PARAM_MEMORY_SWAPPINESS,
          String.valueOf(swappiness));
    } catch (ResourceHandlerException re) {
      cGroupsHandler.deleteCGroup(MEMORY, cgroupId);
      LOG.warn("Could not update cgroup for container", re);
      throw re;
    }
    List<PrivilegedOperation> ret = new ArrayList<>();
    ret.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        PrivilegedOperation.CGROUP_ARG_PREFIX
            + cGroupsHandler.getPathForCGroupTasks(MEMORY, cgroupId)));
    return ret;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    cGroupsHandler.deleteCGroup(MEMORY, containerId.toString());
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }

}
