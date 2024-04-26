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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.util.List;

/**
 * Handler class to handle the memory controller. YARN already ships a
 * physical memory monitor in Java but it isn't as
 * good as CGroups. This handler sets the soft and hard memory limits. The soft
 * limit is set to 90% of the hard limit.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CGroupsMemoryResourceHandlerImpl extends AbstractCGroupsMemoryResourceHandler {

  private static final int OPPORTUNISTIC_SWAPPINESS = 100;
  private int swappiness = 0;

  CGroupsMemoryResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    super(cGroupsHandler);
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration conf)
      throws ResourceHandlerException {
    super.bootstrap(conf);
    swappiness = conf
        .getInt(YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS,
            YarnConfiguration.DEFAULT_NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS);
    if (swappiness < 0 || swappiness > 100) {
      throw new ResourceHandlerException(
          "Illegal value '" + swappiness + "' for "
              + YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS
              + ". Value must be between 0 and 100.");
    }
    return null;
  }

  @VisibleForTesting
  int getSwappiness() {
    return swappiness;
  }

  @Override
  protected void updateMemoryHardLimit(String cgroupId, long containerHardLimit)
      throws ResourceHandlerException {
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES,
        String.valueOf(containerHardLimit) + "M");
  }

  @Override
  protected void updateOpportunisticMemoryLimits(String cgroupId) throws ResourceHandlerException {
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_PARAM_MEMORY_SOFT_LIMIT_BYTES,
        String.valueOf(OPPORTUNISTIC_SOFT_LIMIT) + "M");
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_PARAM_MEMORY_SWAPPINESS,
        String.valueOf(OPPORTUNISTIC_SWAPPINESS));
  }

  @Override
  protected void updateGuaranteedMemoryLimits(String cgroupId, long containerSoftLimit)
      throws ResourceHandlerException {
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_PARAM_MEMORY_SOFT_LIMIT_BYTES,
        String.valueOf(containerSoftLimit) + "M");
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_PARAM_MEMORY_SWAPPINESS,
        String.valueOf(swappiness));
  }
}
