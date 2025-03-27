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

/**
 * Handler class to handle the memory controller. YARN already ships a
 * physical memory monitor in Java but it isn't as
 * good as CGroups. This handler sets the soft and hard memory limits. The soft
 * limit is set to 90% of the hard limit.
 */
public class CGroupsV2MemoryResourceHandlerImpl extends AbstractCGroupsMemoryResourceHandler {

  CGroupsV2MemoryResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    super(cGroupsHandler);
  }

  @Override
  protected void updateMemoryHardLimit(String cgroupId, long containerHardLimit)
      throws ResourceHandlerException {
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_MEMORY_MAX, String.valueOf(containerHardLimit) + "M");
  }

  @Override
  protected void updateOpportunisticMemoryLimits(String cgroupId) throws ResourceHandlerException {
    updateGuaranteedMemoryLimits(cgroupId, OPPORTUNISTIC_SOFT_LIMIT);
  }

  @Override
  protected void updateGuaranteedMemoryLimits(String cgroupId, long containerSoftLimit)
      throws ResourceHandlerException {
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_MEMORY_LOW, String.valueOf(containerSoftLimit) + "M");
  }
}
