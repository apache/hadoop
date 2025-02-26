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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_CURRENT;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_MAX;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_HIGH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_SWAP_MEMORY_MAX;

public class CGroupV2ElasticMemoryControllerImpl extends AbstractCGroupElasticMemoryController{
  public CGroupV2ElasticMemoryControllerImpl(Configuration conf,
      Context context, CGroupsHandler cgroups, boolean controlPhysicalMemory,
      boolean controlVirtualMemory, long limit)
      throws YarnException {
    super(conf, context, cgroups, controlPhysicalMemory, controlVirtualMemory,
        limit);
  }

  @Override
  void setCGroupParameters() throws ResourceHandlerException {
    if (controlPhysicalMemory && !controlVirtualMemory) {
      try {
        cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
            CGROUP_SWAP_MEMORY_MAX, "max");
      } catch (ResourceHandlerException ex) {
        LOG.debug("Swap monitoring is turned off in the kernel");
      }
      // Set physical memory limits
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_MEMORY_MAX, Long.toString(limit) + 5 * 1024 * 1024 * 1024);
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_MEMORY_HIGH, Long.toString(limit));
    } else if (controlVirtualMemory && !controlPhysicalMemory) {
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_MEMORY_HIGH, Long.toString(limit));
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_SWAP_MEMORY_MAX, Long.toString(limit));
    }
  }

  @Override
  void resetCGroupParameters() {
    try {
      try {
        cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
            CGROUP_SWAP_MEMORY_MAX, "max");
      } catch (ResourceHandlerException ex) {
        LOG.debug("Swap monitoring is turned off in the kernel");
      }
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_MEMORY_MAX, "max");
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_MEMORY_HIGH, "max");
    } catch (ResourceHandlerException ex) {
      LOG.warn("Error in cleanup", ex);
    }
  }

  @Override
  boolean isUnderOOM() throws Exception {
    long used =  Long.parseLong(cgroups.getCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
        CGROUP_MEMORY_CURRENT));
    long limit = Long.parseLong(cgroups.getCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
        CGROUP_MEMORY_HIGH));
    return used >= limit;
  }
}
