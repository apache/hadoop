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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@InterfaceStability.Unstable
@InterfaceAudience.Private
public abstract class AbstractCGroupsMemoryResourceHandler implements MemoryResourceHandler {

  static final Logger LOG =
      LoggerFactory.getLogger(CGroupsMemoryResourceHandlerImpl.class);
  protected static final CGroupsHandler.CGroupController MEMORY =
      CGroupsHandler.CGroupController.MEMORY;

  private CGroupsHandler cGroupsHandler;

  protected static final int OPPORTUNISTIC_SOFT_LIMIT = 0;
  // multiplier to set the soft limit - value should be between 0 and 1
  private float softLimit = 0.0f;
  private boolean enforce = true;

  public AbstractCGroupsMemoryResourceHandler(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  protected CGroupsHandler getCGroupsHandler() {
    return cGroupsHandler;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration conf)
      throws ResourceHandlerException {
    this.cGroupsHandler.initializeCGroupController(MEMORY);
    enforce = conf.getBoolean(
        YarnConfiguration.NM_MEMORY_RESOURCE_ENFORCED,
        YarnConfiguration.DEFAULT_NM_MEMORY_RESOURCE_ENFORCED);
    float softLimitPerc = conf.getFloat(
        YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE,
        YarnConfiguration.
            DEFAULT_NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE);
    softLimit = softLimitPerc / 100.0f;
    if (softLimitPerc < 0.0f || softLimitPerc > 100.0f) {
      throw new ResourceHandlerException(
          "Illegal value '" + softLimitPerc + "' "
              + YarnConfiguration.
              NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE
              + ". Value must be between 0 and 100.");
    }
    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    String cgroupId = container.getContainerId().toString();
    File cgroup = new File(cGroupsHandler.getPathForCGroup(MEMORY, cgroupId));
    if (cgroup.exists()) {
      //memory is in MB
      long containerSoftLimit =
          (long) (container.getResource().getMemorySize() * this.softLimit);
      long containerHardLimit = container.getResource().getMemorySize();
      if (enforce) {
        try {
          updateMemoryHardLimit(cgroupId, containerHardLimit);
          ContainerTokenIdentifier id = container.getContainerTokenIdentifier();
          if (id != null && id.getExecutionType() ==
              ExecutionType.OPPORTUNISTIC) {
            updateOpportunisticMemoryLimits(cgroupId);
          } else {
            updateGuaranteedMemoryLimits(cgroupId, containerSoftLimit);
          }
        } catch (ResourceHandlerException re) {
          cGroupsHandler.deleteCGroup(MEMORY, cgroupId);
          LOG.warn("Could not update cgroup for container", re);
          throw re;
        }
      }
    }
    return null;
  }

  protected abstract void updateMemoryHardLimit(String cgroupId, long containerHardLimit)
      throws ResourceHandlerException;

  protected abstract void updateOpportunisticMemoryLimits(String cgroupId)
      throws ResourceHandlerException;

  protected abstract void updateGuaranteedMemoryLimits(String cgroupId, long containerSoftLimit)
      throws ResourceHandlerException;

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String cgroupId = container.getContainerId().toString();
    cGroupsHandler.createCGroup(MEMORY, cgroupId);
    updateContainer(container);
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

  @Override
  public String toString() {
    return AbstractCGroupsMemoryResourceHandler.class.getName();
  }
}
