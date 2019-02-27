/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation for using CGroups to restrict CPU usage on Linux. The
 * implementation supports 3 different controls - restrict usage of all YARN
 * containers, restrict relative usage of individual YARN containers and
 * restrict usage of individual YARN containers. Admins can set the overall CPU
 * to be used by all YARN containers - this is implemented by setting
 * cpu.cfs_period_us and cpu.cfs_quota_us to the ratio desired. If strict
 * resource usage mode is not enabled, cpu.shares is set for individual
 * containers - this prevents containers from exceeding the overall limit for
 * YARN containers but individual containers can use as much of the CPU as
 * available(under the YARN limit). If strict resource usage is enabled, then
 * container can only use the percentage of CPU allocated to them and this is
 * again implemented using cpu.cfs_period_us and cpu.cfs_quota_us.
 *
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class CGroupsCpuResourceHandlerImpl implements CpuResourceHandler {

  static final Logger LOG =
       LoggerFactory.getLogger(CGroupsCpuResourceHandlerImpl.class);

  private CGroupsHandler cGroupsHandler;
  private boolean strictResourceUsageMode = false;
  private float yarnProcessors;
  private int nodeVCores;
  private static final CGroupsHandler.CGroupController CPU =
      CGroupsHandler.CGroupController.CPU;

  @VisibleForTesting
  static final int MAX_QUOTA_US = 1000 * 1000;
  @VisibleForTesting
  static final int MIN_PERIOD_US = 1000;
  @VisibleForTesting
  static final int CPU_DEFAULT_WEIGHT = 1024; // set by kernel
  static final int CPU_DEFAULT_WEIGHT_OPPORTUNISTIC = 2;

  CGroupsCpuResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration conf)
      throws ResourceHandlerException {
    return bootstrap(
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf), conf);
  }

  @VisibleForTesting
  List<PrivilegedOperation> bootstrap(
      ResourceCalculatorPlugin plugin, Configuration conf)
      throws ResourceHandlerException {
    this.strictResourceUsageMode = conf.getBoolean(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
        YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE);
    this.cGroupsHandler.initializeCGroupController(CPU);
    nodeVCores = NodeManagerHardwareUtils.getVCores(plugin, conf);

    // cap overall usage to the number of cores allocated to YARN
    yarnProcessors = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    int systemProcessors = NodeManagerHardwareUtils.getNodeCPUs(plugin, conf);
    boolean existingCpuLimits;
    try {
      existingCpuLimits =
          cpuLimitsExist(cGroupsHandler.getPathForCGroup(CPU, ""));
    } catch (IOException ie) {
      throw new ResourceHandlerException(ie);
    }
    if (systemProcessors != (int) yarnProcessors) {
      LOG.info("YARN containers restricted to " + yarnProcessors + " cores");
      int[] limits = getOverallLimits(yarnProcessors);
      cGroupsHandler
          .updateCGroupParam(CPU, "", CGroupsHandler.CGROUP_CPU_PERIOD_US,
              String.valueOf(limits[0]));
      cGroupsHandler
          .updateCGroupParam(CPU, "", CGroupsHandler.CGROUP_CPU_QUOTA_US,
              String.valueOf(limits[1]));
    } else if (existingCpuLimits) {
      LOG.info("Removing CPU constraints for YARN containers.");
      cGroupsHandler
          .updateCGroupParam(CPU, "", CGroupsHandler.CGROUP_CPU_QUOTA_US,
              String.valueOf(-1));
    }
    return null;
  }

  @InterfaceAudience.Private
  public static boolean cpuLimitsExist(String path)
      throws IOException {
    File quotaFile = new File(path,
        CPU.getName() + "." + CGroupsHandler.CGROUP_CPU_QUOTA_US);
    if (quotaFile.exists()) {
      String contents = FileUtils.readFileToString(quotaFile, "UTF-8");
      int quotaUS = Integer.parseInt(contents.trim());
      if (quotaUS != -1) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  public static int[] getOverallLimits(float yarnProcessors) {

    int[] ret = new int[2];

    if (yarnProcessors < 0.01f) {
      throw new IllegalArgumentException("Number of processors can't be <= 0.");
    }

    int quotaUS = MAX_QUOTA_US;
    int periodUS = (int) (MAX_QUOTA_US / yarnProcessors);
    if (yarnProcessors < 1.0f) {
      periodUS = MAX_QUOTA_US;
      quotaUS = (int) (periodUS * yarnProcessors);
      if (quotaUS < MIN_PERIOD_US) {
        LOG.warn("The quota calculated for the cgroup was too low."
            + " The minimum value is " + MIN_PERIOD_US
            + ", calculated value is " + quotaUS
            + ". Setting quota to minimum value.");
        quotaUS = MIN_PERIOD_US;
      }
    }

    // cfs_period_us can't be less than 1000 microseconds
    // if the value of periodUS is less than 1000, we can't really use cgroups
    // to limit cpu
    if (periodUS < MIN_PERIOD_US) {
      LOG.warn("The period calculated for the cgroup was too low."
          + " The minimum value is " + MIN_PERIOD_US
          + ", calculated value is " + periodUS
          + ". Using all available CPU.");
      periodUS = MAX_QUOTA_US;
      quotaUS = -1;
    }

    ret[0] = periodUS;
    ret[1] = quotaUS;
    return ret;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String cgroupId = container.getContainerId().toString();
    Resource containerResource = container.getResource();
    cGroupsHandler.createCGroup(CPU, cgroupId);
    try {
      int containerVCores = containerResource.getVirtualCores();
      ContainerTokenIdentifier id = container.getContainerTokenIdentifier();
      if (id != null && id.getExecutionType() ==
          ExecutionType.OPPORTUNISTIC) {
        cGroupsHandler
            .updateCGroupParam(CPU, cgroupId, CGroupsHandler.CGROUP_CPU_SHARES,
                String.valueOf(CPU_DEFAULT_WEIGHT_OPPORTUNISTIC));
      } else {
        int cpuShares = CPU_DEFAULT_WEIGHT * containerVCores;
        cGroupsHandler
            .updateCGroupParam(CPU, cgroupId, CGroupsHandler.CGROUP_CPU_SHARES,
                String.valueOf(cpuShares));
      }
      if (strictResourceUsageMode) {
        if (nodeVCores != containerVCores) {
          float containerCPU =
              (containerVCores * yarnProcessors) / (float) nodeVCores;
          int[] limits = getOverallLimits(containerCPU);
          cGroupsHandler.updateCGroupParam(CPU, cgroupId,
              CGroupsHandler.CGROUP_CPU_PERIOD_US, String.valueOf(limits[0]));
          cGroupsHandler.updateCGroupParam(CPU, cgroupId,
              CGroupsHandler.CGROUP_CPU_QUOTA_US, String.valueOf(limits[1]));
        }
      }
    } catch (ResourceHandlerException re) {
      cGroupsHandler.deleteCGroup(CPU, cgroupId);
      LOG.warn("Could not update cgroup for container", re);
      throw re;
    }
    List<PrivilegedOperation> ret = new ArrayList<>();
    ret.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupsHandler
            .getPathForCGroupTasks(CPU, cgroupId)));
    return ret;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    cGroupsHandler.deleteCGroup(CPU, containerId.toString());
    return null;
  }

  @Override public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public String toString() {
    return CGroupsCpuResourceHandlerImpl.class.getName();
  }
}
