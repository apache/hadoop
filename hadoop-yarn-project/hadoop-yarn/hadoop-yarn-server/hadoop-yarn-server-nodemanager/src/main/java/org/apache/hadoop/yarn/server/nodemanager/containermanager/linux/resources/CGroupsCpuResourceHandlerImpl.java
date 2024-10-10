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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
public class CGroupsCpuResourceHandlerImpl extends AbstractCGroupsCpuResourceHandler {
  private static final CGroupsHandler.CGroupController CPU =
      CGroupsHandler.CGroupController.CPU;

  @VisibleForTesting
  static final int CPU_DEFAULT_WEIGHT = 1024; // set by kernel
  static final int CPU_DEFAULT_WEIGHT_OPPORTUNISTIC = 2;


  CGroupsCpuResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    super(cGroupsHandler);
  }

  @Override
  protected void updateCgroupMaxCpuLimit(String cgroupId, String quota, String period) throws ResourceHandlerException {
    if (period != null) {
      cGroupsHandler
          .updateCGroupParam(CPU, cgroupId, CGroupsHandler.CGROUP_CPU_PERIOD_US, period);
    }
    if (quota != null) {
      cGroupsHandler
          .updateCGroupParam(CPU, cgroupId, CGroupsHandler.CGROUP_CPU_QUOTA_US, quota);
    }
  }

  @Override
  protected int getOpportunisticCpuWeight() {
    return CPU_DEFAULT_WEIGHT_OPPORTUNISTIC;
  }
  protected int getCpuWeightByContainerVcores(int containerVCores) {
    return containerVCores * CPU_DEFAULT_WEIGHT;
  }

  @Override
  protected void updateCgroupCpuWeight(String cgroupId, int weight) throws ResourceHandlerException {
    cGroupsHandler.updateCGroupParam(CPU, cgroupId, CGroupsHandler.CGROUP_CPU_SHARES,
            String.valueOf(weight));
  }

  @Override
  public boolean cpuLimitExists(String cgroupPath) throws ResourceHandlerException {
    try {
      return checkCgroupV1CPULimitExists(cgroupPath);
    } catch (IOException e) {
      throw new ResourceHandlerException("Failed to check CPU limit", e);
    }
  }

  @InterfaceAudience.Private
  public static boolean checkCgroupV1CPULimitExists(String path) throws IOException {
    File quotaFile = new File(path,
        CPU.getName() + "." + CGroupsHandler.CGROUP_CPU_QUOTA_US);
    if (quotaFile.exists()) {
      String contents = FileUtils.readFileToString(quotaFile, StandardCharsets.UTF_8);
      return Integer.parseInt(contents.trim()) != -1;
    }
    return false;
  }
}
