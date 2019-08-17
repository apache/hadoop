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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

/**
 * CombinedResourceCalculator is a resource calculator that uses cgroups but
 * it is backward compatible with procfs in terms of virtual memory usage.
 */
public class CombinedResourceCalculator  extends ResourceCalculatorProcessTree {
  protected static final Logger LOG = LoggerFactory
      .getLogger(CombinedResourceCalculator.class);
  private ProcfsBasedProcessTree procfs;
  private CGroupsResourceCalculator cgroup;

  public CombinedResourceCalculator(String pid) {
    super(pid);
    procfs = new ProcfsBasedProcessTree(pid);
    cgroup = new CGroupsResourceCalculator(pid);
  }

  @Override
  public void initialize() throws YarnException {
    procfs.initialize();
    cgroup.initialize();
  }

  @Override
  public void updateProcessTree() {
    procfs.updateProcessTree();
    cgroup.updateProcessTree();
  }

  @Override
  public String getProcessTreeDump() {
    return procfs.getProcessTreeDump();
  }

  @Override
  public float getCpuUsagePercent() {
    float cgroupUsage = cgroup.getCpuUsagePercent();
    if (LOG.isDebugEnabled()) {
      float procfsUsage = procfs.getCpuUsagePercent();
      LOG.debug("CPU Comparison:" + procfsUsage + " " + cgroupUsage);
      LOG.debug("Jiffy Comparison:" +
          procfs.getCumulativeCpuTime() + " " +
          cgroup.getCumulativeCpuTime());
    }

    return cgroupUsage;
  }

  @Override
  public boolean checkPidPgrpidForMatch() {
    return procfs.checkPidPgrpidForMatch();
  }

  @Override
  public long getCumulativeCpuTime() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("CPU Comparison:" +
          procfs.getCumulativeCpuTime() + " " +
          cgroup.getCumulativeCpuTime());
    }
    return cgroup.getCumulativeCpuTime();
  }

  @Override
  public long getRssMemorySize(int olderThanAge) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("MEM Comparison:" +
          procfs.getRssMemorySize(olderThanAge) + " " +
          cgroup.getRssMemorySize(olderThanAge));
    }
    return cgroup.getRssMemorySize(olderThanAge);
  }

  @Override
  public long getVirtualMemorySize(int olderThanAge) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("VMEM Comparison:" +
          procfs.getVirtualMemorySize(olderThanAge) + " " +
          cgroup.getVirtualMemorySize(olderThanAge));
    }
    return procfs.getVirtualMemorySize(olderThanAge);
  }
}
