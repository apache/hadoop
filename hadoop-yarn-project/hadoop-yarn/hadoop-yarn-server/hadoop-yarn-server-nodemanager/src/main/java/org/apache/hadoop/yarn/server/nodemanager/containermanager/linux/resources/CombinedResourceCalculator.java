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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

/**
 * CombinedResourceCalculator is a resource calculator that uses cgroups but
 * it is backward compatible with procfs in terms of virtual memory usage.
 */
public class CombinedResourceCalculator  extends ResourceCalculatorProcessTree {
  private final List<ResourceCalculatorProcessTree> resourceCalculators;
  private final ProcfsBasedProcessTree procfsBasedProcessTree;

  public CombinedResourceCalculator(String pid) {
    super(pid);
    this.procfsBasedProcessTree = new ProcfsBasedProcessTree(pid);
    this.resourceCalculators = Arrays.asList(
        new CGroupsV2ResourceCalculator(pid),
        new CGroupsResourceCalculator(pid),
        procfsBasedProcessTree
    );
  }

  @Override
  public void initialize() throws YarnException {
    for (ResourceCalculatorProcessTree calculator : resourceCalculators) {
      calculator.initialize();
    }
  }

  @Override
  public void updateProcessTree() {
    resourceCalculators.stream().parallel()
        .forEach(ResourceCalculatorProcessTree::updateProcessTree);
  }

  @Override
  public String getProcessTreeDump() {
    return procfsBasedProcessTree.getProcessTreeDump();
  }

  @Override
  public boolean checkPidPgrpidForMatch() {
    return procfsBasedProcessTree.checkPidPgrpidForMatch();
  }

  @Override
  public long getVirtualMemorySize(int olderThanAge) {
    return procfsBasedProcessTree.getVirtualMemorySize(olderThanAge);
  }

  @Override
  public long getRssMemorySize(int olderThanAge) {
    return resourceCalculators.stream()
        .map(calculator -> calculator.getRssMemorySize(olderThanAge))
        .filter(result -> UNAVAILABLE < result)
        .findAny().orElse((long) UNAVAILABLE);
  }

  @Override
  public long getCumulativeCpuTime() {
    return resourceCalculators.stream()
        .map(ResourceCalculatorProcessTree::getCumulativeCpuTime)
        .filter(result -> UNAVAILABLE < result)
        .findAny().orElse((long) UNAVAILABLE);
  }

  @Override
  public float getCpuUsagePercent() {
    return resourceCalculators.stream()
        .map(ResourceCalculatorProcessTree::getCpuUsagePercent)
        .filter(result -> UNAVAILABLE < result)
        .findAny().orElse((float) UNAVAILABLE);
  }
}
