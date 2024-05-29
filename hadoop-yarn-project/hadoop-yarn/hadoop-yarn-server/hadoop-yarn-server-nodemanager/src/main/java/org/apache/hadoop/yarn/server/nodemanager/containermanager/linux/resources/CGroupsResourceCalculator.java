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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Cgroup version 1 file-system based Resource calculator without the process tree features.
 *
 * Warning: this implementation will not work properly
 * when configured using the mapreduce.job.process-tree.class job property.
 * Theoretically the ResourceCalculatorProcessTree can be configured using the
 * mapreduce.job.process-tree.class job property, however it has a dependency on an
 * instantiated ResourceHandlerModule, which is only initialised in the NodeManager process
 * and not in the containers.
 *
 * Limitation:
 * The ResourceCalculatorProcessTree class can be configured using the
 * mapreduce.job.process-tree.class property within a MapReduce job.
 * However, it is important to note that instances of ResourceCalculatorProcessTree operate
 * within the context of a MapReduce task. This presents a limitation:
 * these instances do not have access to the ResourceHandlerModule,
 * which is only initialized within the NodeManager process
 * and not within individual containers where MapReduce tasks execute.
 * As a result, the current implementation of ResourceCalculatorProcessTree is incompatible
 * with the mapreduce.job.process-tree.class property. This incompatibility arises
 * because the ResourceHandlerModule is essential for managing and monitoring resource usage,
 * and without it, the ResourceCalculatorProcessTree cannot function as intended
 * within the confines of a MapReduce task. Therefore, any attempts to utilize this class
 * through the mapreduce.job.process-tree.class property
 * will not succeed under the current architecture.
 */
public class CGroupsResourceCalculator extends AbstractCGroupsResourceCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(CGroupsResourceCalculator.class);

  /**
   * <a href="https://docs.kernel.org/admin-guide/cgroup-v1/cpuacct.html">DOC</a>
   *
   * ...
   * cpuacct.stat file lists a few statistics which further divide the CPU time obtained
   * by the cgroup into user and system times.
   * Currently the following statistics are supported:
   *  - user: Time spent by tasks of the cgroup in user mode.
   *  - system: Time spent by tasks of the cgroup in kernel mode.
   * user and system are in USER_HZ unit.
   *  ...
   *
   * <a href="https://litux.nl/mirror/kerneldevelopment/0672327201/ch10lev1sec3.html">DOC</a>
   *
   * ...
   * In kernels earlier than 2.6, changing the value of HZ resulted in user-space anomalies.
   * This happened because values were exported to user-space in units of ticks-per-second.
   * As these interfaces became permanent, applications grew to rely on a specific value of HZ.
   * Consequently, changing HZ would scale various exported values
   * by some constantwithout user-space knowing!
   * Uptime would read 20 hours when it was in fact two!
   *
   * To prevent such problems, the kernel needs to scale all exported jiffies values.
   * It does this by defining USER_HZ, which is the HZ value that user-space expects. On x86,
   * because HZ was historically 100, USER_HZ is 100. The macro jiffies_to_clock_t()
   * is then used to scale a tick count in terms of HZ to a tick count in terms of USER_HZ.
   * The macro used depends on whether USER_HZ and HZ are integer multiples of themselves.
   * ...
   *
   */
  private static final String CPU_STAT = "cpuacct.stat";

  /**
   * <a href="https://docs.kernel.org/admin-guide/cgroup-v1/memory.html#usage-in-bytes">DOC</a>
   *
   * ...
   * For efficiency, as other kernel components, memory cgroup uses some optimization
   * to avoid unnecessary cacheline false sharing.
   * usage_in_bytes is affected by the method
   * and doesn’t show ‘exact’ value of memory (and swap) usage,
   * it’s a fuzz value for efficient access. (Of course, when necessary, it’s synchronized.)
   *  ...
   *
   */
  private static final String MEM_STAT = "memory.usage_in_bytes";
  private static final String MEMSW_STAT = "memory.memsw.usage_in_bytes";

  public CGroupsResourceCalculator(String pid) {
    super(
        pid,
        Arrays.asList(CPU_STAT + "#user", CPU_STAT + "#system"),
        MEM_STAT,
        MEMSW_STAT
    );
  }

  @Override
  protected List<Path> getCGroupFilesToLoadInStats() {
    List<Path> result = new ArrayList<>();

    try {
      String cpuRelative = getCGroupRelativePath(CGroupsHandler.CGroupController.CPUACCT);
      if (cpuRelative != null) {
        File cpuDir = new File(getcGroupsHandler().getControllerPath(
            CGroupsHandler.CGroupController.CPUACCT), cpuRelative);
        result.add(Paths.get(cpuDir.getAbsolutePath(), CPU_STAT));
      }
    } catch (IOException e) {
      LOG.debug("Exception while looking for CPUACCT controller for pid: " + getPid(), e);
    }

    try {
      String memoryRelative = getCGroupRelativePath(CGroupsHandler.CGroupController.MEMORY);
      if (memoryRelative != null) {
        File memDir = new File(getcGroupsHandler().getControllerPath(
            CGroupsHandler.CGroupController.MEMORY), memoryRelative);
        result.add(Paths.get(memDir.getAbsolutePath(), MEM_STAT));
        result.add(Paths.get(memDir.getAbsolutePath(), MEMSW_STAT));
      }
    } catch (IOException e) {
      LOG.debug("Exception while looking for MEMORY controller for pid: " + getPid(), e);
    }

    return result;
  }

  private String getCGroupRelativePath(CGroupsHandler.CGroupController controller)
      throws IOException {
    for (String line : readLinesFromCGroupFileFromProcDir()) {
      // example line: 6:cpuacct,cpu:/yarn/container_1
      String[] parts = line.split(":");
      if (parts[1].contains(controller.getName())) {
        String cgroupPath = parts[2];
        Path fileName = new File(cgroupPath).toPath().getFileName();
        if (fileName != null) {
          return getcGroupsHandler().getRelativePathForCGroup(fileName.toString());
        }
      }
    }
    LOG.debug("No {} controller found for pid {}", controller, getPid());
    return null;
  }
}
