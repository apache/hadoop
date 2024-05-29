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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;

/**
 * A Cgroup version 2 file-system based Resource calculator without the process tree features.
 *
 * Warning: this implementation will not work properly when configured
 * using the mapreduce.job.process-tree.class job property.
 * Theoretically the ResourceCalculatorProcessTree can be configured
 * using the mapreduce.job.process-tree.class job property, however it
 * has a dependency on an instantiated ResourceHandlerModule,
 * which is only initialised in the NodeManager process and not in the containers.
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
public class CGroupsV2ResourceCalculator extends AbstractCGroupsResourceCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(CGroupsV2ResourceCalculator.class);

  /**
   * <a href="https://docs.kernel.org/admin-guide/cgroup-v2.html#cpu-interface-files">DOC</a>
   *
   * ...
   * cpu.stat
   *  A read-only flat-keyed file. This file exists whether the controller is enabled or not.
   *  It always reports the following three stats:
   *  - usage_usec
   *  - user_usec
   *  - system_usec
   *  ...
   *
   */
  private static final String CPU_STAT = "cpu.stat#usage_usec";

  /**
   * <a href="https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files">DOC</a>
   *
   * ...
   * memory.stat
   *  A read-only flat-keyed file which exists on non-root cgroups.
   *  This breaks down the cgroup’s memory footprint into different types of memory,
   *  type-specific details, and other information on the state
   *  and past events of the memory management system.
   *  All memory amounts are in bytes.
   *  ...
   *  anon
   *   Amount of memory used in anonymous mappings such as brk(), sbrk(), and mmap(MAP_ANONYMOUS)
   * ...
   *
   */
  private static final String MEM_STAT = "memory.stat#anon";

  /**
   * <a href="https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files">DOC</a>
   *
   * ...
   * memory.swap.current
   *  A read-only single value file which exists on non-root cgroups.
   *  The total amount of swap currently being used by the cgroup and its descendants.
   * ...
   *
   */
  private static final String MEMSW_STAT = "memory.swap.current";

  public CGroupsV2ResourceCalculator(String pid) {
    super(
        pid,
        Collections.singletonList(CPU_STAT),
        MEM_STAT,
        MEMSW_STAT
    );
  }

  @Override
  protected List<Path> getCGroupFilesToLoadInStats() {
    List<Path> result = new ArrayList<>();
    try (Stream<Path> cGroupFiles = Files.list(getCGroupPath())){
      cGroupFiles.forEach(result::add);
    } catch (IOException e) {
      LOG.debug("Failed to list cgroup files for pid: " + getPid(), e);
    }
    LOG.debug("Found cgroup files for pid {} is {}", getPid(), result);
    return  result;
  }

  private Path getCGroupPath() throws IOException {
    return Paths.get(
        getcGroupsHandler().getCGroupV2MountPath(),
        StringUtils.substringAfterLast(readLinesFromCGroupFileFromProcDir().get(0), ":")
    );
  }
}
