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
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.CpuTimeTracker;
import org.apache.hadoop.util.SysInfoLinux;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.hadoop.yarn.util.SystemClock;

/**
 * Common code base for the CGroupsResourceCalculator implementations.
 */
public abstract class AbstractCGroupsResourceCalculator extends ResourceCalculatorProcessTree {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractCGroupsResourceCalculator.class);
  private final String pid;
  private final Clock clock = SystemClock.getInstance();
  private final Map<String, String> stats = new ConcurrentHashMap<>();

  private long jiffyLengthMs = SysInfoLinux.JIFFY_LENGTH_IN_MILLIS;
  private CpuTimeTracker cpuTimeTracker;
  private CGroupsHandler cGroupsHandler;
  private String procFs = "/proc";

  private final List<String> totalJiffiesKeys;
  private final String rssMemoryKey;
  private final String virtualMemoryKey;

  protected AbstractCGroupsResourceCalculator(
      String pid,
      List<String> totalJiffiesKeys,
      String rssMemoryKey,
      String virtualMemoryKey
  ) {
    super(pid);
    this.pid = pid;
    this.totalJiffiesKeys = totalJiffiesKeys;
    this.rssMemoryKey = rssMemoryKey;
    this.virtualMemoryKey = virtualMemoryKey;
  }

  @Override
  public void initialize() throws YarnException {
    cpuTimeTracker = new CpuTimeTracker(jiffyLengthMs);
    cGroupsHandler = ResourceHandlerModule.getCGroupsHandler();
  }

  @Override
  public long getCumulativeCpuTime() {
    long totalJiffies = getTotalJiffies();
    return jiffyLengthMs == UNAVAILABLE || totalJiffies == UNAVAILABLE
        ? UNAVAILABLE
        : getTotalJiffies() * jiffyLengthMs;
  }

  @Override
  public long getRssMemorySize(int olderThanAge) {
    return 1 < olderThanAge ? UNAVAILABLE : getStat(rssMemoryKey);
  }

  @Override
  public long getVirtualMemorySize(int olderThanAge) {
    return 1 < olderThanAge ? UNAVAILABLE : getStat(virtualMemoryKey);
  }

  @Override
  public String getProcessTreeDump() {
    // We do not have a process tree in cgroups return just the pid for tracking
    return pid;
  }

  @Override
  public boolean checkPidPgrpidForMatch() {
    // We do not have a process tree in cgroups returning default ok
    return true;
  }

  @Override
  public float getCpuUsagePercent() {
    return cpuTimeTracker.getCpuTrackerUsagePercent();
  }

  @Override
  public void updateProcessTree() {
    stats.clear();
    for (Path statFile : getCGroupFilesToLoadInStats()) {
      try {
        List<String> lines = fileToLines(statFile);
        if (1 == lines.size()) {
          addSingleLineToStat(statFile, lines.get(0));
        } else if (1 < lines.size()) {
          addMultiLineToStat(statFile, lines);
        }
      } catch (IOException e) {
        LOG.debug(String.format("Failed to read cgroup file %s for pid %s", statFile, pid), e);
      }
    }
    LOG.debug("After updateProcessTree the {} pid has stats {}", pid, stats);
    cpuTimeTracker.updateElapsedJiffies(BigInteger.valueOf(getTotalJiffies()), clock.getTime());
  }

  private void addSingleLineToStat(Path file, String line) {
    Path fileName = file.getFileName();
    if (fileName != null) {
      stats.put(fileName.toString(), line.trim());
    }
  }

  private void addMultiLineToStat(Path file, List<String> lines) {
    for (String line : lines) {
      String[] parts = line.split(" ");
      if (1 < parts.length) {
        stats.put(file.getFileName() + "#" + parts[0], parts[1]);
      }
    }
  }

  private long getTotalJiffies() {
    Long reduce = totalJiffiesKeys.stream()
        .map(this::getStat)
        .filter(statValue -> statValue != UNAVAILABLE)
        .reduce(0L, Long::sum);
    return reduce == 0 ? UNAVAILABLE : reduce;
  }

  private long getStat(String key) {
    return Long.parseLong(stats.getOrDefault(key, String.valueOf(UNAVAILABLE)));
  }

  protected abstract List<Path> getCGroupFilesToLoadInStats();

  protected List<String> readLinesFromCGroupFileFromProcDir() throws IOException {
    // https://docs.kernel.org/admin-guide/cgroup-v2.html#processes
    // https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v1/cgroups.html
    Path cgroup = Paths.get(procFs, pid, "cgroup");
    List<String> result = Arrays.asList(fileToString(cgroup).split(System.lineSeparator()));
    LOG.debug("The {} pid has the following lines in the procfs cgroup file {}", pid, result);
    return result;
  }

  protected String fileToString(Path path) throws IOException {
    return FileUtils.readFileToString(path.toFile(), StandardCharsets.UTF_8).trim();
  }

  protected List<String> fileToLines(Path path) throws IOException {
    return !path.toFile().exists() ? Collections.emptyList()
      : Arrays.asList(FileUtils.readFileToString(path.toFile(), StandardCharsets.UTF_8)
        .trim().split(System.lineSeparator()));
  }

  @VisibleForTesting
  void setJiffyLengthMs(long jiffyLengthMs) {
    this.jiffyLengthMs = jiffyLengthMs;
  }

  @VisibleForTesting
  void setCpuTimeTracker(CpuTimeTracker cpuTimeTracker) {
    this.cpuTimeTracker = cpuTimeTracker;
  }

  @VisibleForTesting
  void setcGroupsHandler(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  @VisibleForTesting
  void setProcFs(String procFs) {
    this.procFs = procFs;
  }

  public CGroupsHandler getcGroupsHandler() {
    return cGroupsHandler;
  }

  public String getPid() {
    return pid;
  }
}
