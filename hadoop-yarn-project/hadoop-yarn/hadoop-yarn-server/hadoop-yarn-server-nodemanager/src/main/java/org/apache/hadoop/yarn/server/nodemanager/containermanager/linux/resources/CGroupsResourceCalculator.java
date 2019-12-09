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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.CpuTimeTracker;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.SysInfoLinux;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.hadoop.yarn.util.SystemClock;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A cgroups file-system based Resource calculator without the process tree
 * features.
 *
 * CGroups has its limitations. It can only be enabled, if both CPU and memory
 * cgroups are enabled with yarn.nodemanager.resource.cpu.enabled and
 * yarn.nodemanager.resource.memory.enabled respectively. This means that
 * memory limits are enforced by default. You can turn this off and keep
 * memory reporting only with yarn.nodemanager.resource.memory.enforced.
 *
 * Another limitation is virtual memory measurement. CGroups does not have the
 * ability to measure virtual memory usage. This includes memory reserved but
 * not used. CGroups measures used memory as sa sum of
 * physical memory and swap usage. This will be returned in the virtual
 * memory counters.
 * If the real virtual memory is required please use the legacy procfs based
 * resource calculator or CombinedResourceCalculator.
 */
public class CGroupsResourceCalculator extends ResourceCalculatorProcessTree {
  enum Result {
    Continue,
    Exit
  }
  protected static final Log LOG = LogFactory
      .getLog(CGroupsResourceCalculator.class);
  private static final String PROCFS = "/proc";
  static final String CGROUP = "cgroup";
  static final String CPU_STAT = "cpuacct.stat";
  static final String MEM_STAT = "memory.usage_in_bytes";
  static final String MEMSW_STAT = "memory.memsw.usage_in_bytes";
  private static final String USER = "user ";
  private static final String SYSTEM = "system ";

  private static final Pattern CGROUP_FILE_FORMAT = Pattern.compile(
      "^(\\d+):([^:]+):/(.*)$");
  private final String procfsDir;
  private CGroupsHandler cGroupsHandler;

  private String pid;
  private File cpuStat;
  private File memStat;
  private File memswStat;

  private BigInteger processTotalJiffies;
  private long processPhysicalMemory;
  private long processVirtualMemory;

  private final long jiffyLengthMs;
  private final CpuTimeTracker cpuTimeTracker;
  private Clock clock;

  /**
   * Create resource calculator for all Yarn containers.
   */
  public CGroupsResourceCalculator()
      throws YarnException {
    this(null, PROCFS, ResourceHandlerModule.getCGroupsHandler(),
        SystemClock.getInstance(), SysInfoLinux.JIFFY_LENGTH_IN_MILLIS);
  }

  /**
   * Create resource calculator for the container that has the specified pid.
   * @param pid A pid from the cgroup or null for all containers
   */
  public CGroupsResourceCalculator(String pid) {
    this(pid, PROCFS, ResourceHandlerModule.getCGroupsHandler(),
        SystemClock.getInstance(), SysInfoLinux.JIFFY_LENGTH_IN_MILLIS);
  }

  /**
   * Create resource calculator for testing.
   * @param pid A pid from the cgroup or null for all containers
   * @param procfsDir Path to /proc or a mock /proc directory
   * @param cGroupsHandler Initialized cgroups handler object
   * @param clock A clock object
   * @param jiffyLengthMs0 Jiffy length in milliseconds
   */
  @VisibleForTesting
  CGroupsResourceCalculator(String pid, String procfsDir,
                            CGroupsHandler cGroupsHandler,
                            Clock clock,
                            long jiffyLengthMs0) {
    super(pid);
    this.procfsDir = procfsDir;
    this.cGroupsHandler = cGroupsHandler;
    this.pid = pid != null && pid.equals("0") ? "1" : pid;
    this.jiffyLengthMs = jiffyLengthMs0;
    this.cpuTimeTracker =
        new CpuTimeTracker(this.jiffyLengthMs);
    this.clock = clock;
    this.processTotalJiffies = BigInteger.ZERO;
    this.processPhysicalMemory = UNAVAILABLE;
    this.processVirtualMemory = UNAVAILABLE;
  }

  @Override
  public void initialize() throws YarnException {
    if (!CGroupsResourceCalculator.isAvailable()) {
      throw new YarnException("CGroupsResourceCalculator is not available");
    }
    setCGroupFilePaths();
  }

  @Override
  public float getCpuUsagePercent() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Process " + pid + " jiffies:" + processTotalJiffies);
    }
    return cpuTimeTracker.getCpuTrackerUsagePercent();
  }

  @Override
  public long getCumulativeCpuTime() {
    if (jiffyLengthMs < 0) {
      return UNAVAILABLE;
    }
    return processTotalJiffies.longValue() * jiffyLengthMs;
  }

  @Override
  public long getRssMemorySize(int olderThanAge) {
    if (olderThanAge > 1) {
      return UNAVAILABLE;
    }
    return processPhysicalMemory;
  }

  @Override
  public long getVirtualMemorySize(int olderThanAge) {
    if (olderThanAge > 1) {
      return UNAVAILABLE;
    }
    return processVirtualMemory;
  }

  @Override
  public void updateProcessTree() {
    try {
      this.processTotalJiffies = readTotalProcessJiffies();
      cpuTimeTracker.updateElapsedJiffies(processTotalJiffies,
          clock.getTime());
    } catch (YarnException e) {
      LOG.warn("Failed to parse " + pid, e);
    }
    processPhysicalMemory = getMemorySize(memStat);
    if (memswStat.exists()) {
      processVirtualMemory = getMemorySize(memswStat);
    } else if(LOG.isDebugEnabled()) {
      LOG.debug("Swap cgroups monitoring is not compiled into the kernel " +
          memswStat.getAbsolutePath().toString());
    }
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

  /**
   * Checks if the CGroupsResourceCalculator is available on this system.
   * This assumes that Linux container executor is already initialized.
   *
   * @return true if CGroupsResourceCalculator is available. False otherwise.
   */
  public static boolean isAvailable() {
    try {
      if (!Shell.LINUX) {
        LOG.info("CGroupsResourceCalculator currently is supported only on "
            + "Linux.");
        return false;
      }
      if (ResourceHandlerModule.getCGroupsHandler() == null ||
          ResourceHandlerModule.getCpuResourceHandler() == null ||
          ResourceHandlerModule.getMemoryResourceHandler() == null) {
        LOG.info("CGroupsResourceCalculator requires enabling CGroups" +
            "cpu and memory");
        return false;
      }
    } catch (SecurityException se) {
      LOG.warn("Failed to get Operating System name. " + se);
      return false;
    }
    return true;
  }

  private long getMemorySize(File cgroupUsageFile) {
    long[] mem = new long[1];
    try {
      processFile(cgroupUsageFile, (String line) -> {
        mem[0] = Long.parseLong(line);
        return Result.Exit;
      });
      return mem[0];
    } catch (YarnException e) {
      LOG.warn("Failed to parse cgroups " + memswStat, e);
    }
    return UNAVAILABLE;
  }

  private BigInteger readTotalProcessJiffies() throws YarnException {
    final BigInteger[] totalCPUTimeJiffies = new BigInteger[1];
    totalCPUTimeJiffies[0] = BigInteger.ZERO;
    processFile(cpuStat, (String line) -> {
      if (line.startsWith(USER)) {
        totalCPUTimeJiffies[0] = totalCPUTimeJiffies[0].add(
            new BigInteger(line.substring(USER.length())));
      }
      if (line.startsWith(SYSTEM)) {
        totalCPUTimeJiffies[0] = totalCPUTimeJiffies[0].add(
            new BigInteger(line.substring(SYSTEM.length())));
      }
      return Result.Continue;
    });
    return totalCPUTimeJiffies[0];
  }

  private String getCGroupRelativePath(
      CGroupsHandler.CGroupController controller)
      throws YarnException {
    if (pid == null) {
      return cGroupsHandler.getRelativePathForCGroup("");
    } else {
      return getCGroupRelativePathForPid(controller);
    }
  }

  private String getCGroupRelativePathForPid(
      CGroupsHandler.CGroupController controller)
      throws YarnException {
    File pidCgroupFile = new File(new File(procfsDir, pid), CGROUP);
    String[] result = new String[1];
    processFile(pidCgroupFile, (String line)->{
      Matcher m = CGROUP_FILE_FORMAT.matcher(line);
      boolean mat = m.find();
      if (mat) {
        if (m.group(2).contains(controller.getName())) {
          // Instead of returning the full path we compose it
          // based on the last item as the container id
          // This helps to avoid confusion within a privileged Docker container
          // where the path is referred in /proc/<pid>/cgroup as
          // /docker/<dcontainerid>/hadoop-yarn/<containerid>
          // but it is /hadoop-yarn/<containerid> in the cgroups hierarchy
          String cgroupPath = m.group(3);

          if (cgroupPath != null) {
            String cgroup =
                new File(cgroupPath).toPath().getFileName().toString();
            result[0] = cGroupsHandler.getRelativePathForCGroup(cgroup);
          } else {
            LOG.warn("Invalid cgroup path for " + pidCgroupFile);
          }
          return Result.Exit;
        }
      } else {
        LOG.warn(
            "Unexpected: cgroup file is not in the expected format"
                + " for process with pid " + pid);
      }
      return Result.Continue;
    });
    if (result[0] == null) {
      throw new YarnException(controller.getName() + " CGroup for pid " + pid +
          " not found " + pidCgroupFile);
    }
    return result[0];
  }

  private void processFile(File file, Function<String, Result> processLine)
      throws YarnException {
    // Read "procfsDir/<pid>/stat" file - typically /proc/<pid>/stat
    try (InputStreamReader fReader = new InputStreamReader(
        new FileInputStream(file), Charset.forName("UTF-8"))) {
      try (BufferedReader in = new BufferedReader(fReader)) {
        try {
          String str;
          while ((str = in.readLine()) != null) {
            Result result = processLine.apply(str);
            if (result == Result.Exit) {
              return;
            }
          }
        } catch (IOException io) {
          throw new YarnException("Error reading the stream " + io, io);
        }
      }
    } catch (IOException f) {
      throw new YarnException("The process vanished in the interim " + pid, f);
    }
  }

  void setCGroupFilePaths() throws YarnException {
    if (cGroupsHandler == null) {
      throw new YarnException("CGroups handler is not initialized");
    }
    File cpuDir = new File(
        cGroupsHandler.getControllerPath(
            CGroupsHandler.CGroupController.CPUACCT),
        getCGroupRelativePath(CGroupsHandler.CGroupController.CPUACCT));
    File memDir = new File(
        cGroupsHandler.getControllerPath(
            CGroupsHandler.CGroupController.MEMORY),
        getCGroupRelativePath(CGroupsHandler.CGroupController.MEMORY));
    cpuStat = new File(cpuDir, CPU_STAT);
    memStat = new File(memDir, MEM_STAT);
    memswStat = new File(memDir, MEMSW_STAT);
  }

}
