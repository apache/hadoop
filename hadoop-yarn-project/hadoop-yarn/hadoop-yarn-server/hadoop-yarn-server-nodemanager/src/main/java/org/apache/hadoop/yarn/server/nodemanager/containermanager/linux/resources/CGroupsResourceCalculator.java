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
import java.io.FileNotFoundException;
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
 */

public class CGroupsResourceCalculator extends ResourceCalculatorProcessTree {
  enum Result {
    Continue,
    Exit
  }
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

  private final long jiffyLengthMs;
  private BigInteger processTotalJiffies = BigInteger.ZERO;
  private final CpuTimeTracker cpuTimeTracker;
  private Clock clock;

  private long mem = UNAVAILABLE;
  private long memSw = UNAVAILABLE;

  public CGroupsResourceCalculator(String pid) throws YarnException {
    this(pid, PROCFS, ResourceHandlerModule.getCGroupsHandler(),
        SystemClock.getInstance());
  }

  CGroupsResourceCalculator(String pid, String procfsDir,
                            CGroupsHandler cGroupsHandler, Clock clock)
      throws YarnException {
    super(pid);
    this.procfsDir = procfsDir;
    this.cGroupsHandler = cGroupsHandler;
    this.pid = pid;
    this.cpuTimeTracker =
        new CpuTimeTracker(SysInfoLinux.JIFFY_LENGTH_IN_MILLIS);
    this.clock = clock;
    this.jiffyLengthMs = (clock == SystemClock.getInstance()) ?
      SysInfoLinux.JIFFY_LENGTH_IN_MILLIS : 10;
    setCGroupFilePaths();
  }

  @Override
  public float getCpuUsagePercent() {
    readTotalProcessJiffies();
    cpuTimeTracker.updateElapsedJiffies(
        processTotalJiffies,
        clock.getTime());
    return cpuTimeTracker.getCpuTrackerUsagePercent();
  }

  @Override
  public long getCumulativeCpuTime() {
    if (jiffyLengthMs < 0) {
      return UNAVAILABLE;
    }
    readTotalProcessJiffies();
    return
        processTotalJiffies.longValue() * jiffyLengthMs;
  }

  @Override
  public long getRssMemorySize(int olderThanAge) {
    if (olderThanAge > 0) {
      return 0;
    }
    try {
      processFile(memStat, (String line) -> {
        mem = Long.parseLong(line);
        return Result.Exit;
      });
      return mem;
    } catch (YarnException e) {
      LOG.warn("Failed to parse cgroups " + memswStat, e);
    }
    return UNAVAILABLE;
  }

  @Override
  public long getVirtualMemorySize(int olderThanAge) {
    if (olderThanAge > 0) {
      return 0;
    }
    try {
      processFile(memswStat, (String line) -> {
        memSw = Long.parseLong(line);
        return Result.Exit;
      });
      return memSw;
    } catch (YarnException e) {
      LOG.warn("Failed to parse cgroups " + memswStat, e);
    }
    return UNAVAILABLE;
  }

  @Override
  public void updateProcessTree() {
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
      if (ResourceHandlerModule.getCGroupsHandler() == null) {
        LOG.info("CGroupsResourceCalculator requires enabling CGroups");
        return false;
      }
    } catch (SecurityException se) {
      LOG.warn("Failed to get Operating System name. " + se);
      return false;
    }
    return true;
  }

  private void readTotalProcessJiffies() {
    try {
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
      processTotalJiffies = totalCPUTimeJiffies[0];
    } catch (YarnException e) {
      LOG.warn("Failed to parse " + pid, e);
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
          result[0] = m.group(3);
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
      throw new YarnException(controller.getName() + "CGroup for " + pid +
          " not found " + pidCgroupFile);
    }
    return result[0];
  }

  private void processFile(File file, Function<String, Result> processLine)
      throws YarnException {
    // Read "procfsDir/<pid>/stat" file - typically /proc/<pid>/stat
    BufferedReader in = null;
    InputStreamReader fReader = null;
    try {
      fReader = new InputStreamReader(
          new FileInputStream(
              file), Charset.forName("UTF-8"));
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      throw new YarnException("The process vanished in the interim " + pid, f);
    }

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
    } finally {
      // Close the streams
      try {
        fReader.close();
        try {
          in.close();
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + in, i);
        }
      } catch (IOException i) {
        LOG.warn("Error closing the stream " + fReader, i);
      }
    }
  }

  private void setCGroupFilePaths() throws YarnException {
    if (cGroupsHandler == null) {
      throw new YarnException("CGroups handler is not initialized");
    }
    File cpuDir = new File(
        cGroupsHandler.getControllerPath(
            CGroupsHandler.CGroupController.CPUACCT),
        getCGroupRelativePathForPid(CGroupsHandler.CGroupController.CPUACCT));
    File memDir = new File(
        cGroupsHandler.getControllerPath(
            CGroupsHandler.CGroupController.MEMORY),
        getCGroupRelativePathForPid(CGroupsHandler.CGroupController.MEMORY));
    cpuStat = new File(cpuDir, CPU_STAT);
    memStat = new File(memDir, MEM_STAT);
    memswStat = new File(memDir, MEMSW_STAT);
  }
}
